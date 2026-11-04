// Package openai adapts OpenAI-format Chat Completions APIs (OpenAI itself,
// compatible providers, and the mock) to provider.Provider.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/sse"
)

// maxErrorBody bounds how much of a non-200 response body is read.
const maxErrorBody = 64 << 10

// Provider calls an OpenAI-format API.
type Provider struct {
	name    string
	baseURL string // up to and including /v1, e.g. https://api.openai.com/v1
	apiKey  string
	client  *http.Client
}

// New returns an OpenAI-format provider. apiKey may be empty.
func New(name, baseURL, apiKey string, client *http.Client) *Provider {
	return &Provider{name: name, baseURL: strings.TrimSuffix(baseURL, "/"), apiKey: apiKey, client: client}
}

// Name implements provider.Provider.
func (p *Provider) Name() string { return p.name }

// Supports implements provider.Provider. The internal format is OpenAI's, so
// every request is expressible, including unmodeled fields in Extra.
func (p *Provider) Supports(*provider.ChatRequest) error { return nil }

// Stream implements provider.Provider.
func (p *Provider) Stream(ctx context.Context, t provider.Target, req *provider.ChatRequest) (provider.Stream, error) {
	body, err := json.Marshal(toWire(t, req))
	if err != nil {
		return nil, fmt.Errorf("openai: encode request: %w", err)
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/event-stream")
	if p.apiKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+p.apiKey)
	}
	if id := obs.RequestID(ctx); id != "" {
		hreq.Header.Set(obs.RequestIDHeader, id)
	}
	if n := provider.Attempt(ctx); n > 0 {
		hreq.Header.Set(provider.AttemptHeader, strconv.Itoa(n))
	}

	resp, err := p.client.Do(hreq)
	if err != nil {
		return nil, p.transportError(ctx, err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, p.statusError(resp)
	}
	return &stream{p: p, ctx: ctx, body: resp.Body, rd: sse.NewReader(resp.Body)}, nil
}

func toWire(t provider.Target, req *provider.ChatRequest) oai.ChatCompletionRequest {
	w := oai.ChatCompletionRequest{
		Model:             t.UpstreamModel,
		Messages:          make([]oai.Message, len(req.Messages)),
		MaxTokens:         req.MaxTokens,
		Temperature:       req.Temperature,
		TopP:              req.TopP,
		Stop:              req.Stop,
		ParallelToolCalls: req.ParallelToolCalls,
		User:              req.User,
		Extra:             req.Extra,
		// Always stream upstream and always ask for usage (design §5.3).
		Stream:        true,
		StreamOptions: &oai.StreamOptions{IncludeUsage: true},
	}
	for i, m := range req.Messages {
		w.Messages[i] = messageToWire(m)
	}
	for _, tool := range req.Tools {
		w.Tools = append(w.Tools, oai.Tool{Type: "function", Function: oai.FunctionSpec{
			Name: tool.Name, Description: tool.Description, Parameters: tool.Parameters, Strict: tool.Strict,
		}})
	}
	if tc := req.ToolChoice; tc != nil {
		w.ToolChoice = &oai.ToolChoice{Mode: tc.Mode, Function: tc.Name}
	}
	return w
}

func messageToWire(m provider.Message) oai.Message {
	out := oai.Message{Role: string(m.Role), ToolCallID: m.ToolCallID}
	for _, tc := range m.ToolCalls {
		out.ToolCalls = append(out.ToolCalls, oai.ToolCall{
			ID: tc.ID, Type: "function", Function: oai.FunctionCall{Name: tc.Name, Arguments: tc.Arguments},
		})
	}
	switch {
	case len(m.Content) == 0 && len(m.ToolCalls) > 0:
		out.Content = oai.Content{Null: true}
	case len(m.Content) == 1 && m.Content[0].Type == provider.PartText:
		out.Content = oai.TextContent(m.Content[0].Text)
	default:
		parts := make([]oai.ContentPart, 0, len(m.Content))
		for _, p := range m.Content {
			if p.Type == provider.PartImage {
				parts = append(parts, oai.ContentPart{Type: "image_url",
					ImageURL: &oai.ImageURL{URL: p.ImageURL, Detail: p.ImageDetail}})
				continue
			}
			parts = append(parts, oai.ContentPart{Type: "text", Text: p.Text})
		}
		out.Content = oai.Content{Parts: parts}
	}
	return out
}

// transportError classifies a failure to get or read a response. A caller
// cancellation is returned as the context's error, unclassified, so it is
// never mistaken for a provider failure.
func (p *Provider) transportError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return &provider.UpstreamError{Provider: p.name, Kind: provider.KindTimeout, Retryable: true, Msg: err.Error()}
		}
		return ctxErr
	}
	return &provider.UpstreamError{Provider: p.name, Kind: provider.KindTransport, Retryable: true, Msg: err.Error()}
}

func (p *Provider) statusError(resp *http.Response) error {
	kind, retryable := provider.ClassifyStatus(resp.StatusCode)
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	msg := strings.TrimSpace(string(raw))
	var eb oai.ErrorBody
	if json.Unmarshal(raw, &eb) == nil && eb.Error.Message != "" {
		msg = eb.Error.Message
	}
	return &provider.UpstreamError{Provider: p.name, Status: resp.StatusCode, Kind: kind, Retryable: retryable, Msg: msg}
}

type stream struct {
	p    *Provider
	ctx  context.Context
	body io.ReadCloser
	rd   *sse.Reader

	err      error // sticky terminal error (io.EOF on clean end)
	finished bool  // a finish_reason has been seen
	usage    provider.Reported

	closeOnce sync.Once
}

func (s *stream) Recv() (*provider.Chunk, error) {
	if s.err != nil {
		return nil, s.err
	}
	for {
		ev, err := s.rd.Next()
		if err != nil {
			s.err = s.readError(err)
			return nil, s.err
		}
		if string(ev.Data) == oai.DoneData {
			s.err = io.EOF
			return nil, s.err
		}
		var c oai.ChatCompletionChunk
		if err := json.Unmarshal(ev.Data, &c); err != nil {
			s.err = &provider.UpstreamError{Provider: s.p.name, Kind: provider.KindProtocol, Retryable: true,
				Msg: "invalid stream chunk: " + err.Error()}
			return nil, s.err
		}
		if c.Error != nil {
			s.err = &provider.UpstreamError{Provider: s.p.name, Kind: provider.KindServer, Retryable: true,
				Msg: c.Error.Message}
			return nil, s.err
		}

		var out provider.Chunk
		if len(c.Choices) > 0 {
			ch := c.Choices[0]
			out.Role = ch.Delta.Role
			if ch.Delta.Content != nil {
				out.Content = *ch.Delta.Content
			}
			for _, d := range ch.Delta.ToolCalls {
				td := provider.ToolCallDelta{Index: d.Index, ID: d.ID}
				if d.Function != nil {
					td.Name = d.Function.Name
					td.ArgumentsFragment = d.Function.Arguments
				}
				out.ToolCalls = append(out.ToolCalls, td)
			}
			if ch.FinishReason != nil {
				out.FinishReason = *ch.FinishReason
				s.finished = true
			}
		}
		if c.Usage != nil {
			s.usage = provider.Reported{
				PromptTokens: c.Usage.PromptTokens, CompletionTokens: c.Usage.CompletionTokens,
				HasPrompt: true, HasCompletion: true,
			}
		}
		if out.Role == "" && !out.HasOutput() {
			continue // the usage-only final chunk, or an empty keepalive
		}
		return &out, nil
	}
}

// readError classifies a failure while reading the stream body.
func (s *stream) readError(err error) error {
	if s.ctx.Err() != nil {
		return s.p.transportError(s.ctx, err)
	}
	if errors.Is(err, io.EOF) && s.finished {
		return io.EOF // some compatible providers omit [DONE]
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return &provider.UpstreamError{Provider: s.p.name, Kind: provider.KindTransport, Retryable: true,
			Msg: "stream ended before completion"}
	}
	return s.p.transportError(s.ctx, err)
}

func (s *stream) Usage() provider.Reported { return s.usage }

func (s *stream) Close() error {
	var err error
	s.closeOnce.Do(func() { err = s.body.Close() })
	return err
}
