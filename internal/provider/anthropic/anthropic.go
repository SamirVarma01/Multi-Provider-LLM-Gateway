// Package anthropic adapts the Anthropic Messages API to provider.Provider
// (design §5.4).
//
// It calls the HTTP API directly rather than through the Go SDK: the gateway
// must own retries (the SDK retries 429s and 5xx by itself, which would hide
// failures from the gateway's failover and circuit breakers), point the
// adapter at any base URL (the mock), and see raw stream bytes for timing.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/sse"
)

// APIVersion is the anthropic-version header the adapter sends. At this
// version the API rejects temperature, top_p, and top_k on every model.
const APIVersion = "2026-10-21"

// maxErrorBody bounds how much of a non-200 response body is read.
const maxErrorBody = 64 << 10

// Provider calls an Anthropic Messages API.
type Provider struct {
	name    string
	baseURL string // up to and including /v1, e.g. https://api.anthropic.com/v1
	apiKey  string
	client  *http.Client
}

// New returns an Anthropic provider.
func New(name, baseURL, apiKey string, client *http.Client) *Provider {
	return &Provider{name: name, baseURL: strings.TrimSuffix(baseURL, "/"), apiKey: apiKey, client: client}
}

// Name implements provider.Provider.
func (p *Provider) Name() string { return p.name }

// Supports implements provider.Provider. It rejects, rather than drops,
// anything the Messages API cannot express: unmodeled OpenAI fields and
// non-default sampling values. Default sampling values (1) are equivalent to
// omitting them, so those are accepted and not sent.
func (p *Provider) Supports(req *provider.ChatRequest) error {
	var errs []error
	if len(req.Extra) > 0 {
		keys := make([]string, 0, len(req.Extra))
		for k := range req.Extra {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		errs = append(errs, fmt.Errorf("anthropic: unsupported parameters: %s", strings.Join(keys, ", ")))
	}
	if v := req.Temperature; v != nil && *v != 1 {
		errs = append(errs, fmt.Errorf("anthropic: temperature is not supported (API version %s accepts only the default)", APIVersion))
	}
	if v := req.TopP; v != nil && *v != 1 {
		errs = append(errs, fmt.Errorf("anthropic: top_p is not supported (API version %s accepts only the default)", APIVersion))
	}
	// Validate the conversion itself. The output cap is per target and set
	// later, so a placeholder stands in for it here.
	check := *req
	if check.MaxTokens == nil {
		one := 1
		check.MaxTokens = &one
	}
	if _, err := toWire(provider.Target{}, &check); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Stream implements provider.Provider.
func (p *Provider) Stream(ctx context.Context, t provider.Target, req *provider.ChatRequest) (provider.Stream, error) {
	w, err := toWire(t, req)
	if err != nil {
		return nil, &provider.UpstreamError{Provider: p.name, Kind: provider.KindBadRequest, Msg: err.Error()}
	}
	body, err := json.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("anthropic: encode request: %w", err)
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/messages", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("anthropic: build request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/event-stream")
	hreq.Header.Set("anthropic-version", APIVersion)
	if p.apiKey != "" {
		hreq.Header.Set("x-api-key", p.apiKey)
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
	return &stream{p: p, ctx: ctx, body: resp.Body, rd: sse.NewReader(resp.Body), blocks: map[int]*block{}}, nil
}

// transportError classifies a failure to get or read a response. A caller
// cancellation is returned as the context's error, unclassified.
func (p *Provider) transportError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return &provider.UpstreamError{Provider: p.name, Kind: provider.KindTimeout, Retryable: true, Msg: err.Error()}
		}
		return ctxErr
	}
	return &provider.UpstreamError{Provider: p.name, Kind: provider.KindTransport, Retryable: true, Msg: err.Error()}
}

// errorBody is the Messages API error envelope.
type errorBody struct {
	Type  string `json:"type"`
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func (p *Provider) statusError(resp *http.Response) error {
	kind, retryable := provider.ClassifyStatus(resp.StatusCode)
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	msg := strings.TrimSpace(string(raw))
	var eb errorBody
	if json.Unmarshal(raw, &eb) == nil && eb.Error.Message != "" {
		msg = eb.Error.Type + ": " + eb.Error.Message
	}
	return &provider.UpstreamError{Provider: p.name, Status: resp.StatusCode, Kind: kind, Retryable: retryable, Msg: msg}
}

// stopReason maps a Messages API stop_reason to an OpenAI finish_reason.
func stopReason(s string) string {
	switch s {
	case "max_tokens", "model_context_window_exceeded":
		return provider.FinishLength
	case "tool_use":
		return provider.FinishToolCalls
	case "refusal":
		return provider.FinishContentFilter
	default: // end_turn, stop_sequence, pause_turn
		return provider.FinishStop
	}
}

// Content block kinds the stream tracks.
const (
	blockText  = "text"
	blockTool  = "tool_use"
	blockOther = "other" // thinking, redacted_thinking, server tools: not forwarded
)

type block struct {
	kind      string
	toolIndex int  // OpenAI tool_calls index for tool_use blocks
	sawArgs   bool // any non-empty input_json_delta arrived
}

type stream struct {
	p    *Provider
	ctx  context.Context
	body io.ReadCloser
	rd   *sse.Reader

	err      error // sticky terminal error (io.EOF on clean end)
	usage    provider.Reported
	blocks   map[int]*block
	nextTool int
	finished bool // message_delta carried a stop_reason

	closeOnce sync.Once
}

// event is the union of the stream event fields the adapter reads.
type event struct {
	Type    string `json:"type"`
	Index   int    `json:"index"`
	Message *struct {
		Usage *wireUsage `json:"usage"`
	} `json:"message"`
	ContentBlock *struct {
		Type string `json:"type"`
		Text string `json:"text"`
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"content_block"`
	Delta *struct {
		Type        string `json:"type"`
		Text        string `json:"text"`
		PartialJSON string `json:"partial_json"`
		StopReason  string `json:"stop_reason"`
	} `json:"delta"`
	Usage *wireUsage `json:"usage"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

type wireUsage struct {
	InputTokens              *int `json:"input_tokens"`
	CacheCreationInputTokens int  `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int  `json:"cache_read_input_tokens"`
	OutputTokens             *int `json:"output_tokens"`
}

// promptTokens is everything billed as input: uncached, cache writes, and
// cache reads (design §5.4 prices all three at the input rate).
func (u *wireUsage) promptTokens() (int, bool) {
	if u.InputTokens == nil {
		return 0, false
	}
	return *u.InputTokens + u.CacheCreationInputTokens + u.CacheReadInputTokens, true
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
		var e event
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			s.err = &provider.UpstreamError{Provider: s.p.name, Kind: provider.KindProtocol, Retryable: true,
				Msg: "invalid stream event: " + err.Error()}
			return nil, s.err
		}
		c, err := s.translate(&e)
		if err != nil {
			s.err = err
			return nil, err
		}
		if c != nil {
			return c, nil
		}
	}
}

// translate turns one event into a chunk, nil for events with nothing to
// forward, or a terminal error (io.EOF at message_stop).
func (s *stream) translate(e *event) (*provider.Chunk, error) {
	switch e.Type {
	case "message_start":
		if e.Message != nil && e.Message.Usage != nil {
			// The output count here is provisional; the final one comes in message_delta.
			if n, ok := e.Message.Usage.promptTokens(); ok {
				s.usage.PromptTokens, s.usage.HasPrompt = n, true
			}
		}
		return &provider.Chunk{Role: string(provider.RoleAssistant)}, nil

	case "content_block_start":
		cb := e.ContentBlock
		if cb == nil {
			return nil, nil
		}
		switch cb.Type {
		case blockText:
			s.blocks[e.Index] = &block{kind: blockText}
			if cb.Text != "" {
				return &provider.Chunk{Content: cb.Text}, nil
			}
		case blockTool:
			b := &block{kind: blockTool, toolIndex: s.nextTool}
			s.nextTool++
			s.blocks[e.Index] = b
			return &provider.Chunk{ToolCalls: []provider.ToolCallDelta{{Index: b.toolIndex, ID: cb.ID, Name: cb.Name}}}, nil
		default:
			s.blocks[e.Index] = &block{kind: blockOther}
		}
		return nil, nil

	case "content_block_delta":
		b := s.blocks[e.Index]
		if e.Delta == nil || b == nil {
			return nil, nil
		}
		switch {
		case e.Delta.Type == "text_delta" && b.kind == blockText && e.Delta.Text != "":
			return &provider.Chunk{Content: e.Delta.Text}, nil
		case e.Delta.Type == "input_json_delta" && b.kind == blockTool && e.Delta.PartialJSON != "":
			b.sawArgs = true
			return &provider.Chunk{ToolCalls: []provider.ToolCallDelta{{Index: b.toolIndex, ArgumentsFragment: e.Delta.PartialJSON}}}, nil
		}
		return nil, nil // thinking, signature, citations

	case "content_block_stop":
		// A tool called with no input streams no input_json_delta; OpenAI
		// clients expect valid JSON arguments, so send "{}".
		if b := s.blocks[e.Index]; b != nil && b.kind == blockTool && !b.sawArgs {
			b.sawArgs = true
			return &provider.Chunk{ToolCalls: []provider.ToolCallDelta{{Index: b.toolIndex, ArgumentsFragment: "{}"}}}, nil
		}
		return nil, nil

	case "message_delta":
		if u := e.Usage; u != nil {
			if n, ok := u.promptTokens(); ok {
				s.usage.PromptTokens, s.usage.HasPrompt = n, true
			}
			if u.OutputTokens != nil {
				s.usage.CompletionTokens, s.usage.HasCompletion = *u.OutputTokens, true
			}
		}
		if e.Delta != nil && e.Delta.StopReason != "" {
			s.finished = true
			return &provider.Chunk{FinishReason: stopReason(e.Delta.StopReason)}, nil
		}
		return nil, nil

	case "message_stop":
		return nil, io.EOF

	case "error":
		ue := &provider.UpstreamError{Provider: s.p.name, Kind: provider.KindServer, Retryable: true, Msg: "stream error"}
		if e.Error != nil {
			ue.Msg = e.Error.Type + ": " + e.Error.Message
			switch e.Error.Type {
			case "rate_limit_error":
				ue.Kind = provider.KindRateLimited
			case "invalid_request_error":
				ue.Kind, ue.Retryable = provider.KindBadRequest, false
			}
		}
		return nil, ue
	}
	// ping, and event types added in later API versions.
	return nil, nil
}

// readError classifies a failure while reading the stream body.
func (s *stream) readError(err error) error {
	if s.ctx.Err() != nil {
		return s.p.transportError(s.ctx, err)
	}
	if errors.Is(err, io.EOF) && s.finished {
		return io.EOF // final usage and stop reason arrived; only message_stop is missing
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
