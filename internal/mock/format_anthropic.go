package mock

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/SamirVarma01/llm-gateway/internal/sse"
)

// AnthropicVersion is the API version the mock requires, matching what the
// gateway's Anthropic adapter sends.
const AnthropicVersion = "2026-10-21"

// anthropicFormat speaks the Messages API. It also rejects what the real API
// rejects at this version (no anthropic-version header, sampling parameters,
// a missing max_tokens), so adapter regressions fail tests instead of
// production.
type anthropicFormat struct{}

type anthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int                `json:"max_tokens"`
	Stream    bool               `json:"stream"`
	System    json.RawMessage    `json:"system"`
	Messages  []anthropicMessage `json:"messages"`
}

type anthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

func (anthropicFormat) parse(r *http.Request) (request, error) {
	if v := r.Header.Get("anthropic-version"); v != AnthropicVersion {
		return request{}, fmt.Errorf("anthropic-version must be %s, got %q", AnthropicVersion, v)
	}
	raw, err := readAll(r)
	if err != nil {
		return request{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return request{}, errors.New("invalid JSON body: " + err.Error())
	}
	for _, p := range []string{"temperature", "top_p", "top_k"} {
		if _, ok := fields[p]; ok {
			return request{}, fmt.Errorf("%s: not supported at API version %s", p, AnthropicVersion)
		}
	}
	var req anthropicRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return request{}, errors.New("invalid JSON body: " + err.Error())
	}
	switch {
	case req.Model == "":
		return request{}, errors.New("model: field required")
	case req.MaxTokens < 1:
		return request{}, errors.New("max_tokens: field required and must be >= 1")
	case len(req.Messages) == 0:
		return request{}, errors.New("messages: at least one message is required")
	}
	n := 3 + countWords(req.System)
	for i, m := range req.Messages {
		if m.Role != "user" && m.Role != "assistant" {
			return request{}, fmt.Errorf("messages.%d.role: must be user or assistant", i)
		}
		n += 3 + countWords(m.Content)
	}
	limit := req.MaxTokens
	return request{
		model:        req.Model,
		stream:       req.Stream,
		includeUsage: true, // the Messages API always reports usage in the stream
		outputLimit:  &limit,
		promptTokens: n,
	}, nil
}

// countWords counts whitespace-separated words in every string value of a
// JSON document: string content, text blocks, tool inputs, tool results.
func countWords(raw json.RawMessage) int {
	if len(bytes.TrimSpace(raw)) == 0 {
		return 0
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return 0
	}
	n := 0
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			n += len(strings.Fields(x))
		case []any:
			for _, e := range x {
				walk(e)
			}
		case map[string]any:
			for k, e := range x {
				// Skip structural keys and binary payloads; count text.
				if k == "type" || k == "id" || k == "tool_use_id" || k == "media_type" || k == "data" {
					continue
				}
				walk(e)
			}
		}
	}
	walk(v)
	return n
}

func (anthropicFormat) writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, anthropicError(status, msg))
}

func anthropicError(status int, msg string) map[string]any {
	typ := "api_error"
	switch status {
	case http.StatusBadRequest:
		typ = "invalid_request_error"
	case http.StatusUnauthorized:
		typ = "authentication_error"
	case http.StatusForbidden:
		typ = "permission_error"
	case http.StatusNotFound:
		typ = "not_found_error"
	case http.StatusTooManyRequests:
		typ = "rate_limit_error"
	case 529:
		typ = "overloaded_error"
	}
	return map[string]any{"type": "error", "error": map[string]string{"type": typ, "message": msg}}
}

func anthropicStopReason(truncated bool) string {
	if truncated {
		return "max_tokens"
	}
	return "end_turn"
}

func (anthropicFormat) writeCompletion(w http.ResponseWriter, c completion) {
	msg := map[string]any{
		"id":            "msg_" + c.id,
		"type":          "message",
		"role":          "assistant",
		"model":         c.model,
		"content":       []any{map[string]string{"type": "text", "text": c.text}},
		"stop_reason":   anthropicStopReason(c.truncated),
		"stop_sequence": nil,
	}
	if c.withUsage {
		msg["usage"] = map[string]int{"input_tokens": c.promptTokens, "output_tokens": c.outputTokens}
	}
	writeJSON(w, http.StatusOK, msg)
}

func (anthropicFormat) streamer(sw *sse.Writer, c completion, _ bool) streamer {
	return &anthropicStreamer{sw: sw, c: c}
}

type anthropicStreamer struct {
	sw *sse.Writer
	c  completion
}

func (s *anthropicStreamer) event(typ string, fields map[string]any) error {
	fields["type"] = typ
	return s.sw.EventJSON(typ, fields)
}

func (s *anthropicStreamer) start() error {
	msg := map[string]any{
		"id": "msg_" + s.c.id, "type": "message", "role": "assistant", "model": s.c.model,
		"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
	}
	if s.c.withUsage {
		// The real API reports a provisional output count here; adapters must
		// take the final count from message_delta instead.
		msg["usage"] = map[string]int{"input_tokens": s.c.promptTokens, "output_tokens": 1}
	}
	if err := s.event("message_start", map[string]any{"message": msg}); err != nil {
		return err
	}
	if err := s.event("content_block_start", map[string]any{
		"index": 0, "content_block": map[string]string{"type": "text", "text": ""},
	}); err != nil {
		return err
	}
	return s.event("ping", map[string]any{})
}

func (s *anthropicStreamer) token(_ int, text string) error {
	return s.event("content_block_delta", map[string]any{
		"index": 0, "delta": map[string]string{"type": "text_delta", "text": text},
	})
}

func (s *anthropicStreamer) finish(outputTokens int) error {
	if err := s.event("content_block_stop", map[string]any{"index": 0}); err != nil {
		return err
	}
	delta := map[string]any{
		"delta": map[string]any{"stop_reason": anthropicStopReason(s.c.truncated), "stop_sequence": nil},
	}
	if s.c.withUsage {
		delta["usage"] = map[string]int{"output_tokens": outputTokens}
	}
	return s.event("message_delta", delta)
}

func (s *anthropicStreamer) done() error {
	return s.event("message_stop", map[string]any{})
}

func (s *anthropicStreamer) midStreamError() error {
	return s.sw.EventJSON("error", anthropicError(529, "mock: injected mid-stream failure"))
}

// maxRequestBytes bounds request bodies the mock reads.
const maxRequestBytes = 32 << 20

func readAll(r *http.Request) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(http.MaxBytesReader(nil, r.Body, maxRequestBytes)); err != nil {
		return nil, errors.New("read body: " + err.Error())
	}
	return buf.Bytes(), nil
}
