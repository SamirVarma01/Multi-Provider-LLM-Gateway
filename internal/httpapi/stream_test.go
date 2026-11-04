package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/mock"
	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/sse"
)

// clientStream is what an OpenAI client sees on a streamed response.
type clientStream struct {
	chunks    []oai.ChatCompletionChunk
	content   string
	toolCalls []oai.ToolCall // assembled by index
	finish    string
	usage     *oai.Usage
	sawDone   bool
	errEvent  *oai.ErrorDetail
	err       error
}

func readClientStream(r io.Reader) clientStream {
	var cs clientStream
	rd := sse.NewReader(r)
	for {
		ev, err := rd.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				cs.err = err
			}
			return cs
		}
		if string(ev.Data) == oai.DoneData {
			cs.sawDone = true
			continue
		}
		var c oai.ChatCompletionChunk
		if err := json.Unmarshal(ev.Data, &c); err != nil {
			cs.err = fmt.Errorf("bad chunk %s: %w", ev.Data, err)
			return cs
		}
		if c.Error != nil {
			cs.errEvent = c.Error
			continue
		}
		cs.chunks = append(cs.chunks, c)
		if c.Usage != nil {
			cs.usage = c.Usage
		}
		for _, ch := range c.Choices {
			if ch.Delta.Content != nil {
				cs.content += *ch.Delta.Content
			}
			for _, d := range ch.Delta.ToolCalls {
				for len(cs.toolCalls) <= d.Index {
					cs.toolCalls = append(cs.toolCalls, oai.ToolCall{})
				}
				tc := &cs.toolCalls[d.Index]
				if d.ID != "" {
					tc.ID, tc.Type = d.ID, d.Type
				}
				if d.Function != nil {
					if d.Function.Name != "" {
						tc.Function.Name = d.Function.Name
					}
					tc.Function.Arguments += d.Function.Arguments
				}
			}
			if ch.FinishReason != nil {
				cs.finish = *ch.FinishReason
			}
		}
	}
}

func streamReq(model string, includeUsage bool) oai.ChatCompletionRequest {
	r := chatReq(model, nil)
	r.Stream = true
	if includeUsage {
		r.StreamOptions = &oai.StreamOptions{IncludeUsage: true}
	}
	return r
}

func TestStreamThroughBothFormats(t *testing.T) {
	for _, alias := range []string{"chat", "claude"} {
		t.Run(alias, func(t *testing.T) {
			e := newEnv(t, mock.Behavior{OutputTokens: 12}, nil)
			resp := e.chat(t, streamReq(alias, true))
			if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status %d %s: %s", resp.StatusCode, resp.Header.Get("Content-Type"), body)
			}
			reqID := resp.Header.Get(obs.RequestIDHeader)
			cs := readClientStream(resp.Body)
			if cs.err != nil || !cs.sawDone || cs.errEvent != nil {
				t.Fatalf("stream = %+v", cs)
			}

			first := cs.chunks[0].Choices[0].Delta
			if first.Role != "assistant" || first.Content == nil || *first.Content != "" {
				t.Errorf("first chunk delta = %+v, want role assistant with empty content", first)
			}
			for _, c := range cs.chunks {
				if c.ID != "chatcmpl-"+reqID || c.Object != "chat.completion.chunk" || c.Model != alias {
					t.Fatalf("chunk envelope = %+v", c)
				}
			}
			if len(strings.Fields(cs.content)) != 12 || cs.finish != "stop" {
				t.Errorf("content %q finish %q", cs.content, cs.finish)
			}

			rec := e.mock.Records(reqID)[0]
			want := oai.Usage{PromptTokens: rec.PromptTokens, CompletionTokens: 12, TotalTokens: rec.PromptTokens + 12}
			if cs.usage == nil || *cs.usage != want {
				t.Errorf("usage = %+v, mock recorded %+v", cs.usage, rec)
			}

			o := e.outcome(t)
			if !o.Committed || !o.Billable || o.Result != resultCompleted || o.Usage.Estimated ||
				o.Usage.PromptTokens != rec.PromptTokens || o.Usage.CompletionTokens != 12 || o.TTFT <= 0 {
				t.Errorf("outcome = %+v", o)
			}
		})
	}
}

func TestStreamUsageChunkOnlyWhenRequested(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 3}, nil)
	cs := readClientStream(e.chat(t, streamReq("chat", false)).Body)
	if cs.usage != nil || !cs.sawDone {
		t.Errorf("stream = %+v", cs)
	}
	// The gateway still captured usage for itself.
	if o := e.outcome(t); o.Usage.Estimated || o.Usage.CompletionTokens != 3 {
		t.Errorf("outcome usage = %+v", o.Usage)
	}
}

func TestOmittedUsageIsEstimated(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			e := newEnv(t, mock.Behavior{OutputTokens: 8, OmitUsage: true}, nil)
			var u *oai.Usage
			var content string
			if stream {
				cs := readClientStream(e.chat(t, streamReq("chat", true)).Body)
				u, content = cs.usage, cs.content
			} else {
				out := decode[oai.ChatCompletionResponse](t, e.chat(t, chatReq("chat", nil)))
				u, content = out.Usage, *out.Choices[0].Message.Content
			}
			wantCompletion := (len(content) + 3) / 4
			if u == nil || !u.Estimated || u.CompletionTokens != wantCompletion || u.PromptTokens == 0 {
				t.Errorf("usage = %+v, want estimated with %d completion tokens", u, wantCompletion)
			}
			if o := e.outcome(t); !o.Usage.Estimated || !o.Billable {
				t.Errorf("outcome = %+v", o)
			}
		})
	}
}

func TestStreamPreCommitFailureIsPlainHTTPError(t *testing.T) {
	tests := []struct {
		name   string
		b      mock.Behavior
		status int
		code   string
	}{
		{"provider 503", mock.Behavior{ErrorRate: 1, ErrorStatus: 503}, 502, "upstream_error"},
		{"provider hangs past the request timeout", mock.Behavior{Hang: true}, 504, "upstream_timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, tt.b, func(o *Options) { o.RequestTimeout = 200 * time.Millisecond })
			resp := e.chat(t, streamReq("chat", true))
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q: nothing may be streamed before the commit point", ct)
			}
			wantError(t, resp, tt.status, tt.code)
			if o := e.outcome(t); o.Committed || o.Billable || o.Result != resultUpstreamError {
				t.Errorf("outcome = %+v", o)
			}
		})
	}
}

func TestStreamMidStreamFailure(t *testing.T) {
	for _, alias := range []string{"chat", "claude"} {
		for _, mode := range []string{mock.FailAbort, mock.FailErrorEvent} {
			t.Run(alias+"/"+mode, func(t *testing.T) {
				e := newEnv(t, mock.Behavior{OutputTokens: 10, FailAfterTokens: 3, FailMode: mode}, nil)
				resp := e.chat(t, streamReq(alias, true))
				reqID := resp.Header.Get(obs.RequestIDHeader)
				cs := readClientStream(resp.Body)

				if resp.StatusCode != http.StatusOK || cs.err != nil {
					t.Fatalf("status %d err %v", resp.StatusCode, cs.err)
				}
				if len(strings.Fields(cs.content)) != 3 {
					t.Errorf("client received %q before the failure, want 3 tokens", cs.content)
				}
				if cs.errEvent == nil || cs.errEvent.Code != "stream_interrupted" || cs.errEvent.RequestID != reqID {
					t.Errorf("error event = %+v", cs.errEvent)
				}
				if cs.sawDone || cs.usage != nil {
					t.Error("a truncated stream must not end with usage or [DONE]")
				}

				o := e.outcome(t)
				if !o.Committed || !o.Billable || o.Result != resultUpstreamError || !o.Usage.Estimated {
					t.Errorf("outcome = %+v", o)
				}
				rec := waitMockRecord(t, e.mock, reqID)
				if alias == "claude" && o.Usage.PromptTokens != rec.PromptTokens {
					// Anthropic reports input up front, so only the output count is estimated.
					t.Errorf("prompt tokens = %d, mock reported %d", o.Usage.PromptTokens, rec.PromptTokens)
				}
				if o.Usage.CompletionTokens != (len(cs.content)+3)/4 {
					t.Errorf("completion estimate %d does not match the %d bytes received", o.Usage.CompletionTokens, len(cs.content))
				}
			})
		}
	}
}

func waitMockRecord(t *testing.T, m *mock.Server, reqID string) mock.Record {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if recs := m.Records(reqID); len(recs) > 0 {
			return recs[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("mock has no record for %s", reqID)
	return mock.Record{}
}

func TestClientDisconnectCancelsUpstream(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			// 2000 tokens at 200/s would take 10s to finish.
			e := newEnv(t, mock.Behavior{OutputTokens: 2000, TokensPerSec: 200}, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body, _ := json.Marshal(streamReq("chat", true))
			var rd io.Reader = bytes.NewReader(body)
			if !stream {
				body, _ = json.Marshal(chatReq("chat", nil))
				// A body of unknown length is sent chunked, so its EOF arrives in a
				// read of its own, after the JSON value. The server notices a
				// disconnect only once the body has been read to EOF; the gateway
				// must drain it for this case to pass.
				rd = io.MultiReader(bytes.NewReader(body))
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.public.URL+"/v1/chat/completions", rd)
			req.Header.Set("Authorization", "Bearer "+e.key)

			var reqID string
			if stream {
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				reqID = resp.Header.Get(obs.RequestIDHeader)
				rd := sse.NewReader(resp.Body)
				for range 4 { // role chunk + 3 tokens
					if _, err := rd.Next(); err != nil {
						t.Fatal(err)
					}
				}
				cancel()
				resp.Body.Close()
			} else {
				time.AfterFunc(100*time.Millisecond, cancel)
				if _, err := http.DefaultClient.Do(req); err == nil {
					t.Fatal("request should have been cancelled")
				}
			}
			cancelled := time.Now()

			o := e.outcome(t)
			if o.Result != resultClientDisconnected {
				t.Fatalf("outcome = %+v", o)
			}
			if stream && (!o.Committed || !o.Billable || !o.Usage.Estimated || o.Usage.CompletionTokens == 0) {
				t.Errorf("streamed outcome = %+v: received output is billable and estimated", o)
			}
			if !stream && (o.Committed || o.Billable) {
				t.Errorf("non-stream outcome = %+v: nothing was delivered, nothing is billable", o)
			}

			// The upstream saw the cancellation long before it would have finished.
			recs := e.mock.AllRecords()
			for len(recs) == 0 && time.Since(cancelled) < 2*time.Second {
				time.Sleep(5 * time.Millisecond)
				recs = e.mock.AllRecords()
			}
			if len(recs) != 1 || recs[0].Outcome != mock.OutcomeClientCancelled || recs[0].CompletionTokens >= 2000 {
				t.Fatalf("mock records = %+v", recs)
			}
			if stream && recs[0].RequestID != reqID {
				t.Errorf("mock record %s, response %s", recs[0].RequestID, reqID)
			}
			if lag := recs[0].EndedAt.Sub(cancelled); lag > time.Second {
				t.Errorf("upstream ran %v after the client left", lag)
			}
		})
	}
}

// replayRoutes serves canned upstream streams: openaiBody on an OpenAI-format
// provider (alias "r-openai") and anthropicBody on an Anthropic one ("r-claude").
func replayRoutes(t *testing.T, openaiBody, anthropicBody string) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if strings.HasSuffix(r.URL.Path, "/messages") {
			_, _ = io.WriteString(w, anthropicBody)
			return
		}
		_, _ = io.WriteString(w, openaiBody)
	}))
	t.Cleanup(ts.Close)
	return fmt.Sprintf(`
providers:
  - { name: r-openai, format: openai, base_url: "%[1]s/v1" }
  - { name: r-claude, format: anthropic, base_url: "%[1]s/v1" }
models:
  - alias: r-openai
    candidates: [{ provider: r-openai, model: m, max_output: 100 }]
  - alias: r-claude
    candidates: [{ provider: r-claude, model: m, max_output: 100 }]
`, ts.URL)
}

const openaiToolStream = `data: {"choices":[{"index":0,"delta":{"role":"assistant","content":null},"finish_reason":null}]}

data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}

data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":\"Paris\"}"}}]},"finish_reason":null}]}

data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

data: {"choices":[],"usage":{"prompt_tokens":40,"completion_tokens":9,"total_tokens":49}}

data: [DONE]

`

const anthropicToolStream = `event: message_start
data: {"type":"message_start","message":{"usage":{"input_tokens":40,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_a","name":"get_weather","input":{}}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\":\"Paris\"}"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":9}}

event: message_stop
data: {"type":"message_stop"}

`

func TestToolCallsReachTheClient(t *testing.T) {
	e := newEnvRoutes(t, replayRoutes(t, openaiToolStream, anthropicToolStream), nil)
	for _, alias := range []string{"r-openai", "r-claude"} {
		t.Run(alias+"/stream", func(t *testing.T) {
			cs := readClientStream(e.chat(t, streamReq(alias, true)).Body)
			if cs.err != nil || !cs.sawDone || cs.finish != "tool_calls" || len(cs.toolCalls) != 1 {
				t.Fatalf("stream = %+v", cs)
			}
			tc := cs.toolCalls[0]
			if tc.ID == "" || tc.Type != "function" || tc.Function.Name != "get_weather" ||
				tc.Function.Arguments != `{"city":"Paris"}` {
				t.Errorf("tool call = %+v", tc)
			}
			if *cs.usage != (oai.Usage{PromptTokens: 40, CompletionTokens: 9, TotalTokens: 49}) {
				t.Errorf("usage = %+v", cs.usage)
			}
			<-e.outcomes
		})
		t.Run(alias+"/non-stream", func(t *testing.T) {
			out := decode[oai.ChatCompletionResponse](t, e.chat(t, chatReq(alias, nil)))
			msg := out.Choices[0].Message
			if msg.Content != nil {
				t.Errorf("content = %q, want null for a tool-call-only reply", *msg.Content)
			}
			if out.Choices[0].FinishReason != "tool_calls" || len(msg.ToolCalls) != 1 ||
				msg.ToolCalls[0].Function.Arguments != `{"city":"Paris"}` || msg.ToolCalls[0].Type != "function" {
				t.Errorf("message = %+v", msg)
			}
			<-e.outcomes
		})
	}
}

// A full tool round trip in OpenAI format: tools declared, the assistant's
// call, and the tool's result, sent to providers of both formats.
const toolConversation = `{
	"model": %q,
	"messages": [
		{"role": "developer", "content": "use tools"},
		{"role": "user", "content": [
			{"type": "text", "text": "weather in Paris?"},
			{"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgo="}}
		]},
		{"role": "assistant", "content": null, "tool_calls": [
			{"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}}
		]},
		{"role": "tool", "tool_call_id": "call_1", "content": "18C and sunny"}
	],
	"tools": [{"type": "function", "function": {"name": "get_weather", "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}}],
	"tool_choice": "auto"
}`

func TestToolConversationIsAcceptedByBothFormats(t *testing.T) {
	e := newEnv(t, mock.Behavior{OutputTokens: 5}, nil)
	for _, alias := range []string{"chat", "claude"} {
		t.Run(alias, func(t *testing.T) {
			resp := e.chat(t, fmt.Sprintf(toolConversation, alias))
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status %d: %s", resp.StatusCode, body)
			}
			out := decode[oai.ChatCompletionResponse](t, resp)
			rec := e.mock.Records(resp.Header.Get(obs.RequestIDHeader))[0]
			if out.Usage.PromptTokens != rec.PromptTokens || out.Usage.Estimated {
				t.Errorf("usage = %+v, mock recorded %+v", out.Usage, rec)
			}
			<-e.outcomes
		})
	}
}
