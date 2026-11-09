package mock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/SamirVarma01/llm-gateway/internal/sse"
)

func anthropicBody(stream bool, maxTokens int) []byte {
	b, _ := json.Marshal(map[string]any{
		"model":      "claude-test",
		"max_tokens": maxTokens,
		"stream":     stream,
		"system":     "be brief",
		"messages": []any{
			map[string]any{"role": "user", "content": "hello there mock"},
			map[string]any{"role": "assistant", "content": []any{
				map[string]any{"type": "tool_use", "id": "toolu_1", "name": "f", "input": map[string]string{"q": "two words"}},
			}},
			map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": "toolu_1", "content": "result text"},
			}},
		},
	})
	return b
}

func postAnthropic(t *testing.T, url string, body []byte, hdr map[string]string) *http.Response {
	t.Helper()
	h := map[string]string{"anthropic-version": AnthropicVersion}
	for k, v := range hdr {
		h[k] = v
	}
	return post(t, context.Background(), url, body, h)
}

type anthropicEvent struct {
	name string
	data map[string]any
}

func readAnthropicEvents(t *testing.T, r io.Reader) ([]anthropicEvent, error) {
	t.Helper()
	var out []anthropicEvent
	rd := sse.NewReader(r)
	for {
		ev, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		var d map[string]any
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			t.Fatalf("event %q data is not JSON: %s", ev.Event, ev.Data)
		}
		if d["type"] != ev.Event {
			t.Errorf("event line %q but data type %v", ev.Event, d["type"])
		}
		out = append(out, anthropicEvent{name: ev.Event, data: d})
	}
}

func TestAnthropicStreamAndCompletion(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 6})
	url := ts.URL + "/p/claude-a/v1/messages"
	hdr := map[string]string{RequestIDHeader: "ant-1"}

	resp := postAnthropic(t, url, anthropicBody(true, 100), hdr)
	events, err := readAnthropicEvents(t, resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	var text strings.Builder
	for _, e := range events {
		names = append(names, e.name)
		if e.name == "content_block_delta" {
			text.WriteString(e.data["delta"].(map[string]any)["text"].(string))
		}
	}
	want := "message_start content_block_start ping " + strings.Repeat("content_block_delta ", 6) +
		"content_block_stop message_delta message_stop"
	if strings.Join(names, " ") != want {
		t.Fatalf("events = %v", names)
	}

	rec := s.Records("ant-1")[0]
	// system "be brief" (2) + 3 per message: "hello there mock" (3), tool input "two words" (2) + name "f" (1),
	// tool result "result text" (2), plus 3 priming.
	if rec.PromptTokens != 3+2+(3+3)+(3+3)+(3+2) || rec.Provider != "claude-a" || !rec.Billable {
		t.Errorf("record = %+v", rec)
	}
	start := events[0].data["message"].(map[string]any)["usage"].(map[string]any)
	if start["input_tokens"] != float64(rec.PromptTokens) {
		t.Errorf("message_start usage = %v, record %d", start, rec.PromptTokens)
	}
	md := events[len(events)-2].data
	if md["delta"].(map[string]any)["stop_reason"] != "end_turn" ||
		md["usage"].(map[string]any)["output_tokens"] != float64(6) {
		t.Errorf("message_delta = %v", md)
	}

	resp = postAnthropic(t, url, anthropicBody(false, 100), hdr)
	var msg struct {
		Content    []struct{ Type, Text string }
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		}
	}
	_ = json.NewDecoder(resp.Body).Decode(&msg)
	resp.Body.Close()
	if len(msg.Content) != 1 || msg.Content[0].Text != text.String() || msg.StopReason != "end_turn" ||
		msg.Usage.InputTokens != rec.PromptTokens || msg.Usage.OutputTokens != 6 {
		t.Errorf("non-stream message = %+v (stream text %q)", msg, text.String())
	}
}

func TestAnthropicMaxTokens(t *testing.T) {
	_, ts := newServer(t, Behavior{OutputTokens: 50})
	resp := postAnthropic(t, ts.URL+"/v1/messages", anthropicBody(true, 3), nil)
	events, _ := readAnthropicEvents(t, resp.Body)
	resp.Body.Close()
	md := events[len(events)-2].data
	if md["delta"].(map[string]any)["stop_reason"] != "max_tokens" || md["usage"].(map[string]any)["output_tokens"] != float64(3) {
		t.Errorf("message_delta = %v", md)
	}
}

func TestAnthropicRejectsWhatTheAPIRejects(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 1})
	url := ts.URL + "/v1/messages"
	withTemp := bytes.Replace(anthropicBody(false, 10), []byte(`"max_tokens"`), []byte(`"temperature":0.5,"max_tokens"`), 1)
	tests := map[string]struct {
		body []byte
		hdr  map[string]string
		want string
	}{
		"no version header": {anthropicBody(false, 10), map[string]string{"anthropic-version": ""}, "anthropic-version"},
		"temperature":       {withTemp, nil, "temperature"},
		"no max_tokens":     {anthropicBody(false, 0), nil, "max_tokens"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			resp := postAnthropic(t, url, tt.body, tt.hdr)
			defer resp.Body.Close()
			var e struct {
				Type  string
				Error struct{ Type, Message string }
			}
			_ = json.NewDecoder(resp.Body).Decode(&e)
			if resp.StatusCode != http.StatusBadRequest || e.Type != "error" || e.Error.Type != "invalid_request_error" ||
				!strings.Contains(e.Error.Message, tt.want) {
				t.Errorf("status %d body %+v", resp.StatusCode, e)
			}
		})
	}
	if n := len(s.AllRecords()); n != 0 {
		t.Errorf("rejected requests were recorded: %d", n)
	}
}

func TestAnthropicErrors(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 1, ErrorRate: 1, ErrorStatus: 529})
	resp := postAnthropic(t, ts.URL+"/v1/messages", anthropicBody(true, 10), map[string]string{RequestIDHeader: "ov"})
	defer resp.Body.Close()
	var e struct{ Error struct{ Type string } }
	_ = json.NewDecoder(resp.Body).Decode(&e)
	if resp.StatusCode != 529 || e.Error.Type != "overloaded_error" {
		t.Errorf("status %d type %q", resp.StatusCode, e.Error.Type)
	}
	if r := onlyRecord(t, s, "ov"); r.Outcome != OutcomeInjectedError || r.Billable {
		t.Errorf("record = %+v", r)
	}
}

func TestAnthropicMidStreamErrorEvent(t *testing.T) {
	s, ts := newServer(t, Behavior{OutputTokens: 10, FailAfterTokens: 2, FailMode: FailErrorEvent})
	resp := postAnthropic(t, ts.URL+"/v1/messages", anthropicBody(true, 100), map[string]string{RequestIDHeader: "mid"})
	events, err := readAnthropicEvents(t, resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.name != "error" || last.data["error"].(map[string]any)["type"] != "overloaded_error" {
		t.Errorf("last event = %+v", last)
	}
	if r := waitRecord(t, s, "mid"); r.Outcome != OutcomeMidStreamFailure || r.CompletionTokens != 2 {
		t.Errorf("record = %+v", r)
	}
}

func TestAnthropicOmitUsage(t *testing.T) {
	_, ts := newServer(t, Behavior{OutputTokens: 2, OmitUsage: true})
	resp := postAnthropic(t, ts.URL+"/v1/messages", anthropicBody(true, 10), nil)
	events, _ := readAnthropicEvents(t, resp.Body)
	resp.Body.Close()
	for _, e := range events {
		if _, ok := e.data["usage"]; ok {
			t.Errorf("%s carries usage", e.name)
		}
		if m, ok := e.data["message"].(map[string]any); ok && m["usage"] != nil {
			t.Errorf("message_start carries usage")
		}
	}
}
