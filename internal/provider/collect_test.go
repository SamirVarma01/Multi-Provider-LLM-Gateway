package provider

import (
	"errors"
	"io"
	"reflect"
	"testing"
)

// sliceStream replays chunks, then ends with err (io.EOF for a clean end).
type sliceStream struct {
	chunks []*Chunk
	err    error
	rep    Reported
}

func (s *sliceStream) Recv() (*Chunk, error) {
	if len(s.chunks) == 0 {
		return nil, s.err
	}
	c := s.chunks[0]
	s.chunks = s.chunks[1:]
	return c, nil
}
func (s *sliceStream) Usage() Reported { return s.rep }
func (s *sliceStream) Close() error    { return nil }

func TestCollectMergesToolCalls(t *testing.T) {
	s := &sliceStream{
		chunks: []*Chunk{
			{Role: "assistant"},
			{Content: "Checking "},
			{Content: "both."},
			{ToolCalls: []ToolCallDelta{{Index: 0, ID: "call_a", Name: "weather"}}},
			{ToolCalls: []ToolCallDelta{{Index: 0, ArgumentsFragment: `{"city":`}}},
			{ToolCalls: []ToolCallDelta{{Index: 1, ID: "call_b", Name: "time", ArgumentsFragment: `{}`}}},
			{ToolCalls: []ToolCallDelta{{Index: 0, ArgumentsFragment: `"Paris"}`}}},
			{FinishReason: FinishToolCalls},
		},
		err: io.EOF,
		rep: Reported{PromptTokens: 10, HasPrompt: true},
	}
	r, err := Collect(s)
	if err != nil {
		t.Fatal(err)
	}
	want := Response{
		Content: "Checking both.",
		ToolCalls: []ToolCall{
			{ID: "call_a", Name: "weather", Arguments: `{"city":"Paris"}`},
			{ID: "call_b", Name: "time", Arguments: `{}`},
		},
		FinishReason: FinishToolCalls,
		Reported:     Reported{PromptTokens: 10, HasPrompt: true},
	}
	if !reflect.DeepEqual(r, want) {
		t.Errorf("Collect =\n%+v\nwant\n%+v", r, want)
	}
}

func TestCollectReturnsPartialOnError(t *testing.T) {
	boom := errors.New("boom")
	r, err := Collect(&sliceStream{chunks: []*Chunk{{Content: "par"}, {Content: "tial"}}, err: boom})
	if !errors.Is(err, boom) || r.Content != "partial" {
		t.Errorf("Collect = %+v, %v", r, err)
	}
}

func TestAccumulatorSawOutput(t *testing.T) {
	var a Accumulator
	a.Add(&Chunk{Role: "assistant"})
	if a.SawOutput() {
		t.Error("a role-only chunk is not output")
	}
	a.Add(&Chunk{Content: "x"})
	if !a.SawOutput() {
		t.Error("content is output")
	}
}

func TestClassifyStatus(t *testing.T) {
	tests := []struct {
		status    int
		kind      ErrKind
		retryable bool
	}{
		{400, KindBadRequest, false},
		{413, KindBadRequest, false},
		{422, KindBadRequest, false},
		{401, KindConfig, true},
		{402, KindConfig, true},
		{403, KindConfig, true},
		{404, KindConfig, true},
		{429, KindRateLimited, true},
		{500, KindServer, true},
		{529, KindServer, true},
	}
	for _, tt := range tests {
		kind, retryable := ClassifyStatus(tt.status)
		if kind != tt.kind || retryable != tt.retryable {
			t.Errorf("ClassifyStatus(%d) = %s, %v; want %s, %v", tt.status, kind, retryable, tt.kind, tt.retryable)
		}
	}
}
