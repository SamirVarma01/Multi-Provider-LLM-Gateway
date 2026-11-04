package provider

import (
	"errors"
	"io"
	"strings"
)

// Response is a complete assistant response assembled from a stream.
type Response struct {
	Content      string
	ToolCalls    []ToolCall
	FinishReason string
	Reported     Reported
}

// Accumulator assembles streamed chunks into a Response. Tool call deltas
// are merged by index.
type Accumulator struct {
	content   strings.Builder
	calls     []*callBuilder
	byIndex   map[int]*callBuilder
	finish    string
	sawOutput bool
}

type callBuilder struct {
	id, name string
	args     strings.Builder
}

// Add merges one chunk.
func (a *Accumulator) Add(c *Chunk) {
	if c.HasOutput() {
		a.sawOutput = true
	}
	a.content.WriteString(c.Content)
	for _, d := range c.ToolCalls {
		if a.byIndex == nil {
			a.byIndex = make(map[int]*callBuilder)
		}
		b, ok := a.byIndex[d.Index]
		if !ok {
			b = &callBuilder{}
			a.byIndex[d.Index] = b
			a.calls = append(a.calls, b)
		}
		if d.ID != "" {
			b.id = d.ID
		}
		if d.Name != "" {
			b.name = d.Name
		}
		b.args.WriteString(d.ArgumentsFragment)
	}
	if c.FinishReason != "" {
		a.finish = c.FinishReason
	}
}

// SawOutput reports whether any chunk carried content, a tool call, or a
// finish reason.
func (a *Accumulator) SawOutput() bool { return a.sawOutput }

// Response returns everything accumulated so far, with the given usage.
func (a *Accumulator) Response(rep Reported) Response {
	r := Response{Content: a.content.String(), FinishReason: a.finish, Reported: rep}
	for _, b := range a.calls {
		r.ToolCalls = append(r.ToolCalls, ToolCall{ID: b.id, Name: b.name, Arguments: b.args.String()})
	}
	return r
}

// Collect reads s to the end and assembles the response. On error it returns
// what was received so far along with the error, so the caller can still
// account for partial output.
func Collect(s Stream) (Response, error) {
	var a Accumulator
	for {
		c, err := s.Recv()
		if err != nil {
			r := a.Response(s.Usage())
			if errors.Is(err, io.EOF) {
				return r, nil
			}
			return r, err
		}
		a.Add(c)
	}
}
