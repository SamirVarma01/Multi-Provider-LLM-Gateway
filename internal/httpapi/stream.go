package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/sse"
)

// serveStream serves a streaming request (design §6.4–6.5).
//
// Nothing is written until the first upstream chunk that carries output
// (content, a tool call, or a finish reason). That moment is the commit
// point: before it the request can still fail with an ordinary HTTP error
// (and, in Phase 3, fail over); after it the response is a 200 event stream,
// and an upstream failure ends it with an error event and no [DONE].
//
// Each upstream chunk is written and flushed as soon as it arrives. If the
// client disconnects, the request context is cancelled, which cancels the
// upstream call; a failed write cancels it explicitly in case the
// disconnect has not been noticed yet.
func (s *Server) serveStream(ctx context.Context, cancel context.CancelFunc,
	w http.ResponseWriter, r *http.Request, c *call) outcome {
	out := outcome{Provider: c.cand.Target.Provider, UpstreamModel: c.cand.Target.UpstreamModel, Stream: true}
	start := s.now()
	st, err := c.cand.Provider.Stream(provider.WithAttempt(ctx, 1), c.cand.Target, c.req)
	if err != nil {
		return s.failPreCommit(w, r, out, err)
	}
	defer st.Close()

	// Pre-commit: hold back role-only chunks until real output arrives.
	var first *provider.Chunk
	for first == nil {
		ch, err := st.Recv()
		if errors.Is(err, io.EOF) {
			err = &provider.UpstreamError{Provider: out.Provider, Kind: provider.KindProtocol, Retryable: true,
				Msg: "stream ended without output"}
		}
		if err != nil {
			out.Upstream = s.now().Sub(start)
			return s.failPreCommit(w, r, out, err)
		}
		if ch.HasOutput() {
			first = ch
		}
	}
	out.TTFT = s.now().Sub(start)

	// Commit.
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // ask reverse proxies not to buffer
	h.Set(headerProvider, out.Provider)
	h.Set(headerAttempts, "1")
	w.WriteHeader(http.StatusOK)
	out.Committed, out.Billable = true, true
	enc := &chunkEncoder{sw: sse.NewWriter(w), id: c.id, created: c.created, model: c.alias}

	var acc provider.Accumulator
	acc.Add(first)
	writeErr := enc.role()
	if writeErr == nil {
		writeErr = enc.chunk(first)
	}
	var upErr error
	for writeErr == nil {
		ch, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			upErr = err
			break
		}
		if !ch.HasOutput() {
			continue
		}
		acc.Add(ch)
		writeErr = enc.chunk(ch)
	}
	out.Upstream = s.now().Sub(start)

	resp := acc.Response(st.Usage())
	out.Usage = s.opts.Estimator.Resolve(c.req, resp)
	out.FinishReason = resp.FinishReason
	switch {
	case writeErr != nil || clientGone(r):
		cancel() // stop the upstream now, even if the disconnect is not yet visible to ctx
		out.Result, out.Err = resultClientDisconnected, errors.Join(writeErr, upErr)
	case upErr != nil:
		out.Result, out.Err = resultUpstreamError, upErr
		obs.Logger(ctx).Warn("upstream failed mid-stream", "provider", out.Provider, "err", upErr)
		_ = enc.streamError(r, upErr)
	default:
		out.Result = resultCompleted
		if c.req.IncludeUsage {
			_ = enc.usage(out.Usage)
		}
		_ = enc.done()
	}
	return out
}

// chunkEncoder writes internal chunks as OpenAI chat.completion.chunk events.
type chunkEncoder struct {
	sw      *sse.Writer
	id      string
	created int64
	model   string
}

func (e *chunkEncoder) frame(d oai.Delta, finish *string) oai.ChatCompletionChunk {
	return oai.ChatCompletionChunk{
		ID: e.id, Object: "chat.completion.chunk", Created: e.created, Model: e.model,
		Choices: []oai.ChunkChoice{{Delta: d, FinishReason: finish}},
	}
}

// role writes the opening chunk OpenAI streams start with.
func (e *chunkEncoder) role() error {
	empty := ""
	return e.sw.JSON(e.frame(oai.Delta{Role: string(provider.RoleAssistant), Content: &empty}, nil))
}

func (e *chunkEncoder) chunk(c *provider.Chunk) error {
	var d oai.Delta
	if c.Content != "" {
		d.Content = &c.Content
	}
	for _, tc := range c.ToolCalls {
		td := oai.ToolCallDelta{Index: tc.Index, ID: tc.ID,
			Function: &oai.FunctionCallDelta{Name: tc.Name, Arguments: tc.ArgumentsFragment}}
		if tc.ID != "" {
			td.Type = "function"
		}
		d.ToolCalls = append(d.ToolCalls, td)
	}
	var finish *string
	if c.FinishReason != "" {
		finish = &c.FinishReason
	}
	return e.sw.JSON(e.frame(d, finish))
}

// usage writes the usage-only chunk sent when the client set
// stream_options.include_usage.
func (e *chunkEncoder) usage(u provider.Usage) error {
	f := e.frame(oai.Delta{}, nil)
	f.Choices = []oai.ChunkChoice{}
	f.Usage = wireUsage(u)
	return e.sw.JSON(f)
}

func (e *chunkEncoder) done() error { return e.sw.Data([]byte(oai.DoneData)) }

// streamError ends a committed stream after an upstream failure: one error
// event, then no [DONE] (design §6.5, D5), so clients that only watch for
// [DONE] can tell the stream was truncated.
func (e *chunkEncoder) streamError(r *http.Request, err error) error {
	code, msg := "stream_interrupted", "The provider failed mid-stream."
	var ue *provider.UpstreamError
	if errors.As(err, &ue) && ue.Kind == provider.KindTimeout {
		code, msg = "upstream_timeout", "The provider stopped responding mid-stream."
	}
	return e.sw.JSON(oai.ErrorBody{Error: oai.ErrorDetail{
		Message: msg, Type: oai.ErrTypeUpstream, Code: code, RequestID: obs.RequestID(r.Context()),
	}})
}
