package mock

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/sse"
)

// openAIFormat speaks the Chat Completions API.
type openAIFormat struct{}

func (openAIFormat) parse(r *http.Request) (request, error) {
	var req oai.ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return request{}, errors.New("invalid JSON body: " + err.Error())
	}
	if len(req.Messages) == 0 {
		return request{}, errors.New("messages must not be empty")
	}
	return request{
		model:        req.Model,
		stream:       req.Stream,
		includeUsage: req.StreamOptions != nil && req.StreamOptions.IncludeUsage,
		outputLimit:  req.OutputLimit(),
		promptTokens: CountPromptTokens(req.Messages),
	}, nil
}

func (openAIFormat) writeError(w http.ResponseWriter, status int, msg string) {
	writeError(w, status, openAIErrorType(status), msg)
}

func openAIErrorType(status int) string {
	switch {
	case status == http.StatusTooManyRequests:
		return "rate_limit_error"
	case status >= 500:
		return "server_error"
	default:
		return "invalid_request_error"
	}
}

func openAIFinish(truncated bool) string {
	if truncated {
		return "length"
	}
	return "stop"
}

func (openAIFormat) writeCompletion(w http.ResponseWriter, c completion) {
	resp := oai.ChatCompletionResponse{
		ID:      "chatcmpl-" + c.id,
		Object:  "chat.completion",
		Created: c.created,
		Model:   c.model,
		Choices: []oai.Choice{{
			Message:      oai.ResponseMessage{Role: "assistant", Content: &c.text},
			FinishReason: openAIFinish(c.truncated),
		}},
	}
	if c.withUsage {
		resp.Usage = usage(c.promptTokens, c.outputTokens)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (openAIFormat) streamer(sw *sse.Writer, c completion, includeUsage bool) streamer {
	return &openAIStreamer{sw: sw, c: c, includeUsage: includeUsage}
}

type openAIStreamer struct {
	sw           *sse.Writer
	c            completion
	includeUsage bool
}

func (s *openAIStreamer) chunk(d oai.Delta, finish *string) oai.ChatCompletionChunk {
	return oai.ChatCompletionChunk{
		ID: "chatcmpl-" + s.c.id, Object: "chat.completion.chunk", Created: s.c.created, Model: s.c.model,
		Choices: []oai.ChunkChoice{{Delta: d, FinishReason: finish}},
	}
}

func (s *openAIStreamer) start() error {
	empty := ""
	return s.sw.JSON(s.chunk(oai.Delta{Role: "assistant", Content: &empty}, nil))
}

func (s *openAIStreamer) token(_ int, text string) error {
	return s.sw.JSON(s.chunk(oai.Delta{Content: &text}, nil))
}

func (s *openAIStreamer) finish(outputTokens int) error {
	reason := openAIFinish(s.c.truncated)
	if err := s.sw.JSON(s.chunk(oai.Delta{}, &reason)); err != nil {
		return err
	}
	if !s.includeUsage || !s.c.withUsage {
		return nil
	}
	u := s.chunk(oai.Delta{}, nil)
	u.Choices = []oai.ChunkChoice{}
	u.Usage = usage(s.c.promptTokens, outputTokens)
	return s.sw.JSON(u)
}

func (s *openAIStreamer) done() error { return s.sw.Data([]byte(oai.DoneData)) }

func (s *openAIStreamer) midStreamError() error {
	return s.sw.JSON(oai.ErrorBody{Error: oai.ErrorDetail{
		Message: "mock: injected mid-stream failure", Type: "server_error",
	}})
}

// CountPromptTokens is the mock's tokenizer: whitespace-separated words plus a
// fixed per-message overhead. It deliberately differs from the gateway's
// byte-based estimator, so tests can tell reported usage from estimates.
func CountPromptTokens(msgs []oai.Message) int {
	n := 3
	for _, m := range msgs {
		n += 3 + len(strings.Fields(m.Content.String()))
	}
	return n
}

func usage(prompt, completion int) *oai.Usage {
	return &oai.Usage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: prompt + completion}
}
