package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

// Response headers describing how the gateway served a request.
const (
	headerProvider = "X-Gateway-Provider"
	headerAttempts = "X-Gateway-Attempts"
)

// apiError is a client-facing error.
type apiError struct {
	status int
	typ    string
	code   string
	msg    string
}

func badRequest(code, format string, args ...any) *apiError {
	return &apiError{http.StatusBadRequest, oai.ErrTypeInvalidRequest, code, fmt.Sprintf(format, args...)}
}

func (e *apiError) write(w http.ResponseWriter, r *http.Request) {
	oai.WriteError(w, r, e.status, e.typ, e.code, e.msg)
}

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.opts.RequestTimeout)
	defer cancel()
	log := obs.Logger(ctx)

	body, aerr := s.decodeChatRequest(w, r)
	if aerr != nil {
		aerr.write(w, r)
		return
	}
	if body.Stream {
		badRequest("unsupported_parameter", "Streaming is not supported yet.").write(w, r)
		return
	}
	req, aerr := toInternal(body)
	if aerr != nil {
		aerr.write(w, r)
		return
	}

	model, err := s.opts.Router.Resolve(body.Model)
	if err != nil {
		(&apiError{http.StatusNotFound, oai.ErrTypeNotFound, "model_not_found",
			fmt.Sprintf("The model %q does not exist.", body.Model)}).write(w, r)
		return
	}
	// Phase 1: the first candidate in priority order, one attempt.
	cand := model.Candidates[0]
	maxOut := model.MaxOutput(body.OutputLimit(), cand)
	req.MaxTokens = &maxOut
	if err := cand.Provider.Supports(req); err != nil {
		badRequest("unsupported_request", "%v", err).write(w, r)
		return
	}

	start := s.now()
	stream, err := cand.Provider.Stream(provider.WithAttempt(ctx, 1), cand.Target, req)
	if err != nil {
		s.writeUpstreamError(w, r, err)
		return
	}
	defer stream.Close()
	resp, err := provider.Collect(stream)
	upstreamDur := s.now().Sub(start)
	if err != nil {
		s.writeUpstreamError(w, r, err)
		return
	}

	out := oai.ChatCompletionResponse{
		ID:      "chatcmpl-" + obs.RequestID(ctx),
		Object:  "chat.completion",
		Created: start.Unix(),
		Model:   body.Model,
		Choices: []oai.Choice{{
			Message:      oai.ResponseMessage{Role: string(provider.RoleAssistant), Content: &resp.Content},
			FinishReason: resp.FinishReason,
		}},
	}
	// Phase 2 estimates usage when the provider omits it; until then it is left out.
	rep := resp.Reported
	if rep.HasPrompt && rep.HasCompletion {
		out.Usage = &oai.Usage{
			PromptTokens:     rep.PromptTokens,
			CompletionTokens: rep.CompletionTokens,
			TotalTokens:      rep.PromptTokens + rep.CompletionTokens,
		}
	}
	w.Header().Set(headerProvider, cand.Target.Provider)
	w.Header().Set(headerAttempts, "1")
	writeJSON(w, http.StatusOK, out)

	log.Info("chat completion",
		"model", body.Model,
		"provider", cand.Target.Provider,
		"upstream_model", cand.Target.UpstreamModel,
		"prompt_tokens", rep.PromptTokens,
		"completion_tokens", rep.CompletionTokens,
		"usage_reported", rep.HasPrompt && rep.HasCompletion,
		"finish_reason", resp.FinishReason,
		"upstream_duration", upstreamDur,
	)
}

func (s *Server) decodeChatRequest(w http.ResponseWriter, r *http.Request) (*oai.ChatCompletionRequest, *apiError) {
	var body oai.ChatCompletionRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.opts.MaxBodyBytes))
	if err := dec.Decode(&body); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return nil, &apiError{http.StatusRequestEntityTooLarge, oai.ErrTypeInvalidRequest, "request_too_large",
				fmt.Sprintf("Request body exceeds %d bytes.", tooBig.Limit)}
		}
		return nil, badRequest("invalid_json", "Invalid JSON body: %v", err)
	}
	return &body, nil
}

// toInternal validates a client request and converts it to the internal format.
func toInternal(b *oai.ChatCompletionRequest) (*provider.ChatRequest, *apiError) {
	if b.Model == "" {
		return nil, badRequest("missing_model", "'model' is required.")
	}
	if len(b.Messages) == 0 {
		return nil, badRequest("missing_messages", "'messages' must contain at least one message.")
	}
	if b.N != nil && *b.N != 1 {
		return nil, badRequest("unsupported_parameter", "Only n=1 is supported.")
	}
	if lim := b.OutputLimit(); lim != nil && *lim < 1 {
		return nil, badRequest("invalid_max_tokens", "max_tokens must be at least 1.")
	}
	if b.Temperature != nil && (*b.Temperature < 0 || *b.Temperature > 2) {
		return nil, badRequest("invalid_temperature", "temperature must be between 0 and 2.")
	}
	if b.TopP != nil && (*b.TopP < 0 || *b.TopP > 1) {
		return nil, badRequest("invalid_top_p", "top_p must be between 0 and 1.")
	}

	req := &provider.ChatRequest{
		Model:       b.Model,
		Messages:    make([]provider.Message, len(b.Messages)),
		Temperature: b.Temperature,
		TopP:        b.TopP,
		Stop:        b.Stop,
		Stream:      b.Stream,
		User:        b.User,
	}
	if b.StreamOptions != nil {
		req.IncludeUsage = b.StreamOptions.IncludeUsage
	}
	for i, m := range b.Messages {
		role := provider.Role(m.Role)
		switch role {
		case provider.RoleSystem, provider.RoleUser, provider.RoleAssistant:
		default:
			return nil, badRequest("unsupported_role", "messages[%d]: role %q is not supported.", i, m.Role)
		}
		msg := provider.Message{Role: role}
		if m.Content.Parts == nil {
			msg.Content = []provider.ContentPart{{Type: "text", Text: m.Content.Text}}
		} else {
			for j, p := range m.Content.Parts {
				if p.Type != "text" {
					return nil, badRequest("unsupported_content",
						"messages[%d].content[%d]: content type %q is not supported.", i, j, p.Type)
				}
				msg.Content = append(msg.Content, provider.ContentPart{Type: "text", Text: p.Text})
			}
		}
		req.Messages[i] = msg
	}
	return req, nil
}

// writeUpstreamError maps an upstream failure to a client response.
func (s *Server) writeUpstreamError(w http.ResponseWriter, r *http.Request, err error) {
	log := obs.Logger(r.Context())
	if errors.Is(err, context.Canceled) && r.Context().Err() != nil {
		log.Info("client disconnected before response", "err", err)
		return
	}
	var ue *provider.UpstreamError
	if !errors.As(err, &ue) {
		log.Error("upstream call failed", "err", err)
		oai.WriteError(w, r, http.StatusInternalServerError, oai.ErrTypeAPI, "internal_error", "Internal error.")
		return
	}
	log.Warn("upstream error", "provider", ue.Provider, "kind", ue.Kind.String(), "status", ue.Status, "msg", ue.Msg)
	w.Header().Set(headerProvider, ue.Provider)
	w.Header().Set(headerAttempts, "1")
	switch ue.Kind {
	case provider.KindBadRequest:
		oai.WriteError(w, r, http.StatusBadRequest, oai.ErrTypeInvalidRequest, "upstream_rejected",
			"The provider rejected the request: "+ue.Msg)
	case provider.KindTimeout:
		oai.WriteError(w, r, http.StatusGatewayTimeout, oai.ErrTypeUpstream, "upstream_timeout",
			"The provider did not respond in time.")
	default:
		oai.WriteError(w, r, http.StatusBadGateway, oai.ErrTypeUpstream, "upstream_error",
			"The provider failed to complete the request.")
	}
}
