package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/provider"
	"github.com/SamirVarma01/llm-gateway/internal/router"
)

// Headers. X-Route-Policy is a request header that overrides the alias's
// routing policy; the others describe how the gateway served a request.
const (
	headerRoutePolicy = "X-Route-Policy"
	headerProvider    = "X-Gateway-Provider"
	headerAttempts    = "X-Gateway-Attempts"
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

// Request results recorded in an outcome.
const (
	resultCompleted          = "completed"
	resultUpstreamError      = "upstream_error"
	resultClientDisconnected = "client_disconnected"
)

// outcome is how one chat request ended. It is what settlement (Phase 4)
// will charge from; today it is logged.
type outcome struct {
	Provider      string
	UpstreamModel string
	Policy        string
	Stream        bool
	Result        string
	Committed     bool // the first byte of the response reached the client
	Billable      bool // design §10: charge Usage for this request
	Usage         provider.Usage
	FinishReason  string
	TTFT          time.Duration // to the first upstream chunk with output
	Upstream      time.Duration // from the upstream call to its last byte
	Err           error
}

// call is one routed chat request.
type call struct {
	alias   string
	id      string // response id: chatcmpl-<request id>
	created int64
	plan    *router.Plan
	choice  router.Choice
}

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.opts.RequestTimeout)
	defer cancel()

	body, aerr := s.decodeChatRequest(w, r)
	if aerr != nil {
		aerr.write(w, r)
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
	plan, err := s.opts.Router.Plan(model, router.Request{
		Chat:      req,
		MaxTokens: body.OutputLimit(),
		Policy:    r.Header.Get(headerRoutePolicy),
		EstInput:  s.opts.Estimator.Input(req),
	})
	var unsupported *router.UnsupportedError
	switch {
	case errors.Is(err, router.ErrUnknownPolicy):
		badRequest("invalid_route_policy", "%s must be priority, cheapest, or fastest.", headerRoutePolicy).write(w, r)
		return
	case errors.As(err, &unsupported):
		badRequest("unsupported_request", "%v", err).write(w, r)
		return
	case err != nil:
		obs.Logger(ctx).Error("routing failed", "err", err)
		oai.WriteError(w, r, http.StatusInternalServerError, oai.ErrTypeAPI, "internal_error", "Internal error.")
		return
	}

	c := &call{
		alias:   body.Model,
		id:      "chatcmpl-" + obs.RequestID(ctx),
		created: s.now().Unix(),
		plan:    plan,
		choice:  plan.Choices[0], // one attempt until failover lands
	}
	var out outcome
	if req.Stream {
		out = s.serveStream(ctx, cancel, w, r, c)
	} else {
		out = s.serveComplete(ctx, w, r, c)
	}
	s.finish(r.Context(), out)
}

// serveComplete serves a non-streaming request. The response is written only
// after the whole upstream stream has been read, so every failure is
// pre-commit and nothing is charged for it.
func (s *Server) serveComplete(ctx context.Context, w http.ResponseWriter, r *http.Request, c *call) outcome {
	out := outcome{Provider: c.choice.Target.Provider, UpstreamModel: c.choice.Target.UpstreamModel, Policy: c.plan.Policy}
	start := s.now()
	st, err := c.choice.Provider.Stream(provider.WithAttempt(ctx, 1), c.choice.Target, c.choice.Req)
	if err != nil {
		return s.failPreCommit(w, r, out, err)
	}
	defer st.Close()
	resp, err := provider.Collect(st)
	out.Upstream = s.now().Sub(start)
	if err != nil {
		return s.failPreCommit(w, r, out, err)
	}

	out.Usage = s.opts.Estimator.Resolve(c.choice.Req, resp)
	out.FinishReason = resp.FinishReason
	msg := oai.ResponseMessage{Role: string(provider.RoleAssistant)}
	if resp.Content != "" || len(resp.ToolCalls) == 0 {
		msg.Content = &resp.Content
	}
	for _, tc := range resp.ToolCalls {
		msg.ToolCalls = append(msg.ToolCalls, oai.ToolCall{
			ID: tc.ID, Type: "function", Function: oai.FunctionCall{Name: tc.Name, Arguments: tc.Arguments},
		})
	}
	w.Header().Set(headerProvider, c.choice.Target.Provider)
	w.Header().Set(headerAttempts, "1")
	writeJSON(w, http.StatusOK, oai.ChatCompletionResponse{
		ID:      c.id,
		Object:  "chat.completion",
		Created: c.created,
		Model:   c.alias,
		Choices: []oai.Choice{{Message: msg, FinishReason: resp.FinishReason}},
		Usage:   wireUsage(out.Usage),
	})
	out.Result, out.Committed, out.Billable = resultCompleted, true, true
	return out
}

// failPreCommit answers a request that failed before any byte was sent.
// Nothing is billable (design §10).
func (s *Server) failPreCommit(w http.ResponseWriter, r *http.Request, out outcome, err error) outcome {
	out.Err = err
	if clientGone(r) {
		out.Result = resultClientDisconnected
		return out
	}
	out.Result = resultUpstreamError
	s.writeUpstreamError(w, r, err)
	return out
}

// finish logs the outcome. Settlement and metrics attach here in later phases.
func (s *Server) finish(ctx context.Context, out outcome) {
	if s.onOutcome != nil {
		s.onOutcome(out)
	}
	attrs := []any{
		"provider", out.Provider,
		"upstream_model", out.UpstreamModel,
		"policy", out.Policy,
		"stream", out.Stream,
		"result", out.Result,
		"committed", out.Committed,
		"billable", out.Billable,
		"prompt_tokens", out.Usage.PromptTokens,
		"completion_tokens", out.Usage.CompletionTokens,
		"usage_estimated", out.Usage.Estimated,
		"finish_reason", out.FinishReason,
		"ttft", out.TTFT,
		"upstream_duration", out.Upstream,
	}
	if out.Err != nil {
		attrs = append(attrs, "err", out.Err.Error())
	}
	obs.Logger(ctx).Info("chat completion", attrs...)
}

func wireUsage(u provider.Usage) *oai.Usage {
	return &oai.Usage{
		PromptTokens:     u.PromptTokens,
		CompletionTokens: u.CompletionTokens,
		TotalTokens:      u.PromptTokens + u.CompletionTokens,
		Estimated:        u.Estimated,
	}
}

// clientGone reports whether the client has disconnected.
func clientGone(r *http.Request) bool {
	return r.Context().Err() != nil
}

func (s *Server) decodeChatRequest(w http.ResponseWriter, r *http.Request) (*oai.ChatCompletionRequest, *apiError) {
	var body oai.ChatCompletionRequest
	limited := http.MaxBytesReader(w, r.Body, s.opts.MaxBodyBytes)
	err := json.NewDecoder(limited).Decode(&body)
	if err == nil {
		// Read to EOF: net/http only starts watching the connection for a
		// client disconnect once the body is fully consumed, and the decoder
		// stops after the first JSON value.
		_, err = io.Copy(io.Discard, limited)
	}
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return nil, &apiError{http.StatusRequestEntityTooLarge, oai.ErrTypeInvalidRequest, "request_too_large",
				fmt.Sprintf("Request body exceeds %d bytes.", tooBig.Limit)}
		}
		return nil, badRequest("invalid_json", "Invalid JSON body: %v", err)
	}
	return &body, nil
}

// rejectedFields change the response shape in ways the gateway does not
// re-encode, so forwarding them would silently lose data.
var rejectedFields = []string{"audio", "function_call", "functions", "logprobs", "modalities", "top_logprobs"}

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
	for _, f := range rejectedFields {
		if _, ok := b.Extra[f]; ok {
			return nil, badRequest("unsupported_parameter", "'%s' is not supported by this gateway.", f)
		}
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
		Model:             b.Model,
		Messages:          make([]provider.Message, len(b.Messages)),
		ParallelToolCalls: b.ParallelToolCalls,
		Temperature:       b.Temperature,
		TopP:              b.TopP,
		Stop:              b.Stop,
		Stream:            b.Stream,
		User:              b.User,
		Extra:             b.Extra,
	}
	if b.StreamOptions != nil {
		req.IncludeUsage = b.StreamOptions.IncludeUsage
	}
	for i, m := range b.Messages {
		msg, aerr := messageToInternal(i, m)
		if aerr != nil {
			return nil, aerr
		}
		req.Messages[i] = msg
	}

	declared := map[string]bool{}
	for i, t := range b.Tools {
		if t.Type != "function" || t.Function.Name == "" {
			return nil, badRequest("invalid_tools", "tools[%d]: must be {\"type\":\"function\",\"function\":{\"name\":...}}.", i)
		}
		if p := t.Function.Parameters; len(p) > 0 && !isJSONObject(p) {
			return nil, badRequest("invalid_tools", "tools[%d].function.parameters must be a JSON Schema object.", i)
		}
		declared[t.Function.Name] = true
		req.Tools = append(req.Tools, provider.Tool{
			Name: t.Function.Name, Description: t.Function.Description,
			Parameters: t.Function.Parameters, Strict: t.Function.Strict,
		})
	}
	if tc := b.ToolChoice; tc != nil {
		switch {
		case tc.Mode == oai.ToolChoiceRequired && len(req.Tools) == 0:
			return nil, badRequest("invalid_tool_choice", "tool_choice \"required\" needs at least one tool.")
		case tc.Mode == oai.ToolChoiceFunction && !declared[tc.Function]:
			return nil, badRequest("invalid_tool_choice", "tool_choice names %q, which is not in tools.", tc.Function)
		}
		req.ToolChoice = &provider.ToolChoice{Mode: tc.Mode, Name: tc.Function}
	}
	return req, nil
}

func messageToInternal(i int, m oai.Message) (provider.Message, *apiError) {
	role := provider.Role(m.Role)
	if m.Role == "developer" { // OpenAI's newer name for system instructions
		role = provider.RoleSystem
	}
	msg := provider.Message{Role: role}
	switch role {
	case provider.RoleSystem, provider.RoleUser:
	case provider.RoleAssistant:
		for j, tc := range m.ToolCalls {
			if tc.ID == "" || tc.Function.Name == "" || (tc.Type != "" && tc.Type != "function") {
				return msg, badRequest("invalid_tool_calls",
					"messages[%d].tool_calls[%d]: id and function.name are required.", i, j)
			}
			msg.ToolCalls = append(msg.ToolCalls, provider.ToolCall{
				ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments,
			})
		}
	case provider.RoleTool:
		if m.ToolCallID == "" {
			return msg, badRequest("missing_tool_call_id", "messages[%d]: tool messages need tool_call_id.", i)
		}
		msg.ToolCallID = m.ToolCallID
	default:
		return msg, badRequest("unsupported_role", "messages[%d]: role %q is not supported.", i, m.Role)
	}
	if len(m.ToolCalls) > 0 && role != provider.RoleAssistant {
		return msg, badRequest("invalid_tool_calls", "messages[%d]: only assistant messages carry tool_calls.", i)
	}

	if m.Content.Parts == nil {
		switch {
		case m.Content.Null && role == provider.RoleAssistant && len(msg.ToolCalls) > 0:
			// A message that only calls tools has no content.
		case m.Content.Null:
			return msg, badRequest("missing_content",
				"messages[%d]: content may be null only for assistant messages with tool_calls.", i)
		default:
			msg.Content = []provider.ContentPart{{Type: provider.PartText, Text: m.Content.Text}}
		}
		return msg, nil
	}
	for j, p := range m.Content.Parts {
		switch {
		case p.Type == "text":
			msg.Content = append(msg.Content, provider.ContentPart{Type: provider.PartText, Text: p.Text})
		case p.Type == "image_url" && p.ImageURL != nil && p.ImageURL.URL != "":
			if role != provider.RoleUser {
				return msg, badRequest("unsupported_content", "messages[%d].content[%d]: images are allowed only in user messages.", i, j)
			}
			msg.Content = append(msg.Content, provider.ContentPart{
				Type: provider.PartImage, ImageURL: p.ImageURL.URL, ImageDetail: p.ImageURL.Detail,
			})
		case p.Type == "image_url":
			return msg, badRequest("invalid_content", "messages[%d].content[%d]: image_url.url is required.", i, j)
		default:
			return msg, badRequest("unsupported_content",
				"messages[%d].content[%d]: content type %q is not supported.", i, j, p.Type)
		}
	}
	return msg, nil
}

func isJSONObject(raw json.RawMessage) bool {
	var obj map[string]json.RawMessage
	return json.Unmarshal(raw, &obj) == nil
}

// writeUpstreamError maps an upstream failure to a client response.
func (s *Server) writeUpstreamError(w http.ResponseWriter, r *http.Request, err error) {
	log := obs.Logger(r.Context())
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
