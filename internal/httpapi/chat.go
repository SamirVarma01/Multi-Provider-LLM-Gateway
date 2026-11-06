package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/SamirVarma01/llm-gateway/internal/auth"
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
	status     int
	typ        string
	code       string
	msg        string
	retryAfter int // seconds; 0 sends no Retry-After
}

func badRequest(code, format string, args ...any) *apiError {
	return &apiError{status: http.StatusBadRequest, typ: oai.ErrTypeInvalidRequest, code: code, msg: fmt.Sprintf(format, args...)}
}

func (e *apiError) write(w http.ResponseWriter, r *http.Request) {
	if e.retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(e.retryAfter))
	}
	oai.WriteError(w, r, e.status, e.typ, e.code, e.msg)
}

// Request results recorded in an outcome.
const (
	resultCompleted          = "completed"
	resultUpstreamError      = "upstream_error"
	resultClientDisconnected = "client_disconnected"
)

// outcome is how one chat request ended, and what it is charged from.
type outcome struct {
	Provider      string
	UpstreamModel string
	Policy        string
	Attempts      int // upstream attempts made, including failed ones
	Stream        bool
	Result        string
	Committed     bool // the first byte of the response reached the client
	Billable      bool // design §10: charge Usage for this request
	Usage         provider.Usage
	Price         provider.Price // of the target that served the request
	CostNUSD      int64
	FinishReason  string
	TTFT          time.Duration // the serving attempt's time to its first output chunk
	Upstream      time.Duration // from the first upstream call to the last byte read
	Err           error
}

// call is one routed chat request.
type call struct {
	alias   string
	id      string // response id: chatcmpl-<request id>
	created int64
	plan    *router.Plan
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
		(&apiError{status: http.StatusNotFound, typ: oai.ErrTypeNotFound, code: "model_not_found",
			msg: fmt.Sprintf("The model %q does not exist.", body.Model)}).write(w, r)
		return
	}
	estIn := s.opts.Estimator.Reserve(req)
	plan, err := s.opts.Router.Plan(model, router.Request{
		Chat:      req,
		MaxTokens: body.OutputLimit(),
		Policy:    r.Header.Get(headerRoutePolicy),
		EstInput:  estIn,
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

	key, _ := auth.FromContext(ctx)
	adm, aerr := s.admit(ctx, key, plan, estIn)
	if aerr != nil {
		aerr.write(w, r)
		return
	}

	c := &call{
		alias:   body.Model,
		id:      "chatcmpl-" + obs.RequestID(ctx),
		created: s.now().Unix(),
		plan:    plan,
	}
	var out outcome
	if req.Stream {
		out = s.serveStream(ctx, cancel, w, r, c)
	} else {
		out = s.serveComplete(ctx, w, r, c)
	}
	s.conclude(ctx, adm, out)
}

// serveComplete serves a non-streaming request. The response is written only
// after an attempt's whole upstream stream has been read, so every upstream
// failure is pre-commit: it can fail over, and nothing is charged for it.
func (s *Server) serveComplete(ctx context.Context, w http.ResponseWriter, r *http.Request, c *call) outcome {
	out := outcome{Policy: c.plan.Policy}
	start := s.now()
	var resp provider.Response
	a, err := s.opts.Router.Execute(ctx, c.plan, func(a *router.Attempt) error {
		var err error
		resp, err = provider.Collect(a.Stream)
		return err
	})
	out.Upstream = s.now().Sub(start)
	if err != nil {
		return s.failPreCommit(w, r, out, err)
	}
	defer a.Abandoned() // no-op once the attempt has ended
	out.served(a)

	out.Usage = s.opts.Estimator.Resolve(a.Choice.Req, resp)
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
	setRouteHeaders(w, out.Provider, out.Attempts)
	writeJSON(w, http.StatusOK, oai.ChatCompletionResponse{
		ID:      c.id,
		Object:  "chat.completion",
		Created: c.created,
		Model:   c.alias,
		Choices: []oai.Choice{{Message: msg, FinishReason: resp.FinishReason}},
		Usage:   wireUsage(out.Usage),
	})
	a.Succeeded(out.Usage.CompletionTokens)
	out.Result, out.Committed, out.Billable = resultCompleted, true, true
	return out
}

// served fills in the attempt that committed.
func (o *outcome) served(a *router.Attempt) {
	o.Provider = a.Choice.Target.Provider
	o.UpstreamModel = a.Choice.Target.UpstreamModel
	o.Attempts = a.N
	o.TTFT = a.TTFT()
	o.Price = a.Choice.Target.Price
}

func setRouteHeaders(w http.ResponseWriter, provider string, attempts int) {
	if provider != "" {
		w.Header().Set(headerProvider, provider)
	}
	w.Header().Set(headerAttempts, strconv.Itoa(attempts))
}

// failPreCommit answers a request where no attempt committed: nothing was
// sent to the client, and nothing is billable (design §10).
func (s *Server) failPreCommit(w http.ResponseWriter, r *http.Request, out outcome, err error) outcome {
	out.Err = err
	var f *router.Failure
	if errors.As(err, &f) {
		out.Attempts = f.Attempts
	}
	var ue *provider.UpstreamError
	if errors.As(err, &ue) {
		out.Provider = ue.Provider
	}
	if clientGone(r) {
		out.Result = resultClientDisconnected
		return out
	}
	out.Result = resultUpstreamError
	s.writeUpstreamError(w, r, out, err)
	return out
}

// finish logs a request once it is fully accounted for. Metrics attach here
// in a later phase.
func (s *Server) finish(ctx context.Context, out outcome) {
	if s.onOutcome != nil {
		s.onOutcome(out)
	}
	attrs := []any{
		"provider", out.Provider,
		"upstream_model", out.UpstreamModel,
		"policy", out.Policy,
		"attempts", out.Attempts,
		"stream", out.Stream,
		"result", out.Result,
		"committed", out.Committed,
		"billable", out.Billable,
		"prompt_tokens", out.Usage.PromptTokens,
		"completion_tokens", out.Usage.CompletionTokens,
		"usage_estimated", out.Usage.Estimated,
		"cost_nusd", out.CostNUSD,
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
			return nil, &apiError{status: http.StatusRequestEntityTooLarge, typ: oai.ErrTypeInvalidRequest, code: "request_too_large",
				msg: fmt.Sprintf("Request body exceeds %d bytes.", tooBig.Limit)}
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

// writeUpstreamError answers a request where no attempt committed
// (design §6.4): the client's bad request is passed through as 400,
// timeouts are 504, every breaker open is 503, and other failures are 502.
func (s *Server) writeUpstreamError(w http.ResponseWriter, r *http.Request, out outcome, err error) {
	log := obs.Logger(r.Context())
	setRouteHeaders(w, out.Provider, out.Attempts)
	var f *router.Failure
	if errors.Is(err, router.ErrNoHealthyTarget) && errors.As(err, &f) {
		log.Warn("no healthy upstream", "retry_after", f.RetryAfter)
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(f.RetryAfter.Seconds())))))
		oai.WriteError(w, r, http.StatusServiceUnavailable, oai.ErrTypeUpstream, "no_healthy_upstream",
			"Every provider for this model is failing; try again later.")
		return
	}
	var ue *provider.UpstreamError
	switch {
	case errors.As(err, &ue) && ue.Kind == provider.KindBadRequest:
		oai.WriteError(w, r, http.StatusBadRequest, oai.ErrTypeInvalidRequest, "upstream_rejected",
			"The provider rejected the request: "+ue.Msg)
	case errors.As(err, &ue) && ue.Kind == provider.KindTimeout, errors.Is(err, context.DeadlineExceeded):
		oai.WriteError(w, r, http.StatusGatewayTimeout, oai.ErrTypeUpstream, "upstream_timeout",
			"The provider did not respond in time.")
	case errors.As(err, &ue):
		oai.WriteError(w, r, http.StatusBadGateway, oai.ErrTypeUpstream, "upstream_error",
			"The provider failed to complete the request.")
	default:
		log.Error("upstream call failed", "err", err)
		oai.WriteError(w, r, http.StatusInternalServerError, oai.ErrTypeAPI, "internal_error", "Internal error.")
	}
}
