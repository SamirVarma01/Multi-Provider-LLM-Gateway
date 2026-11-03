// Package provider defines the gateway's internal chat format and the
// interface every upstream adapter implements (design §5).
//
// The internal format is OpenAI-shaped because the public API is OpenAI's.
// Adapters always call their upstream in streaming mode; non-streaming client
// responses are assembled with Collect.
package provider

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// Role is a chat message role.
type Role string

// Roles.
const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

// ContentPart is one piece of message content.
type ContentPart struct {
	Type string // "text"
	Text string
}

// Message is one chat message.
type Message struct {
	Role    Role
	Content []ContentPart
}

// Text concatenates the message's text parts.
func (m Message) Text() string {
	var sb strings.Builder
	for _, p := range m.Content {
		sb.WriteString(p.Text)
	}
	return sb.String()
}

// ChatRequest is a provider-neutral chat completion request.
type ChatRequest struct {
	Model        string // the public model alias the client asked for
	Messages     []Message
	MaxTokens    *int // effective output cap sent upstream
	Temperature  *float64
	TopP         *float64
	Stop         []string
	Stream       bool // client-facing mode; upstream always streams
	IncludeUsage bool // client set stream_options.include_usage
	User         string
}

// Price is a target's price in integer micro-USD per 1M tokens.
type Price struct {
	InputMicroPerM  int64
	OutputMicroPerM int64
}

// Target is one routable (provider, upstream model) pair.
type Target struct {
	Provider      string
	UpstreamModel string
	Price         Price
	MaxOutput     int
}

// Usage is token usage for one request.
type Usage struct {
	PromptTokens     int
	CompletionTokens int
	Estimated        bool
}

// Chunk is one streamed increment of the assistant's response.
type Chunk struct {
	Role         string
	Content      string
	FinishReason string // stop | length | tool_calls | content_filter
	Usage        *Usage // set on the chunk that carries provider-reported usage
}

// Provider is an upstream LLM API.
type Provider interface {
	Name() string
	// Supports reports whether this provider can honor req.
	Supports(req *ChatRequest) error
	// Stream starts a streaming call and returns once the upstream status is
	// known. Errors are *UpstreamError, or the context's error if the caller
	// cancelled.
	Stream(ctx context.Context, t Target, req *ChatRequest) (Stream, error)
}

// Stream is an in-progress upstream response.
type Stream interface {
	// Recv returns the next chunk; io.EOF at a clean end, *UpstreamError on
	// failure, or the context's error if the caller cancelled.
	Recv() (*Chunk, error)
	// Usage returns the usage the provider reported, if it has.
	Usage() (Usage, bool)
	// Close releases the upstream connection. It is idempotent.
	Close() error
}

// ErrKind classifies upstream failures for failover and breaker decisions.
type ErrKind int

// Error kinds.
const (
	KindTransport   ErrKind = iota + 1 // connect failure, reset, truncated stream
	KindTimeout                        // deadline exceeded
	KindRateLimited                    // upstream 429
	KindServer                         // upstream 5xx or error event
	KindAuth                           // upstream 401/403: our credentials
	KindBadRequest                     // upstream rejected the request itself
	KindProtocol                       // response we could not parse
)

func (k ErrKind) String() string {
	switch k {
	case KindTransport:
		return "transport"
	case KindTimeout:
		return "timeout"
	case KindRateLimited:
		return "rate_limited"
	case KindServer:
		return "server"
	case KindAuth:
		return "auth"
	case KindBadRequest:
		return "bad_request"
	case KindProtocol:
		return "protocol"
	}
	return fmt.Sprintf("ErrKind(%d)", int(k))
}

// UpstreamError is a classified upstream failure.
type UpstreamError struct {
	Provider  string
	Status    int // upstream HTTP status; 0 if none
	Kind      ErrKind
	Retryable bool // failover to another target may succeed (design §6.3)
	Msg       string
}

func (e *UpstreamError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("upstream %s: %s (HTTP %d): %s", e.Provider, e.Kind, e.Status, e.Msg)
	}
	return fmt.Sprintf("upstream %s: %s: %s", e.Provider, e.Kind, e.Msg)
}

// ClassifyStatus maps a non-200 upstream status to an error kind and whether
// failing over to another target could help.
func ClassifyStatus(status int) (ErrKind, bool) {
	switch {
	case status == http.StatusTooManyRequests:
		return KindRateLimited, true
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return KindAuth, true
	case status >= 400 && status < 500:
		return KindBadRequest, false
	default:
		return KindServer, true
	}
}

// AttemptHeader carries the attempt number on upstream requests, so the mock
// can record each failover attempt separately.
const AttemptHeader = "X-Attempt"

type attemptKey struct{}

// WithAttempt records the 1-based attempt number for upstream headers.
func WithAttempt(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, attemptKey{}, n)
}

// Attempt returns the attempt number in ctx, or 0.
func Attempt(ctx context.Context) int {
	n, _ := ctx.Value(attemptKey{}).(int)
	return n
}
