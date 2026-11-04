package oai

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
)

// ChatCompletionRequest is the body of POST /v1/chat/completions.
type ChatCompletionRequest struct {
	Model               string         `json:"model"`
	Messages            []Message      `json:"messages"`
	MaxTokens           *int           `json:"max_tokens,omitempty"`
	MaxCompletionTokens *int           `json:"max_completion_tokens,omitempty"`
	Temperature         *float64       `json:"temperature,omitempty"`
	TopP                *float64       `json:"top_p,omitempty"`
	N                   *int           `json:"n,omitempty"`
	Stop                StringOrList   `json:"stop,omitempty"`
	Stream              bool           `json:"stream,omitempty"`
	StreamOptions       *StreamOptions `json:"stream_options,omitempty"`
	User                string         `json:"user,omitempty"`
}

// OutputLimit returns max_completion_tokens if set, else max_tokens, else nil.
func (r *ChatCompletionRequest) OutputLimit() *int {
	if r.MaxCompletionTokens != nil {
		return r.MaxCompletionTokens
	}
	return r.MaxTokens
}

// StreamOptions configures streaming responses.
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// Message is one chat message in a request.
type Message struct {
	Role    string  `json:"role"`
	Content Content `json:"content"`
	Name    string  `json:"name,omitempty"`
}

// Content is message content, which OpenAI accepts either as a string or as
// an array of typed parts. Parts is non-nil exactly when the JSON was an array.
type Content struct {
	Text  string
	Parts []ContentPart
}

// ContentPart is one element of array-form content.
type ContentPart struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// TextContent returns string-form content.
func TextContent(s string) Content { return Content{Text: s} }

// UnmarshalJSON accepts a string, an array of parts, or null.
func (c *Content) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	switch {
	case bytes.Equal(b, []byte("null")):
		*c = Content{}
		return nil
	case len(b) > 0 && b[0] == '"':
		*c = Content{}
		return json.Unmarshal(b, &c.Text)
	case len(b) > 0 && b[0] == '[':
		var parts []ContentPart
		if err := json.Unmarshal(b, &parts); err != nil {
			return err
		}
		if parts == nil {
			parts = []ContentPart{}
		}
		*c = Content{Parts: parts}
		return nil
	default:
		return errors.New("content must be a string or an array of parts")
	}
}

// MarshalJSON emits array form if the content was given as parts, else a string.
func (c Content) MarshalJSON() ([]byte, error) {
	if c.Parts != nil {
		return json.Marshal(c.Parts)
	}
	return json.Marshal(c.Text)
}

// String concatenates all text in the content.
func (c Content) String() string {
	if c.Parts == nil {
		return c.Text
	}
	var sb strings.Builder
	for _, p := range c.Parts {
		sb.WriteString(p.Text)
	}
	return sb.String()
}

// StringOrList is a field OpenAI accepts as a string or an array of strings.
type StringOrList []string

// UnmarshalJSON accepts a string, an array of strings, or null.
func (s *StringOrList) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if bytes.Equal(b, []byte("null")) {
		*s = nil
		return nil
	}
	if len(b) > 0 && b[0] == '"' {
		var one string
		if err := json.Unmarshal(b, &one); err != nil {
			return err
		}
		*s = StringOrList{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return err
	}
	*s = many
	return nil
}

// ChatCompletionResponse is a non-streaming chat completion.
type ChatCompletionResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"` // "chat.completion"
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

// Choice is one completion choice.
type Choice struct {
	Index        int             `json:"index"`
	Message      ResponseMessage `json:"message"`
	FinishReason string          `json:"finish_reason"`
}

// ResponseMessage is the assistant message in a non-streaming response.
type ResponseMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatCompletionChunk is one streaming event. A mid-stream error arrives as an
// event whose body is an ErrorBody instead; Error is set in that case.
type ChatCompletionChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"` // "chat.completion.chunk"
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	Usage   *Usage        `json:"usage,omitempty"`
	Error   *ErrorDetail  `json:"error,omitempty"`
}

// ChunkChoice is one choice's delta in a streaming event.
type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

// Delta is the incremental message content in a streaming event.
type Delta struct {
	Role    string  `json:"role,omitempty"`
	Content *string `json:"content,omitempty"`
}

// Usage is token usage as OpenAI reports it.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// DoneData is the payload of the final SSE event of an OpenAI stream.
const DoneData = "[DONE]"
