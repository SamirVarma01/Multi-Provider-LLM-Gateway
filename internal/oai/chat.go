package oai

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
)

// ChatCompletionRequest is the body of POST /v1/chat/completions.
//
// Fields the gateway does not model are kept in Extra when decoding and
// written back when encoding, so they can be forwarded to OpenAI-format
// providers unchanged.
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
	Tools               []Tool         `json:"tools,omitempty"`
	ToolChoice          *ToolChoice    `json:"tool_choice,omitempty"`
	ParallelToolCalls   *bool          `json:"parallel_tool_calls,omitempty"`
	User                string         `json:"user,omitempty"`

	Extra map[string]json.RawMessage `json:"-"`
}

// chatRequestFields is ChatCompletionRequest without its methods, so the
// custom (un)marshalers can use the default encoding for the known fields.
type chatRequestFields ChatCompletionRequest

// UnmarshalJSON decodes the known fields and keeps every other top-level
// field in Extra.
func (r *ChatCompletionRequest) UnmarshalJSON(b []byte) error {
	var known chatRequestFields
	if err := json.Unmarshal(b, &known); err != nil {
		return err
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(b, &all); err != nil {
		return err
	}
	for _, name := range knownRequestFields {
		delete(all, name)
	}
	if len(all) == 0 {
		all = nil
	}
	*r = ChatCompletionRequest(known)
	r.Extra = all
	return nil
}

// MarshalJSON encodes the known fields plus Extra. A known field always wins
// over an Extra entry of the same name.
func (r ChatCompletionRequest) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(chatRequestFields(r))
	if err != nil || len(r.Extra) == 0 {
		return b, err
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(b, &all); err != nil {
		return nil, err
	}
	for k, v := range r.Extra {
		if _, ok := all[k]; !ok {
			all[k] = v
		}
	}
	return json.Marshal(all)
}

// knownRequestFields are the JSON names of ChatCompletionRequest's fields,
// read from its struct tags so the list cannot drift from the struct.
var knownRequestFields = func() []string {
	var names []string
	t := reflect.TypeOf(chatRequestFields{})
	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			names = append(names, name)
		}
	}
	return names
}()

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
	Role       string     `json:"role"`
	Content    Content    `json:"content"`
	Name       string     `json:"name,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`   // assistant
	ToolCallID string     `json:"tool_call_id,omitempty"` // tool
}

// Content is message content, which OpenAI accepts as a string, an array of
// typed parts, or null (an assistant message that only calls tools). Parts is
// non-nil exactly when the JSON was an array.
type Content struct {
	Text  string
	Parts []ContentPart
	Null  bool
}

// ContentPart is one element of array-form content.
type ContentPart struct {
	Type     string    `json:"type"` // "text" | "image_url"
	Text     string    `json:"text,omitempty"`
	ImageURL *ImageURL `json:"image_url,omitempty"`
}

// ImageURL is an image part's source: an https URL or a data: URI.
type ImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
}

// TextContent returns string-form content.
func TextContent(s string) Content { return Content{Text: s} }

// UnmarshalJSON accepts a string, an array of parts, or null.
func (c *Content) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	switch {
	case bytes.Equal(b, []byte("null")):
		*c = Content{Null: true}
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
		return errors.New("content must be a string, an array of parts, or null")
	}
}

// MarshalJSON emits null, array form, or a string, matching how it was given.
func (c Content) MarshalJSON() ([]byte, error) {
	switch {
	case c.Null:
		return []byte("null"), nil
	case c.Parts != nil:
		return json.Marshal(c.Parts)
	default:
		return json.Marshal(c.Text)
	}
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

// Tool is a function the model may call.
type Tool struct {
	Type     string       `json:"type"` // "function"
	Function FunctionSpec `json:"function"`
}

// FunctionSpec describes a callable function.
type FunctionSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"` // JSON Schema
	Strict      *bool           `json:"strict,omitempty"`
}

// Tool choice modes.
const (
	ToolChoiceAuto     = "auto"
	ToolChoiceNone     = "none"
	ToolChoiceRequired = "required"
	ToolChoiceFunction = "function" // a specific function, named in Function
)

// ToolChoice is "auto", "none", "required", or {"type":"function","function":{"name":...}}.
type ToolChoice struct {
	Mode     string
	Function string
}

// UnmarshalJSON accepts the string and object forms.
func (t *ToolChoice) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) > 0 && b[0] == '"' {
		var mode string
		if err := json.Unmarshal(b, &mode); err != nil {
			return err
		}
		switch mode {
		case ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired:
			*t = ToolChoice{Mode: mode}
			return nil
		}
		return errors.New(`tool_choice must be "auto", "none", "required", or a function object`)
	}
	var obj struct {
		Type     string `json:"type"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}
	if obj.Type != "function" || obj.Function.Name == "" {
		return errors.New(`tool_choice object must be {"type":"function","function":{"name":...}}`)
	}
	*t = ToolChoice{Mode: ToolChoiceFunction, Function: obj.Function.Name}
	return nil
}

// MarshalJSON emits the string or object form.
func (t ToolChoice) MarshalJSON() ([]byte, error) {
	if t.Mode == ToolChoiceFunction {
		return json.Marshal(map[string]any{"type": "function", "function": map[string]string{"name": t.Function}})
	}
	return json.Marshal(t.Mode)
}

// ToolCall is a complete function call made by the assistant.
type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"` // "function"
	Function FunctionCall `json:"function"`
}

// FunctionCall is the function and JSON-encoded arguments of a tool call.
type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolCallDelta is an incremental piece of a tool call in a streaming event.
// The first delta for an index carries ID and name; later ones carry argument
// fragments to append.
type ToolCallDelta struct {
	Index    int                `json:"index"`
	ID       string             `json:"id,omitempty"`
	Type     string             `json:"type,omitempty"`
	Function *FunctionCallDelta `json:"function,omitempty"`
}

// FunctionCallDelta is the function part of a ToolCallDelta.
type FunctionCallDelta struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments"`
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
// Content is null when the assistant only called tools.
type ResponseMessage struct {
	Role      string     `json:"role"`
	Content   *string    `json:"content"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
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
	Role      string          `json:"role,omitempty"`
	Content   *string         `json:"content,omitempty"`
	ToolCalls []ToolCallDelta `json:"tool_calls,omitempty"`
}

// Usage is token usage as OpenAI reports it. Estimated is a gateway extension,
// present only when the gateway estimated a count the provider did not report.
type Usage struct {
	PromptTokens     int  `json:"prompt_tokens"`
	CompletionTokens int  `json:"completion_tokens"`
	TotalTokens      int  `json:"total_tokens"`
	Estimated        bool `json:"estimated,omitempty"`
}

// DoneData is the payload of the final SSE event of an OpenAI stream.
const DoneData = "[DONE]"
