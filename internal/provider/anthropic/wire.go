package anthropic

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

// wireRequest is a Messages API request body.
type wireRequest struct {
	Model         string          `json:"model"`
	MaxTokens     int             `json:"max_tokens"`
	System        string          `json:"system,omitempty"`
	Messages      []wireMessage   `json:"messages"`
	StopSequences []string        `json:"stop_sequences,omitempty"`
	Tools         []wireTool      `json:"tools,omitempty"`
	ToolChoice    *wireToolChoice `json:"tool_choice,omitempty"`
	Metadata      *wireMetadata   `json:"metadata,omitempty"`
	Stream        bool            `json:"stream"`
}

type wireMessage struct {
	Role    string      `json:"role"`
	Content []wireBlock `json:"content"`
}

// wireBlock is a content block; which fields are set depends on Type.
type wireBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`        // text
	Source    *wireSource     `json:"source,omitempty"`      // image
	ID        string          `json:"id,omitempty"`          // tool_use
	Name      string          `json:"name,omitempty"`        // tool_use
	Input     json.RawMessage `json:"input,omitempty"`       // tool_use
	ToolUseID string          `json:"tool_use_id,omitempty"` // tool_result
	Content   string          `json:"content,omitempty"`     // tool_result
}

type wireSource struct {
	Type      string `json:"type"` // base64 | url
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
}

type wireTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
	Strict      *bool           `json:"strict,omitempty"`
}

type wireToolChoice struct {
	Type                   string `json:"type"` // auto | any | tool | none
	Name                   string `json:"name,omitempty"`
	DisableParallelToolUse bool   `json:"disable_parallel_tool_use,omitempty"`
}

type wireMetadata struct {
	UserID string `json:"user_id"`
}

// emptySchema is the input_schema for a tool declared without parameters.
var emptySchema = json.RawMessage(`{"type":"object","properties":{}}`)

// toWire translates a request (design §5.4). System messages are hoisted
// into the top-level system prompt, tool results become user tool_result
// blocks, and consecutive same-role messages are merged because the
// Messages API requires roles to alternate. Sampling parameters are never
// sent; Supports has already rejected non-default values.
func toWire(t provider.Target, req *provider.ChatRequest) (wireRequest, error) {
	if req.MaxTokens == nil || *req.MaxTokens < 1 {
		return wireRequest{}, errors.New("anthropic: max_tokens is required")
	}
	w := wireRequest{
		Model:         t.UpstreamModel,
		MaxTokens:     *req.MaxTokens,
		StopSequences: req.Stop,
		Stream:        true,
	}
	if req.User != "" {
		w.Metadata = &wireMetadata{UserID: req.User}
	}

	var system []string
	appendBlocks := func(role string, blocks []wireBlock) {
		if len(blocks) == 0 {
			return
		}
		if n := len(w.Messages); n > 0 && w.Messages[n-1].Role == role {
			w.Messages[n-1].Content = append(w.Messages[n-1].Content, blocks...)
			return
		}
		w.Messages = append(w.Messages, wireMessage{Role: role, Content: blocks})
	}

	var errs []error
	for i, m := range req.Messages {
		switch m.Role {
		case provider.RoleSystem:
			if txt := m.Text(); txt != "" {
				system = append(system, txt)
			}
		case provider.RoleUser:
			blocks, err := contentBlocks(m.Content)
			if err != nil {
				errs = append(errs, fmt.Errorf("anthropic: messages[%d]: %w", i, err))
			}
			appendBlocks("user", blocks)
		case provider.RoleAssistant:
			blocks, err := contentBlocks(m.Content)
			if err != nil {
				errs = append(errs, fmt.Errorf("anthropic: messages[%d]: %w", i, err))
			}
			for j, tc := range m.ToolCalls {
				input, err := toolInput(tc.Arguments)
				if err != nil {
					errs = append(errs, fmt.Errorf("anthropic: messages[%d].tool_calls[%d]: %w", i, j, err))
				}
				blocks = append(blocks, wireBlock{Type: "tool_use", ID: tc.ID, Name: tc.Name, Input: input})
			}
			appendBlocks("assistant", blocks)
		case provider.RoleTool:
			appendBlocks("user", []wireBlock{{Type: "tool_result", ToolUseID: m.ToolCallID, Content: m.Text()}})
		default:
			errs = append(errs, fmt.Errorf("anthropic: messages[%d]: unsupported role %q", i, m.Role))
		}
	}
	w.System = strings.Join(system, "\n\n")

	for _, tool := range req.Tools {
		schema := tool.Parameters
		if len(schema) == 0 {
			schema = emptySchema
		}
		w.Tools = append(w.Tools, wireTool{Name: tool.Name, Description: tool.Description, InputSchema: schema, Strict: tool.Strict})
	}
	if tc := req.ToolChoice; tc != nil {
		switch tc.Mode {
		case provider.ToolChoiceAuto:
			w.ToolChoice = &wireToolChoice{Type: "auto"}
		case provider.ToolChoiceNone:
			w.ToolChoice = &wireToolChoice{Type: "none"}
		case provider.ToolChoiceRequired:
			w.ToolChoice = &wireToolChoice{Type: "any"}
		case provider.ToolChoiceFunction:
			w.ToolChoice = &wireToolChoice{Type: "tool", Name: tc.Name}
		}
	}
	if p := req.ParallelToolCalls; p != nil && !*p && len(req.Tools) > 0 {
		if w.ToolChoice == nil {
			w.ToolChoice = &wireToolChoice{Type: "auto"}
		}
		if w.ToolChoice.Type != "none" {
			w.ToolChoice.DisableParallelToolUse = true
		}
	}
	return w, errors.Join(errs...)
}

// contentBlocks converts message content. Empty text parts are dropped: the
// Messages API rejects empty text blocks.
func contentBlocks(parts []provider.ContentPart) ([]wireBlock, error) {
	var out []wireBlock
	for _, p := range parts {
		switch p.Type {
		case provider.PartText:
			if p.Text != "" {
				out = append(out, wireBlock{Type: "text", Text: p.Text})
			}
		case provider.PartImage:
			src, err := imageSource(p.ImageURL)
			if err != nil {
				return nil, err
			}
			out = append(out, wireBlock{Type: "image", Source: src})
		default:
			return nil, fmt.Errorf("unsupported content type %q", p.Type)
		}
	}
	return out, nil
}

// imageSource converts an OpenAI image_url (a data: URI or an http(s) URL).
func imageSource(u string) (*wireSource, error) {
	if rest, ok := strings.CutPrefix(u, "data:"); ok {
		meta, data, ok := strings.Cut(rest, ",")
		mediaType, isBase64 := strings.CutSuffix(meta, ";base64")
		if !ok || !isBase64 || mediaType == "" {
			return nil, errors.New("image data URIs must be base64 with a media type (data:image/png;base64,...)")
		}
		return &wireSource{Type: "base64", MediaType: mediaType, Data: data}, nil
	}
	if strings.HasPrefix(u, "https://") || strings.HasPrefix(u, "http://") {
		return &wireSource{Type: "url", URL: u}, nil
	}
	return nil, errors.New("image_url must be an http(s) URL or a base64 data URI")
}

// toolInput parses OpenAI's JSON-string arguments into a tool_use input object.
func toolInput(args string) (json.RawMessage, error) {
	if strings.TrimSpace(args) == "" {
		return json.RawMessage(`{}`), nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(args), &obj); err != nil {
		return nil, errors.New("tool call arguments must be a JSON object")
	}
	return json.RawMessage(args), nil
}
