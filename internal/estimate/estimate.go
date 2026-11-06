// Package estimate approximates token counts when a provider does not report
// them (design §5.5, §8.3).
//
// The heuristic is one token per 4 bytes of UTF-8 text plus small fixed
// overheads. Counting bytes rather than characters keeps non-Latin scripts
// (several bytes per character, and often about one token per character)
// from being underestimated as badly as a character count would. It is a
// rough approximation, and every count it produces is flagged as estimated.
package estimate

import (
	"math"

	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

const (
	bytesPerToken   = 4
	messageOverhead = 4 // role and separator tokens per message
	requestOverhead = 3 // tokens that prime the assistant reply
)

// DefaultImageTokens is the tokens assumed per image when not configured.
const DefaultImageTokens = 1000

// DefaultSafetyFactor is the reservation margin when not configured.
const DefaultSafetyFactor = 1.25

// Estimator approximates token counts.
type Estimator struct {
	ImageTokens  int     // tokens assumed per input image
	SafetyFactor float64 // margin on Reserve; values below 1 mean none
}

// Text estimates the tokens in s.
func (e Estimator) Text(s string) int {
	return (len(s) + bytesPerToken - 1) / bytesPerToken
}

// Input estimates the prompt tokens of req: messages, tool calls in the
// history, and tool definitions.
func (e Estimator) Input(req *provider.ChatRequest) int {
	n := requestOverhead
	for _, m := range req.Messages {
		n += messageOverhead
		for _, p := range m.Content {
			if p.Type == provider.PartImage {
				n += e.ImageTokens
				continue
			}
			n += e.Text(p.Text)
		}
		for _, tc := range m.ToolCalls {
			n += e.Text(tc.Name) + e.Text(tc.Arguments)
		}
	}
	for _, t := range req.Tools {
		n += e.Text(t.Name) + e.Text(t.Description) + e.Text(string(t.Parameters))
	}
	return n
}

// Reserve estimates the prompt tokens to admit and reserve for (design §8.3):
// Input with a safety margin, because the heuristic can undercount and an
// actual cost above the reservation can overspend the budget.
func (e Estimator) Reserve(req *provider.ChatRequest) int {
	n := e.Input(req)
	if e.SafetyFactor <= 1 {
		return n
	}
	return int(math.Ceil(float64(n) * e.SafetyFactor))
}

// Output estimates the completion tokens in what was received of a response.
func (e Estimator) Output(r provider.Response) int {
	n := e.Text(r.Content)
	for _, tc := range r.ToolCalls {
		n += e.Text(tc.Name) + e.Text(tc.Arguments)
	}
	return n
}

// Resolve returns the usage to account for a response: each count the
// provider reported is used as is, and each it did not is estimated, in
// which case the result is marked Estimated.
func (e Estimator) Resolve(req *provider.ChatRequest, resp provider.Response) provider.Usage {
	var u provider.Usage
	rep := resp.Reported
	if rep.HasPrompt {
		u.PromptTokens = rep.PromptTokens
	} else {
		u.PromptTokens = e.Input(req)
		u.Estimated = true
	}
	if rep.HasCompletion {
		u.CompletionTokens = rep.CompletionTokens
	} else {
		u.CompletionTokens = e.Output(resp)
		u.Estimated = true
	}
	return u
}
