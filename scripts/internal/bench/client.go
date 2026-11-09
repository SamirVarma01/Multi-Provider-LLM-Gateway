package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Result is one chat request as the client saw it.
type Result struct {
	Intended   time.Time `json:"-"` // when the load schedule meant to send it
	Sent       time.Time `json:"-"`
	FirstToken time.Time `json:"-"` // first content received; streaming only
	End        time.Time `json:"-"` // response fully read, or the request failed

	Stream    bool   `json:"stream"`
	Status    int    `json:"status"` // 0 if no response arrived
	OK        bool   `json:"ok"`     // a complete, well-formed response
	Err       string `json:"err,omitempty"`
	RequestID string `json:"request_id,omitempty"` // X-Request-ID
	Provider  string `json:"provider,omitempty"`   // X-Gateway-Provider
	Attempts  int    `json:"attempts,omitempty"`   // X-Gateway-Attempts
	Chunks    int    `json:"chunks,omitempty"`     // content chunks received; streaming only
	Usage     *Usage `json:"usage,omitempty"`      // as the response reported it
}

// Latency is the time from Intended to the end of the response.
func (r Result) Latency() time.Duration { return r.End.Sub(r.Intended) }

// TTFT is the time from Intended to the first content, or false if none arrived.
func (r Result) TTFT() (time.Duration, bool) {
	if r.FirstToken.IsZero() {
		return 0, false
	}
	return r.FirstToken.Sub(r.Intended), true
}

// Usage is the usage object of an OpenAI response.
type Usage struct {
	PromptTokens     int  `json:"prompt_tokens"`
	CompletionTokens int  `json:"completion_tokens"`
	Estimated        bool `json:"estimated,omitempty"` // gateway extension
}

// ChatRequest is the subset of an OpenAI chat request the scripts send.
type ChatRequest struct {
	Model         string         `json:"model"`
	Messages      []Message      `json:"messages"`
	MaxTokens     int            `json:"max_tokens,omitempty"`
	Stream        bool           `json:"stream,omitempty"`
	StreamOptions *StreamOptions `json:"stream_options,omitempty"`
}

// Message is a chat message.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// StreamOptions asks for usage in a stream.
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// NewHTTPClient returns a client sized for many concurrent requests to one
// host. It has no overall timeout: callers bound each request with its
// context, because streams are long-lived.
func NewHTTPClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost: 4096,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}}
}

// Chat sends a chat request with an already-encoded body to url, authorized
// with key unless it is empty, and reads the whole response.
func Chat(ctx context.Context, client *http.Client, url, key string, body []byte, stream bool) Result {
	r := Result{Stream: stream, Sent: time.Now()}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		r.Err = err.Error()
		r.End = time.Now()
		return r
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := client.Do(req)
	if err != nil {
		r.Err = shortErr(err)
		r.End = time.Now()
		return r
	}
	defer resp.Body.Close()
	r.Status = resp.StatusCode
	r.RequestID = resp.Header.Get("X-Request-ID")
	r.Provider = resp.Header.Get("X-Gateway-Provider")
	r.Attempts, _ = strconv.Atoi(resp.Header.Get("X-Gateway-Attempts"))
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		r.Err = fmt.Sprintf("status %d: %s", resp.StatusCode, errorMessage(msg))
		r.End = time.Now()
		return r
	}
	if stream {
		readStream(resp.Body, &r)
	} else {
		readCompletion(resp.Body, &r)
	}
	r.End = time.Now()
	return r
}

type chunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *Usage `json:"usage"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// readStream reads an OpenAI SSE stream. It is OK only if it ends with
// [DONE] after a finish reason and no error event.
func readStream(body io.Reader, r *Result) {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 16<<10), 1<<20)
	var done, finished bool
	for sc.Scan() {
		data, ok := bytes.CutPrefix(sc.Bytes(), []byte("data: "))
		if !ok {
			continue
		}
		if string(data) == "[DONE]" {
			done = true
			continue
		}
		var c chunk
		if err := json.Unmarshal(data, &c); err != nil {
			r.Err = "bad chunk: " + err.Error()
			return
		}
		if c.Error != nil {
			r.Err = "stream error: " + c.Error.Message
			continue
		}
		if c.Usage != nil {
			r.Usage = c.Usage
		}
		for _, ch := range c.Choices {
			if ch.Delta.Content != "" {
				if r.FirstToken.IsZero() {
					r.FirstToken = time.Now()
				}
				r.Chunks++
			}
			if ch.FinishReason != nil {
				finished = true
			}
		}
	}
	switch {
	case r.Err != "":
	case sc.Err() != nil:
		r.Err = "stream: " + shortErr(sc.Err())
	case !done || !finished:
		r.Err = "stream ended early"
	default:
		r.OK = true
	}
}

func readCompletion(body io.Reader, r *Result) {
	var resp struct {
		Choices []struct {
			Message struct {
				Content *string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage *Usage `json:"usage"`
	}
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		r.Err = "body: " + shortErr(err)
		return
	}
	r.Usage = resp.Usage
	if len(resp.Choices) == 0 || resp.Choices[0].Message.Content == nil {
		r.Err = "no content"
		return
	}
	r.OK = true
}

func errorMessage(body []byte) string {
	var e struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && (e.Error.Code != "" || e.Error.Message != "") {
		if e.Error.Code != "" {
			return e.Error.Code
		}
		return e.Error.Message
	}
	return strings.TrimSpace(string(body))
}

// shortErr drops the method and URL from a net/http error, so errors from
// different requests read the same and can be counted together.
func shortErr(err error) string {
	if ue, ok := errors.AsType[*url.Error](err); ok {
		return ue.Err.Error()
	}
	return err.Error()
}
