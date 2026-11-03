package provider

import (
	"errors"
	"io"
	"strings"
)

// Response is a complete assistant response assembled from a stream.
type Response struct {
	Content       string
	FinishReason  string
	Usage         Usage
	UsageReported bool
}

// Collect reads s to the end and assembles the response. On error it returns
// what was received so far along with the error, so the caller can still
// account for partial output.
func Collect(s Stream) (Response, error) {
	var sb strings.Builder
	var r Response
	for {
		c, err := s.Recv()
		if err != nil {
			r.Content = sb.String()
			r.Usage, r.UsageReported = s.Usage()
			if errors.Is(err, io.EOF) {
				return r, nil
			}
			return r, err
		}
		sb.WriteString(c.Content)
		if c.FinishReason != "" {
			r.FinishReason = c.FinishReason
		}
	}
}
