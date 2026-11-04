// Package sse reads and writes Server-Sent Events.
package sse

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// MaxLineBytes bounds a single SSE line (one serialized chunk).
const MaxLineBytes = 4 << 20

// Event is one dispatched SSE event.
type Event struct {
	Event string // the "event:" field, "" for the default type
	Data  []byte // "data:" lines joined with '\n'
}

// Reader decodes an SSE stream.
type Reader struct {
	s *bufio.Scanner
}

// NewReader returns a Reader over r.
func NewReader(r io.Reader) *Reader {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, 64<<10), MaxLineBytes)
	return &Reader{s: s}
}

// Next returns the next event. It returns io.EOF if the stream ends between
// events and io.ErrUnexpectedEOF if it ends partway through one.
func (r *Reader) Next() (Event, error) {
	var ev Event
	var data []byte
	pending := false
	for r.s.Scan() {
		line := r.s.Bytes()
		if len(line) == 0 {
			if !pending {
				continue
			}
			ev.Data = data
			return ev, nil
		}
		if line[0] == ':' {
			continue // comment / keepalive
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "data":
			if pending && data != nil {
				data = append(data, '\n')
			}
			data = append(data, value...)
			if data == nil {
				data = []byte{}
			}
			pending = true
		case "event":
			ev.Event = string(value)
			pending = true
		}
	}
	if err := r.s.Err(); err != nil {
		return Event{}, err
	}
	if pending {
		return Event{}, io.ErrUnexpectedEOF
	}
	return Event{}, io.EOF
}

// Writer encodes SSE events onto an HTTP response, flushing after each one.
type Writer struct {
	w   io.Writer
	rc  *http.ResponseController
	buf []byte
}

// NewWriter returns a Writer for w. The caller sets headers and the status.
func NewWriter(w http.ResponseWriter) *Writer {
	return &Writer{w: w, rc: http.NewResponseController(w)}
}

// Data writes one "data:" event and flushes it. data must not contain newlines.
func (w *Writer) Data(data []byte) error {
	if bytes.IndexByte(data, '\n') >= 0 {
		return errors.New("sse: data contains a newline")
	}
	w.buf = append(w.buf[:0], "data: "...)
	w.buf = append(w.buf, data...)
	w.buf = append(w.buf, '\n', '\n')
	if _, err := w.w.Write(w.buf); err != nil {
		return err
	}
	return w.rc.Flush()
}

// JSON marshals v and writes it as one event.
func (w *Writer) JSON(v any) error {
	b, err := json.Marshal(v) // compact: never contains raw newlines
	if err != nil {
		return err
	}
	return w.Data(b)
}
