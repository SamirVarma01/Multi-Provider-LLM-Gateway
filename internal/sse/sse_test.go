package sse

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReader(t *testing.T) {
	stream := ": keepalive\n\n" +
		"data: {\"a\":1}\n\n" +
		"event: error\r\ndata: line1\r\ndata: line2\r\n\r\n" +
		"data:no-space\n\n" +
		"data: [DONE]\n\n"
	r := NewReader(strings.NewReader(stream))

	want := []Event{
		{Data: []byte(`{"a":1}`)},
		{Event: "error", Data: []byte("line1\nline2")},
		{Data: []byte("no-space")},
		{Data: []byte("[DONE]")},
	}
	for i, w := range want {
		ev, err := r.Next()
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if ev.Event != w.Event || string(ev.Data) != string(w.Data) {
			t.Errorf("event %d = {%q %q}, want {%q %q}", i, ev.Event, ev.Data, w.Event, w.Data)
		}
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("after last event: err = %v, want io.EOF", err)
	}
}

func TestReaderTruncated(t *testing.T) {
	r := NewReader(strings.NewReader("data: {\"a\":1}\n\ndata: {\"partial\""))
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestWriter(t *testing.T) {
	rr := httptest.NewRecorder()
	w := NewWriter(rr)
	if err := w.JSON(map[string]string{"k": "line\nbreak"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Data([]byte("[DONE]")); err != nil {
		t.Fatal(err)
	}
	if err := w.Data([]byte("a\nb")); err == nil {
		t.Error("Data with a raw newline must fail")
	}
	if err := w.EventJSON("message_stop", map[string]string{"type": "message_stop"}); err != nil {
		t.Fatal(err)
	}
	want := "data: {\"k\":\"line\\nbreak\"}\n\ndata: [DONE]\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	if rr.Body.String() != want {
		t.Errorf("body = %q, want %q", rr.Body.String(), want)
	}
	if !rr.Flushed {
		t.Error("writer did not flush")
	}

	// What the writer produces, the reader reads back.
	r := NewReader(strings.NewReader(rr.Body.String()))
	ev, err := r.Next()
	if err != nil || string(ev.Data) != `{"k":"line\nbreak"}` {
		t.Errorf("round trip = %q, %v", ev.Data, err)
	}
}
