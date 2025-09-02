package sse

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

const (
	EventTypeComment = "comment"
)

// Pre-allocated heartbeat message to avoid allocations
var (
	heartbeatData    = []byte("hb")
	heartbeatComment = Event{
		Type: EventTypeComment,
		Data: heartbeatData,
	}
)

type Event struct {
	ID    string
	Type  string
	Retry int

	Data []byte
}

func (e Event) String() string {
	var builder strings.Builder

	if e.Type == EventTypeComment {
		builder.WriteString(fmt.Sprintf(": %s\n\n", e.Data))
		return builder.String()
	}

	if e.ID != "" {
		builder.WriteString(fmt.Sprintf("id: %s\n", e.ID))
	}

	if e.Type != "" {
		builder.WriteString(fmt.Sprintf("event: %s\n", e.Type))
	}

	if e.Retry > 0 {
		builder.WriteString(fmt.Sprintf("retry: %d\n", e.Retry))
	}

	// Add data field
	builder.WriteString(fmt.Sprintf("data: %s\n\n", e.Data)) // Empty line to end the event

	return builder.String()
}

func (e Event) Write(w io.Writer) error {
	var builder bytes.Buffer

	if e.Type == EventTypeComment {
		builder.WriteString(fmt.Sprintf(": %s\n\n", e.Data))
		_, err := w.Write(builder.Bytes())
		return err
	}

	if e.ID != "" {
		builder.WriteString(fmt.Sprintf("id: %s\n", e.ID))
	}

	if e.Type != "" {
		builder.WriteString(fmt.Sprintf("event: %s\n", e.Type))
	}

	if e.Retry > 0 {
		builder.WriteString(fmt.Sprintf("retry: %d\n", e.Retry))
	}

	// Add data field
	builder.WriteString(fmt.Sprintf("data: %s\n\n", e.Data)) // Empty line to end the event

	_, err := w.Write(builder.Bytes())

	return err
}

// IsHeartbeat returns true if this is a heartbeat event
func (e Event) IsHeartbeat() bool {
	return e.Type == EventTypeComment && string(e.Data) == string(heartbeatData)
}
