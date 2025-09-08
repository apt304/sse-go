package sse

import (
	"bytes"
	"io"
	"strconv"
	"strings"
)

const (
	EventTypeComment = "comment"
)

var (
	connectedComment = Event{
		Type: EventTypeComment,
		Data: []byte("connected"),
	}
	connectedCommentBytes = []byte(connectedComment.String())

	// Pre-allocated heartbeat message to avoid allocations
	heartbeatData    = []byte("hb")
	heartbeatComment = Event{
		Type: EventTypeComment,
		Data: heartbeatData,
	}
	heartbeatCommentBytes = []byte(heartbeatComment.String())
)

type Event struct {
	ID    string
	Type  string
	Retry int

	Data []byte
}

func (e Event) String() string {
	var builder strings.Builder
	e.Write(&builder)

	return builder.String()
}

func (e Event) Write(w io.Writer) error {
	var builder bytes.Buffer

	if e.Type == EventTypeComment {
		err := WriteLines(&builder, ": ", e.Data)
		if err != nil {
			return err
		}

		_, err = builder.WriteString("\n\n")
		if err != nil {
			return err
		}

		_, err = w.Write(builder.Bytes())
		return err
	}

	if e.ID != "" {
		_, err := builder.WriteString("id: ")
		if err != nil {
			return err
		}

		_, err = builder.WriteString(e.ID)
		if err != nil {
			return err
		}

		_, err = builder.WriteString("\n")
		if err != nil {
			return err
		}
	}

	if e.Type != "" {
		_, err := builder.WriteString("event: ")
		if err != nil {
			return err
		}

		_, err = builder.WriteString(e.Type)
		if err != nil {
			return err
		}

		_, err = builder.WriteString("\n")
		if err != nil {
			return err
		}
	}

	if e.Retry > 0 {
		_, err := builder.WriteString("retry: ")
		if err != nil {
			return err
		}

		_, err = builder.WriteString(strconv.Itoa(e.Retry))
		if err != nil {
			return err
		}

		_, err = builder.WriteString("\n")
		if err != nil {
			return err
		}
	}

	// Add data fields. Each newline (/r, /n, /r/n) is written as a separate data field.
	err := WriteLines(&builder, "data: ", e.Data)
	if err != nil {
		return err
	}

	_, err = builder.WriteString("\n\n")
	if err != nil {
		return err
	}

	_, err = w.Write(builder.Bytes())

	return err
}

// Each newline (/r, /n, /r/n) is written as a separate data field.
func WriteLines(w io.Writer, prefix string, data []byte) error {
	prevIdx := 0
	nextIdx := 0
	for nextIdx = 0; nextIdx < len(data); nextIdx++ {
		if data[nextIdx] == '\r' || data[nextIdx] == '\n' {
			// increment i if the next character is a newline
			if nextIdx < len(data)-1 && data[nextIdx] == '\r' && data[nextIdx+1] == '\n' {
				nextIdx++
			}

			_, err := w.Write([]byte(prefix))
			if err != nil {
				return err
			}

			_, err = w.Write(data[prevIdx : nextIdx+1])
			if err != nil {
				return err
			}

			prevIdx = nextIdx + 1
		}
	}

	_, err := w.Write([]byte(prefix))
	if err != nil {
		return err
	}

	_, err = w.Write(data[prevIdx:])
	if err != nil {
		return err
	}

	return nil
}

// IsHeartbeat returns true if this is a heartbeat event
func (e Event) IsHeartbeat() bool {
	return e.Type == EventTypeComment && string(e.Data) == string(heartbeatData)
}
