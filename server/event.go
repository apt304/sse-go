package sse

import (
	"bufio"
	"bytes"
	"fmt"
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

// readLine reads a line from the reader, handling LF, CR, and CRLF line endings
func readLine(reader *bufio.Reader) (string, error) {
	var line []byte

	for {
		ch, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				// Return the line even if we hit EOF (no final newline)
				return string(line), io.EOF
			}
			return "", err
		}

		if ch == '\n' {
			// Found LF - check if previous char was CR
			if len(line) > 0 && line[len(line)-1] == '\r' {
				// Remove the CR to handle CRLF
				line = line[:len(line)-1]
			}
			return string(line), nil
		} else if ch == '\r' {
			// Found CR - check if next char is LF
			nextCh, err := reader.ReadByte()
			if err != nil {
				if err == io.EOF {
					// CR at end of stream
					return string(line), nil
				}
				return "", err
			}
			if nextCh == '\n' {
				// CRLF - return the line
				return string(line), nil
			} else {
				// CR only - put the next char back and return
				reader.UnreadByte()
				return string(line), nil
			}
		} else {
			// Regular character
			line = append(line, ch)
		}
	}
}

// ParseEvent parses a single SSE event from a reader
func ParseEvent(reader *bufio.Reader) (Event, error) {
	var event Event
	var dataLines []string

	for {
		line, err := readLine(reader)
		if err != nil {
			if err == io.EOF {
				// Process the last line if it exists
				if line != "" {
					processLine(line, &event, &dataLines)
				}

				// If we have accumulated data, return the event
				if event.ID != "" || event.Type != "" || len(dataLines) > 0 {
					if len(dataLines) > 0 {
						event.Data = []byte(strings.Join(dataLines, "\n"))
					}
					return event, nil
				}
				return Event{}, io.EOF
			}
			return Event{}, fmt.Errorf("read error: %w", err)
		}

		// Process the line
		processLine(line, &event, &dataLines)

		// Empty line indicates end of event
		if line == "" {
			if event.ID != "" || event.Type != "" || len(dataLines) > 0 {
				// Join data lines with newlines to preserve multi-line data
				if len(dataLines) > 0 {
					event.Data = []byte(strings.Join(dataLines, "\n"))
				}
				return event, nil
			}
			continue
		}
	}
}

// processLine processes a single line and updates the event and dataLines
func processLine(line string, event *Event, dataLines *[]string) {
	// Parse event fields
	if strings.HasPrefix(line, "id:") {
		event.ID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
	} else if strings.HasPrefix(line, "event:") {
		event.Type = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
	} else if strings.HasPrefix(line, "retry:") {
		retryStr := strings.TrimSpace(strings.TrimPrefix(line, "retry:"))
		if retry, err := strconv.Atoi(retryStr); err == nil {
			event.Retry = retry
		}
	} else if strings.HasPrefix(line, "data:") {
		// Handle multi-line data by collecting all data: lines
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		*dataLines = append(*dataLines, data)
	} else if strings.HasPrefix(line, ":") {
		// Comment line (heartbeat)
		comment := strings.TrimSpace(strings.TrimPrefix(line, ":"))
		if comment == "hb" {
			event.Type = "hb"
			event.Data = []byte("hb")
		}
		// Regular comments are ignored
	}
}
