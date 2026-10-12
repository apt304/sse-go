package sse

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvent(t *testing.T) {

	var tests = []struct {
		name        string
		event       Event
		expected    string
		isHeartbeat bool
	}{
		{
			name:     "basic event",
			event:    Event{Data: []byte("test")},
			expected: "data: test\n\n",
		},
		{
			name:     "event with ID and type",
			event:    Event{ID: "123", Type: "test", Data: []byte("test")},
			expected: "id: 123\nevent: test\ndata: test\n\n",
		},
		{
			name:     "event with retry",
			event:    Event{Retry: 1000, Data: []byte("test")},
			expected: "retry: 1000\ndata: test\n\n",
		},
		{
			name:     "event with ID, type, and retry",
			event:    Event{ID: "123", Type: "test", Retry: 1000, Data: []byte("test")},
			expected: "id: 123\nevent: test\nretry: 1000\ndata: test\n\n",
		},
		{
			name:     "event with ID, type, retry, and multiple data lines",
			event:    Event{ID: "123", Type: "test", Retry: 1000, Data: []byte("test\nline2\rline3\r\nline4")},
			expected: "id: 123\nevent: test\nretry: 1000\ndata: test\ndata: line2\rdata: line3\r\ndata: line4\n\n",
		},
		{
			name:     "event with comment",
			event:    Event{Type: EventTypeComment, Data: []byte("a comment")},
			expected: ": a comment\n\n",
		},
		{
			name:     "event with comment and fields",
			event:    Event{ID: "123", Type: EventTypeComment, Retry: 1000, Data: []byte("a comment")},
			expected: ": a comment\n\n",
		},
		{
			name:     "event with multi-line comment",
			event:    Event{ID: "123", Type: EventTypeComment, Retry: 1000, Data: []byte("a comment\nline2\rline3\r\nline4")},
			expected: ": a comment\n: line2\r: line3\r\n: line4\n\n",
		},
		{
			name:        "heartbeat",
			event:       Event{ID: "123", Type: EventTypeComment, Retry: 1000, Data: []byte("hb")},
			expected:    ": hb\n\n",
			isHeartbeat: true,
		},
		{
			name:        "heartbeat with fields",
			event:       Event{ID: "123", Type: EventTypeComment, Retry: 1000, Data: []byte("hb")},
			expected:    ": hb\n\n",
			isHeartbeat: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.event.String()
			assert.Equal(t, tt.expected, result)

			var buf bytes.Buffer
			err := tt.event.Write(&buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, buf.String())

			assert.Equal(t, tt.isHeartbeat, tt.event.IsHeartbeat())

			//assert.Equal(t, tt.event.GetData(), tt.expected)
		})
	}
}

func TestParseEvent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Event
		hasError bool
	}{
		{
			name:  "basic event",
			input: "data: hello world\n\n",
			expected: Event{
				Data: []byte("hello world"),
			},
		},
		{
			name:  "event with ID and type",
			input: "id: 123\nevent: test\ndata: hello world\n\n",
			expected: Event{
				ID:   "123",
				Type: "test",
				Data: []byte("hello world"),
			},
		},
		{
			name:  "event with retry",
			input: "retry: 5000\ndata: hello world\n\n",
			expected: Event{
				Retry: 5000,
				Data:  []byte("hello world"),
			},
		},
		{
			name:  "event with all fields",
			input: "id: 123\nevent: test\nretry: 5000\ndata: hello world\n\n",
			expected: Event{
				ID:    "123",
				Type:  "test",
				Retry: 5000,
				Data:  []byte("hello world"),
			},
		},
		{
			name:  "multi-line data",
			input: "id: 123\ndata: line1\ndata: line2\ndata: line3\n\n",
			expected: Event{
				ID:   "123",
				Data: []byte("line1\nline2\nline3"),
			},
		},
		{
			name:  "multi-line data with mixed line endings",
			input: "id: 123\ndata: line1\ndata: line2\ndata: line3\n\n",
			expected: Event{
				ID:   "123",
				Data: []byte("line1\nline2\nline3"),
			},
		},
		{
			name:  "empty data lines",
			input: "id: 123\ndata:\ndata: \ndata: content\n\n",
			expected: Event{
				ID:   "123",
				Data: []byte("\n\ncontent"),
			},
		},
		{
			name:  "heartbeat comment",
			input: ": hb\n\n",
			expected: Event{
				Type: EventTypeComment,
				Data: []byte("hb"),
			},
		},
		{
			name:     "regular comment",
			input:    ": this is a comment\n\n",
			expected: Event{},
			hasError: true, // Regular comments should be ignored and return EOF
		},
		{
			name:  "event with only ID",
			input: "id: 123\n\n",
			expected: Event{
				ID: "123",
			},
		},
		{
			name:  "event with only type",
			input: "event: test\n\n",
			expected: Event{
				Type: "test",
			},
		},
		{
			name:  "event with only data",
			input: "data: hello\n\n",
			expected: Event{
				Data: []byte("hello"),
			},
		},
		{
			name:     "empty event",
			input:    "\n\n",
			expected: Event{},
			hasError: true, // Empty events should return EOF
		},
		{
			name:  "event with spaces in fields",
			input: "id:  123  \nevent:  test  \ndata:  hello world  \n\n",
			expected: Event{
				// Only the single space after the colon is removed
				ID:   " 123  ",
				Type: " test  ",
				Data: []byte(" hello world  "),
			},
		},
		{
			name:  "invalid retry value",
			input: "retry: invalid\ndata: hello\n\n",
			expected: Event{
				Retry: 0, // Should remain 0 for invalid values
				Data:  []byte("hello"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			event, err := ParseEvent(reader)

			if tt.hasError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected.ID, event.ID)
			assert.Equal(t, tt.expected.Type, event.Type)
			assert.Equal(t, tt.expected.Retry, event.Retry)
			assert.Equal(t, tt.expected.Data, event.Data)
		})
	}
}

func TestParseEvent_Heartbeat(t *testing.T) {
	event, err := ParseEvent(bufio.NewReader(strings.NewReader(heartbeatComment.String())))
	assert.NoError(t, err)
	assert.True(t, event.IsHeartbeat(), "Expected a parsed heartbeat to be recognized as a heartbeat")
}

func TestEvent_Write_LineBreakInField(t *testing.T) {
	tests := []struct {
		name  string
		event Event
	}{
		{name: "newline in ID", event: Event{ID: "1\nevent: injected", Data: []byte("x")}},
		{name: "carriage return in ID", event: Event{ID: "1\revent: injected", Data: []byte("x")}},
		{name: "newline in type", event: Event{Type: "message\ndata: injected", Data: []byte("x")}},
		{name: "carriage return in type", event: Event{Type: "message\rdata: injected", Data: []byte("x")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := tt.event.Write(&buf)
			assert.ErrorIs(t, err, ErrInvalidEventField)
			assert.Empty(t, buf.String(), "Expected nothing to be written")
		})
	}
}

func TestParseEvent_PreservesWhitespace(t *testing.T) {
	event := Event{ID: " id ", Data: []byte("  indented  ")}

	var buf bytes.Buffer
	assert.NoError(t, event.Write(&buf))

	parsed, err := ParseEvent(bufio.NewReader(&buf))
	assert.NoError(t, err)
	assert.Equal(t, event.ID, parsed.ID, "Expected ID whitespace to be preserved")
	assert.Equal(t, event.Data, parsed.Data, "Expected data whitespace to be preserved")
}

func TestParseEvent_EOF(t *testing.T) {
	// Test EOF handling with empty input
	reader := bufio.NewReader(strings.NewReader(""))
	_, err := ParseEvent(reader)
	assert.Equal(t, io.EOF, err)

	// Test EOF with partial event (no final newline)
	reader = bufio.NewReader(strings.NewReader("id: 123\ndata: hello"))
	event, err := ParseEvent(reader)
	assert.NoError(t, err)
	assert.Equal(t, "123", event.ID)
	assert.Equal(t, []byte("hello"), event.Data)
}

// errorReader is a test helper that always returns an error
type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

func TestParseEvent_RoundTrip(t *testing.T) {
	// Test that parsing and writing events produces consistent results
	originalEvents := []Event{
		{ID: "1", Type: "test", Data: []byte("hello world")},
		{ID: "2", Data: []byte("multi\nline\ndata")},
		{Type: "message", Retry: 5000, Data: []byte("retry test")},
		{Data: []byte("simple data")},
	}

	for i, original := range originalEvents {
		t.Run(fmt.Sprintf("event_%d", i), func(t *testing.T) {
			// Write event to string
			eventStr := original.String()

			// Parse it back
			reader := bufio.NewReader(strings.NewReader(eventStr))
			parsed, err := ParseEvent(reader)
			assert.NoError(t, err)

			// Should match original
			assert.Equal(t, original.ID, parsed.ID)
			assert.Equal(t, original.Type, parsed.Type)
			assert.Equal(t, original.Retry, parsed.Retry)
			assert.Equal(t, original.Data, parsed.Data)
		})
	}
}

func TestParseEvent_LineEndings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Event
		hasError bool
	}{
		{
			name:  "LF line endings",
			input: "id: 123\ndata: hello\n\n",
			expected: Event{
				ID:   "123",
				Data: []byte("hello"),
			},
		},
		{
			name:  "CR line endings",
			input: "id: 123\rdata: hello\r\r",
			expected: Event{
				ID:   "123",
				Data: []byte("hello"),
			},
		},
		{
			name:  "CRLF line endings",
			input: "id: 123\r\ndata: hello\r\n\r\n",
			expected: Event{
				ID:   "123",
				Data: []byte("hello"),
			},
		},
		{
			name:  "Mixed line endings",
			input: "id: 123\ndata: line1\rdata: line2\r\ndata: line3\n\n",
			expected: Event{
				ID:   "123",
				Data: []byte("line1\nline2\nline3"),
			},
		},
		{
			name:  "Multi-line data with CRLF",
			input: "id: 123\r\ndata: line1\r\ndata: line2\r\ndata: line3\r\n\r\n",
			expected: Event{
				ID:   "123",
				Data: []byte("line1\nline2\nline3"),
			},
		},
		{
			name:  "Multi-line data with CR",
			input: "id: 123\rdata: line1\rdata: line2\rdata: line3\r\r",
			expected: Event{
				ID:   "123",
				Data: []byte("line1\nline2\nline3"),
			},
		},
		{
			name:  "Event with all fields and CRLF",
			input: "id: 123\r\nevent: test\r\nretry: 5000\r\ndata: hello world\r\n\r\n",
			expected: Event{
				ID:    "123",
				Type:  "test",
				Retry: 5000,
				Data:  []byte("hello world"),
			},
		},
		{
			name:  "Heartbeat with CRLF",
			input: ": hb\r\n\r\n",
			expected: Event{
				Type: EventTypeComment,
				Data: []byte("hb"),
			},
		},
		{
			name:     "Empty event with CRLF",
			input:    "\r\n\r\n",
			expected: Event{},
			hasError: true, // Empty events should return EOF
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			event, err := ParseEvent(reader)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, event)
			}
		})
	}
}

func TestParseEvent_LineEndings_EOF(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Event
	}{
		{
			name:  "EOF with LF",
			input: "id: 123\ndata: hello",
			expected: Event{
				ID:   "123",
				Data: []byte("hello"),
			},
		},
		{
			name:  "EOF with CR",
			input: "id: 123\rdata: hello",
			expected: Event{
				ID:   "123",
				Data: []byte("hello"),
			},
		},
		{
			name:  "EOF with CRLF",
			input: "id: 123\r\ndata: hello",
			expected: Event{
				ID:   "123",
				Data: []byte("hello"),
			},
		},
		{
			name:  "EOF with multi-line data and CRLF",
			input: "id: 123\r\ndata: line1\r\ndata: line2",
			expected: Event{
				ID:   "123",
				Data: []byte("line1\nline2"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			event, err := ParseEvent(reader)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, event)
		})
	}
}
