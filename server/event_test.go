package sse

import (
	"bytes"
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
