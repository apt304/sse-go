package client

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConnection(t *testing.T) {
	conn := NewConnection("http://localhost:8080/events", 100, nil, "test")

	assert.Equal(t, "http://localhost:8080/events", conn.url, "Expected URL to be 'http://localhost:8080/events'")
	assert.Equal(t, []string{"test"}, conn.topics, "Expected topics to be ['test']")
	assert.NotNil(t, conn.client, "Expected HTTP client to be initialized")
}

func TestNewConnection_HTTPClient(t *testing.T) {
	httpClient := &http.Client{}
	conn := NewConnection("http://localhost:8080/events", 100, httpClient)

	assert.Same(t, httpClient, conn.client, "Expected provided HTTP client to be used")
}

func TestConnection_SetHeader(t *testing.T) {
	conn := NewConnection("http://localhost:8080/events", 100, nil)

	conn.SetHeader("X-Custom", "value")

	assert.Equal(t, "value", conn.headers["X-Custom"], "Expected header 'X-Custom' to be 'value'")
}

func TestConnection_SetLastEventID(t *testing.T) {
	conn := NewConnection("http://localhost:8080/events", 100, nil)

	conn.SetLastEventID("event-123")

	assert.Equal(t, "event-123", conn.lastEventID, "Expected LastEventID to be 'event-123'")
}

func TestConnection_IsClosed(t *testing.T) {
	conn := NewConnection("http://localhost:8080/events", 100, nil)

	assert.False(t, conn.IsClosed(), "Expected connection to not be closed initially")

	conn.Close()

	assert.True(t, conn.IsClosed(), "Expected connection to be closed after Close()")
}
