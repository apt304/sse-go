package client

import (
	"net/http"
	"net/http/httptest"
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

	assert.Equal(t, "value", conn.headers.Get("X-Custom"), "Expected header 'X-Custom' to be 'value'")
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

func TestConnection_Connect_Headers(t *testing.T) {
	received := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Clone()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := NewConnection(server.URL, 10, nil)
	conn.SetHeader("Authorization", "Bearer token")
	conn.SetHeader("Accept", "application/json")
	conn.SetHeader("Cache-Control", "max-age=60")
	conn.SetHeader("Last-Event-ID", "custom")
	conn.SetLastEventID("event-1")
	defer conn.Close()

	_, err := conn.Connect()
	assert.NoError(t, err)

	header := <-received
	assert.Equal(t, "Bearer token", header.Get("Authorization"), "Expected custom header to be sent")
	assert.Equal(t, []string{"text/event-stream"}, header.Values("Accept"), "Expected Accept not to be overridden")
	assert.Equal(t, []string{"no-cache"}, header.Values("Cache-Control"), "Expected Cache-Control not to be overridden")
	assert.Equal(t, []string{"event-1"}, header.Values("Last-Event-ID"), "Expected Last-Event-ID not to be overridden")
}

func TestConnection_Connect_LastEventIDHeaderIgnored(t *testing.T) {
	received := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Clone()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := NewConnection(server.URL, 10, nil)
	conn.SetHeader("Last-Event-ID", "custom")
	defer conn.Close()

	_, err := conn.Connect()
	assert.NoError(t, err)

	assert.Empty(t, (<-received).Values("Last-Event-ID"), "Expected custom Last-Event-ID to be ignored")
}
