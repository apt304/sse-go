package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	sse "github.com/apt304/sse-go/server"
)

func TestNewClient(t *testing.T) {
	client := NewClient("http://localhost:8080/events", []string{"test"})

	assert.Equal(t, "http://localhost:8080/events", client.url, "Expected URL to match")
	assert.Len(t, client.topics, 1, "Expected exactly 1 topic")
	assert.Equal(t, "test", client.topics[0], "Expected topic to be 'test'")
	assert.True(t, client.options.Reconnect, "Expected reconnect to be true by default")
	assert.Equal(t, 1*time.Second, client.options.Backoff, "Expected backoff to be 1s")
	assert.Equal(t, 30*time.Second, client.options.MaxBackoff, "Expected maxBackoff to be 30s")
}

func TestClient_Options(t *testing.T) {
	// Test default options
	client := NewClient("http://localhost:8080/events", nil)

	assert.True(t, client.options.Reconnect, "Expected reconnect to be true by default")
	assert.Equal(t, 1*time.Second, client.options.Backoff, "Expected backoff to be 1s")
	assert.Equal(t, 30*time.Second, client.options.MaxBackoff, "Expected maxBackoff to be 30s")
	assert.Equal(t, 10, client.options.MaxRetries, "Expected maxRetries to be 10")

	// Test custom options
	client = NewClient("http://localhost:8080/events", nil, Options{
		Reconnect:  false,
		Backoff:    5 * time.Second,
		MaxBackoff: 60 * time.Second,
		MaxRetries: 5,
	})

	assert.False(t, client.options.Reconnect, "Expected reconnect to be false with custom options")
	assert.Equal(t, 5*time.Second, client.options.Backoff, "Expected backoff to be 5s")
	assert.Equal(t, 60*time.Second, client.options.MaxBackoff, "Expected maxBackoff to be 60s")
	assert.Equal(t, 5, client.options.MaxRetries, "Expected maxRetries to be 5")
}

func TestClient_OnMessage(t *testing.T) {
	client := NewClient("http://localhost:8080/events", nil)

	handler := func(event sse.Event) {
		// Handler function
	}

	client.OnMessage(handler)

	assert.NotNil(t, client.messageHandler, "Expected messageHandler to be set")
}

func TestClient_OnEvent(t *testing.T) {
	client := NewClient("http://localhost:8080/events", nil)

	handler := func(event sse.Event) {
		// Handler function
	}

	client.OnEvent("test", handler)

	assert.Len(t, client.eventHandlers["test"], 1, "Expected event handler to be registered")
}

func TestClient_OnError(t *testing.T) {
	client := NewClient("http://localhost:8080/events", nil)

	handler := func(err error) {
		// Handler function
	}

	client.OnError(handler)

	assert.NotNil(t, client.errorHandler, "Expected errorHandler to be set")
}

func TestAddJitter(t *testing.T) {
	// Test that jitter adds reasonable variation to backoff
	baseBackoff := 1 * time.Second

	// Test multiple calls to ensure jitter varies
	jittered1 := addJitter(baseBackoff)
	jittered2 := addJitter(baseBackoff)

	// Jitter should be within +/- 2% of base
	maxJitter := time.Duration(float64(baseBackoff) * 0.02)

	assert.GreaterOrEqual(t, jittered1, baseBackoff-maxJitter, "Jittered backoff should not be below minimum")
	assert.LessOrEqual(t, jittered1, baseBackoff+maxJitter, "Jittered backoff should not be above maximum")

	assert.GreaterOrEqual(t, jittered2, baseBackoff-maxJitter, "Jittered backoff should not be below minimum")
	assert.LessOrEqual(t, jittered2, baseBackoff+maxJitter, "Jittered backoff should not be above maximum")

	// Jitter should add some variation (not always the same)
	if jittered1 == jittered2 {
		t.Log("Note: Jitter produced same values, this can happen rarely but is not an error")
	}
}

func TestClient_GetState(t *testing.T) {
	client := NewClient("http://localhost:8080/events", nil)

	state := client.GetState()
	assert.Equal(t, StateDisconnected, state, "Expected initial state to be StateDisconnected")
}

func TestClient_IsConnected(t *testing.T) {
	client := NewClient("http://localhost:8080/events", nil)

	assert.False(t, client.IsConnected(), "Expected client to not be connected initially")
}

func TestClient_GetLastEventID(t *testing.T) {
	client := NewClient("http://localhost:8080/events", nil)

	id := client.GetLastEventID()
	assert.Empty(t, id, "Expected initial LastEventID to be empty")
}

func TestClient_Disconnect(t *testing.T) {
	client := NewClient("http://localhost:8080/events", nil)

	// Should not panic when disconnecting without connecting
	client.Disconnect()

	state := client.GetState()
	assert.Equal(t, StateDisconnected, state, "Expected state to be StateDisconnected after disconnect")
}

func TestNewConnection(t *testing.T) {
	conn := NewConnection("http://localhost:8080/events", []string{"test"})

	assert.Equal(t, "http://localhost:8080/events", conn.url, "Expected URL to match")
	assert.Len(t, conn.topics, 1, "Expected exactly 1 topic")
	assert.Equal(t, "test", conn.topics[0], "Expected topic to be 'test'")
	assert.NotNil(t, conn.client, "Expected HTTP client to be initialized")
}

func TestConnection_SetHeader(t *testing.T) {
	conn := NewConnection("http://localhost:8080/events", nil)

	conn.SetHeader("X-Custom", "value")

	assert.Equal(t, "value", conn.headers["X-Custom"], "Expected header 'X-Custom' to be 'value'")
}

func TestConnection_SetLastEventID(t *testing.T) {
	conn := NewConnection("http://localhost:8080/events", nil)

	conn.SetLastEventID("event-123")

	assert.Equal(t, "event-123", conn.lastEventID, "Expected LastEventID to be 'event-123'")
}

func TestConnection_IsClosed(t *testing.T) {
	conn := NewConnection("http://localhost:8080/events", nil)

	assert.False(t, conn.IsClosed(), "Expected connection to not be closed initially")

	conn.Close()

	assert.True(t, conn.IsClosed(), "Expected connection to be closed after Close()")
}

func TestEvent_GetData(t *testing.T) {
	event := sse.Event{
		Data: []byte("test data"),
	}

	data := event.GetData()
	assert.Equal(t, "test data", data, "Expected GetData() to return 'test data'")
}

func TestEvent_IsHeartbeat(t *testing.T) {
	heartbeatEvent := sse.Event{
		Type: "comment",
		Data: []byte("hb"),
	}

	assert.True(t, heartbeatEvent.IsHeartbeat(), "Expected heartbeat event to be identified as heartbeat")

	normalEvent := sse.Event{
		Type: "message",
		Data: []byte("hello"),
	}

	assert.False(t, normalEvent.IsHeartbeat(), "Expected normal event to not be identified as heartbeat")
}

func TestEvent_String(t *testing.T) {
	event := sse.Event{
		ID:    "123",
		Type:  "message",
		Retry: 5000,
		Data:  []byte("hello world"),
	}

	expected := "id: 123\nevent: message\nretry: 5000\ndata: hello world\n\n"
	result := event.String()

	assert.Equal(t, expected, result, "Expected event string to match")
}

func TestEvent_String_Heartbeat(t *testing.T) {
	event := sse.Event{
		Type: "comment",
		Data: []byte("hb"),
	}

	expected := ": hb\n\n"
	result := event.String()

	assert.Equal(t, expected, result, "Expected heartbeat event string to match")
}

func TestEvent_String_NoFields(t *testing.T) {
	event := sse.Event{}

	expected := "data: \n\n"
	result := event.String()

	assert.Equal(t, expected, result, "Expected empty event string to match")
}
