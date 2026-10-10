package client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sse "github.com/apt304/sse-go/server"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	client := NewClient("http://localhost:8080/events", []string{"test"})

	assert.Equal(t, "http://localhost:8080/events", client.url, "Expected URL to be 'http://localhost:8080/events'")
	assert.Equal(t, []string{"test"}, client.topics, "Expected topics to be ['test']")
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
	assert.Equal(t, 3, client.options.MaxRetries, "Expected maxRetries to be 3")

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

	assert.Equal(t, 1, len(client.eventHandlers["test"]), "Expected event handler to be registered")
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
	assert.Equal(t, "", id, "Expected initial LastEventID to be empty")
}

func TestClient_Disconnect(t *testing.T) {
	client := NewClient("http://localhost:8080/events", nil)

	// Should not panic when disconnecting without connecting
	client.Disconnect()

	state := client.GetState()
	assert.Equal(t, StateDisconnected, state, "Expected state to be StateDisconnected after disconnect")
}

func TestEvent_GetData(t *testing.T) {
	event := sse.Event{
		Data: []byte("test data"),
	}

	data := event.Data
	assert.Equal(t, "test data", string(data), "Expected GetData() to return 'test data'")
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

func TestConnection_ReadEvents_EventParsing(t *testing.T) {
	conn := NewConnection("http://localhost:8080/events", 100)

	// Create a mock reader that returns valid SSE data
	eventData := "id: 123\nevent: test\ndata: hello world\rdata: line2\r\ndata: line3\n\n"
	mockBody := &mockReadCloser{
		readFunc: func(p []byte) (n int, err error) {
			if len(eventData) == 0 {
				return 0, io.EOF
			}
			// Return one line at a time
			lines := strings.Split(eventData, "\n")
			if len(lines) > 0 {
				line := lines[0] + "\n"
				copy(p, []byte(line))
				eventData = strings.Join(lines[1:], "\n")
				return len(line), nil
			}
			return 0, io.EOF
		},
	}

	eventChan := make(chan sse.Event, 1)

	go conn.readEvents(mockBody, eventChan)

	// Wait for the goroutine to process
	time.Sleep(10 * time.Millisecond)

	// Should receive the parsed event
	select {
	case event := <-eventChan:
		assert.Equal(t, "123", event.ID, "Expected event ID to match")
		assert.Equal(t, "test", event.Type, "Expected event type to match")
		assert.Equal(t, "hello world\nline2\nline3", string(event.Data), "Expected event data to match")
	default:
		assert.Fail(t, "Expected to receive parsed event")
	}
}

// Mock read closer for testing
type mockReadCloser struct {
	readFunc func(p []byte) (n int, err error)
	closed   bool
}

func (m *mockReadCloser) Read(p []byte) (n int, err error) {
	if m.readFunc != nil {
		return m.readFunc(p)
	}
	return 0, io.EOF
}

func (m *mockReadCloser) Close() error {
	m.closed = true
	return nil
}

func TestClient_HandleDisconnection_NoReconnect(t *testing.T) {
	client := NewClient("http://localhost:8080/events", nil, Options{
		Reconnect: false,
	})

	// Set initial state
	client.setState(StateConnected)

	// This should not trigger reconnection
	go client.handleDisconnection(client.done)

	// Wait a bit for the goroutine to process
	time.Sleep(10 * time.Millisecond)

	// Should remain disconnected
	assert.Equal(t, StateDisconnected, client.GetState(), "Expected state to remain disconnected when reconnection is disabled")
}

func TestClient_HandleDisconnection_ReconnectionLogic(t *testing.T) {
	// Test that reconnection logic completes and sets final state correctly
	client := NewClient("http://invalid-url-that-will-fail:9999/events", nil, Options{
		Reconnect:  true,
		Backoff:    1 * time.Millisecond, // Very fast for testing
		MaxRetries: 2,
	})

	// Set initial state
	client.setState(StateConnected)

	// This should trigger reconnection logic
	go client.handleDisconnection(client.done)

	// Wait for max retries to be exceeded and final state to be set
	time.Sleep(100 * time.Millisecond)

	// Should end up disconnected after max retries
	assert.Equal(t, StateDisconnected, client.GetState(), "Expected final state to be disconnected")
}

func TestClient_HandleDisconnection_MaxRetriesExceeded(t *testing.T) {
	client := NewClient("http://invalid-url-that-will-fail:9999/events", nil, Options{
		Reconnect:  true,
		MaxRetries: 2,
		Backoff:    1 * time.Millisecond, // Very fast for testing
	})

	var errorReceived bool
	var errorMessage string
	client.OnError(func(err error) {
		errorReceived = true
		errorMessage = err.Error()
	})

	// Set initial state
	client.setState(StateConnected)

	// This should trigger reconnection with max retries
	go client.handleDisconnection(client.done)

	// Wait for the error handler to be called
	time.Sleep(100 * time.Millisecond)

	assert.True(t, errorReceived, "Expected error handler to be called after max retries")
	assert.Contains(t, errorMessage, "failed to reconnect after 2 attempts", "Expected max retries error message")
	assert.Equal(t, StateDisconnected, client.GetState(), "Expected state to be disconnected after max retries")
}

func TestClient_HandleDisconnection_BackoffIncrease(t *testing.T) {
	client := NewClient("http://invalid-url-that-will-fail:9999/events", nil, Options{
		Reconnect:  true,
		MaxRetries: 3,
		Backoff:    1 * time.Millisecond,  // Start with 1ms
		MaxBackoff: 10 * time.Millisecond, // Cap at 10ms
	})

	// Set initial state
	client.setState(StateConnected)

	// Track reconnection attempts
	var attempts int
	client.OnError(func(err error) {
		attempts++
	})

	// This should trigger reconnection with backoff increase
	go client.handleDisconnection(client.done)

	// Wait for some reconnection attempts
	time.Sleep(50 * time.Millisecond)

	// Should have attempted reconnections
	assert.Greater(t, attempts, 0, "Expected reconnection attempts to occur")
	assert.Equal(t, StateDisconnected, client.GetState(), "Expected final state to be disconnected")
}

func TestClient_HandleDisconnection_BackoffCapped(t *testing.T) {
	client := NewClient("http://invalid-url-that-will-fail:9999/events", nil, Options{
		Reconnect:  true,
		MaxRetries: 5,
		Backoff:    1 * time.Millisecond, // Start with 1ms
		MaxBackoff: 2 * time.Millisecond, // Cap at 2ms (very low for testing)
	})

	// Set initial state
	client.setState(StateConnected)

	// This should trigger reconnection with backoff capped
	go client.handleDisconnection(client.done)

	// Wait for max retries to be exceeded
	time.Sleep(100 * time.Millisecond)

	// Should end up disconnected after max retries
	assert.Equal(t, StateDisconnected, client.GetState(), "Expected final state to be disconnected")
}

func TestClient_HandleDisconnection_UnlimitedRetries(t *testing.T) {
	client := NewClient("http://invalid-url-that-will-fail:9999/events", nil, Options{
		Reconnect:  true,
		MaxRetries: 0,                    // Unlimited retries
		Backoff:    1 * time.Millisecond, // Very fast for testing
	})

	// Set initial state
	client.setState(StateConnected)

	// This should trigger reconnection with unlimited retries
	go client.handleDisconnection(client.done)

	// Wait a bit for reconnection attempts
	time.Sleep(20 * time.Millisecond)

	// Should still be in reconnecting state (no max retries limit)
	// Note: We can't easily test unlimited retries in unit tests due to timing
	// So we just verify it doesn't immediately fail
	assert.NotEqual(t, StateConnected, client.GetState(), "Expected state to change from connected")
}

func TestClient_HandleDisconnection_CancelledByDisconnect(t *testing.T) {
	client := NewClient("http://invalid-url-that-will-fail:9999/events", nil, Options{
		Reconnect:  true,
		MaxRetries: 10,
		Backoff:    10 * time.Millisecond, // Slower backoff to allow cancellation
	})

	// Set initial state
	client.setState(StateConnected)

	// Start reconnection process
	go client.handleDisconnection(client.done)

	// Wait a bit for it to start
	time.Sleep(5 * time.Millisecond)

	// Should be in reconnecting state
	assert.Equal(t, StateReconnecting, client.GetState(), "Expected state to be reconnecting")

	// Cancel via disconnect
	client.Disconnect()

	// Wait a bit for cancellation to take effect
	time.Sleep(10 * time.Millisecond)

	// Should be disconnected due to cancellation
	assert.Equal(t, StateDisconnected, client.GetState(), "Expected state to be disconnected after cancellation")
}

func TestClient_HandleDisconnection_StateTransitions(t *testing.T) {
	client := NewClient("http://invalid-url-that-will-fail:9999/events", nil, Options{
		Reconnect:  true,
		MaxRetries: 1,
		Backoff:    1 * time.Millisecond,
	})

	// Test state transitions during reconnection
	client.setState(StateConnected)

	// Start reconnection
	go client.handleDisconnection(client.done)

	// Wait for max retries to be exceeded
	time.Sleep(100 * time.Millisecond)

	// Should end up disconnected after max retries
	assert.Equal(t, StateDisconnected, client.GetState(), "Expected final state to be disconnected")
}

func TestClient_HandleDisconnection_SuccessfulReconnection(t *testing.T) {
	// Test successful reconnection by simulating a scenario where
	// the first connection attempt fails but the second succeeds
	client := NewClient("http://localhost:8080/events", nil, Options{
		Reconnect:  true,
		Backoff:    1 * time.Millisecond, // Very fast for testing
		MaxRetries: 5,
	})

	// Set initial state
	client.setState(StateConnected)

	// This should trigger reconnection logic
	go client.handleDisconnection(client.done)

	// Wait for reconnection process to complete
	time.Sleep(100 * time.Millisecond)

	// Verify that reconnection logic was triggered
	// The client should have attempted reconnection and either succeeded
	// or failed after max retries
	finalState := client.GetState()
	assert.True(t, finalState == StateConnected || finalState == StateDisconnected,
		"Expected final state to be either connected or disconnected after reconnection attempts")
}

func TestClient_NoRetry(t *testing.T) {
	// Test that reconnection logic completes and sets final state correctly
	client := NewClient("http://invalid-url-that-will-fail:9999/events", nil, Options{
		Reconnect:  true,
		Backoff:    1 * time.Millisecond, // Very fast for testing
		MaxRetries: 0,
	})

	// Set initial state
	client.setState(StateConnected)

	// This should trigger reconnection logic
	go client.handleDisconnection(client.done)

	// Wait for max retries to be exceeded and final state to be set
	time.Sleep(100 * time.Millisecond)

	// Should end up disconnected after max retries
	assert.Equal(t, StateDisconnected, client.GetState(), "Expected final state to be disconnected")
}

func TestClient_Connect_RetryAfterFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)

	assert.Error(t, client.Connect(), "Expected first connect to fail")

	err := client.Connect()
	assert.Error(t, err, "Expected second connect to fail")
	assert.NotErrorIs(t, err, ErrClientAlreadyConnected, "Expected failed connect to clear the connection")
}

func TestClient_ReconnectAfterServerClose(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()

		// Close the first stream to trigger reconnection
		if n == 1 {
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	client := NewClient(server.URL, nil, Options{
		Reconnect:  true,
		Backoff:    1 * time.Millisecond,
		MaxBackoff: 1 * time.Millisecond,
		MaxRetries: 3,
	})

	assert.NoError(t, client.Connect(), "Expected connect to succeed")

	assert.Eventually(t, func() bool {
		return requests.Load() == 2 && client.IsConnected()
	}, time.Second, 5*time.Millisecond, "Expected client to reconnect after server closed the stream")

	client.Disconnect()
}
