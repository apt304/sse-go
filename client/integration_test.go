package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sse "github.com/apt304/sse-go/server"
	"github.com/stretchr/testify/assert"
)

func TestClientIntegration(t *testing.T) {
	// Create SSE subscriber
	subscriber := sse.NewDropOldestSubscriber(sse.Options{
		Buffer:            10,
		HeartbeatInterval: 100 * time.Millisecond, // Fast heartbeat for testing
	})

	// Create SSE server
	sseServer := sse.NewServer(subscriber)

	// The server runs lifecycle hooks in their own goroutines. Wait on them instead of sleeping.
	connected := make(chan struct{}, 2)
	disconnected := make(chan struct{}, 2)

	// Create HTTP server with SSE endpoint
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hooks := sse.LifecycleHooks{
			OnConnect: func(sub sse.Subscription) {
				t.Logf("SSE connection established for topics: %v", sub.Topics)
				connected <- struct{}{}
			},
			OnDisconnect: func(sub sse.Subscription) {
				t.Logf("SSE connection closed for topics: %v", sub.Topics)
				disconnected <- struct{}{}
			},
		}
		sseServer.ServeHTTP(w, r, []string{"test"}, hooks)
	}))

	// Create client
	client := NewClient(httpServer.URL+"/events", []string{"test"})

	// Forward published events. Heartbeats have no ID.
	received := make(chan sse.Event, 2)
	client.OnMessage(func(event sse.Event) {
		t.Logf("Received message event: type=%s, data=%s, isHeartbeat=%v", event.Type, event.Data, event.IsHeartbeat())
		if event.ID != "" {
			received <- event
		}
	})

	wait := func(ch <-chan struct{}, msg string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal(msg)
		}
	}

	expectEvent := func(want sse.Event) {
		t.Helper()
		select {
		case event := <-received:
			assert.Equal(t, want, event, "Expected to receive the published event")
		case <-time.After(5 * time.Second):
			t.Fatalf("Expected to receive event %s", want.ID)
		}
	}

	// Connect and wait for the server to register the subscriber
	if err := client.Connect(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	wait(connected, "Expected server to accept the connection")

	testEvent := sse.Event{
		ID:   "test-1",
		Type: "message",
		Data: []byte("Hello from SSE server!"),
	}
	assert.NoError(t, sseServer.Publish(testEvent, "test"), "Failed to publish event")
	expectEvent(testEvent)

	client.Disconnect()

	// Reconnect
	if err := client.Connect(); err != nil {
		t.Fatalf("Failed to reconnect: %v", err)
	}
	wait(connected, "Expected server to accept the reconnection")

	testEvent = sse.Event{
		ID:   "test-2",
		Type: "message",
		Data: []byte("Hello from SSE server! (reconnected)"),
	}
	assert.NoError(t, sseServer.Publish(testEvent, "test"), "Failed to publish event")
	expectEvent(testEvent)

	client.Disconnect()

	// Shutdown servers
	httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	assert.NoError(t, subscriber.Shutdown(ctx), "Failed to shutdown SSE subscriber")

	wait(disconnected, "Expected OnDisconnect for the first connection")
	wait(disconnected, "Expected OnDisconnect for the second connection")
}
