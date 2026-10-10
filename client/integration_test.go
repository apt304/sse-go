package client

import (
	"context"
	"fmt"
	"net/http"
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

	// Create HTTP server with SSE endpoint
	port := "8888"
	httpServer := &http.Server{
		Addr: ":" + port,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/events" {
				// Handle SSE connection
				topics := []string{"test"}
				hooks := sse.LifecycleHooks{
					OnConnect: func(sub sse.Subscription) {
						t.Logf("SSE connection established for topics: %v", sub.Topics)
					},
					OnDisconnect: func(sub sse.Subscription) {
						t.Logf("SSE connection closed for topics: %v", sub.Topics)
					},
				}
				sseServer.ServeHTTP(w, r, topics, hooks)
			} else {
				http.NotFound(w, r)
			}
		}),
	}

	// Start HTTP server in background
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			assert.Fail(t, "HTTP server error", err)
		}
	}()

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)

	// Create client
	client := NewClient(fmt.Sprintf("http://localhost:%s/events", port), []string{"test"})

	// Set up event handlers
	var receivedEvents []sse.Event
	var receivedMessage bool

	client.OnMessage(func(event sse.Event) {
		receivedMessage = true
		receivedEvents = append(receivedEvents, event)
		t.Logf("Received message event: type=%s, data=%s, isHeartbeat=%v", event.Type, event.Data, event.IsHeartbeat())
	})

	// Connect
	if err := client.Connect(); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Wait a bit for connection to establish
	time.Sleep(100 * time.Millisecond)

	// Publish some test events
	testEvent := sse.Event{
		ID:   "test-1",
		Type: "message",
		Data: []byte("Hello from SSE server!"),
	}

	assert.NoError(t, sseServer.Publish(testEvent, "test"), "Failed to publish event")

	// Wait for events to be processed
	time.Sleep(200 * time.Millisecond)

	// Disconnect
	client.Disconnect()

	// Reconnect
	if err := client.Connect(); err != nil {
		t.Fatalf("Failed to reconnect: %v", err)
	}

	// Wait a bit for reconnection to establish
	time.Sleep(100 * time.Millisecond)

	// Publish another test event
	testEvent = sse.Event{
		ID:   "test-2",
		Type: "message",
		Data: []byte("Hello from SSE server! (reconnected)"),
	}

	assert.NoError(t, sseServer.Publish(testEvent, "test"), "Failed to publish event")

	// Wait for events to be processed
	time.Sleep(200 * time.Millisecond)

	// Disconnect
	client.Disconnect()

	// Shutdown servers
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	assert.NoError(t, httpServer.Shutdown(ctx), "Failed to shutdown HTTP server")
	assert.NoError(t, subscriber.Shutdown(ctx), "Failed to shutdown SSE subscriber")

	// Verify we received events
	assert.True(t, receivedMessage, "Expected to receive message event")
	assert.GreaterOrEqual(t, len(receivedEvents), 1, "Expected at least 1 event")

	// Verify the specific event we published
	foundTestEvent := false
	for _, event := range receivedEvents {
		if event.ID == "test-1" && event.Type == "message" && string(event.Data) == "Hello from SSE server!" {
			foundTestEvent = true
			break
		}
	}

	assert.True(t, foundTestEvent, "Expected to receive the test event we published",
		"Received events: %+v", receivedEvents)
}
