package sse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServer_ServeHTTP(t *testing.T) {

	var tests = []struct {
		name          string
		newSubscriber func() Subscriber
	}{
		{
			name: "drop oldest subscriber",
			newSubscriber: func() Subscriber {
				return NewDropOldestSubscriber(Options{Buffer: 10})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("single subscription", func(t *testing.T) {
				subscriber := tt.newSubscriber()
				assert.NotNil(t, subscriber)

				server := NewServer(subscriber)
				assert.NotNil(t, server)

				openedWg := sync.WaitGroup{}
				openedWg.Add(1)
				closedWg := sync.WaitGroup{}
				closedWg.Add(1)

				onConnect := func(sub Subscription) {
					assert.Equal(t, []string{"test"}, sub.Topics)
					openedWg.Done()
				}
				onDisconnect := func(sub Subscription) {
					assert.Equal(t, []string{"test"}, sub.Topics)
					closedWg.Done()
				}

				go func() {
					req := httptest.NewRequest("GET", "/events", nil)
					w := httptest.NewRecorder()
					server.ServeHTTP(w, req, []string{"test"}, LifecycleHooks{
						OnConnect:    onConnect,
						OnDisconnect: onDisconnect,
					})
				}()

				openedWg.Wait()

				err := server.Shutdown(context.Background())
				assert.NoError(t, err)

				closedWg.Wait()
			})

			t.Run("multiple subscriptions", func(t *testing.T) {
				subscriber := tt.newSubscriber()
				assert.NotNil(t, subscriber)

				server := NewServer(subscriber)
				assert.NotNil(t, server)

				// Set up three subscriptions with different topic combinations
				openedWg := sync.WaitGroup{}
				openedWg.Add(3)
				closedWg := sync.WaitGroup{}
				closedWg.Add(3)

				// Create recorders to capture responses
				recorder1 := httptest.NewRecorder()
				recorder2 := httptest.NewRecorder()
				recorder3 := httptest.NewRecorder()

				// Subscriber 1: topics A, B
				go func() {
					req := httptest.NewRequest("GET", "/events", nil)
					server.ServeHTTP(recorder1, req, []string{"A", "B"}, LifecycleHooks{
						OnConnect: func(sub Subscription) {
							assert.Equal(t, []string{"A", "B"}, sub.Topics)
							openedWg.Done()
						},
						OnDisconnect: func(sub Subscription) {
							assert.Equal(t, []string{"A", "B"}, sub.Topics)
							closedWg.Done()
						},
					})
				}()

				// Subscriber 2: topics A, C
				go func() {
					req := httptest.NewRequest("GET", "/events", nil)
					server.ServeHTTP(recorder2, req, []string{"A", "C"}, LifecycleHooks{
						OnConnect: func(sub Subscription) {
							assert.Equal(t, []string{"A", "C"}, sub.Topics)
							openedWg.Done()
						},
						OnDisconnect: func(sub Subscription) {
							assert.Equal(t, []string{"A", "C"}, sub.Topics)
							closedWg.Done()
						},
					})
				}()

				// Subscriber 3: topics B, C
				go func() {
					req := httptest.NewRequest("GET", "/events", nil)
					server.ServeHTTP(recorder3, req, []string{"B", "C"}, LifecycleHooks{
						OnConnect: func(sub Subscription) {
							assert.Equal(t, []string{"B", "C"}, sub.Topics)
							openedWg.Done()
						},
						OnDisconnect: func(sub Subscription) {
							assert.Equal(t, []string{"B", "C"}, sub.Topics)
							closedWg.Done()
						},
					})
				}()

				// Wait for all subscriptions to be established
				openedWg.Wait()

				// Send messages to different topics
				// Message to topic A: should be received by sub1 and sub2
				err := server.Publish(Event{Data: []byte("message-A")}, "A")
				assert.NoError(t, err)

				// Message to topic B: should be received by sub1 and sub3
				err = server.Publish(Event{Data: []byte("message-B")}, "B")
				assert.NoError(t, err)

				// Message to topic C: should be received by sub2 and sub3
				err = server.Publish(Event{Data: []byte("message-C")}, "C")
				assert.NoError(t, err)

				// Message to topics A and B: should be received by sub1 and sub2
				err = server.Publish(Event{Data: []byte("message-AB")}, "A", "B")
				assert.NoError(t, err)

				// Message to topics A and C: should be received by sub1 and sub3
				err = server.Publish(Event{Data: []byte("message-AC")}, "A", "C")
				assert.NoError(t, err)

				// Message to topics B and C: should be received by sub2 and sub3
				err = server.Publish(Event{Data: []byte("message-BC")}, "B", "C")
				assert.NoError(t, err)

				// Message to all topics: should be received by all subscribers
				err = server.Publish(Event{Data: []byte("message-ABC")}, "A", "B", "C")
				assert.NoError(t, err)

				// Give more time for message processing and delivery
				time.Sleep(500 * time.Millisecond)

				// Shutdown and wait for cleanup
				err = server.Shutdown(context.Background())
				assert.NoError(t, err)

				closedWg.Wait()

				// Get final recorder contents after shutdown
				body1 := recorder1.Body.String()
				body2 := recorder2.Body.String()
				body3 := recorder3.Body.String()

				// Basic verification that at least some messages were received
				// Note: The current implementation may only capture the first message per subscriber
				// due to how httptest.Recorder works with long-lived connections
				assert.Contains(t, body1, "data: message-A\n\n", "Subscriber 1 should receive message-A")
				assert.Contains(t, body1, "data: message-B\n\n", "Subscriber 1 should receive message-B")
				assert.NotContains(t, body1, "data: message-C\n\n", "Subscriber 1 should not receive message-C")
				assert.Contains(t, body1, "data: message-AB\n\n", "Subscriber 1 should receive message-AB")
				assert.Contains(t, body1, "data: message-AC\n\n", "Subscriber 1 should receive message-AC")
				assert.Contains(t, body1, "data: message-BC\n\n", "Subscriber 1 should receive message-BC")
				assert.Contains(t, body1, "data: message-ABC\n\n", "Subscriber 1 should receive message-ABC")

				assert.Contains(t, body2, "data: message-A\n\n", "Subscriber 2 should receive message-A")
				assert.NotContains(t, body2, "data: message-B\n\n", "Subscriber 2 should not receive message-B")
				assert.Contains(t, body2, "data: message-C\n\n", "Subscriber 2 should receive message-C")
				assert.Contains(t, body1, "data: message-AB\n\n", "Subscriber 1 should receive message-AB")
				assert.Contains(t, body1, "data: message-AC\n\n", "Subscriber 1 should receive message-AC")
				assert.Contains(t, body1, "data: message-BC\n\n", "Subscriber 1 should receive message-BC")
				assert.Contains(t, body1, "data: message-ABC\n\n", "Subscriber 1 should receive message-ABC")

				assert.NotContains(t, body3, "data: message-A\n\n", "Subscriber 3 should not receive message-A")
				assert.Contains(t, body3, "data: message-B\n\n", "Subscriber 3 should receive message-B")
				assert.Contains(t, body3, "data: message-C\n\n", "Subscriber 3 should receive message-C")
				assert.Contains(t, body3, "data: message-AB\n\n", "Subscriber 3 should receive message-AB")
				assert.Contains(t, body3, "data: message-ABC\n\n", "Subscriber 3 should receive message-ABC")
				assert.Contains(t, body1, "data: message-AB\n\n", "Subscriber 1 should receive message-AB")
				assert.Contains(t, body1, "data: message-AC\n\n", "Subscriber 1 should receive message-AC")
				assert.Contains(t, body1, "data: message-BC\n\n", "Subscriber 1 should receive message-BC")
				assert.Contains(t, body1, "data: message-ABC\n\n", "Subscriber 1 should receive message-ABC")
			})

			t.Run("publish with no topics", func(t *testing.T) {
				subscriber := tt.newSubscriber()
				assert.NotNil(t, subscriber)

				server := NewServer(subscriber)
				assert.NotNil(t, server)

				err := server.Publish(Event{Data: []byte("message-A")})
				assert.ErrorIs(t, err, ErrNoTopics)
			})

			t.Run("non-flushable response writer", func(t *testing.T) {
				subscriber := tt.newSubscriber()
				assert.NotNil(t, subscriber)

				server := NewServer(subscriber)
				assert.NotNil(t, server)

				// Create a non-flushable response writer
				nonFlushableWriter := &nonFlushableResponseWriter{}

				req := httptest.NewRequest("GET", "/events", nil)
				hooks := LifecycleHooks{}

				// This should now succeed in setSSEHeaders but fail in rc.Flush()
				server.ServeHTTP(nonFlushableWriter, req, []string{"test"}, hooks)

				// Verify that the response status is set to 500 Internal Server Error
				assert.Equal(t, http.StatusInternalServerError, nonFlushableWriter.statusCode)

				// Verify that the error message is written
				body := nonFlushableWriter.body.String()
				assert.Contains(t, body, "unable to flush sse headers and connected comment on connection")
			})

			t.Run("flush error after headers", func(t *testing.T) {
				subscriber := tt.newSubscriber()
				assert.NotNil(t, subscriber)

				server := NewServer(subscriber)
				assert.NotNil(t, server)

				// Create a response writer that implements Flusher but fails on flush
				flushErrorWriter := &flushErrorResponseWriter{}

				req := httptest.NewRequest("GET", "/", nil)
				hooks := LifecycleHooks{}

				// This should fail when trying to flush the headers
				server.ServeHTTP(flushErrorWriter, req, []string{"test"}, hooks)

				// Verify that the response status is set to 500 Internal Server Error
				assert.Equal(t, http.StatusInternalServerError, flushErrorWriter.statusCode)

				// Verify that the error message is written
				body := flushErrorWriter.body.String()
				assert.Contains(t, body, "unable to flush sse headers and connected comment on connection")
			})

			t.Run("subscribe error", func(t *testing.T) {
				subscriber := tt.newSubscriber()
				assert.NotNil(t, subscriber)

				// Close the subscriber before calling Subscribe to make it fail
				err := subscriber.Shutdown(context.Background())
				assert.NoError(t, err)

				server := NewServer(subscriber)
				assert.NotNil(t, server)

				testResponseWriter := &testResponseWriter{}
				req := httptest.NewRequest("GET", "/events", nil)
				server.ServeHTTP(testResponseWriter, req, []string{"test"}, LifecycleHooks{})

				// Verify that the response status is set to 500 Internal Server Error
				assert.Equal(t, http.StatusInternalServerError, testResponseWriter.statusCode)

				// Verify that the error message is written
				assert.Contains(t, testResponseWriter.body.String(), "subscriber closed")
			})
		})
	}
}

func TestServer_SSEProtocol(t *testing.T) {
	t.Run("basic SSE headers", func(t *testing.T) {
		// Create test server
		subscriber := NewDropOldestSubscriber(Options{})
		server := NewServer(subscriber)

		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)

		connected := make(chan struct{})
		disconnected := make(chan struct{})
		hooks := LifecycleHooks{
			OnConnect: func(sub Subscription) {
				close(connected)
			},
			OnDisconnect: func(sub Subscription) {
				close(disconnected)
			},
		}

		go server.ServeHTTP(w, req, []string{"test-topic"}, hooks)

		<-connected

		server.Shutdown(context.Background())

		<-disconnected

		result := w.Result()

		// Check headers
		contentType := result.Header.Get("Content-Type")
		assert.Equal(t, "text/event-stream", contentType)

		cacheControl := result.Header.Get("Cache-Control")
		assert.Equal(t, "no-cache", cacheControl)
	})

	t.Run("header passthrough", func(t *testing.T) {
		// Test that additional headers set before ServeHTTP are preserved
		subscriber := NewDropOldestSubscriber(Options{})
		server := NewServer(subscriber)

		w := httptest.NewRecorder()
		w.Header().Set("X-Custom-Header", "custom-value")
		w.Header().Set("X-Another-Header", "another-value")
		w.Header().Set("Server", "custom-server")

		req := httptest.NewRequest("GET", "/", nil)

		connected := make(chan struct{})
		disconnected := make(chan struct{})
		hooks := LifecycleHooks{
			OnConnect: func(sub Subscription) {
				close(connected)
			},
			OnDisconnect: func(sub Subscription) {
				close(disconnected)
			},
		}

		go server.ServeHTTP(w, req, []string{"test-topic"}, hooks)

		<-connected

		server.Shutdown(context.Background())

		<-disconnected

		result := w.Result()

		// Verify that custom headers are preserved
		customHeader := result.Header.Get("X-Custom-Header")
		assert.Equal(t, "custom-value", customHeader)

		anotherHeader := result.Header.Get("X-Another-Header")
		assert.Equal(t, "another-value", anotherHeader)

		// Verify that SSE headers are still set correctly
		contentType := result.Header.Get("Content-Type")
		assert.Equal(t, "text/event-stream", contentType)

		cacheControl := result.Header.Get("Cache-Control")
		assert.Equal(t, "no-cache", cacheControl)

		// Verify that the custom Server header is preserved (not overridden)
		serverHeader := result.Header.Get("Server")
		assert.Equal(t, "custom-server", serverHeader)
	})
}

func TestServer_HTTPCompatibility(t *testing.T) {
	subscriber := NewDropOldestSubscriber(Options{})
	server := NewServer(subscriber)

	// Test HTTP/1.1
	t.Run("HTTP/1.1", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/events", nil)
		req.ProtoMajor = 1
		w := httptest.NewRecorder()

		server.setSSEHeaders(w, req)

		connection := w.Header().Get("Connection")
		assert.Equal(t, "keep-alive", connection)

		keepAlive := w.Header().Get("Keep-Alive")
		assert.Equal(t, "timeout=60", keepAlive)
	})

	// Test HTTP/2
	t.Run("HTTP/2", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/events", nil)
		req.ProtoMajor = 2
		w := httptest.NewRecorder()

		server.setSSEHeaders(w, req)

		connection := w.Header().Get("Connection")
		assert.Empty(t, connection, "Expected no Connection header for HTTP/2")

		keepAlive := w.Header().Get("Keep-Alive")
		assert.Empty(t, keepAlive, "Expected no Keep-Alive header for HTTP/2")
	})
}

func TestLastEventID(t *testing.T) {
	subscriber := NewDropOldestSubscriber(Options{})
	server := NewServer(subscriber)

	// Create request with Last-Event-ID header
	req := httptest.NewRequest("GET", "/events", nil)
	req.Header.Set("Last-Event-ID", "test-123")
	w := httptest.NewRecorder()

	done := make(chan bool)
	go func() {
		hooks := LifecycleHooks{
			OnConnect: func(sub Subscription) {
				assert.Equal(t, "test-123", sub.LastEventID)
				done <- true
			},
		}
		server.ServeHTTP(w, req, []string{"test-topic"}, hooks)
	}()

	// Cancel request by creating a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	cancel()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for connection")
	}
}

// nonFlushableResponseWriter is a response writer that doesn't implement http.Flusher
type nonFlushableResponseWriter struct {
	statusCode int
	body       strings.Builder
	headers    http.Header
}

func (w *nonFlushableResponseWriter) Header() http.Header {
	if w.headers == nil {
		w.headers = make(http.Header)
	}
	return w.headers
}

func (w *nonFlushableResponseWriter) Write(data []byte) (int, error) {
	return w.body.Write(data)
}

func (w *nonFlushableResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

// flushErrorResponseWriter is a response writer that implements http.Flusher but fails on flush
type flushErrorResponseWriter struct {
	statusCode int
	body       strings.Builder
	headers    http.Header
}

func (w *flushErrorResponseWriter) Header() http.Header {
	if w.headers == nil {
		w.headers = make(http.Header)
	}
	return w.headers
}

func (w *flushErrorResponseWriter) Write(data []byte) (int, error) {
	return w.body.Write(data)
}

func (w *flushErrorResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func (w *flushErrorResponseWriter) Flush() error {
	return fmt.Errorf("unable to flush sse headers on connection")
}

type testResponseWriter struct {
	statusCode int
	body       strings.Builder
	headers    http.Header
}

func (w *testResponseWriter) Header() http.Header {
	if w.headers == nil {
		w.headers = make(http.Header)
	}
	return w.headers
}

func (w *testResponseWriter) Write(data []byte) (int, error) {
	return w.body.Write(data)
}

func (w *testResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func (w *testResponseWriter) Flush() {}
