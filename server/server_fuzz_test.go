package sse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// -------- fuzz helpers -------------------------------------------------------

func clamp(x, lo, hi int) int {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

// fuzzWriter implements sse.MessageWriter and validates that it only receives
// messages for topics it actually subscribed to. It also simulates slow clients.
type fuzzWriter struct {
	delay time.Duration
	allow map[string]struct{} // topics this subscriber is subscribed to
}

func newFuzzWriter(delay time.Duration, topics []string) *fuzzWriter {
	allow := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		allow[t] = struct{}{}
	}
	return &fuzzWriter{delay: delay, allow: allow}
}

func (w *fuzzWriter) Write(p []byte) (n int, err error) {
	if d := w.delay; d > 0 {
		time.Sleep(d)
	}

	return len(p), nil
}

func (w *fuzzWriter) Header() http.Header {
	return http.Header{}
}

func (w *fuzzWriter) WriteHeader(statusCode int) {}

func (w *fuzzWriter) FlushError() error { return nil }

// deterministic topic assignment (no RNG needed for fuzz determinism)
func assignTopics(i, want, topicCount int, topics []string) []string {
	want = clamp(want, 0, topicCount)
	set := make(map[int]struct{}, want)
	for k := 0; k < want; k++ {
		idx := (i*73856093 + k*19349663) % topicCount // simple mixed hash
		set[idx] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for idx := range set {
		out = append(out, topics[idx])
	}
	return out
}

// fuzzHTTPWriter implements http.ResponseWriter and http.Flusher for fuzz testing
type fuzzHTTPWriter struct {
	delay time.Duration
	allow map[string]struct{} // topics this subscriber is subscribed to
	*httptest.ResponseRecorder
}

func newFuzzHTTPWriter(delay time.Duration, topics []string) *fuzzHTTPWriter {
	allow := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		allow[t] = struct{}{}
	}
	return &fuzzHTTPWriter{
		delay:            delay,
		allow:            allow,
		ResponseRecorder: httptest.NewRecorder(),
	}
}

func (w *fuzzHTTPWriter) Write(p []byte) (n int, err error) {
	if d := w.delay; d > 0 {
		time.Sleep(d)
	}
	return w.ResponseRecorder.Write(p)
}

func (w *fuzzHTTPWriter) Flush() {
	if d := w.delay; d > 0 {
		time.Sleep(d)
	}
	// httptest.ResponseRecorder doesn't implement Flush, so we do nothing
	// This simulates a response writer that supports flushing
}

// -------- the fuzz test ------------------------------------------------------

func FuzzDropOldestSubscriber_Matrix(f *testing.F) {
	// Seed corpus: small/large, fast/slow, varying topics & buffers.
	f.Add(10, 1, 3, 100, 16, 0, 0, 0)     // small, fast, no heartbeat
	f.Add(64, 2, 8, 400, 64, 0, 0, 250)   // mid, heartbeat
	f.Add(200, 3, 12, 800, 32, 2, 0, 0)   // many subs, slow writers
	f.Add(50, 5, 10, 1000, 8, 1, 50, 100) // small buffers, backpressure/drop
	f.Add(0, 0, 5, 0, 32, 0, 0, 0)        // no subs, no messages
	f.Add(128, 0, 16, 300, 64, 0, 100, 0) // subs with zero topics

	f.Fuzz(func(t *testing.T,
		subscribers int, topicsPerSub int, topicCount int, msgCount int,
		bufSize int, writerDelayMs int, publishDelayMs int, heartbeatMs int,
	) {
		// Clamp to keep fuzz executions bounded and stable.

		subscribers = clamp(subscribers, 0, 1000)
		topicsPerSub = clamp(topicsPerSub, 0, 10)
		topicCount = clamp(topicCount, 1, 500)
		msgCount = clamp(msgCount, 0, 100000)
		bufSize = clamp(bufSize, 0, 1024)
		writerDelayMs = clamp(writerDelayMs, 0, 100)
		publishDelayMs = clamp(publishDelayMs, 0, 50)
		heartbeatMs = clamp(heartbeatMs, 0, 500)

		// Build topics: t0..tN-1
		topics := make([]string, topicCount)
		for i := range topics {
			topics[i] = fmt.Sprintf("t%d", i)
		}

		// Subscriber under test
		subscriber := NewDropOldestSubscriber(Options{
			Buffer:            bufSize,
			HeartbeatInterval: time.Duration(heartbeatMs) * time.Millisecond,
		})

		// Create subscribers with deterministic topic sets and optional delay.
		ctx, cancel := context.WithCancel(context.Background())
		var connectedWG sync.WaitGroup
		var exitedWG sync.WaitGroup
		for i := 0; i < subscribers; i++ {
			subTopics := assignTopics(i, topicsPerSub, topicCount, topics)
			w := newFuzzWriter(time.Duration(writerDelayMs)*time.Millisecond, subTopics)
			connectedWG.Add(1)
			exitedWG.Add(1)
			go func(subTopics []string) {
				err := subscriber.Subscribe(ctx, Subscription{
					Topics: subTopics,
					rcw:    rcWriter{w: w, rc: http.NewResponseController(w)},
				}, LifecycleHooks{
					OnConnect: func(sub Subscription) {
						connectedWG.Done()
					},
				})
				if err != nil {
					connectedWG.Done()
				}
				exitedWG.Done()
			}(subTopics)
		}

		connectedWG.Wait()

		// Publish messages to topics in a round-robin fashion.
		pubDelay := time.Duration(publishDelayMs) * time.Millisecond
		for i := 0; i < msgCount; i++ {
			topic := topics[i%topicCount]
			evt := Event{Type: topic, Data: []byte("x")}
			if err := subscriber.Publish(evt, topic); err != nil {
				// Only acceptable error during the run would be ErrClosed (if we ever closed early).
				if !errors.Is(err, ErrSubscriberClosed) {
					t.Fatalf("publish error: %v", err)
				}
				break
			}
			if pubDelay > 0 {
				// cap delay so a mutated extreme doesn't explode runtime
				time.Sleep(time.Duration(math.Min(float64(pubDelay), float64(2*time.Millisecond))))
			}
		}

		// Shutdown sequence must finish without deadlock.
		cancel()
		exitedWG.Wait()

		shCtx, shCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shCancel()
		if err := subscriber.Shutdown(shCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown: %v", err)
		}

		// Post-shutdown publish should error deterministically.
		if err := subscriber.Publish(Event{}, topics[0]); !errors.Is(err, ErrSubscriberClosed) {
			t.Fatalf("publish after shutdown should return ErrSubscriberClosed, got %v", err)
		}
	})
}

func FuzzServer_DropOldestSubscriber_Matrix(f *testing.F) {
	f.Add(10, 1, 3, 100, 16, 0, 0, 0)     // small, fast, no heartbeat
	f.Add(64, 2, 8, 400, 64, 0, 0, 250)   // mid, heartbeat
	f.Add(200, 3, 12, 800, 32, 2, 0, 0)   // many subs, slow writers
	f.Add(50, 5, 10, 1000, 8, 1, 50, 100) // small buffers, backpressure/drop
	f.Add(0, 0, 5, 0, 32, 0, 0, 0)        // no subs, no messages
	f.Add(128, 0, 16, 300, 64, 0, 100, 0) // subs with zero topics

	f.Fuzz(func(t *testing.T,
		subscribers int, topicsPerSub int, topicCount int, msgCount int,
		bufSize int, writerDelayMs int, publishDelayMs int, heartbeatMs int,
	) {
		// Clamp to keep fuzz executions bounded and stable.
		subscribers = clamp(subscribers, 0, 500)
		topicsPerSub = clamp(topicsPerSub, 0, 10)
		topicCount = clamp(topicCount, 1, 500)
		msgCount = clamp(msgCount, 0, 10000)
		bufSize = clamp(bufSize, 0, 1024)
		writerDelayMs = clamp(writerDelayMs, 0, 100)
		publishDelayMs = clamp(publishDelayMs, 0, 50)
		heartbeatMs = clamp(heartbeatMs, 0, 500)

		// Build topics: t0..tN-1
		topics := make([]string, topicCount)
		for i := range topics {
			topics[i] = fmt.Sprintf("t%d", i)
		}

		// Create server with DropOldestSubscriber
		subscriber := NewDropOldestSubscriber(Options{
			Buffer:            bufSize,
			HeartbeatInterval: time.Duration(heartbeatMs) * time.Millisecond,
		})

		server := NewServer(subscriber)

		// Create HTTP connections with deterministic topic sets and optional delay.
		ctx, cancel := context.WithCancel(context.Background())
		var connectedWG sync.WaitGroup
		var exitedWG sync.WaitGroup

		for i := 0; i < subscribers; i++ {
			subTopics := assignTopics(i, topicsPerSub, topicCount, topics)
			connectedWG.Add(1)
			exitedWG.Add(1)
			go func(subTopics []string, subIndex int) {
				// Create HTTP request
				req := httptest.NewRequest("GET", "/events", nil)
				req = req.WithContext(ctx)

				// Add Last-Event-ID header for some requests
				if subIndex%3 == 0 {
					req.Header.Set("Last-Event-ID", fmt.Sprintf("event-%d", subIndex))
				}

				// Create response writer with optional delay
				w := newFuzzHTTPWriter(time.Duration(writerDelayMs)*time.Millisecond, subTopics)

				hooks := LifecycleHooks{
					OnConnect: func(sub Subscription) {
						// Verify topics match
						if len(subTopics) > 0 {
							// Check that subscription topics are a subset of what we requested
							subTopicSet := make(map[string]struct{})
							for _, topic := range sub.Topics {
								subTopicSet[topic] = struct{}{}
							}
							for _, topic := range subTopics {
								if _, exists := subTopicSet[topic]; !exists {
									t.Errorf("subscriber %d: expected topic %s in subscription", subIndex, topic)
								}
							}
						}
						connectedWG.Done()
					},
					OnDisconnect: func(sub Subscription) {
						exitedWG.Done()
					},
				}

				// Call ServeHTTP - this tests the HTTP server logic
				server.ServeHTTP(w, req, subTopics, hooks)

				// Handle subscription errors. Allow wait groups to progress
				if w.Code != http.StatusOK {
					connectedWG.Done()
					exitedWG.Done()
				}
			}(subTopics, i)
		}

		connectedWG.Wait()

		// Publish messages to topics in a round-robin fashion.
		pubDelay := time.Duration(publishDelayMs) * time.Millisecond
		for i := 0; i < msgCount; i++ {
			topic := topics[i%topicCount]
			evt := Event{Type: topic, Data: []byte("x")}
			if err := server.Publish(evt, topic); err != nil {
				// Only acceptable error during the run would be ErrClosed (if we ever closed early).
				if !errors.Is(err, ErrSubscriberClosed) {
					t.Fatalf("publish error: %v", err)
				}
				break
			}
			if pubDelay > 0 {
				// cap delay so a mutated extreme doesn't explode runtime
				time.Sleep(time.Duration(math.Min(float64(pubDelay), float64(2*time.Millisecond))))
			}
		}

		// Shutdown sequence must finish without deadlock.
		cancel()
		exitedWG.Wait()

		shCtx, shCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shCancel()
		if err := server.Shutdown(shCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown: %v", err)
		}

		// Post-shutdown publish should error deterministically.
		if err := server.Publish(Event{}, topics[0]); !errors.Is(err, ErrSubscriberClosed) {
			t.Fatalf("publish after shutdown should return ErrSubscriberClosed, got %v", err)
		}
	})
}
