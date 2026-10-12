package sse

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// testWriter implements sse.MessageWriter for testing
type trackingWriter struct {
	received      []Event
	mu            sync.Mutex
	expected      int64 // Track expected message count
	receivedCount int64 // Track actual received count
	closed        bool
}

func newTrackingWriter(expected ...int) *trackingWriter {
	exp := int64(0)
	if len(expected) > 0 {
		exp = int64(expected[0])
	}
	return &trackingWriter{
		expected: exp,
	}
}

func (w *trackingWriter) Header() http.Header {
	return http.Header{}
}

func (w *trackingWriter) WriteHeader(statusCode int) {}

func (w *trackingWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	closed := w.closed
	w.mu.Unlock()

	if closed {
		return 0, fmt.Errorf("writer closed")
	}

	w.mu.Lock()
	w.receivedCount++
	w.mu.Unlock()

	return len(p), nil
}

func (w *trackingWriter) FlushError() error {
	return nil
}

func (w *trackingWriter) Close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
}

func (w *trackingWriter) GetReceivedCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return int(w.receivedCount)
}

func (w *trackingWriter) GetReceivedMessages() []Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]Event, len(w.received))
	copy(result, w.received)
	return result
}

func (w *trackingWriter) GetDropCount() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.expected == 0 {
		return 0
	}

	return w.expected - w.receivedCount
}

// Benchmark tests for performance
func BenchmarkDropOldestSubscriber(b *testing.B) {
	scenarios := []struct {
		name     string
		buffer   int
		subCount int
	}{
		{"Buffer_1_Sub_1", 1, 1},
		{"Buffer_1_Sub_10", 1, 10},
		{"Buffer_1_Sub_100", 1, 100},
		{"Buffer_1_Sub_1000", 1, 1000},
		{"Buffer_10_Sub_1", 10, 1},
		{"Buffer_10_Sub_10", 10, 10},
		{"Buffer_10_Sub_100", 10, 100},
		{"Buffer_10_Sub_1000", 10, 1000},
		{"Buffer_100_Sub_1", 100, 1},
		{"Buffer_100_Sub_10", 100, 10},
		{"Buffer_100_Sub_100", 100, 100},
		{"Buffer_100_Sub_1000", 100, 1000},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			subscriber := NewDropOldestSubscriber(Options{
				Buffer: scenario.buffer,
			})

			defer subscriber.Shutdown(context.Background())

			// Create subscribers with drop tracking
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var connectedWG sync.WaitGroup
			var exitedWG sync.WaitGroup
			writers := make([]*trackingWriter, scenario.subCount)

			noop := 0

			for i := 0; i < scenario.subCount; i++ {
				connectedWG.Add(1)
				exitedWG.Add(1)
				writers[i] = newTrackingWriter() // No delay, will set expected count later
				go func(writer *trackingWriter) {
					_ = subscriber.Subscribe(ctx, Subscription{
						Topics: []string{"topic-1"},
						rcw:    rcWriter{w: writer, rc: http.NewResponseController(writer)},
					}, LifecycleHooks{
						OnConnect: func(sub Subscription) {
							connectedWG.Done()
						},
						OnDisconnect: func(sub Subscription) {
							// Do nothing
							noop++
						},
					})
					exitedWG.Done()
				}(writers[i])
			}

			// Let subscriptions register
			connectedWG.Wait()

			// Prebuild message
			evt := Event{
				Type: "test",
				Data: []byte("test-data"),
			}
			topics := []string{"topic-1"}

			b.ReportAllocs()

			// Publish messages
			for b.Loop() {
				time.Sleep(20 * time.Microsecond)
				if err := subscriber.Publish(evt, topics...); err != nil {
					b.Fatalf("publish error: %v", err)
				}
			}

			time.Sleep(1 * time.Second)

			// Tear down
			cancel()
			exitedWG.Wait()

			// Calculate and report drop rate
			var totalReceived int64
			for _, writer := range writers {
				totalReceived += int64(writer.GetReceivedCount())
			}
			expectedMessages := int64(b.N * scenario.subCount)
			dropRate := float64(expectedMessages-totalReceived) / float64(expectedMessages)

			b.ReportMetric(dropRate*100, "drop_rate_%")
		})
	}
}
