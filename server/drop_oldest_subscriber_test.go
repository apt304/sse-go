package sse

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type testWriter struct {
	mu   *sync.Mutex
	data [][]byte
}

func NewTestWriter() *testWriter {
	return &testWriter{
		mu: &sync.Mutex{},
	}
}

func (t *testWriter) Write(p []byte) (n int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data = append(t.data, p)
	return len(p), nil
}

// Data returns a copy of the written data.
func (t *testWriter) Data() [][]byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([][]byte(nil), t.data...)
}

func (t *testWriter) FlushError() error {
	return nil
}

func (t *testWriter) Header() http.Header {
	return http.Header{}
}

func (t *testWriter) WriteHeader(_ int) {}

type testSyncWriter struct {
	mu   *sync.Mutex
	wg   *sync.WaitGroup
	data [][]byte
}

func NewTestSyncWriter() *testSyncWriter {
	return &testSyncWriter{
		mu: &sync.Mutex{},
		wg: &sync.WaitGroup{},
	}
}

func (t *testSyncWriter) Write(p []byte) (n int, err error) {
	defer t.wg.Done()

	t.mu.Lock()
	t.data = append(t.data, p)
	t.mu.Unlock()

	return len(p), nil
}

func (t *testSyncWriter) FlushError() error {
	return nil
}

func (t *testSyncWriter) Header() http.Header {
	return http.Header{}
}

func (t *testSyncWriter) WriteHeader(_ int) {}

type errorWriter struct {
	returnsSendError  bool
	returnsFlushError bool
}

func (e errorWriter) Write(_ []byte) (int, error) {
	if e.returnsSendError {
		return 0, fmt.Errorf("send error")
	}
	return 0, nil
}

func (e errorWriter) FlushError() error {
	if e.returnsFlushError {
		return fmt.Errorf("flush error")
	}
	return nil
}

func (e errorWriter) Header() http.Header {
	return http.Header{}
}

func (e errorWriter) WriteHeader(_ int) {}

// gateWriter blocks every Write until release is closed or a write deadline is set. started receives a value
// when a Write begins.
type gateWriter struct {
	started      chan struct{}
	release      chan struct{}
	deadline     chan struct{}
	deadlineOnce sync.Once
}

func newGateWriter() *gateWriter {
	return &gateWriter{
		started:  make(chan struct{}, 100),
		release:  make(chan struct{}),
		deadline: make(chan struct{}),
	}
}

func (w *gateWriter) Write(p []byte) (int, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}

	select {
	case <-w.release:
		return len(p), nil
	case <-w.deadline:
		return 0, os.ErrDeadlineExceeded
	}
}

// SetWriteDeadline fails blocked and future writes, as a connection does once its deadline passes.
func (w *gateWriter) SetWriteDeadline(_ time.Time) error {
	w.deadlineOnce.Do(func() {
		close(w.deadline)
	})
	return nil
}

func (w *gateWriter) FlushError() error {
	return nil
}

func (w *gateWriter) Header() http.Header {
	return http.Header{}
}

func (w *gateWriter) WriteHeader(_ int) {}

// subscribeAsync runs Subscribe in a goroutine and waits for OnConnect. The returned channel is closed when
// Subscribe returns. hooks.OnConnect, if set, runs before the connection is reported.
func subscribeAsync(t *testing.T, ctx context.Context, subscriber Subscriber, w http.ResponseWriter, hooks LifecycleHooks) <-chan struct{} {
	t.Helper()

	connected := make(chan struct{})
	onConnect := hooks.OnConnect
	hooks.OnConnect = func(sub Subscription) {
		close(connected)
		if onConnect != nil {
			onConnect(sub)
		}
	}

	subDone := make(chan struct{})
	go func() {
		assert.NoError(t, subscriber.Subscribe(ctx, Subscription{
			Topics: []string{"test"},
			rcw:    rcWriter{w: w, rc: http.NewResponseController(w)},
		}, hooks))
		close(subDone)
	}()

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected subscriber to connect")
	}
	return subDone
}

// waitClosed fails the test if ch is not closed within 5 seconds.
func waitClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

func TestDropOldestSubscriber_New(t *testing.T) {
	t.Run("subscriber with default options", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})

		assert.NotNil(t, subscriber)
		assert.Equal(t, 0, subscriber.opts.Buffer)
		assert.Equal(t, time.Duration(0), subscriber.opts.HeartbeatInterval)
	})

	t.Run("subscriber with buffer", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{
			Buffer: 100,
		})

		assert.NotNil(t, subscriber)
		assert.Equal(t, 100, subscriber.opts.Buffer)
	})

	t.Run("subscriber with heartbeat interval", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{
			HeartbeatInterval: 100,
		})

		assert.NotNil(t, subscriber)
		assert.Equal(t, time.Duration(100), subscriber.opts.HeartbeatInterval)
	})

	t.Run("subscriber with buffer and heartbeat interval", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{
			Buffer:            100,
			HeartbeatInterval: 10,
		})

		assert.NotNil(t, subscriber)
		assert.Equal(t, 100, subscriber.opts.Buffer)
		assert.Equal(t, time.Duration(10), subscriber.opts.HeartbeatInterval)
	})

	t.Run("subscriber with negative buffer", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{
			Buffer: -1,
		})

		assert.NotNil(t, subscriber)
		assert.Equal(t, 0, subscriber.opts.Buffer)
	})

	t.Run("subscriber with negative heartbeat interval", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{
			HeartbeatInterval: -1,
		})

		assert.NotNil(t, subscriber)
		assert.Equal(t, time.Duration(0), subscriber.opts.HeartbeatInterval)
	})

	t.Run("subscriber with immediate shutdown", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)
	})
}

func TestDropOldestSubscriber_Subscribe(t *testing.T) {
	t.Run("subscribe errors with closed subscriber", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)

		err = subscriber.Subscribe(context.Background(), Subscription{}, LifecycleHooks{})
		assert.ErrorIs(t, err, ErrSubscriberClosed)
	})

	t.Run("subscribe errors with no topics", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		err := subscriber.Subscribe(context.Background(), Subscription{}, LifecycleHooks{})
		assert.ErrorIs(t, err, ErrNoTopics)
	})

	t.Run("subscribe and shutdown succeeds", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		subClosed := make(chan struct{})

		go func() {
			// Subscribe blocks. Call shutdown in the onConnection callback.
			subErr := subscriber.Subscribe(context.Background(), Subscription{
				Topics: []string{"test"},
			}, LifecycleHooks{
				OnConnect: func(sub Subscription) {
					err := subscriber.Shutdown(context.Background())
					assert.NoError(t, err)
				},
			})
			assert.NoError(t, subErr)
			close(subClosed)
		}()

		// Wait for the subscriber to close
		<-subClosed
	})

	t.Run("subscribe with heartbeat interval", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{
			HeartbeatInterval: 10,
		})
		assert.NotNil(t, subscriber)

		subClosed := make(chan struct{})

		go func() {
			// Subscribe blocks. Call shutdown in the onConnection callback.
			errWriter := errorWriter{}
			subErr := subscriber.Subscribe(context.Background(), Subscription{
				Topics: []string{"test"},
				rcw:    rcWriter{w: errWriter, rc: http.NewResponseController(errWriter)},
			}, LifecycleHooks{
				OnConnect: func(sub Subscription) {
					err := subscriber.Shutdown(context.Background())
					assert.NoError(t, err)
				},
			})
			assert.NoError(t, subErr)
			close(subClosed)
		}()

		<-subClosed
	})

	t.Run("subscribe exits when context is done", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		subErr := subscriber.Subscribe(ctx, Subscription{
			Topics: []string{"test"},
		}, LifecycleHooks{})
		assert.NoError(t, subErr)
	})

	t.Run("multiple subscribe calls", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		connectedWg := sync.WaitGroup{}
		connectedWg.Add(2)

		closedWg := sync.WaitGroup{}
		closedWg.Add(2)

		go func() {
			subErr := subscriber.Subscribe(context.Background(), Subscription{
				Topics: []string{"test"},
			}, LifecycleHooks{
				OnConnect: func(sub Subscription) {
					connectedWg.Done()
				},
				OnDisconnect: func(sub Subscription) {
					closedWg.Done()
				},
			})
			assert.NoError(t, subErr)
		}()

		go func() {
			subErr := subscriber.Subscribe(context.Background(), Subscription{
				Topics: []string{"test"},
			}, LifecycleHooks{
				OnConnect: func(sub Subscription) {
					connectedWg.Done()
				},
				OnDisconnect: func(sub Subscription) {
					closedWg.Done()
				},
			})
			assert.NoError(t, subErr)
		}()

		// Wait for subscribers to connect
		connectedWg.Wait()

		// Verify subscribers
		assert.Equal(t, 2, len(subscriber.subscribers))

		// Shutdown
		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)

		// Wait for subscribers to close
		closedWg.Wait()

		// Verify subscribers are removed
		assert.Equal(t, 0, len(subscriber.subscribers))
	})
}

func TestDropOldestSubscriber_Publish(t *testing.T) {
	t.Run("publish errors with closed subscriber", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)

		err = subscriber.Publish(Event{
			Data: []byte("test"),
		}, "test")
		assert.ErrorIs(t, err, ErrSubscriberClosed)
	})

	t.Run("publish errors with nil topics", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		err := subscriber.Publish(Event{
			Data: []byte("test"),
		})
		assert.ErrorIs(t, err, ErrNoTopics)
	})

	t.Run("publish errors with empty topics", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		err := subscriber.Publish(Event{
			Data: []byte("test"),
		})
		assert.ErrorIs(t, err, ErrNoTopics)
	})

	t.Run("publish events to subscriber", func(t *testing.T) {
		evtCount := 5

		subscriber := NewDropOldestSubscriber(Options{
			Buffer: evtCount,
		})
		assert.NotNil(t, subscriber)

		subOpened := make(chan struct{})
		subClosed := make(chan struct{})

		writer := NewTestSyncWriter()

		go func() {
			// Subscribe blocks. Call shutdown in the onConnection callback.
			subErr := subscriber.Subscribe(context.Background(), Subscription{
				Topics: []string{"test"},
				rcw:    rcWriter{w: writer, rc: http.NewResponseController(writer)},
			}, LifecycleHooks{
				OnConnect: func(sub Subscription) {
					close(subOpened)
				},
				OnDisconnect: func(sub Subscription) {
					close(subClosed)
				},
			})
			assert.NoError(t, subErr)
		}()

		<-subOpened

		data := []byte("test-data")
		expectedData := []byte(fmt.Sprintf("data: %s\n\n", data))

		writer.wg.Add(evtCount)

		for range evtCount {
			go func() {
				err := subscriber.Publish(Event{
					Data: data,
				}, "test")
				assert.NoError(t, err)
			}()
		}

		writer.wg.Wait()

		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)

		<-subClosed

		assert.Equal(t, evtCount, len(writer.data))
		for i := range evtCount {
			assert.Equal(t, expectedData, writer.data[i])
		}
	})

	t.Run("publish events to blocked subscriber drops oldest event", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		writer := NewTestSyncWriter()

		subOpened := make(chan struct{})
		go func() {
			subErr := subscriber.Subscribe(context.Background(), Subscription{
				Topics: []string{"test"},
				rcw:    rcWriter{w: writer, rc: http.NewResponseController(writer)},
			}, LifecycleHooks{
				OnConnect: func(sub Subscription) {
					close(subOpened)
				},
			})
			assert.NoError(t, subErr)
		}()

		<-subOpened

		// Lock writer
		writer.mu.Lock()
		writer.wg.Add(2)

		err := subscriber.Publish(Event{
			Data: []byte("test 0"),
		}, "test")
		assert.NoError(t, err)

		closeChan := make(chan struct{})

		// Publish again while the writer is locked
		go func() {
			err = subscriber.Publish(Event{
				Data: []byte("test 1"),
			}, "test")
			assert.NoError(t, err)

			// Unlock the writer
			writer.mu.Unlock()

			time.Sleep(10 * time.Millisecond)

			err = subscriber.Publish(Event{
				Data: []byte("test 2"),
			}, "test")
			assert.NoError(t, err)

			close(closeChan)
		}()

		time.Sleep(100 * time.Millisecond)

		<-closeChan

		// Close the subscriber
		err = subscriber.Shutdown(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, 2, len(writer.data))
		assert.Equal(t, []byte("data: test 0\n\n"), writer.data[0])
		assert.Equal(t, []byte("data: test 2\n\n"), writer.data[1])
	})

	t.Run("publish with writer error", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		writer := errorWriter{
			returnsSendError: true,
		}

		subOpened := make(chan struct{})
		subDone := make(chan struct{})

		go func() {
			subErr := subscriber.Subscribe(context.Background(), Subscription{
				Topics: []string{"test"},
				rcw:    rcWriter{w: writer, rc: http.NewResponseController(writer)},
			}, LifecycleHooks{
				OnConnect: func(sub Subscription) {
					close(subOpened)
				},
			})
			assert.NoError(t, subErr)
			close(subDone)
		}()

		<-subOpened

		err := subscriber.Publish(Event{
			Data: []byte("test"),
		}, "test")
		assert.NoError(t, err)

		// Subscribe returns after removing the subscriber
		select {
		case <-subDone:
		case <-time.After(5 * time.Second):
			t.Fatal("Expected Subscribe to return after the error")
		}

		assert.Equal(t, 0, len(subscriber.subscribers))
	})

	t.Run("publish with flusher error", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		writer := errorWriter{
			returnsFlushError: true,
		}

		subOpened := make(chan struct{})
		subDone := make(chan struct{})

		go func() {
			subErr := subscriber.Subscribe(context.Background(), Subscription{
				Topics: []string{"test"},
				rcw:    rcWriter{w: writer, rc: http.NewResponseController(writer)},
			}, LifecycleHooks{
				OnConnect: func(sub Subscription) {
					close(subOpened)
				},
			})
			assert.NoError(t, subErr)
			close(subDone)
		}()

		<-subOpened

		err := subscriber.Publish(Event{
			Data: []byte("test"),
		}, "test")
		assert.NoError(t, err)

		// Subscribe returns after removing the subscriber
		select {
		case <-subDone:
		case <-time.After(5 * time.Second):
			t.Fatal("Expected Subscribe to return after the error")
		}

		assert.Equal(t, 0, len(subscriber.subscribers))
	})
}

func TestDropOldestSubscriber_Heartbeat(t *testing.T) {
	t.Run("heartbeat is sent", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{
			HeartbeatInterval: 10 * time.Millisecond,
		})
		assert.NotNil(t, subscriber)

		subOpened := make(chan struct{})

		writer := NewTestWriter()

		go func() {
			subErr := subscriber.Subscribe(context.Background(), Subscription{
				Topics: []string{"test"},
				rcw:    rcWriter{w: writer, rc: http.NewResponseController(writer)},
			}, LifecycleHooks{
				OnConnect: func(sub Subscription) {
					close(subOpened)
				},
			})
			assert.NoError(t, subErr)
		}()

		<-subOpened

		assert.Eventually(t, func() bool {
			return len(writer.Data()) >= 8
		}, 5*time.Second, time.Millisecond, "Expected heartbeats to be sent")

		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)

		for _, data := range writer.Data() {
			assert.Equal(t, []byte(": hb\n\n"), data)
		}
	})

	t.Run("heartbeat does not block if buffer is full", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{
			HeartbeatInterval: 10 * time.Millisecond,
		})
		assert.NotNil(t, subscriber)

		subOpened := make(chan struct{})

		writer := NewTestWriter()
		writer.mu.Lock()

		go func() {
			subErr := subscriber.Subscribe(context.Background(), Subscription{
				Topics: []string{"test"},
				rcw:    rcWriter{w: writer, rc: http.NewResponseController(writer)},
			}, LifecycleHooks{
				OnConnect: func(sub Subscription) {
					close(subOpened)
				},
			})
			assert.NoError(t, subErr)
		}()

		<-subOpened

		time.Sleep(100 * time.Millisecond)

		writer.mu.Unlock()

		// Shutdown waits for the blocked write to finish
		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)

		data := writer.Data()
		assert.LessOrEqual(t, len(data), 2)
		if assert.NotEmpty(t, data) {
			assert.Equal(t, []byte(": hb\n\n"), data[0])
		}
	})

	// t.Run("heartbeat is sent with heartbeat interval", func(t *testing.T) {
	// 	subscriber := NewDropOldestSubscriber(Options{
	// 		HeartbeatInterval: 10,
	// 	})
	// })

	// t.Run("heartbeat is not sent with heartbeat interval of 0", func(t *testing.T) {
	// 	subscriber := NewDropOldestSubscriber(Options{
	// 		HeartbeatInterval: 0,
	// 	})
	// })
}

func TestDropOldestSubscriber_Shutdown(t *testing.T) {
	t.Run("shutdown does not return an error", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)
	})

	t.Run("multiple shutdowns do not return an error", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)

		err = subscriber.Shutdown(context.Background())
		assert.NoError(t, err)
	})

	t.Run("shutdown returns when the context expires with a blocked writer", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{Buffer: 1})
		writer := newGateWriter()
		defer close(writer.release)
		subDone := subscribeAsync(t, context.Background(), subscriber, writer, LifecycleHooks{})

		// Block the worker in Write
		assert.NoError(t, subscriber.Publish(Event{Data: []byte("test")}, "test"))
		select {
		case <-writer.started:
		case <-time.After(5 * time.Second):
			t.Fatal("Expected the worker to start writing")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		shutdownErr := make(chan error, 1)
		go func() {
			shutdownErr <- subscriber.Shutdown(ctx)
		}()

		select {
		case err := <-shutdownErr:
			assert.ErrorIs(t, err, context.DeadlineExceeded, "Expected Shutdown to return the context error")
		case <-time.After(5 * time.Second):
			t.Error("Expected Shutdown to return when its context expires")
		}

		// The write deadline fails the blocked write without the writer being released
		waitClosed(t, subDone, "Expected Subscribe to return after the write deadline")
	})
}
