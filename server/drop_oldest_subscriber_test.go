package sse

import (
	"context"
	"fmt"
	"net/http"
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

		err := subscriber.Publish(Event{
			Data: []byte("test"),
		}, "test")
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
		_, open := <-subOpened
		assert.False(t, open)

		assert.Equal(t, 0, len(subscriber.subscribers))
	})

	t.Run("publish with flusher error", func(t *testing.T) {
		subscriber := NewDropOldestSubscriber(Options{})
		assert.NotNil(t, subscriber)

		writer := errorWriter{
			returnsFlushError: true,
		}

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

		err := subscriber.Publish(Event{
			Data: []byte("test"),
		}, "test")
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
		_, open := <-subOpened
		assert.False(t, open)

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

		time.Sleep(100 * time.Millisecond)

		assert.GreaterOrEqual(t, len(writer.data), 8)
		for i := range writer.data {
			assert.Equal(t, []byte(": hb\n\n"), writer.data[i])
		}

		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)
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

		time.Sleep(1 * time.Millisecond) // Allow the heartbeat to be sent

		assert.LessOrEqual(t, len(writer.data), 2)
		assert.Equal(t, []byte(": hb\n\n"), writer.data[0])

		err := subscriber.Shutdown(context.Background())
		assert.NoError(t, err)
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
}
