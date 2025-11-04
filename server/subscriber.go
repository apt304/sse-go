package sse

import (
	"context"
	"io"
	"net/http"
)

type Subscriber interface {
	Subscribe(ctx context.Context, subscription Subscription, lifecycleHooks LifecycleHooks) error
	Publish(event Event, topics ...string) error
	Shutdown(ctx context.Context) error
}

type Subscription struct {
	// The last event ID to resume from.
	LastEventID string
	// The topics to receive events from. Must be a non-empty list.
	Topics []string

	// The response writer to use for sending events.
	rcw rcWriter
}

type rcWriter struct {
	w  io.Writer
	rc *http.ResponseController
}

func (w rcWriter) Write(p []byte) (n int, err error) {
	return w.w.Write(p)
}

func (w rcWriter) Flush() error {
	return w.rc.Flush()
}

type LifecycleHook func(sub Subscription)

type LifecycleHooks struct {
	// OnConnect is called after the SSE connection is fully established. It is safe
	// to send events to the subscriber after this hook is called.
	OnConnect LifecycleHook
	// OnDisconnect is called after the SSE connection is closed. Events sent to the
	// subscriber after this hook is called will not be delivered.
	OnDisconnect LifecycleHook
}
