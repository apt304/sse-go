package sse

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
)

var (
	ErrSubscriberClosed    = fmt.Errorf("subscriber closed")
	ErrNoTopics            = fmt.Errorf("at least one topic must be specified")
	ErrFlusherNotSupported = fmt.Errorf("response writer does not implement http.Flusher")
)

// Config holds SSE server configuration
type Config struct {
	HeartbeatIntervalSeconds int // 0 means no heartbeats
	CORSOrigin               string
}

type Server struct {
	config     Config
	subscriber Subscriber
}

func NewServer(config Config, subscriber Subscriber) *Server {
	return &Server{
		config:     config,
		subscriber: subscriber,
	}
}

// ServeHTTP handles SSE connections. Callers must not set the http status response after ServeHTTP returns. It automatically
// returns 204 No Content when the connection successfully closes, and 500 Internal Server Error on error.
func (s Server) ServeHTTP(w http.ResponseWriter, r *http.Request, topics []string, lifecycleHooks LifecycleHooks) {
	ctx := r.Context()

	s.setSSEHeaders(w, r)

	rc := http.NewResponseController(w)
	err := rc.Flush()
	if err != nil {
		err = fmt.Errorf("unable to flush sse headers on connection: %w", err)
		slog.WarnContext(ctx, "sse: unable to flush sse headers on connection", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sub := Subscription{
		LastEventID: r.Header.Get("Last-Event-ID"),
		Topics:      topics,
		rcw:         rcWriter{w: w, rc: rc},
	}

	slog.DebugContext(ctx, "sse: subscribing session", "topics", sub.Topics, "lastEventID", sub.LastEventID)

	if err = s.subscriber.Subscribe(ctx, sub, lifecycleHooks); err != nil {
		slog.ErrorContext(ctx, "sse: subscribe error", "error", err)

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	slog.DebugContext(ctx, "sse: session ended")

	w.WriteHeader(http.StatusNoContent)
}

// setSSEHeaders sets the appropriate headers for SSE
func (s Server) setSSEHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	// Set connection headers for HTTP/1 connections. If sent on HTTP/2 connections, Chrome and Firefox ignore them,
	// and Safari rejects the response.
	if r.ProtoMajor == 1 {
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Keep-Alive", "timeout=60")
	}

	// Set CORS headers if enabled
	if s.config.CORSOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", s.config.CORSOrigin)
	}
}

func (s *Server) Publish(event Event, topics ...string) error {
	if len(topics) == 0 {
		return ErrNoTopics
	}

	return s.subscriber.Publish(event, topics)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.subscriber.Shutdown(ctx)
}
