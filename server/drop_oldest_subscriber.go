package sse

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"
)

type Options struct {
	// Per-subscriber channel capacity.
	Buffer int
	// HeartbeatInterval - set 0 to disable.
	HeartbeatInterval time.Duration
}

type DropOldestSubscriber struct {
	opts Options

	mu          *sync.RWMutex
	subscribers []*subscriptionChan

	wg    *sync.WaitGroup
	close chan struct{}
}

func NewDropOldestSubscriber(opts Options) *DropOldestSubscriber {
	if opts.Buffer < 0 {
		opts.Buffer = 0
	}
	if opts.HeartbeatInterval < 0 {
		opts.HeartbeatInterval = 0
	}

	return &DropOldestSubscriber{
		mu:    &sync.RWMutex{},
		opts:  opts,
		wg:    &sync.WaitGroup{},
		close: make(chan struct{}),
	}
}

func (s *DropOldestSubscriber) Subscribe(ctx context.Context, sub Subscription, lifecycleHooks LifecycleHooks) error {
	// Check and register under s.mu so Shutdown cannot close s.close between the check and wg.Add
	s.mu.Lock()
	select {
	case <-s.close:
		s.mu.Unlock()
		return ErrSubscriberClosed
	default:
	}

	if len(sub.Topics) == 0 {
		s.mu.Unlock()
		return ErrNoTopics
	}

	s.wg.Add(1)
	defer s.wg.Done()
	subChan := &subscriptionChan{
		Subscription: sub,
		events:       make(chan []byte, s.opts.Buffer),
	}
	s.subscribers = append(s.subscribers, subChan)
	s.mu.Unlock()

	// Start the subscriber worker
	ready := make(chan struct{})
	stop := make(chan struct{})
	done := make(chan struct{})
	go s.run(subChan, ready, stop, done)

	// Start heartbeat goroutine if enabled
	if s.opts.HeartbeatInterval > 0 {
		go s.runHeartbeat(subChan, stop)
	}

	// The subscriber is registered and its worker is about to wait for events, so events published from here on are
	// delivered. With Buffer 0, an event is only delivered while the worker is waiting.
	<-ready
	if lifecycleHooks.OnConnect != nil {
		lifecycleHooks.OnConnect(sub)
	}

	// Block until the context, the provider, or the subscriber done channel is closed.
	select {
	case <-ctx.Done():
		slog.Debug("subscriber context done", "context", ctx.Err())
	case <-s.close:
		slog.Debug("subscriber closed")
	case <-done:
		slog.Debug("subscriber done")
	}

	// signal the subscriber and heartbeat workers to stop
	close(stop)

	// Wait for run() to stop writing to the response
	<-done

	// Remove the subscriber from the list
	s.mu.Lock()
	for i, subscriber := range s.subscribers {
		if subscriber == subChan {
			s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	if lifecycleHooks.OnDisconnect != nil {
		lifecycleHooks.OnDisconnect(sub)
	}

	return nil
}

func (s *DropOldestSubscriber) Publish(event Event, topics ...string) error {
	select {
	case <-s.close:
		return ErrSubscriberClosed
	default:
	}

	if len(topics) == 0 {
		return ErrNoTopics
	}

	// Get subscribers that match the topics.
	// Subscribers may be subscribed to multiple publish topics. Only send one message if a subscriber
	// matches multiple topics.
	var subs []*subscriptionChan
	s.mu.RLock()
	for _, subChan := range s.subscribers {
		for _, topic := range topics {
			if slices.Contains(subChan.Topics, topic) {
				subs = append(subs, subChan)
				break
			}
		}
	}
	s.mu.RUnlock()

	if len(subs) == 0 {
		// No subscribers for the topics, do nothing
		return nil
	}

	var eventBytes bytes.Buffer
	if err := event.Write(&eventBytes); err != nil {
		return err
	}

	// Send the event to the subscribers
	for _, sub := range subs {
		select {
		case sub.events <- eventBytes.Bytes():
		case <-s.close:
			return ErrSubscriberClosed
		default:
			// Channel is full - drop oldest event
			select {
			case <-sub.events: // Remove oldest event
			default:
			}

			select {
			case sub.events <- eventBytes.Bytes(): // Try to send new event
			default:
				// Still full, drop the event (slow client)
				slog.Warn("subscriber buffer full, dropping event", "topics", topics, "eventId", event.ID)
			}
		}
	}

	return nil
}

// Shutdown shuts down the subscriber. It is safe to call multiple times. It waits for subscribers to finish
// until ctx is done, then sets an immediate write deadline on remaining connections and returns ctx.Err().
func (s *DropOldestSubscriber) Shutdown(ctx context.Context) error {
	// Check and close under s.mu so concurrent calls close s.close once
	s.mu.Lock()
	select {
	case <-s.close:
		s.mu.Unlock()
		return nil
	default:
	}
	close(s.close)
	s.mu.Unlock()

	// Wait for all subscribers to finish
	finished := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		return nil
	case <-ctx.Done():
	}

	// Fail writes blocked on clients that are not reading. Subscribers are removed under s.mu before
	// Subscribe returns, so each response is still active. Writers without deadline support are skipped.
	s.mu.RLock()
	for _, sub := range s.subscribers {
		_ = sub.rcw.rc.SetWriteDeadline(time.Now())
	}
	s.mu.RUnlock()

	return ctx.Err()
}

type subscriptionChan struct {
	Subscription
	events chan []byte
}

// run writes events to the subscriber until stop is closed or a write fails. It closes ready before waiting for
// events, and done when it returns.
func (s *DropOldestSubscriber) run(sub *subscriptionChan, ready, stop, done chan struct{}) {
	defer close(done)
	close(ready)

	for {
		select {
		case <-stop:
			return
		case evt := <-sub.events:
			if _, err := sub.rcw.Write(evt); err != nil {
				// On write error, stop work and remove the subscriber
				slog.Warn("subscriber write error", "error", err)
				return
			}
			if err := sub.rcw.Flush(); err != nil {
				// On flush error, stop work and remove the subscriber
				slog.Warn("subscriber flush error", "error", err)
				return
			}
		}
	}
}

// Heartbeats are sent on the configured interval. They send regardless of any other messages
// that have been sent. If the subscriber buffer is full, the heartbeat is dropped.
func (s *DropOldestSubscriber) runHeartbeat(sub *subscriptionChan, stop chan struct{}) {
	ticker := time.NewTicker(s.opts.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// Send heartbeat non-blocking
			select {
			case sub.events <- heartbeatCommentBytes:
			default:
				// If they're saturated, don't make it worse
			}
		}
	}
}
