package sse

import (
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
	select {
	case <-s.close:
		return ErrSubscriberClosed
	default:
	}

	if len(sub.Topics) == 0 {
		return ErrNoTopics
	}

	s.wg.Add(1)
	defer s.wg.Done()

	subChan := &subscriptionChan{
		Subscription: sub,
		events:       make(chan Event, s.opts.Buffer),
	}

	s.mu.Lock()
	s.subscribers = append(s.subscribers, subChan)
	s.mu.Unlock()

	// Start the subscriber worker
	stop := make(chan struct{})
	done := make(chan struct{})
	go s.run(subChan, lifecycleHooks, stop, done)

	// Start heartbeat goroutine if enabled
	if s.opts.HeartbeatInterval > 0 {
		go s.runHeartbeat(subChan, stop)
	}

	// Block until the context, the provider, or the subscriber done channel is closed.
	select {
	case <-ctx.Done():
	case <-s.close:
	case <-done:
	}

	// signal the subscriber and heartbeat workers to stop
	close(stop)

	// Remove the subscriber from the list
	s.mu.Lock()
	for i, subscriber := range s.subscribers {
		if subscriber == subChan {
			s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	return nil
}

func (s *DropOldestSubscriber) Publish(event Event, topics []string) error {
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

	// Send the event to the subscribers
	for _, sub := range subs {
		select {
		case sub.events <- event:
		case <-s.close:
			return ErrSubscriberClosed
		default:
			// Channel is full - drop oldest event
			select {
			case <-sub.events: // Remove oldest event
			default:
			}

			select {
			case sub.events <- event: // Try to send new event
			default:
				// Still full, drop the event (slow client)
			}
		}
	}

	return nil
}

// Shutdown shuts down the subscriber. It is safe to call multiple times.
func (s *DropOldestSubscriber) Shutdown(ctx context.Context) error {
	select {
	case <-s.close:
		return nil
	default:
	}

	close(s.close)

	// Wait for all subscribers to finish
	s.wg.Wait()

	return nil
}

type subscriptionChan struct {
	Subscription
	events chan Event
}

func (s *DropOldestSubscriber) run(sub *subscriptionChan, lifecycleHooks LifecycleHooks, stop, done chan struct{}) {
	// Signal that the subscriber is ready
	if lifecycleHooks.OnConnect != nil {
		go lifecycleHooks.OnConnect(sub.Subscription)
	}

	if lifecycleHooks.OnDisconnect != nil {
		defer func() {
			go lifecycleHooks.OnDisconnect(sub.Subscription)
		}()
	}

	for {
		select {
		case <-stop:
			return
		case evt := <-sub.events:
			if err := evt.Write(sub.rcw); err != nil {
				// On write error, stop work and remove the subscriber
				slog.Warn("subscriber write error", "error", err)
				close(done)
				return
			}
			if err := sub.rcw.Flush(); err != nil {
				// On flush error, stop work and remove the subscriber
				slog.Warn("subscriber flush error", "error", err)
				close(done)
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
			case sub.events <- heartbeatComment:
			default:
				// If they're saturated, don't make it worse
			}
		}
	}
}
