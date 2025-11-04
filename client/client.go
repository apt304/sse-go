package client

import (
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	sse "github.com/apt304/sse-go/server"
)

var (
	ErrClientAlreadyConnected = fmt.Errorf("client already connected")
)

// EventHandler is a function that handles SSE events
type EventHandler func(event sse.Event)

// ErrorHandler is a function that handles errors
type ErrorHandler func(err error)

// ConnectionState represents the current connection state
type ConnectionState int

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
	StateReconnecting
)

// Options configures the SSE client
type Options struct {
	Buffer     int           // Per-subscriber channel capacity. Default is 10.
	Reconnect  bool          // Enable automatic reconnection. Default is true.
	Backoff    time.Duration // Initial reconnection backoff. Default is 1 second.
	MaxBackoff time.Duration // Maximum reconnection backoff. Default is 30 seconds.
	MaxRetries int           // Maximum number of reconnection attempts. Default is 3.
}

// Client represents an SSE client
type Client struct {
	mu   sync.RWMutex
	done chan struct{}

	state   ConnectionState
	stateMu sync.RWMutex

	conn   *Connection
	connMu sync.Mutex

	url     string
	topics  []string
	options Options

	lastEventMu    sync.RWMutex
	lastEventID    string
	eventHandlers  map[string][]EventHandler
	messageHandler EventHandler
	errorHandler   ErrorHandler
}

// NewClient creates a new SSE client
func NewClient(url string, topics []string, opts ...Options) *Client {
	options := Options{
		Buffer:     10,
		Reconnect:  true,
		Backoff:    1 * time.Second,
		MaxBackoff: 30 * time.Second,
		MaxRetries: 3,
	}

	// Apply any provided options
	if len(opts) > 0 {
		options = opts[0]
	}

	return &Client{
		url:           url,
		topics:        topics,
		options:       options,
		eventHandlers: make(map[string][]EventHandler),
		done:          make(chan struct{}),
	}
}

// addJitter adds +/- 2% jitter to the backoff duration
func addJitter(backoff time.Duration) time.Duration {
	// Add random jitter of +/- 2%
	jitterPercent := 0.02
	jitterRange := float64(backoff) * jitterPercent

	// Generate random jitter between -jitterRange and +jitterRange
	jitter := time.Duration(rand.Float64()*2*jitterRange - jitterRange)

	return backoff + jitter
}

// OnMessage sets the default message handler for events without a specific type
func (c *Client) OnMessage(handler EventHandler) {
	c.messageHandler = handler
}

// OnEvent sets a handler for a specific event type
func (c *Client) OnEvent(eventType string, handler EventHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.eventHandlers[eventType] == nil {
		c.eventHandlers[eventType] = make([]EventHandler, 0)
	}
	c.eventHandlers[eventType] = append(c.eventHandlers[eventType], handler)
}

// OnError sets the error handler
func (c *Client) OnError(handler ErrorHandler) {
	c.errorHandler = handler
}

// Connect establishes the SSE connection. An error is returned if the client is already connected.
func (c *Client) Connect() error {
	c.connMu.Lock()

	if c.conn != nil {
		c.connMu.Unlock()
		return ErrClientAlreadyConnected
	}

	conn := NewConnection(c.url, c.options.Buffer, c.topics...)
	c.lastEventMu.RLock()
	if c.lastEventID != "" {
		conn.SetLastEventID(c.lastEventID)
	}
	c.lastEventMu.RUnlock()

	c.conn = conn
	c.setState(StateConnecting)

	c.connMu.Unlock()

	eventChan, err := conn.Connect()
	if err != nil {
		c.setState(StateDisconnected)
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.setState(StateConnected)

	go c.handleEvents(eventChan)

	return nil
}

// handleEvents processes events from the connection
func (c *Client) handleEvents(eventChan <-chan sse.Event) {
	for {
		select {
		case <-c.done:
			return
		case event, ok := <-eventChan:
			if !ok {
				// Connection closed
				c.handleDisconnection()
				return
			}

			c.handleEvent(event)
		}
	}
}

// handleEvent routes an event to the appropriate handler
func (c *Client) handleEvent(event sse.Event) {
	// Update last event ID for reconnection
	if event.ID != "" {
		c.lastEventMu.Lock()
		c.lastEventID = event.ID
		c.lastEventMu.Unlock()
	}

	// Handle heartbeat events
	if event.IsHeartbeat() {
		return
	}

	// Handle events with specific types
	if event.Type != "" && event.Type != "message" {
		c.mu.RLock()
		handlers := c.eventHandlers[event.Type]
		c.mu.RUnlock()

		for _, handler := range handlers {
			handler(event)
		}
	}

	// Handle default message handler
	if c.messageHandler != nil {
		c.messageHandler(event)
	}
}

// handleDisconnection handles connection loss and reconnection
func (c *Client) handleDisconnection() {
	c.setState(StateDisconnected)

	if !c.options.Reconnect || c.options.MaxRetries <= 0 {
		slog.Info("client disconnected without retry")
		return
	}

	c.setState(StateReconnecting)

	// Exponential backoff reconnection
	backoff := c.options.Backoff
	retryCount := 0

	for {
		select {
		case <-c.done:
			return
		case <-time.After(addJitter(backoff)):
			retryCount++
			slog.Info("attempting to reconnect", "url", c.url, "backoff", backoff, "jitteredBackoff", addJitter(backoff), "attempt", retryCount)

			if err := c.Connect(); err != nil {
				slog.Warn("reconnection failed", "error", err, "backoff", backoff, "attempt", retryCount)

				// Check if we've exceeded max retries
				if c.options.MaxRetries > 0 && retryCount >= c.options.MaxRetries {
					slog.Warn("max reconnection attempts exceeded", "maxRetries", c.options.MaxRetries)
					c.setState(StateDisconnected)

					// Call error handler if set
					if c.errorHandler != nil {
						c.errorHandler(fmt.Errorf("failed to reconnect after %d attempts", c.options.MaxRetries))
					}
					return
				}

				// Increase backoff for next attempt
				backoff *= 2
				if backoff > c.options.MaxBackoff {
					backoff = c.options.MaxBackoff
				}
				continue
			}

			slog.Info("reconnection successful", "attempts", retryCount)
			return
		}
	}
}

// Disconnect closes the connection
func (c *Client) Disconnect() {
	close(c.done)

	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.connMu.Unlock()

	c.setState(StateDisconnected)
}

// GetState returns the current connection state
func (c *Client) GetState() ConnectionState {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state
}

// setState updates the connection state
func (c *Client) setState(state ConnectionState) {
	c.stateMu.Lock()
	c.state = state
	c.stateMu.Unlock()
}

// IsConnected returns true if the client is currently connected
func (c *Client) IsConnected() bool {
	return c.GetState() == StateConnected
}

// GetLastEventID returns the last event ID received
func (c *Client) GetLastEventID() string {
	c.lastEventMu.RLock()
	defer c.lastEventMu.RUnlock()
	return c.lastEventID
}
