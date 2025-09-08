package client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	sse "github.com/apt304/sse-go/server"
)

// Connection represents an SSE connection to the server
type Connection struct {
	client      *http.Client
	url         string
	topics      []string
	lastEventID string
	headers     map[string]string
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewConnection creates a new SSE connection
func NewConnection(url string, topics []string) *Connection {
	ctx, cancel := context.WithCancel(context.Background())

	return &Connection{
		client: &http.Client{
			Timeout: 0, // No timeout for SSE connections
		},
		url:     url,
		topics:  topics,
		headers: make(map[string]string),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// SetHeader sets a custom HTTP header
func (c *Connection) SetHeader(key, value string) {
	c.headers[key] = value
}

// SetLastEventID sets the Last-Event-ID header for reconnection
func (c *Connection) SetLastEventID(id string) {
	c.lastEventID = id
}

// Connect establishes the SSE connection and returns a channel for events
func (c *Connection) Connect() (<-chan sse.Event, error) {
	req, err := http.NewRequestWithContext(c.ctx, "GET", c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set SSE headers
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	// Set Last-Event-ID if provided
	if c.lastEventID != "" {
		req.Header.Set("Last-Event-ID", c.lastEventID)
	}

	// Set custom headers
	for key, value := range c.headers {
		req.Header.Set(key, value)
	}

	// Add topics as query parameters if provided
	if len(c.topics) > 0 {
		q := req.URL.Query()
		for _, topic := range c.topics {
			q.Add("topic", topic)
		}
		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Verify content type
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/event-stream") {
		resp.Body.Close()
		return nil, fmt.Errorf("invalid content type: %s", contentType)
	}

	eventChan := make(chan sse.Event, 100)

	go c.readEvents(resp.Body, eventChan)

	return eventChan, nil
}

// readEvents reads SSE events from the response body and sends them to the channel
func (c *Connection) readEvents(body io.ReadCloser, eventChan chan<- sse.Event) {
	defer body.Close()
	defer close(eventChan)

	reader := bufio.NewReader(body)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		event, err := sse.ParseEvent(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			// Send error event
			select {
			case eventChan <- sse.Event{
				Type: "error",
				Data: []byte(fmt.Sprintf("parse error: %v", err)),
			}:
			case <-c.ctx.Done():
				return
			}
			return
		}

		// Send the parsed event
		select {
		case eventChan <- event:
		case <-c.ctx.Done():
			return
		}
	}
}

// Close closes the connection
func (c *Connection) Close() {
	c.cancel()
}

// IsClosed returns true if the connection is closed
func (c *Connection) IsClosed() bool {
	select {
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}
