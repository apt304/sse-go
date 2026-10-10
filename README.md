# SSE Go Library

A minimal, high-performance Server-Sent Events (SSE) implementation in Go. This library provides both server and client implementations for building real-time applications.

## Features

- **Topic-based Broadcasting**: Subscribe to specific topics for targeted event delivery
- **Heartbeat Support**: Configurable heartbeat comments to keep connections alive
- **Lifecycle Hooks**: Connection lifecycle callbacks (OnConnect, OnDisconnect)
- **Graceful Shutdown**: Clean resource cleanup with synchronous shutdown
- **Default Server Implementation**: Configurable per-subscriber event buffers with drop-oldest strategy
- **Go Client**: High-level client API with automatic reconnection and event handling

## Installation

```bash
go get github.com/apt304/sse-go
```

## Server

### Quick Start

```go
package main

import (
    "context"
    "log"
    "net/http"
    "time"

    "github.com/apt304/sse-go/server"
)

func main() {
    // Create subscriber with configuration
    subscriber := server.NewDropOldestSubscriber(server.Options{
        Buffer:             10,                    // Events per subscriber
        HeartbeatInterval:  15 * time.Second,      // Heartbeat frequency
    })

    // Create SSE server
    sseServer := server.NewServer(subscriber)

    // Create HTTP server
    mux := http.NewServeMux()
    mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
        hooks := server.LifecycleHooks{
            OnConnect: func(sub server.Subscription) {
                log.Printf("Connected: %v", sub.Topics)
            },
            OnDisconnect: func(sub server.Subscription) {
                log.Printf("Disconnected: %v", sub.Topics)
            },
        }
        
        // Subscribe to topics
        topics := []string{"notifications", "updates"}
        sseServer.ServeHTTP(w, r, topics, hooks)
    })

    // Start server
    log.Fatal(http.ListenAndServe(":8080", mux))
}
```

### Publishing Events

```go
// Publish to specific topics
event := server.Event{
    ID:   "event-1",
    Type: "message",
    Data: []byte("Hello, World!"),
}

// Send to "notifications" and "updates" topics
err := sseServer.Publish(event, "notifications", "updates")
if err != nil {
    log.Printf("Publish error: %v", err)
}
```

### Server API Reference

#### Core Types

*Event*
```go
type Event struct {
    ID    string // Event ID for reconnection support
    Type  string // Event type for client filtering
    Retry int    // Reconnection retry interval (ms)
    Data  []byte // Event data
}
```

*Subscription*
```go
type Subscription struct {
    LastEventID string   // Last event ID to resume from
    Topics      []string // Topics to subscribe to
}
```

*LifecycleHooks*
```go
type LifecycleHooks struct {
    OnConnect    func(sub Subscription) // Called after connection established
    OnDisconnect func(sub Subscription) // Called after connection closed
}
```

*Server Options*
```go
type Options struct {
    Buffer             int           // Per-subscriber event buffer size
    HeartbeatInterval  time.Duration // Heartbeat frequency (0 = disabled)
}
```

## Client

### Quick Start

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/apt304/sse-go/client"
    sse "github.com/apt304/sse-go/server"
)

func main() {
    // Create a new SSE client with custom options
    opts := client.DefaultOptions()
    opts.Backoff = 2 * time.Second
    opts.MaxRetries = 5 // Give up after 5 attempts
    sseClient := client.NewClient("http://localhost:8080/events", []string{"notifications"}, opts)

    // Set up event handlers
    sseClient.OnMessage(func(event sse.Event) {
        fmt.Printf("Message: %s\n", event.GetData())
    })

    sseClient.OnEvent("notification", func(event sse.Event) {
        fmt.Printf("Notification: %s\n", event.GetData())
    })

    // Set up error handling
    sseClient.OnError(func(err error) {
        log.Printf("Error: %v", err)
    })

    // Connect to the server
    if err := sseClient.Connect(); err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }

    // Keep the client running
    select {}
}
```

### Client API Reference

#### Client Methods

##### NewClient(url string, topics []string, opts ...Options) *Client
Creates a new SSE client that will connect to the specified URL and subscribe to the given topics. Accepts optional configuration options.

##### OnMessage(handler EventHandler)
Sets the default message handler for events without a specific type.

##### OnEvent(eventType string, handler EventHandler)
Sets a handler for a specific event type.

##### OnError(handler ErrorHandler)
Sets the error handler.

##### Connect() error
Establishes the SSE connection.

##### Disconnect()
Closes the connection and stops reconnection attempts.

##### GetState() ConnectionState
Returns the current connection state.

##### IsConnected() bool
Returns true if the client is currently connected.

##### GetLastEventID() string
Returns the last event ID received.

#### Event Methods

##### GetData() string
Returns the event data as a string.

##### IsHeartbeat() bool
Returns true if this is a heartbeat event.

##### String() string
Returns the SSE-formatted string representation of the event.

#### Connection States

- `StateDisconnected`: Client is not connected
- `StateConnecting`: Client is attempting to connect
- `StateConnected`: Client is connected and receiving events
- `StateReconnecting`: Client is attempting to reconnect

### Client Configuration

#### Options
The client accepts configuration options when creating a new instance. `NewClient` uses `DefaultOptions()` when no options are provided. Provided options replace the defaults entirely, so start from `DefaultOptions()` and change the fields you need:

```go
opts := DefaultOptions()
opts.MaxRetries = 10 // Give up after 10 attempts
client := NewClient("http://localhost:8080/events", []string{"notifications"}, opts)
```

#### Reconnection
The client supports automatic reconnection with exponential backoff. Configure this through the Options struct when creating the client.

**MaxRetries**: Set to 0 to disable reconnection, or specify a positive number to limit reconnection attempts. When the limit is reached, the error handler will be called with a "failed to reconnect after N attempts" error.

**Jitter**: The client automatically adds +/- 2% random jitter to backoff delays to prevent multiple clients from reconnecting simultaneously (thundering herd problem).

#### Topics
Topics can be specified when creating the client and will be sent as query parameters:

```go
client := NewClient("http://localhost:8080/events", []string{"user-events", "notifications"})
```
