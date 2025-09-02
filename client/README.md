# SSE Go Client

A Go client implementation for Server-Sent Events (SSE) that provides a clean, high-level API for consuming SSE streams.

## Features

- **EventSource-like API**: Similar to the browser's EventSource with Go idioms
- **Automatic Reconnection**: Configurable exponential backoff reconnection
- **Event Filtering**: Listen to specific event types
- **Heartbeat Support**: Dedicated heartbeat event handling
- **Context Support**: Cancellation and timeouts
- **Error Handling**: Robust error handling and recovery
- **Thread Safe**: Safe for concurrent use

## Quick Start

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
    sseClient := client.NewClient("http://localhost:8080/events", []string{"notifications"}, client.Options{
        Reconnect:  true,
        Backoff:    2 * time.Second,
        MaxBackoff: 30 * time.Second,
        MaxRetries: 5, // Give up after 5 attempts
    })

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

## API Reference

### Client

#### NewClient(url string, topics []string, opts ...Options) *Client
Creates a new SSE client that will connect to the specified URL and subscribe to the given topics. Accepts optional configuration options.

#### OnMessage(handler EventHandler)
Sets the default message handler for events without a specific type.

#### OnEvent(eventType string, handler EventHandler)
Sets a handler for a specific event type.

#### OnHeartbeat(handler EventHandler)
Sets a handler for heartbeat events.

#### OnError(handler ErrorHandler)
Sets the error handler.

#### Connect() error
Establishes the SSE connection.

#### Disconnect()
Closes the connection and stops reconnection attempts.

#### GetState() ConnectionState
Returns the current connection state.

#### IsConnected() bool
Returns true if the client is currently connected.

#### GetLastEventID() string
Returns the last event ID received.

### Event

#### GetData() string
Returns the event data as a string.

#### IsHeartbeat() bool
Returns true if this is a heartbeat event.

#### String() string
Returns the SSE-formatted string representation of the event.

### Connection States

- `StateDisconnected`: Client is not connected
- `StateConnecting`: Client is attempting to connect
- `StateConnected`: Client is connected and receiving events
- `StateReconnecting`: Client is attempting to reconnect

## Configuration

### Options
The client accepts configuration options when creating a new instance:

```go
client := NewClient("http://localhost:8080/events", []string{"notifications"}, Options{
    Reconnect:  true,
    Backoff:    1 * time.Second,
    MaxBackoff: 30 * time.Second,
    MaxRetries: 10, // Give up after 10 attempts
})
```

### Reconnection
The client supports automatic reconnection with exponential backoff. Configure this through the Options struct when creating the client.

**MaxRetries**: Set to 0 for unlimited retries, or specify a positive number to limit reconnection attempts. When the limit is reached, the error handler will be called with a "max reconnection attempts exceeded" error.

**Jitter**: The client automatically adds +/- 2% random jitter to backoff delays to prevent multiple clients from reconnecting simultaneously (thundering herd problem).

### Topics
Topics can be specified when creating the client and will be sent as query parameters:

```go
client := NewClient("http://localhost:8080/events", []string{"user-events", "notifications"})
```

## Error Handling

The client provides comprehensive error handling:

```go
client.OnError(func(err error) {
    switch {
    case strings.Contains(err.Error(), "connection refused"):
        log.Println("Server is down, will retry...")
    case strings.Contains(err.Error(), "timeout"):
        log.Println("Connection timeout, will retry...")
    default:
        log.Printf("Unexpected error: %v", err)
    }
})
```

## Thread Safety

All client methods are thread-safe and can be called from multiple goroutines.

## Examples

See the `example/` directory for complete working examples.
