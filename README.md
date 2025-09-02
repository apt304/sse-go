# SSE Go Server

A minimal, high-performance Server-Sent Events (SSE) server implementation in Go, following the HTML5 SSE specification.

## Features

- **SSE Protocol Compliance**: Full compliance with HTML5 SSE specification
- **HTTP/1.1 & HTTP/2 Support**: Automatic protocol detection and appropriate header handling
- **Topic-based Broadcasting**: Subscribe to specific topics for targeted event delivery
- **Heartbeat Support**: Configurable heartbeat comments to keep connections alive
- **Lifecycle Hooks**: Connection lifecycle callbacks (OnConnect, OnDisconnect)
- **Graceful Shutdown**: Clean resource cleanup with synchronous shutdown
- **CORS Support**: Configurable CORS headers for cross-origin requests
- **Buffer Management**: Configurable per-subscriber event buffers with drop-oldest strategy

## Installation

```bash
go get github.com/apt304/sse-go
```

## Quick Start

### Basic Usage

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
    sseServer := server.NewServer(server.Config{
        CORSOrigin: "*", // Enable CORS
    }, subscriber)

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

err := sseServer.Publish(event, "notifications", "updates")
if err != nil {
    log.Printf("Publish error: %v", err)
}
```

### Client Example

```html
<!DOCTYPE html>
<html>
<head>
    <title>SSE Client</title>
</head>
<body>
    <div id="messages"></div>
    <script>
        const eventSource = new EventSource('/events');
        const messagesDiv = document.getElementById('messages');

        eventSource.onmessage = function(event) {
            const message = document.createElement('div');
            message.textContent = event.data;
            messagesDiv.appendChild(message);
        };

        eventSource.onerror = function(event) {
            console.error('SSE error:', event);
        };
    </script>
</body>
</html>
```

## API Reference

### Core Types

#### Event
```go
type Event struct {
    ID    string // Event ID for reconnection support
    Type  string // Event type for client filtering
    Retry int    // Reconnection retry interval (ms)
    Data  []byte // Event data
}
```

#### Subscription
```go
type Subscription struct {
    LastEventID string   // Last event ID to resume from
    Topics      []string // Topics to subscribe to
}
```

#### LifecycleHooks
```go
type LifecycleHooks struct {
    OnConnect    func(sub Subscription) // Called after connection established
    OnDisconnect func(sub Subscription) // Called after connection closed
}
```

### Configuration

#### Server Options
```go
type Options struct {
    Buffer             int           // Per-subscriber event buffer size
    HeartbeatInterval  time.Duration // Heartbeat frequency (0 = disabled)
}
```

#### Server Config
```go
type Config struct {
    CORSOrigin string // CORS origin header value
}
```

## Architecture

The library is modular:

- **Server**: Handles HTTP requests and SSE protocol details
- **Subscriber**: Manages connections and event distribution
- **Event**: Represents SSE events with proper formatting

The `DropOldestSubscriber` implementation provides:
- Topic-based event routing
- Configurable event buffers
- Automatic heartbeat support
- Graceful connection cleanup
