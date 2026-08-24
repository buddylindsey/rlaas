<div align="center">

# RLaaS

### Rate Limiting as a Service

A standalone service for consistent, shared rate limiting across applications.

![Go](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white)
![Tests](https://github.com/buddylindsey/rlaas/actions/workflows/test.yml/badge.svg)
![Status](https://img.shields.io/badge/status-early_development-orange)

</div>

> [!WARNING]
> RLaaS is in active, early-stage development. It is **not production-ready**.
> Limiter state currently lives only in memory, is lost when the server restarts,
> and is not coordinated across server instances.

## 📌 Table of Contents

- [What is RLaaS?](#what-is-rlaas)
- [Why does this project exist?](#why-does-this-project-exist)
- [What works today?](#what-works-today)
- [Quick start](#quick-start)
- [Try the concurrent Python example](#try-the-concurrent-python-example)
- [How it works](#how-it-works)
- [Project status and direction](#project-status-and-direction)
- [Development](#development)
- [License](#license)

## 💡 What is RLaaS?

RLaaS is a rate-limiting server written in Go. Applications connect over TCP,
create named limiters, and ask the service to acquire permits. This gives
multiple clients one shared place to enforce request budgets instead of
implementing and coordinating independent rate limiters in every application.

The first supported algorithm is a fixed window. A limiter such as “300 calls
per minute” allows the first 300 acquisitions in its window and denies later
requests until the next window begins.

## 🎯 Why does this project exist?

Rate limiting is commonly embedded inside individual applications. That works
for local protection, but it becomes harder to maintain when multiple processes
or services need to share the same budget. RLaaS exists to move that concern
behind a dedicated service with a consistent protocol and a single source of
truth.

The project is being built from a focused single-node core toward a durable,
distributed service. Its design emphasizes:

- TCP connections and message framing
- Protocol boundaries and typed request handling
- Correct concurrent state updates
- Swappable storage abstractions
- Persistence and distributed coordination
- Predictable client behavior under concurrent load

The current in-memory implementation establishes those boundaries while the
storage, coordination, reliability, and operational capabilities needed for
production are developed incrementally.

## ✅ What works today?

- Concurrent TCP server with persistent client connections
- Four-byte, big-endian, length-prefixed message frames
- JSON request and response codec
- Named fixed-window limiters
- Atomic permit acquisition with per-limiter locking
- Idempotent creation for identical limiter configurations
- Configuration conflict detection for duplicate names with different settings
- Thread-safe in-memory limiter storage behind a storage interface
- Race-tested Go test suite
- Runnable concurrent Python example

## 🚀 Quick start

### Prerequisites

- Go 1.24 or newer
- Python 3 for the optional client example

Clone the repository and start the server:

```sh
git clone https://github.com/buddylindsey/rlaas.git
cd rlaas
go run ./src/cmd/server
```

RLaaS listens on `0.0.0.0:6342`.

### Create a fixed-window limiter

Requests use JSON inside a four-byte length-prefixed TCP frame. A create request
has this payload:

```json
{
  "request_id": "create-1",
  "operation": "create_limiter",
  "body": {
    "name": "github-api",
    "type": "fixed_window",
    "time_window_ms": 60000,
    "budget": 300
  }
}
```

The easiest way to send correctly framed requests is the included Python
example.

## 🐍 Try the concurrent Python example

With the server running, open another terminal and run:

```sh
python3 examples/python/fixed_window.py
```

The example:

1. Opens a pool of three persistent TCP connections.
2. Creates a fixed-window limiter with a budget of 300 calls per minute.
3. Sends 375 acquisition requests from three threads.
4. Reports allowed requests, denied requests, total request time, and average
   round-trip latency.

A first run against a fresh server should finish with:

```text
Requests: 375
Allowed: 300 (expected 300)
Denied: 75 (expected 75)
Result: PASSED
```

The example deliberately reuses the same limiter name. Run it again before the
minute expires to see the server retain the exhausted window and deny all 375
requests. See [the Python example documentation](examples/python/README.md) for
details.

## 🧭 How it works

```text
TCP connection
  → length-prefixed frame
  → JSON codec
  → typed service request
  → service handler
  → limiter store
  → fixed-window limiter
  → typed service response
  → JSON codec
  → length-prefixed frame
```

The layers are intentionally separated:

- **Transport** owns TCP connections and framing, but treats payloads as bytes.
- **Protocol** translates JSON payloads into typed service requests and
  responses.
- **Service** validates operations and coordinates limiter behavior.
- **Storage** owns limiter lookup and atomic acquisition behind an interface.
- **Fixed-window limiter** owns mutable window state behind its own mutex.

See [the architecture notes](docs/architecture.md) for more detail.

## 🧪 Project status and direction

RLaaS currently provides a functional single-process path from a network
request through an atomic fixed-window acquisition. It does not yet provide:

- Persistent limiter configuration or state
- Coordination between multiple RLaaS instances
- Authentication, authorization, or transport encryption
- Graceful shutdown and production-grade connection management
- Operational metrics, tracing, or service-level guarantees
- Additional algorithms such as sliding windows or token buckets

The goal is to evolve RLaaS into a service that teams can confidently operate
in production. Planned work includes persistent storage, distributed
coordination, additional limiter implementations, structured errors for every
failure path, resilient connection handling, and production-grade
observability.

## 🛠 Development

Format and test the project from the repository root:

```sh
go fmt ./...
go vet ./...
go test -race ./...
```

Contributions should remain focused and include tests when behavior changes.
See [CONTRIBUTING.md](CONTRIBUTING.md) for project conventions.

## License

A license has not yet been selected.
