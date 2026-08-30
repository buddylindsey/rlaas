# Architecture Notes

RLaaS separates network transport from payload representation and application
behavior:

```text
TCP connection
  → length-prefixed frame
  → protocol codec
  → typed service request
  → service handler
  → typed service response
  → protocol codec
  → length-prefixed frame
```

## Transport

`internal/transport/tcp` accepts TCP connections and processes each one in its
own goroutine. It reads and writes frames, but treats each payload as opaque
bytes.

The server bounds connection resource usage with a maximum connection count,
an idle timeout while waiting for a new frame, and shorter deadlines for
finishing frames and writing responses. During graceful shutdown it closes idle
connections immediately, allows active requests a bounded time to finish, and
then cancels and closes remaining work.

The transport emits structured JSON lifecycle logs. A stable connection ID
correlates events before a request can be decoded; after decoding, every request
event carries its client-provided request ID and operation through handling and
response delivery. Payload bodies are deliberately excluded from logs.

Each connection goroutine also contains unexpected panics from request decoding
or handling. The affected connection is closed because its request outcome may
be ambiguous, while the server continues accepting other connections. The panic
record includes the connection metadata, available request metadata, and a stack
trace for diagnosis.

The frame format is a four-byte unsigned big-endian payload length followed by
that number of payload bytes. This allows payloads to contain arbitrary data,
including newline characters.

## Protocol

`internal/protocol` owns translation between payload bytes and typed service
values. The active codec is JSON. Its request envelope contains:

```json
{
  "request_id": "01JXYZ456",
  "operation": "acquire",
  "body": {
    "name": "github-api"
  }
}
```

The codec decodes the envelope, dispatches on `operation`, validates its
operation-specific body, and returns a typed service request. If validation
fails after the envelope is decoded, the protocol error retains `request_id` so
the client can correlate the error response. A future binary codec can produce
the same service request types using a different byte layout.

## Service

`internal/service` contains the application-level request and response types
and the handler interface. It deliberately has no knowledge of TCP, frame
lengths, or JSON field names.

Limiter configuration and atomic acquisition are accessed through the
`LimiterStore` interface, so handlers do not depend on the persistence model.
Configurations are normalized to trimmed, lowercase names and validated as
immutable service values before they reach the store. Stores defensively reject
invalid zero values.
The initial implementation keeps fixed-window limiters in a map protected by
an RWMutex. Each limiter has its own mutex protecting window state, allowing
different named limiters to process acquisitions independently.

The next implementation work is persistent storage and additional limiter
algorithms.
