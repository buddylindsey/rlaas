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
operation-specific body, and returns a typed service request. A future binary
codec can produce the same service request types using a different byte layout.

## Service

`internal/service` contains the application-level request and response types
and the handler interface. It deliberately has no knowledge of TCP, frame
lengths, or JSON field names.

The next implementation work is rate-limiter state and the business behavior
for creating, acquiring from, and deleting limiters.
