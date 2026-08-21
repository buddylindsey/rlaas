# RLaaS

RLaaS is an early-stage distributed rate-limiting service. Its goal is to give
applications a shared, reusable rate limiter instead of having every service
rebuild and operate its own.

The project is being built as an infrastructure-focused Go learning project,
starting with the network and protocol foundations before adding limiter state
and distributed behavior.

## Current foundation

- Concurrent TCP server with length-prefixed message frames
- JSON request parsing for create, acquire, and delete limiter operations
- Separate transport, protocol, and service layers
- A codec boundary that can support a future binary protocol

RLaaS is under active early development and is not production-ready.

## Development

```sh
go test ./...
go run ./src/cmd/server
```

The server listens on `0.0.0.0:6342`.

## License

A license has not yet been selected.
