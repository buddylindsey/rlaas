# Contributing

RLaaS is in its early foundation stage. Keep changes focused, small, and
covered by tests where behavior changes.

## Local checks

Run these commands from the repository root before opening a change:

```sh
go fmt ./...
go test ./...
```

Use `gofmt` for all Go source files. Prefer standard-library solutions and add
new dependencies only when they solve a concrete need.

## Design guidelines

- Keep TCP transport concerns inside `internal/transport`.
- Keep payload representation concerns inside `internal/protocol`.
- Keep rate-limiting rules and use cases inside `internal/service`.
- Avoid changing the wire protocol without documenting and testing the change.

## Commit guidance

Use short, imperative commit subjects, such as `Add token bucket state` or
`Decode delete limiter requests`.
