# Python examples

## Fixed window

This example opens three persistent TCP connections and shares them between
three worker threads. The workers make 375 acquisition requests against a
fixed-window limiter with a budget of 300 requests per minute.

Start RLaaS from the repository root:

```sh
go run ./src/cmd/server
```

In another terminal, run the example:

```sh
python3 examples/python/fixed_window.py
```

The script uses the stable limiter name `python-fixed-window-example`. The
first run against a fresh server reports 300 allowed requests, 75 denied
requests, and exits with status zero. Running it again before the one-minute
window resets reuses the exhausted limiter, so all 375 requests are denied and
the script exits with a failure. Restarting the server clears its in-memory
state.

The script also reports the total measured request time and average round-trip
latency. Because requests run concurrently, total request time is the sum of
individual request durations rather than wall-clock runtime. The example uses
only the Python standard library.

## Token bucket

The token-bucket example creates a fresh limiter with a capacity of four permits
and a two-second full refill window:

```sh
python3 examples/python/token_bucket.py
```

It immediately exhausts the bucket and confirms that another request is
denied. It then waits for the server-provided `retry_after_ms`, which is roughly
500 milliseconds, and confirms that one request succeeds before the complete
two-second window has elapsed. An immediate request after that is denied again.

This partial-window refill is the visible difference from fixed-window
behavior. The script prints `Algorithm: token_bucket`, reports each observation,
and exits with status zero only when the expected behavior occurs. It deletes
and recreates its named limiter on each run so repeated runs start with a full
bucket.
