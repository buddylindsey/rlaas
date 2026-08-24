# Python fixed-window example

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
