#!/usr/bin/env python3
"""Exercise RLaaS with concurrent requests over persistent connections."""

import json
import queue
import socket
import struct
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any


SERVER_ADDRESS = ("127.0.0.1", 6342)
POOL_SIZE = 3
THREAD_COUNT = 3
CALLS_PER_THREAD = 125
EXPECTED_ALLOWED = 300
EXPECTED_DENIED = 75
LIMITER_NAME = "python-fixed-window-example"


class ConnectionPool:
    def __init__(self, address: tuple[str, int], size: int) -> None:
        self._connections: queue.Queue[socket.socket] = queue.Queue(maxsize=size)
        for _ in range(size):
            connection = socket.create_connection(address, timeout=5)
            connection.settimeout(5)
            self._connections.put(connection)

    def request(self, message: dict[str, Any]) -> dict[str, Any]:
        connection = self._connections.get()
        try:
            payload = json.dumps(message).encode("utf-8")
            connection.sendall(struct.pack(">I", len(payload)) + payload)

            response_length = struct.unpack(">I", receive_exact(connection, 4))[0]
            response_payload = receive_exact(connection, response_length)
            return json.loads(response_payload)
        finally:
            self._connections.put(connection)

    def close(self) -> None:
        while not self._connections.empty():
            self._connections.get_nowait().close()


def receive_exact(connection: socket.socket, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = connection.recv(length - len(chunks))
        if not chunk:
            raise ConnectionError("server closed the connection")
        chunks.extend(chunk)
    return bytes(chunks)


def create_limiter(pool: ConnectionPool, name: str) -> None:
    response = pool.request(
        {
            "request_id": f"create-{uuid.uuid4()}",
            "operation": "create_limiter",
            "body": {
                "name": name,
                "type": "fixed_window",
                "time_window_ms": 60_000,
                "budget": EXPECTED_ALLOWED,
            },
        }
    )
    if response.get("status") != "ok":
        raise RuntimeError(f"create limiter failed: {response}")


def run_acquires(
    pool: ConnectionPool, limiter_name: str, worker_id: int
) -> tuple[int, int, float]:
    allowed = 0
    denied = 0
    total_request_seconds = 0.0

    for call_number in range(CALLS_PER_THREAD):
        request_started = time.perf_counter()
        response = pool.request(
            {
                "request_id": f"acquire-{worker_id}-{call_number}-{uuid.uuid4()}",
                "operation": "acquire",
                "body": {"name": limiter_name},
            }
        )
        total_request_seconds += time.perf_counter() - request_started
        if response.get("status") != "ok":
            raise RuntimeError(f"acquire failed: {response}")

        if response["body"]["allowed"]:
            allowed += 1
        else:
            denied += 1

    return allowed, denied, total_request_seconds


def main() -> int:
    pool = ConnectionPool(SERVER_ADDRESS, POOL_SIZE)

    try:
        create_limiter(pool, LIMITER_NAME)

        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            results = list(
                executor.map(
                    lambda worker_id: run_acquires(pool, LIMITER_NAME, worker_id),
                    range(THREAD_COUNT),
                )
            )
    finally:
        pool.close()

    allowed = sum(result[0] for result in results)
    denied = sum(result[1] for result in results)
    total_requests = allowed + denied
    total_request_seconds = sum(result[2] for result in results)
    average_request_ms = total_request_seconds / total_requests * 1_000

    print(f"Limiter: {LIMITER_NAME}")
    print(f"Connections: {POOL_SIZE}")
    print(f"Requests: {total_requests}")
    print(f"Allowed: {allowed} (expected {EXPECTED_ALLOWED})")
    print(f"Denied: {denied} (expected {EXPECTED_DENIED})")
    print(f"Total request time: {total_request_seconds:.3f} seconds")
    print(f"Average request latency: {average_request_ms:.3f} ms")

    if allowed != EXPECTED_ALLOWED or denied != EXPECTED_DENIED:
        print("Result: FAILED", file=sys.stderr)
        return 1

    print("Result: PASSED")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ConnectionError, OSError, RuntimeError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
