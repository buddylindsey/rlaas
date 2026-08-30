#!/usr/bin/env python3
"""Demonstrate continuous token-bucket refill over one persistent connection."""

import json
import socket
import struct
import sys
import time
import uuid
from typing import Any


SERVER_ADDRESS = ("127.0.0.1", 6342)
LIMITER_NAME = "python-token-bucket-example"
BUDGET = 4
TIME_WINDOW_MS = 2_000


class Client:
    def __init__(self, address: tuple[str, int]) -> None:
        self._connection = socket.create_connection(address, timeout=5)
        self._connection.settimeout(5)

    def request(self, operation: str, body: dict[str, Any]) -> dict[str, Any]:
        request_id = f"python-token-{uuid.uuid4()}"
        payload = json.dumps(
            {"request_id": request_id, "operation": operation, "body": body}
        ).encode("utf-8")
        self._connection.sendall(struct.pack(">I", len(payload)) + payload)

        response_length = struct.unpack(">I", receive_exact(self._connection, 4))[0]
        response = json.loads(receive_exact(self._connection, response_length))
        if response.get("request_id") != request_id:
            raise RuntimeError(f"response request_id mismatch: {response}")
        return response

    def close(self) -> None:
        self._connection.close()


def receive_exact(connection: socket.socket, length: int) -> bytes:
    payload = bytearray()
    while len(payload) < length:
        chunk = connection.recv(length - len(payload))
        if not chunk:
            raise ConnectionError("server closed the connection")
        payload.extend(chunk)
    return bytes(payload)


def require_ok(response: dict[str, Any], operation: str) -> dict[str, Any]:
    if response.get("status") != "ok":
        raise RuntimeError(f"{operation} failed: {response}")
    return response["body"]


def delete_if_present(client: Client) -> None:
    response = client.request("delete_limiter", {"name": LIMITER_NAME})
    if response.get("status") == "ok":
        return
    if response.get("error", {}).get("code") != "limiter_not_found":
        raise RuntimeError(f"delete limiter failed: {response}")


def create_limiter(client: Client) -> None:
    body = require_ok(
        client.request(
            "create_limiter",
            {
                "name": LIMITER_NAME,
                "type": "token_bucket",
                "time_window_ms": TIME_WINDOW_MS,
                "budget": BUDGET,
            },
        ),
        "create limiter",
    )
    if body.get("created") is not True:
        raise RuntimeError(f"expected a fresh limiter: {body}")


def acquire(client: Client) -> dict[str, Any]:
    return require_ok(
        client.request("acquire", {"name": LIMITER_NAME}), "acquire"
    )


def main() -> int:
    client = Client(SERVER_ADDRESS)
    result = "FAILED"
    try:
        delete_if_present(client)
        create_limiter(client)

        initial = [acquire(client) for _ in range(BUDGET + 1)]
        initial_allowed = sum(1 for response in initial if response["allowed"])
        denied = initial[-1]
        retry_after_ms = denied["retry_after_ms"]

        wait_seconds = (retry_after_ms + 25) / 1_000
        time.sleep(wait_seconds)
        after_refill = acquire(client)
        immediately_after = acquire(client)

        passed = (
            initial_allowed == BUDGET
            and not denied["allowed"]
            and 0 < retry_after_ms < TIME_WINDOW_MS
            and after_refill["allowed"]
            and not immediately_after["allowed"]
        )
        result = "PASSED" if passed else "FAILED"

        print(f"Limiter: {LIMITER_NAME}")
        print("Algorithm: token_bucket")
        print(f"Capacity: {BUDGET}")
        print(f"Full refill window: {TIME_WINDOW_MS} ms")
        print(f"Initial burst allowed: {initial_allowed} (expected {BUDGET})")
        print(f"Next request denied: {not denied['allowed']}")
        print(f"Server retry_after_ms: {retry_after_ms}")
        print(f"Waited for one token: {wait_seconds * 1_000:.0f} ms")
        print(f"Allowed before full window elapsed: {after_refill['allowed']}")
        print(f"Immediate next request denied: {not immediately_after['allowed']}")
        print(f"Result: {result}")
        return 0 if passed else 1
    finally:
        try:
            delete_if_present(client)
        finally:
            client.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ConnectionError, KeyError, OSError, RuntimeError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
