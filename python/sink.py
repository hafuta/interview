"""The Sink: the destination where processed Results are written."""

from __future__ import annotations

import threading
import time

from event import Result

# writeLatency simulates per-write I/O cost at the Sink.
WRITE_LATENCY = 0.005  # seconds


class Sink:
    """The destination where processed Results are written.

    In the real system this is a shared store (e.g. Redis / BigQuery / Spanner).
    Here it is a simulated writer that (a) costs a little latency per write,
    (b) can enforce a maximum concurrency so it behaves like a real downstream
    with a rate/quota limit, and (c) keeps a per-account collected count -- the
    kind of per-account state a real collection service maintains in the store.
    """

    def __init__(self, max_concurrent: int = 0) -> None:
        # max_concurrent caps how many writes may be in flight at once. 0 means
        # unlimited. If more than max_concurrent writes are in flight, the extra
        # ones are recorded as overloads.
        self._max_concurrent = max_concurrent
        self._in_flight = 0
        self._overload = 0
        self._lock = threading.Lock()
        self._written: list[Result] = []
        # account_id -> number of results written for it
        self._per_account: dict[str, int] = {}

    def write(self, r: Result) -> None:
        """Persist a single Result and advance that account's collected count.

        It is safe for concurrent use.

        If max_concurrent is set and exceeded, the write still succeeds but is
        counted as an overload -- a stand-in for "you got throttled / 429'd by
        the backend".
        """
        with self._lock:
            self._in_flight += 1
            cur = self._in_flight
            if self._max_concurrent > 0 and cur > self._max_concurrent:
                self._overload += 1

        try:
            # Simulated write latency (I/O bound).
            time.sleep(WRITE_LATENCY)

            with self._lock:
                self._written.append(r)
                self._per_account[r.account_id] = (
                    self._per_account.get(r.account_id, 0) + 1
                )
        finally:
            with self._lock:
                self._in_flight -= 1

    def written(self) -> list[Result]:
        """Return a copy of everything written so far."""
        with self._lock:
            return list(self._written)

    def count(self) -> int:
        """Return how many results have been written."""
        with self._lock:
            return len(self._written)

    def per_account_counts(self) -> dict[str, int]:
        """Return a copy of the per-account collected counts."""
        with self._lock:
            return dict(self._per_account)

    def overloads(self) -> int:
        """Return how many writes breached the concurrency limit."""
        with self._lock:
            return self._overload

    def __str__(self) -> str:
        return (
            f"sink{{written={self.count()}, overloads={self.overloads()}, "
            f"max_concurrent={self._max_concurrent}}}"
        )
