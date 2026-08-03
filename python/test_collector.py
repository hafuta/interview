"""Correctness tests for the collector.

They define what "correct" means and should keep passing as the implementation
changes.

A correct collector, for N input events, must:
  1. Write exactly N results to the sink (no loss, no duplicates).
  2. Produce, for each event ID, the same enriched value and checksum that the
     reference process() function produces.
  3. Cover every input ID exactly once.

Run with:  python -m pytest        (or)   python -m unittest
"""

from __future__ import annotations

import unittest

import event
import sink as sink_module
from collector import Collector
from event import Result, process
from sink import Sink
from source import Source


def _expected_results(count: int) -> dict[int, Result]:
    """Ground-truth output for the given event count, keyed by event ID."""
    return {e.id: process(e) for e in Source(count).events()}


class CollectorTest(unittest.TestCase):
    def setUp(self) -> None:
        # Keep the suite fast: shrink the per-event costs for tests only.
        self._orig = (
            event.BASE_LATENCY,
            event.LATENCY_STEP,
            event.HASH_ROUNDS,
            sink_module.WRITE_LATENCY,
        )
        event.BASE_LATENCY = 0.001
        event.LATENCY_STEP = 0.001
        event.HASH_ROUNDS = 1000
        sink_module.WRITE_LATENCY = 0.001

    def tearDown(self) -> None:
        (
            event.BASE_LATENCY,
            event.LATENCY_STEP,
            event.HASH_ROUNDS,
            sink_module.WRITE_LATENCY,
        ) = self._orig

    def test_collector_correctness(self) -> None:
        count = 50

        source = Source(count)
        sink = Sink(0)
        collector = Collector(source, sink)

        processed = collector.run()
        self.assertEqual(processed, count, "run reported wrong processed count")

        got = sink.written()
        self.assertEqual(len(got), count, "sink has wrong number of results")

        want = _expected_results(count)

        seen: set[int] = set()
        for r in got:
            self.assertNotIn(r.id, seen, f"duplicate result for ID {r.id}")
            seen.add(r.id)

            self.assertIn(r.id, want, f"unexpected result for ID {r.id}")
            exp = want[r.id]
            self.assertEqual(r.enriched, exp.enriched, f"ID {r.id}: enriched mismatch")
            self.assertEqual(r.checksum, exp.checksum, f"ID {r.id}: checksum mismatch")

        self.assertEqual(
            sorted(seen), list(range(count)), "did not cover every ID exactly once"
        )

    def test_collector_empty(self) -> None:
        sink = Sink(0)
        collector = Collector(Source(0), sink)

        processed = collector.run()
        self.assertEqual(processed, 0)
        self.assertEqual(sink.count(), 0)


if __name__ == "__main__":
    unittest.main()
