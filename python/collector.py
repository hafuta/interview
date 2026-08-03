"""The Collector: reads events, processes each one, writes the Result."""

from __future__ import annotations

from event import process
from sink import Sink
from source import Source


class Collector:
    """Reads events from a Source, processes each one, and writes the Result to a
    Sink.

    The current implementation (run) is the simplest thing that works: a single
    thread that handles one event at a time, start to finish. It is correct but
    slow -- total time is roughly:

        count * (enrich_latency + cpu_cost + write_latency)
    """

    def __init__(self, source: Source, sink: Sink) -> None:
        self.source = source
        self.sink = sink

    def run(self) -> int:
        """Process every event from the Source sequentially and write each Result
        to the Sink. Returns the number of events processed.

        v1: one event at a time. No concurrency, no batching, no rate limiting,
        no error handling beyond stopping on the first Sink failure.
        """
        events = self.source.events()

        processed = 0
        for e in events:
            result = process(e)
            self.sink.write(result)
            processed += 1
        return processed
