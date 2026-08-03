"""Runs the collection demo end to end and prints timing/throughput.

    python main.py                  # defaults: 200 events, unlimited sink
    python main.py --events 500     # heavier load
    python main.py --sink-limit 8   # cap sink concurrency to surface rate limiting
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import Counter

from collector import Collector
from sink import Sink
from source import Source


def main() -> int:
    parser = argparse.ArgumentParser(description="Collection system demo")
    parser.add_argument(
        "--events", type=int, default=200, help="number of events to collect"
    )
    parser.add_argument(
        "--sink-limit",
        type=int,
        default=0,
        help="max concurrent sink writes (0 = unlimited)",
    )
    args = parser.parse_args()

    source = Source(args.events)
    sink = Sink(args.sink_limit)
    collector = Collector(source, sink)

    print(f"CPUs available: {os.cpu_count()}")
    print(f"collecting {args.events} events ...")

    start = time.perf_counter()
    processed = collector.run()
    elapsed = time.perf_counter() - start

    throughput = processed / elapsed if elapsed > 0 else 0.0
    print(f"done: processed={processed} in {elapsed:.3f}s ({throughput:.1f} events/sec)")
    print(sink)

    # Show the per-account skew so the "one account dominates" reality is visible.
    dist = Counter(e.account_id for e in source.events())
    print(f"account distribution (source): {dict(dist)}")

    # The sink keeps a real per-account collected count -- the per-account state
    # a collection service maintains in its store. One account dominating here is
    # the hot-key you'd have to reckon with.
    print(f"per-account collected (sink):  {sink.per_account_counts()}")

    if sink.count() != args.events:
        print(
            f"WARNING: sink has {sink.count()} results but {args.events} events "
            f"were expected -- data was lost!"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
