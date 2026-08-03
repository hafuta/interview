"""Event/Result model and the per-event ``process`` function."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class Event:
    """A single raw record pulled from the Source.

    Think of it as one cloud resource description, one log line, or one API page.

    - account_id: which customer/cloud account this event belongs to. One account
      owns many resources.
    - resource_id: which specific thing inside the account (a bucket, a VM, a role).
    """

    id: int
    account_id: str
    resource_id: str
    payload: str


@dataclass
class Result:
    """What the collector produces after processing an Event.

    In a real system this is the normalized record we write to the Sink
    (BigQuery, etc.).
    """

    id: int
    account_id: str
    resource_id: str
    enriched: str
    checksum: str
    processed: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# Tunable costs. Chosen so the demo takes a sensible amount of time: long enough
# to feel slow, short enough to re-run live. Tests shrink these (see conftest).
BASE_LATENCY = 0.002  # seconds: minimum per-event I/O latency (the fastest event)
LATENCY_STEP = 0.001  # seconds: extra latency each earlier event in the window adds
HASH_ROUNDS = 50_000  # controls the CPU-bound cost per event


def enrich_latency_for(event_id: int) -> float:
    """Return a per-event I/O latency (seconds).

    It varies with the ID within a fixed window so events do not all take exactly
    the same time; the window keeps total runtime bounded regardless of event
    count.
    """
    window = 20
    pos = event_id % window
    steps = (window - 1 - pos) * LATENCY_STEP
    return BASE_LATENCY + steps


def process(e: Event) -> Result:
    """Transform a single Event into a Result.

    It mixes two kinds of cost:
      - CPU-bound work: hashing the payload many times (burns a core).
      - I/O-bound / latency work: a sleep simulating a slow upstream API call
        (enrichment lookup) that does NOT burn CPU but blocks the caller.

    Both costs together are what make the sequential collector slow.
    """
    # I/O-bound cost: pretend we call an external enrichment API. The latency
    # varies per event (see enrich_latency_for).
    time.sleep(enrich_latency_for(e.id))

    # CPU-bound cost: hash the payload repeatedly.
    digest = hashlib.sha256(e.payload.encode()).digest()
    for _ in range(HASH_ROUNDS):
        digest = hashlib.sha256(digest).digest()
    checksum = digest.hex()

    return Result(
        id=e.id,
        account_id=e.account_id,
        resource_id=e.resource_id,
        enriched=f"enriched({e.payload})",
        checksum=checksum,
    )
