# Collection System — Interview (candidate)

A single hands-on exercise followed by a design discussion.

The same exercise is provided in **two languages — pick whichever you prefer**:

```
interview/
  go/       a slow sequential event collector you IMPROVE (Go)
  python/   the same exercise (Python)
```

The exercise models a real collection system: pull events from a queue,
transform each one, and write the result to a downstream store.

You are encouraged to use AI tools. We care less about whether the AI can write
the code and more about whether you can direct it, understand the result, and
reason about the trade-offs afterwards.

---

## The exercise

A small, self-contained program:

```
Source  ──▶  process(event)  ──▶  Sink
(events)     (enrich + hash)      (write out)
```

It works correctly but is **slow** — it processes one event at a time. Your task:

> **Here is a small collection system. How would you improve it?**

There is no single right answer. Keep the correctness tests passing, and be ready
to discuss what new problem each change introduces.

### Files (same roles in both languages)
| Go | Python | Role |
|----|--------|------|
| [`go/collector.go`](go/collector.go) | [`python/collector.py`](python/collector.py) | **The code you improve.** `run` is a naive sequential loop. |
| [`go/event.go`](go/event.go) | [`python/event.py`](python/event.py) | The `Event`/`Result` model and the per-event `process` function (CPU + I/O cost). |
| [`go/source.go`](go/source.go) | [`python/source.py`](python/source.py) | Produces the events (returns them all as a list/slice). |
| [`go/sink.go`](go/sink.go) | [`python/sink.py`](python/sink.py) | Simulated destination writer with an optional concurrency limit. |
| [`go/main.go`](go/main.go) | [`python/main.py`](python/main.py) | Wires it together and prints throughput. |
| [`go/collector_test.go`](go/collector_test.go) | [`python/test_collector.py`](python/test_collector.py) | Correctness tests — your safety net while refactoring. |

### Running it — Go
```bash
cd go
go run .                       # default 200 events, unlimited sink
go run . -events 500           # heavier load
go run . -sink-limit 8 -events 300   # cap the sink to surface a rate limit
go test ./...                  # prove correctness (keep green through refactors)
go test -bench=. -benchtime=5x # measure throughput
```

### Running it — Python
```bash
cd python
python3 main.py                        # default 200 events, unlimited sink
python3 main.py --events 500           # heavier load
python3 main.py --sink-limit 8 --events 300   # cap the sink to surface a rate limit
python3 -m unittest test_collector     # prove correctness (keep green through refactors)
```
