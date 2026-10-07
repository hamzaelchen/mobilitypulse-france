"""Measure the latency between a GBFS poll and its arrival on the live stream.

Listens to ``/api/stream`` (SSE) for a fixed time. Each message that carries a *new* ``observed_at``
(the poll time of the data) gives one sample: reception time minus ``observed_at``. The first
message is the initial snapshot of whatever data existed already, so it is not a sample.

This measures the pipeline up to the browser's network layer (producer, Kafka, Bronze, Silver, live
updater, PostgreSQL, API stream). Rendering on screen then takes a few milliseconds, which is not
included. Producer, services and this script run on the same machine, so there is no clock skew.

Usage: python scripts/measure_sse_latency.py [--seconds 1140] [--out latency.jsonl] [--port 18000]
"""

from __future__ import annotations

import argparse
import http.client
import json
import statistics
import time
from datetime import UTC, datetime


def listen(host: str, port: int, seconds: float):
    """Yield (event_name, payload, received_at) for every SSE message until the time is up."""
    conn = http.client.HTTPConnection(host, port, timeout=30)
    conn.request("GET", "/api/stream", headers={"Accept": "text/event-stream"})
    response = conn.getresponse()
    end = time.time() + seconds
    name, data = None, []
    while time.time() < end:
        try:
            line = response.fp.readline().decode().rstrip("\n")
        except TimeoutError:
            continue
        if line.startswith("event:"):
            name = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            data.append(line.split(":", 1)[1].strip())
        elif line == "" and data:
            yield name, json.loads("\n".join(data)), datetime.now(UTC)
            name, data = None, []
    conn.close()


def summarize(latencies: list[float]) -> dict[str, float]:
    ordered = sorted(latencies)
    pick = lambda q: ordered[min(len(ordered) - 1, round(q * (len(ordered) - 1)))]  # noqa: E731
    return {
        "samples": len(ordered),
        "min": ordered[0],
        "median": statistics.median(ordered),
        "p95": pick(0.95),
        "max": ordered[-1],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=18000)
    parser.add_argument("--seconds", type=float, default=1140)
    parser.add_argument("--out", default="latency.jsonl")
    args = parser.parse_args()

    last_observed, latencies = None, []
    with open(args.out, "w", encoding="utf-8") as out:
        for name, payload, received in listen(args.host, args.port, args.seconds):
            observed = (payload.get("kpis") or {}).get("observed_at")
            if not observed or observed == last_observed:
                continue
            first = last_observed is None
            last_observed = observed
            seconds = (
                received - datetime.fromisoformat(observed.replace("Z", "+00:00"))
            ).total_seconds()
            out.write(
                json.dumps(
                    {
                        "event": name,
                        "observed_at": observed,
                        "received_at": received.isoformat(),
                        "latency_s": round(seconds, 2),
                        "initial_snapshot": first,
                    }
                )
                + "\n"
            )
            out.flush()
            if not first:
                latencies.append(seconds)
    if latencies:
        print(json.dumps({k: round(v, 1) for k, v in summarize(latencies).items()}))
    else:
        print("no sample collected (is the stack running, and is the collection live?)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
