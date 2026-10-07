"""Take a snapshot of the running application's API, for the frozen portfolio demo.

Reads the read-only API (the stack must be running with data) and writes one JSON file per
endpoint, plus the 24-hour series of every station and a ``meta.json`` that records when the
snapshot was taken. ``scripts/build_demo.py`` turns these files into a static website.

Usage: python scripts/export_demo.py [--api http://localhost:18000] [--out demo/data]
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import quote

GetJson = Callable[[str], Any]


def export_demo(get_json: GetJson, out_dir: Path, *, series_hours: int = 24) -> dict[str, Any]:
    status = get_json("/api/status")
    stations = get_json("/api/stations")
    if not stations:
        raise SystemExit("no station in the API: is the stack running, with data in PostgreSQL?")
    files = {
        "overview": get_json("/api/overview"),
        "stations": stations,
        "priorities": get_json("/api/priorities?limit=30"),
        "transfers": get_json("/api/transfers?limit=30"),
        "hourly": get_json("/api/hourly?hours=48"),
        "quality": get_json("/api/quality"),
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, payload in files.items():
        write_json(out_dir / f"{name}.json", payload)
    series_dir = out_dir / "series"
    series_dir.mkdir(exist_ok=True)
    for station in stations:
        key = quote(str(station["station_id"]), safe="")
        rows = get_json(f"/api/stations/{key}/series?hours={series_hours}")
        write_json(series_dir / f"{key}.json", rows)
    meta = {
        "captured_at": status["server_time"],
        "observed_at": status["observed_at"],
        "stations": len(stations),
    }
    write_json(out_dir / "meta.json", meta)
    return meta


def write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )


def http_getter(api: str) -> GetJson:
    def get(path: str) -> Any:
        with urllib.request.urlopen(api.rstrip("/") + path, timeout=30) as response:  # noqa: S310
            return json.load(response)

    return get


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://localhost:18000")
    parser.add_argument("--out", default="demo/data")
    args = parser.parse_args()
    meta = export_demo(http_getter(args.api), Path(args.out))
    print(f"Snapshot of {meta['stations']} stations, data observed at {meta['observed_at']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
