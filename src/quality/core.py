"""Data-quality results and verdicts (pure Python, no I/O).

A check measures one number and compares it with explicit thresholds. The thresholds are
CONVENTIONS (what we consider worrying), documented next to each check, not measured facts.
A check that cannot be computed is a WARN with the reason, never a silent PASS.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
_ORDER = {PASS: 0, WARN: 1, FAIL: 2}


@dataclass(frozen=True)
class CheckResult:
    layer: str  # "lake" or "serving"
    name: str
    status: str
    value: float | None
    threshold: str  # human readable, e.g. "warn > 10, fail > 60"
    detail: str = ""


def judge(
    value: float | None,
    *,
    warn_above: float | None = None,
    fail_above: float | None = None,
    warn_below: float | None = None,
    fail_below: float | None = None,
) -> str:
    """Verdict for one measured value. ``None`` (not computable) is a WARN."""
    if value is None:
        return WARN
    if (fail_above is not None and value > fail_above) or (
        fail_below is not None and value < fail_below
    ):
        return FAIL
    if (warn_above is not None and value > warn_above) or (
        warn_below is not None and value < warn_below
    ):
        return WARN
    return PASS


def describe_thresholds(
    *,
    warn_above: float | None = None,
    fail_above: float | None = None,
    warn_below: float | None = None,
    fail_below: float | None = None,
) -> str:
    parts = []
    for label, op, limit in (
        ("warn", ">", warn_above),
        ("fail", ">", fail_above),
        ("warn", "<", warn_below),
        ("fail", "<", fail_below),
    ):
        if limit is not None:
            parts.append(f"{label} {op} {limit:g}")
    return ", ".join(parts) or "informational"


def measure(
    layer: str,
    name: str,
    value: float | None,
    detail: str = "",
    **thresholds: float | None,
) -> CheckResult:
    """Build a result from a measured value and its thresholds."""
    status = judge(value, **thresholds)
    note = detail if value is not None else (detail + " (not computable)").strip()
    return CheckResult(layer, name, status, value, describe_thresholds(**thresholds), note)


def format_results(results: list[CheckResult]) -> str:
    """One aligned line per check: status, layer, name, value, thresholds, detail."""
    width = max((len(r.name) for r in results), default=0)
    lines = []
    for r in results:
        value = "n/a" if r.value is None else f"{r.value:g}"
        head = f"[{r.status:4}] {r.layer:7} {r.name:<{width}}  {value:>10}"
        lines.append(f"{head}  ({r.threshold})  {r.detail}".rstrip())
    return "\n".join(lines)


def overall(results: Iterable[CheckResult]) -> str:
    worst = max((_ORDER[r.status] for r in results), default=0)
    return next(status for status, rank in _ORDER.items() if rank == worst)


def summarize(results: Iterable[CheckResult]) -> dict[str, int]:
    counts = Counter(r.status for r in results)
    return {PASS: counts[PASS], WARN: counts[WARN], FAIL: counts[FAIL]}


def to_json(results: Iterable[CheckResult], *, checked_at: str) -> str:
    return json.dumps(
        {"checked_at": checked_at, "results": [asdict(r) for r in results]},
        indent=2,
        ensure_ascii=False,
    )


def from_json(text: str) -> tuple[str, list[CheckResult]]:
    payload = json.loads(text)
    return payload["checked_at"], [CheckResult(**r) for r in payload["results"]]
