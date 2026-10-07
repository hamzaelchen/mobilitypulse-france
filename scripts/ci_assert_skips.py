"""Fail a CI job whose pytest run skipped more tests than allowed (or ran none).

Usage: ``python scripts/ci_assert_skips.py junit.xml --max-skipped 0``

A green run made of skipped tests proves nothing: this makes "the Spark / PostgreSQL tests really
ran" an explicit, checked fact of the job.
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def counts(junit_xml: Path) -> dict[str, int]:
    root = ET.parse(junit_xml).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    total = {"tests": 0, "skipped": 0, "failures": 0, "errors": 0}
    for suite in suites:
        for key in total:
            total[key] += int(suite.get(key, 0))
    return total


def check(junit_xml: Path, max_skipped: int, min_tests: int) -> list[str]:
    c = counts(junit_xml)
    problems = []
    if c["tests"] < min_tests:
        problems.append(f"only {c['tests']} tests ran (expected at least {min_tests})")
    if c["skipped"] > max_skipped:
        problems.append(f"{c['skipped']} tests were skipped (allowed: {max_skipped})")
    if c["failures"] or c["errors"]:
        problems.append(f"{c['failures']} failures, {c['errors']} errors")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("junit_xml", type=Path)
    parser.add_argument("--max-skipped", type=int, default=0)
    parser.add_argument("--min-tests", type=int, default=1)
    args = parser.parse_args()
    problems = check(args.junit_xml, args.max_skipped, args.min_tests)
    c = counts(args.junit_xml)
    print(
        f"{c['tests']} tests, {c['skipped']} skipped, {c['failures']} failed, {c['errors']} errors"
    )
    for problem in problems:
        print(f"::error::{problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
