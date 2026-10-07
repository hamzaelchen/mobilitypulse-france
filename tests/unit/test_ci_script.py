import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ci_assert_skips.py"
if not SCRIPT.exists():
    pytest.skip("scripts/ is not mounted in this environment", allow_module_level=True)

spec = importlib.util.spec_from_file_location("ci_assert_skips", SCRIPT)
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


def junit(tmp_path, *, tests, skipped=0, failures=0, errors=0, wrapped=True):
    suite = (
        f'<testsuite name="pytest" tests="{tests}" skipped="{skipped}" '
        f'failures="{failures}" errors="{errors}"></testsuite>'
    )
    path = tmp_path / "junit.xml"
    path.write_text(f"<testsuites>{suite}</testsuites>" if wrapped else suite)
    return path


def test_a_clean_run_passes(tmp_path):
    assert ci.check(junit(tmp_path, tests=40), max_skipped=0, min_tests=1) == []


def test_skipped_tests_beyond_the_allowance_fail(tmp_path):
    problems = ci.check(junit(tmp_path, tests=40, skipped=3), max_skipped=0, min_tests=1)
    assert problems == ["3 tests were skipped (allowed: 0)"]
    assert ci.check(junit(tmp_path, tests=40, skipped=1), max_skipped=1, min_tests=1) == []


def test_an_empty_run_fails(tmp_path):
    problems = ci.check(junit(tmp_path, tests=0), max_skipped=0, min_tests=5)
    assert problems == ["only 0 tests ran (expected at least 5)"]


def test_failures_and_errors_fail(tmp_path):
    problems = ci.check(junit(tmp_path, tests=10, failures=1, errors=2), 0, 1)
    assert problems == ["1 failures, 2 errors"]


def test_both_junit_layouts_are_read(tmp_path):
    assert ci.counts(junit(tmp_path, tests=7, skipped=2, wrapped=False))["tests"] == 7
    assert ci.counts(junit(tmp_path, tests=7, skipped=2, wrapped=True))["skipped"] == 2
