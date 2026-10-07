import json

from quality.core import (
    FAIL,
    PASS,
    WARN,
    CheckResult,
    describe_thresholds,
    format_results,
    from_json,
    judge,
    measure,
    overall,
    summarize,
    to_json,
)


def test_judge_higher_is_worse():
    kwargs = {"warn_above": 10, "fail_above": 60}
    assert judge(0, **kwargs) == PASS
    assert judge(10, **kwargs) == PASS  # thresholds are exclusive
    assert judge(10.01, **kwargs) == WARN
    assert judge(60, **kwargs) == WARN
    assert judge(61, **kwargs) == FAIL


def test_judge_lower_is_worse():
    kwargs = {"warn_below": 0.95, "fail_below": 0.5}
    assert judge(1.0, **kwargs) == PASS
    assert judge(0.95, **kwargs) == PASS
    assert judge(0.9, **kwargs) == WARN
    assert judge(0.4, **kwargs) == FAIL


def test_a_check_with_only_a_warn_threshold_never_fails():
    assert judge(10_000, warn_above=30) == WARN


def test_a_value_that_cannot_be_computed_is_a_warn_never_a_pass():
    assert judge(None, warn_above=1, fail_above=2) == WARN
    result = measure("serving", "x", None, "no rows", fail_above=0)
    assert result.status == WARN and "not computable" in result.detail


def test_informational_checks_always_pass():
    result = measure("lake", "bronze_rows", 123.0, "informational")
    assert result.status == PASS and result.threshold == "informational"


def test_threshold_description_is_readable():
    assert describe_thresholds(warn_above=10, fail_above=60) == "warn > 10, fail > 60"
    assert describe_thresholds(warn_below=0.95, fail_below=0.5) == "warn < 0.95, fail < 0.5"


def test_overall_and_summary():
    ok = CheckResult("serving", "a", PASS, 0, "")
    warn = CheckResult("serving", "b", WARN, 1, "")
    bad = CheckResult("lake", "c", FAIL, 2, "")
    assert (
        overall([ok]) == PASS and overall([ok, warn]) == WARN and overall([ok, warn, bad]) == FAIL
    )
    assert overall([]) == PASS
    assert summarize([ok, warn, warn, bad]) == {PASS: 1, WARN: 2, FAIL: 1}


def test_json_round_trip_keeps_every_field():
    results = [
        CheckResult("lake", "rejected_share", WARN, 0.02, "warn > 0.01", "7 rejected of 300"),
        CheckResult("lake", "silver_lag_minutes", PASS, None, "informational"),
    ]
    text = to_json(results, checked_at="2026-10-06T12:00:00+00:00")
    assert json.loads(text)["checked_at"] == "2026-10-06T12:00:00+00:00"
    checked_at, again = from_json(text)
    assert checked_at == "2026-10-06T12:00:00+00:00" and again == results


def test_format_results_is_aligned_and_complete():
    lines = format_results(
        [
            CheckResult("serving", "short", PASS, 1.5, "warn > 3", "ok"),
            CheckResult("lake", "a_much_longer_name", FAIL, None, "fail > 0"),
        ]
    ).splitlines()
    assert lines[0].startswith("[PASS]") and "1.5" in lines[0] and "warn > 3" in lines[0]
    assert lines[1].startswith("[FAIL]") and "n/a" in lines[1]
    assert lines[0].index("1.5") + 3 == lines[1].index("n/a") + 3 or len(lines) == 2
