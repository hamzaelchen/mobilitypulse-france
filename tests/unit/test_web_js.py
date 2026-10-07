"""Pure front-end helpers (static/js/common.js), run under node with a minimal window stub."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

COMMON = Path(__file__).resolve().parents[2] / "src" / "web" / "static" / "js" / "common.js"

pytestmark = [
    pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed"),
    pytest.mark.skipif(not COMMON.exists(), reason="web static files are not mounted here"),
]

MEDIA = "matchMedia: () => ({ matches: false, addEventListener() {} })"
STUB_FETCH = """
const calls = [];
global.fetch = async (url) => {
  calls.push(url);
  return { ok: true, json: async () => ({}) };
};
"""


def run_node(script: str):
    done = subprocess.run(
        ["node", "-"], input=script.encode("utf-8"), capture_output=True, check=True
    )
    return json.loads(done.stdout.decode("utf-8"))


def call(expression: str):
    script = (
        f"global.window = {{ {MEDIA} }};\n"
        + COMMON.read_text(encoding="utf-8")
        + f"\nconsole.log(JSON.stringify({expression}));\n"
    )
    return run_node(script)


def fetched_urls(window_fields: str, call_expression: str):
    """URLs that ``window.MP.getJson`` asks fetch for, under a given window configuration."""
    script = (
        f"global.window = {{ {window_fields} {MEDIA} }};\n"
        + STUB_FETCH
        + COMMON.read_text(encoding="utf-8")
        + f"\n(async () => {{ await {call_expression}; "
        + "console.log(JSON.stringify(calls)); })();\n"
    )
    return run_node(script)


@pytest.mark.parametrize(
    ("count", "expected"),
    [(0, "0 vélo"), (1, "1 vélo"), (2, "2 vélos"), (12345, "12 345 vélos")],
)
def test_plural_follows_french_rules(count, expected):
    assert call(f"window.MP.plural({count}, 'vélo', 'vélos')") == expected


def test_plural_with_an_adjective_agrees_too():
    assert call("window.MP.plural(1, 'place libre', 'places libres')") == "1 place libre"
    assert call("window.MP.plural(3, 'place libre', 'places libres')") == "3 places libres"


@pytest.mark.parametrize(
    ("hours", "lower_bound", "expected"),
    [
        (None, False, "–"),
        (0.4, False, "0,4 h"),
        (1.5, True, "≥ 1,5 h"),
        (47.9, False, "47,9 h"),
        (2184.37, False, "91 j"),
        (72, True, "≥ 3 j"),
    ],
)
def test_time_in_state_never_splits_its_unit_and_switches_to_days(hours, lower_bound, expected):
    h = "null" if hours is None else hours
    assert call(f"window.MP.hoursLabel({h}, {str(lower_bound).lower()})") == expected


def call_demo(call_expression: str, base: str = "../"):
    return fetched_urls(f"MP_DEMO: {{ base: {json.dumps(base)} }},", call_expression)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/api/overview", "../api/overview.json"),
        ("/api/priorities?limit=30", "../api/priorities.json"),
        ("/api/hourly?hours=48", "../api/hourly.json"),
        ("/api/config", "../api/config.json"),
        ("/api/stations/12/series?hours=24", "../api/stations/12/series.json"),
        ("/api/stations/a%2Fb/series?hours=24", "../api/stations/a%2Fb/series.json"),
    ],
)
def test_in_demo_mode_api_calls_read_the_snapshot_files(path, expected):
    assert call_demo(f"window.MP.getJson({json.dumps(path)})") == [expected]


def test_the_demo_base_may_be_empty_for_the_landing():
    assert call_demo("window.MP.getJson('/api/stations')", base="") == ["api/stations.json"]


def test_without_demo_mode_api_calls_are_unchanged():
    assert fetched_urls("", "window.MP.getJson('/api/overview')") == ["/api/overview"]
