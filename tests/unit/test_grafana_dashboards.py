"""Provisioned Grafana dashboards: valid JSON, unique ids, the right datasource, no secret."""

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DASHBOARDS = sorted((ROOT / "grafana" / "dashboards").glob("*.json"))
pytestmark = pytest.mark.skipif(not DASHBOARDS, reason="grafana/dashboards is not mounted")


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_every_dashboard_is_valid_json_with_a_unique_uid_and_title():
    dashboards = [load(p) for p in DASHBOARDS]
    uids = [d["uid"] for d in dashboards]
    titles = [d["title"] for d in dashboards]
    assert len(set(uids)) == len(uids) and len(set(titles)) == len(titles)
    assert all(d["title"].strip() and d["panels"] for d in dashboards)


def test_panel_ids_are_unique_inside_a_dashboard():
    for path in DASHBOARDS:
        ids = [p["id"] for p in load(path)["panels"]]
        assert len(set(ids)) == len(ids), path.name


def test_every_query_uses_the_provisioned_datasource():
    for path in DASHBOARDS:
        for panel in load(path)["panels"]:
            for target in panel.get("targets", []):
                assert target["datasource"]["uid"] == "mobilitypulse-pg", (
                    path.name,
                    panel["title"],
                )
                assert target["rawSql"].strip(), (path.name, panel["title"])


def test_dashboards_are_read_only_and_hold_no_credential():
    for path in DASHBOARDS:
        dashboard = load(path)
        assert dashboard["editable"] is False, path.name
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"(?i)password|secret|token", text), path.name


def test_the_dashboards_of_the_documentation_exist():
    assert {p.stem for p in DASHBOARDS} == {
        "stations-map",
        "critical-stations",
        "history",
        "rebalancing",
        "data-quality",
    }
