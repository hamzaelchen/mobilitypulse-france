"""Frozen demo: snapshot export, static build (relative paths), demo mode of the front."""

import importlib.util
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
STATIC = ROOT / "src" / "web" / "static"
if not (ROOT / "scripts").exists() or not STATIC.exists():
    pytest.skip("scripts/ or web static files are not mounted here", allow_module_level=True)


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


export = load("export_demo")
build = load("build_demo")

CAPTURED = "2026-10-07T19:12:30+00:00"
RESPONSES = {
    "/api/status": {"server_time": CAPTURED, "observed_at": "2026-10-07T19:11:20+00:00"},
    "/api/overview": {"stations": 2, "observed_at": "2026-10-07T19:11:20+00:00"},
    "/api/stations": [
        {"station_id": "12", "station_name": "A"},
        {"station_id": "a/b", "station_name": "B"},
    ],
    "/api/priorities?limit=30": [],
    "/api/transfers?limit=30": [],
    "/api/hourly?hours=48": [],
    "/api/quality": [],
    "/api/stations/12/series?hours=24": [{"slot_start": "x", "bikes_available": 3}],
    "/api/stations/a%2Fb/series?hours=24": [],
}


def fake_get(path):
    return RESPONSES[path]


# ----------------------------------------------------------------------------- export


def test_export_writes_one_file_per_endpoint_and_per_station(tmp_path):
    export.export_demo(fake_get, tmp_path)
    for name in ("overview", "stations", "priorities", "transfers", "hourly", "quality"):
        assert (tmp_path / f"{name}.json").is_file(), name
    assert (
        json.loads((tmp_path / "series" / "12.json").read_text(encoding="utf-8"))[0][
            "bikes_available"
        ]
        == 3
    )
    assert (tmp_path / "series" / "a%2Fb.json").is_file()  # unsafe ids are percent-encoded


def test_export_records_when_the_snapshot_was_taken(tmp_path):
    export.export_demo(fake_get, tmp_path)
    meta = json.loads((tmp_path / "meta.json").read_text(encoding="utf-8"))
    assert meta["captured_at"] == CAPTURED and meta["observed_at"].startswith("2026-10-07T19:11")
    assert meta["stations"] == 2


def test_export_refuses_an_empty_snapshot(tmp_path):
    empty = dict(RESPONSES, **{"/api/stations": []})
    with pytest.raises(SystemExit, match="no station"):
        export.export_demo(empty.__getitem__, tmp_path)


# ------------------------------------------------------------------------------ build


@pytest.fixture
def site(tmp_path):
    data = tmp_path / "data"
    export.export_demo(fake_get, data)
    out = tmp_path / "site"
    build.build_demo(data, out, STATIC, repo_url="https://github.com/me/mobilitypulse-france")
    return out


def test_the_site_has_both_pages_and_a_nojekyll_marker(site):
    assert (site / "index.html").is_file() and (site / "app" / "index.html").is_file()
    assert (site / ".nojekyll").is_file()


def test_no_absolute_local_path_is_left_so_any_base_path_works(site):
    for page in (site / "index.html", site / "app" / "index.html"):
        html = page.read_text(encoding="utf-8")
        assert not re.search(r'(?:href|src)="/(?!/)', html), page
    css = (site / "css" / "tokens.css").read_text(encoding="utf-8")
    assert "url(/" not in css and "url(../fonts/" in css


def test_pages_point_to_each_other_and_to_their_assets_relatively(site):
    landing = (site / "index.html").read_text(encoding="utf-8")
    app = (site / "app" / "index.html").read_text(encoding="utf-8")
    assert 'href="css/tokens.css"' in landing and 'href="app/"' in landing
    assert 'href="../css/tokens.css"' in app and 'href="../"' in app


def test_demo_config_is_injected_with_the_right_base_for_each_page(site):
    landing = (site / "index.html").read_text(encoding="utf-8")
    app = (site / "app" / "index.html").read_text(encoding="utf-8")
    assert re.search(r'window\.MP_DEMO\s*=\s*\{[^}]*"base":\s*""', landing)
    assert re.search(r'window\.MP_DEMO\s*=\s*\{[^}]*"base":\s*"\.\./"', app)
    assert landing.index("MP_DEMO") < landing.index("js/common.js")  # defined before use


def test_every_asset_a_page_references_exists_in_the_site(site):
    for page in (site / "index.html", site / "app" / "index.html"):
        html = page.read_text(encoding="utf-8")
        for ref in re.findall(r'(?:href|src)="((?:\.\./)*(?:css|js|fonts)/[^"]+)"', html):
            assert (page.parent / ref).resolve().is_file(), (page.name, ref)


def test_api_files_are_laid_out_where_the_front_will_ask_for_them(site):
    assert (site / "api" / "overview.json").is_file()
    assert (site / "api" / "stations.json").is_file()
    assert (site / "api" / "stations" / "12" / "series.json").is_file()
    assert (site / "api" / "stations" / "a%2Fb" / "series.json").is_file()
    config = json.loads((site / "api" / "config.json").read_text(encoding="utf-8"))
    assert config == {"repo_url": "https://github.com/me/mobilitypulse-france"}


def test_without_a_repo_url_the_config_says_so(tmp_path):
    data = tmp_path / "d"
    export.export_demo(fake_get, data)
    build.build_demo(data, tmp_path / "s", STATIC, repo_url=None)
    config = json.loads((tmp_path / "s" / "api" / "config.json").read_text(encoding="utf-8"))
    assert config == {"repo_url": None}


# ----------------------------------------------------------------------- demo mode copy


def test_every_claim_of_live_data_on_the_landing_has_a_demo_wording():
    html = (STATIC / "landing.html").read_text(encoding="utf-8")
    for needle in (
        'id="live-when"',
        'id="now-label"',
        'class="foot"',
        'id="fin"',
        'id="demo-note"',
    ):
        assert needle in html, needle
    # the closing paragraph and footer that say "live" must carry their frozen alternative
    assert html.count("data-demo=") >= 4


def test_the_front_does_not_open_a_stream_or_poll_in_demo_mode():
    app = (STATIC / "js" / "app.js").read_text(encoding="utf-8")
    landing = (STATIC / "js" / "landing.js").read_text(encoding="utf-8")
    assert "if (!DEMO) { connectStream()" in app
    assert "if (!DEMO) setInterval(refresh" in landing
