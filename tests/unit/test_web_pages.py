"""Landing page at /, application at /app (no database needed: static routes only)."""

import asyncio
import json
import re

import pytest

pytest.importorskip("psycopg")
pytest.importorskip("fastapi")

from web import app as web_app  # noqa: E402


def request(path):
    """Call the ASGI app directly (no lifespan, no network): (status, headers, body)."""
    sent = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "GET",
        "path": path, "raw_path": path.encode(), "query_string": b"", "headers": [],
        "server": ("test", 80), "client": ("test", 1), "scheme": "http", "root_path": "",
    }  # fmt: skip
    asyncio.run(web_app.app(scope, receive, send))
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return start["status"], dict(start["headers"]), body.decode(errors="replace")


def test_the_landing_page_is_served_at_the_root():
    status, headers, body = request("/")
    assert status == 200 and headers[b"content-type"].startswith(b"text/html")
    assert '<html lang="fr">' in body and "viewport" in body
    assert body.count("<h1") == 1


def test_the_application_moves_to_app():
    status, _, body = request("/app")
    assert status == 200 and 'id="map"' in body and "/js/app.js" in body


def test_the_old_index_path_no_longer_serves_two_copies_of_the_app():
    assert 'id="map"' not in request("/")[2]


def test_the_main_button_is_a_real_keyboard_reachable_link_to_the_app():
    body = request("/")[2]
    assert re.search(r'<a[^>]+href="/app"[^>]*>\s*Accéder au projet', body)


def test_the_landing_explains_the_business_problem_and_the_three_levels():
    body = request("/")[2].lower()
    for expected in (
        "descriptif",
        "prédictif",
        "prescriptif",
        "gbfs",
        "kafka",
        "spark",
        "postgresql",
    ):
        assert expected in body, expected


def test_the_landing_is_honest_about_what_is_not_done():
    body = request("/")[2]
    assert "Ce que le projet fait" in body and "Ce qu'il ne fait pas encore" in body
    assert "heuristique" in body
    assert "aucun modèle prédictif" in body.lower()


def test_every_api_call_of_the_landing_is_a_real_route():
    body = request("/")[2]
    called = set(re.findall(r'["\'`](/api/[a-z/]+)', body))
    routes = {route.path for route in web_app.app.routes}
    assert called, "the landing must show live numbers from the API"
    assert called <= routes, called - routes


def test_the_secondary_link_comes_from_the_configuration(monkeypatch):
    monkeypatch.setenv("PROJECT_REPO_URL", "https://example.org/repo")
    status, _, body = request("/api/config")
    assert status == 200 and json.loads(body) == {"repo_url": "https://example.org/repo"}


@pytest.mark.parametrize("value", ["", "javascript:alert(1)", "ftp://x", "not a url"])
def test_an_unset_or_unsafe_repository_url_is_not_exposed(monkeypatch, value):
    monkeypatch.setenv("PROJECT_REPO_URL", value)
    assert json.loads(request("/api/config")[2]) == {"repo_url": None}


@pytest.mark.parametrize("path", ["/", "/app", "/js/app.js", "/js/common.js", "/css/tokens.css"])
def test_pages_scripts_and_styles_are_revalidated_so_an_update_is_never_stale(path):
    status, headers, _ = request(path)
    assert status == 200 and headers[b"cache-control"] == b"no-cache"


def test_fonts_may_be_cached_for_a_day():
    status, headers, _ = request("/fonts/bricolage-grotesque-latin-wght-normal.woff2")
    assert status == 200 and headers[b"cache-control"] == b"public, max-age=86400"


def test_api_responses_are_never_cached_by_the_browser():
    status, headers, _ = request("/api/config")
    assert status == 200 and headers[b"cache-control"] == b"no-store"
