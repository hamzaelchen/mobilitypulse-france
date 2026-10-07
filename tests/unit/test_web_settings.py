import pytest

pytest.importorskip("psycopg")
pytest.importorskip("fastapi")

from web import app as web_app  # noqa: E402


def _base_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", "db")
    monkeypatch.setenv("GRAFANA_DB_PASSWORD", "secret-value")
    monkeypatch.delenv("POSTGRES_SCHEMA", raising=False)


def test_settings_use_the_read_only_role_and_hide_the_password(monkeypatch):
    _base_env(monkeypatch)
    s = web_app.WebSettings()
    assert s.user == "grafana_ro"
    assert "secret-value" not in repr(s)


@pytest.mark.parametrize("schema", ["mobility; DROP TABLE x", "Mob", "a-b", "1abc", ""])
def test_unsafe_schema_names_are_refused(monkeypatch, schema):
    _base_env(monkeypatch)
    monkeypatch.setenv("POSTGRES_SCHEMA", schema)
    with pytest.raises(RuntimeError):
        web_app.WebSettings()


def test_missing_password_is_an_error(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.delenv("GRAFANA_DB_PASSWORD")
    with pytest.raises(RuntimeError, match="GRAFANA_DB_PASSWORD"):
        web_app.WebSettings()


def test_static_pages_ship_with_the_package():
    for name in ("landing.html", "app.html"):
        assert (web_app.STATIC_DIR / name).is_file(), name
