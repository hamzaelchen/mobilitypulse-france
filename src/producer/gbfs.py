"""GBFS 2.x data sources: live HTTP API or local fixture files."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Protocol

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class GbfsFetchError(Exception):
    """A GBFS document could not be retrieved or is not usable."""


class GbfsSource(Protocol):
    def fetch_station_information(self) -> dict[str, Any]: ...

    def fetch_station_status(self) -> dict[str, Any]: ...


def resolve_feed_urls(discovery: dict[str, Any], language: str) -> dict[str, str]:
    """Extract ``{feed_name: url}`` from a GBFS 2.x discovery document (gbfs.json)."""
    data = discovery.get("data")
    if not isinstance(data, dict):
        raise GbfsFetchError("Discovery document has no 'data' object")
    languages = [v for v in data.values() if isinstance(v, dict) and "feeds" in v]
    if not languages:
        raise GbfsFetchError(
            f"No 'feeds' found in discovery document (version={discovery.get('version')!r}); "
            "only GBFS 2.x is supported"
        )
    chosen = data.get(language) if isinstance(data.get(language), dict) else languages[0]
    feeds = chosen["feeds"]
    return {f["name"]: f["url"] for f in feeds if "name" in f and "url" in f}


class HttpGbfsSource:
    def __init__(self, discovery_url: str, language: str, timeout_seconds: float) -> None:
        self._discovery_url = discovery_url
        self._language = language
        self._timeout = timeout_seconds
        self._feed_urls: dict[str, str] | None = None
        self._session = requests.Session()
        retry = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        self._session.mount("https://", HTTPAdapter(max_retries=retry))
        self._session.mount("http://", HTTPAdapter(max_retries=retry))
        self._session.headers["User-Agent"] = "mobilitypulse-france/0.1"

    def _get_json(self, url: str) -> dict[str, Any]:
        try:
            response = self._session.get(url, timeout=self._timeout)
            response.raise_for_status()
            payload = response.json()
        except requests.Timeout as exc:
            raise GbfsFetchError(f"Timeout after {self._timeout}s on {url}") from exc
        except requests.HTTPError as exc:
            raise GbfsFetchError(f"HTTP {exc.response.status_code} on {url}") from exc
        except (requests.RequestException, ValueError) as exc:
            raise GbfsFetchError(f"Cannot read {url}: {exc}") from exc
        if not isinstance(payload, dict):
            raise GbfsFetchError(f"Unexpected JSON root type on {url}")
        return payload

    def _feed_url(self, name: str) -> str:
        if self._feed_urls is None:
            self._feed_urls = resolve_feed_urls(self._get_json(self._discovery_url), self._language)
            logger.info("GBFS feeds discovered: %s", sorted(self._feed_urls))
        try:
            return self._feed_urls[name]
        except KeyError as exc:
            raise GbfsFetchError(f"Feed {name!r} not listed in discovery document") from exc

    def fetch_station_information(self) -> dict[str, Any]:
        return self._get_json(self._feed_url("station_information"))

    def fetch_station_status(self) -> dict[str, Any]:
        return self._get_json(self._feed_url("station_status"))


class FixtureGbfsSource:
    """Offline fallback reading ``station_information.json`` / ``station_status.json``."""

    def __init__(self, directory: Path) -> None:
        self._directory = directory

    def _read(self, name: str) -> dict[str, Any]:
        path = self._directory / f"{name}.json"
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise GbfsFetchError(f"Cannot read fixture {path}: {exc}") from exc

    def fetch_station_information(self) -> dict[str, Any]:
        return self._read("station_information")

    def fetch_station_status(self) -> dict[str, Any]:
        return self._read("station_status")
