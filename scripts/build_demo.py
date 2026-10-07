"""Build the frozen demo as a static website (GitHub Pages, Render static site, any file host).

Takes the front (``src/web/static``) and a snapshot (``demo/data``, see ``export_demo.py``) and
writes a site where every path is relative, so it works under any base path (``/repo-name/`` on
GitHub Pages as well as the root of a domain). The pages read the snapshot files instead of the API
(``window.MP_DEMO``, defined before the scripts run).

Usage: python scripts/build_demo.py [--data demo/data] [--out demo-site] [--repo-url URL]
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ASSET_DIRS = ("css", "js", "fonts")
SNAPSHOT_FILES = ("overview", "stations", "priorities", "transfers", "hourly", "quality")


def relative_paths(html: str, prefix: str) -> str:
    """Rewrite href/src values that start with "/" into paths relative to the page."""

    def fix(match: re.Match[str]) -> str:
        attr, path = match.group(1), match.group(2)
        if path == "":
            target = prefix or "./"
        elif path == "app":
            target = prefix + "app/"
        elif path.startswith("api/"):
            target = "#"  # the API does not exist in the frozen demo
        else:
            target = prefix + path
        return f'{attr}="{target}"'

    return re.sub(r'\b(href|src)="/([^"]*)"', fix, html)


def inject_demo_config(html: str, config: dict) -> str:
    tag = f"<script>window.MP_DEMO = {json.dumps(config, ensure_ascii=False)};</script>\n"
    marker = re.search(r'<script src="[^"]*js/common\.js"></script>', html)
    if not marker:
        raise SystemExit("common.js script tag not found in the page")
    return html[: marker.start()] + tag + html[marker.start() :]


def build_demo(
    data_dir: Path, out_dir: Path, static_dir: Path, repo_url: str | None = None
) -> None:
    data_dir, out_dir, static_dir = Path(data_dir), Path(out_dir), Path(static_dir)
    meta = json.loads((data_dir / "meta.json").read_text(encoding="utf-8"))
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    for name in ASSET_DIRS:
        shutil.copytree(static_dir / name, out_dir / name)
    for stylesheet in (out_dir / "css").glob("*.css"):
        css = stylesheet.read_text(encoding="utf-8").replace("url(/", "url(../")
        stylesheet.write_text(css, encoding="utf-8")

    base_config = {"capturedAt": meta["captured_at"], "observedAt": meta["observed_at"]}
    for source, target, depth in (
        ("landing.html", "index.html", 0),
        ("app.html", "app/index.html", 1),
    ):
        prefix = "../" * depth
        html = relative_paths((static_dir / source).read_text(encoding="utf-8"), prefix)
        html = inject_demo_config(html, {"base": prefix, **base_config})
        destination = out_dir / target
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(html, encoding="utf-8")

    api = out_dir / "api"
    (api / "stations").mkdir(parents=True)
    for name in SNAPSHOT_FILES:
        shutil.copy(data_dir / f"{name}.json", api / f"{name}.json")
    for series in (data_dir / "series").glob("*.json"):
        station_dir = api / "stations" / series.stem
        station_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(series, station_dir / "series.json")
    (api / "config.json").write_text(json.dumps({"repo_url": repo_url}), encoding="utf-8")
    (out_dir / ".nojekyll").write_text("", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default=str(ROOT / "demo" / "data"))
    parser.add_argument("--out", default=str(ROOT / "demo-site"))
    parser.add_argument("--static", default=str(ROOT / "src" / "web" / "static"))
    parser.add_argument("--repo-url", default=None, help="link shown as 'Code source et README'")
    args = parser.parse_args()
    build_demo(Path(args.data), Path(args.out), Path(args.static), args.repo_url or None)
    print(f"Demo site written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
