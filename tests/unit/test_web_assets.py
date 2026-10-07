"""Static front-end: every referenced asset exists, motion is gated, and design tells stay out."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

STATIC = Path(__file__).resolve().parents[2] / "src" / "web" / "static"
PAGES = ("landing.html", "app.html")

pytestmark = pytest.mark.skipif(not STATIC.exists(), reason="web static files are not mounted here")


def local_refs(html: str) -> set[str]:
    refs = re.findall(r'(?:href|src)="(/[^"#?]+)"', html)
    return {ref for ref in refs if ref.startswith(("/css/", "/js/", "/fonts/"))}


@pytest.mark.parametrize("page", PAGES)
def test_every_local_asset_a_page_references_exists(page):
    refs = local_refs((STATIC / page).read_text(encoding="utf-8"))
    assert refs, f"{page} should load the shared stylesheet and scripts"
    for ref in refs:
        assert (STATIC / ref.lstrip("/")).is_file(), ref


def test_both_pages_share_the_design_tokens():
    for page in PAGES:
        assert "/css/tokens.css" in (STATIC / page).read_text(encoding="utf-8"), page


def test_declared_font_files_exist():
    css = (STATIC / "css" / "tokens.css").read_text(encoding="utf-8")
    fonts = re.findall(r"url\((/fonts/[^)]+)\)", css)
    assert len(fonts) >= 3
    for font in fonts:
        assert (STATIC / font.lstrip("/")).is_file(), font


@pytest.mark.parametrize("sheet", sorted(p.name for p in (STATIC / "css").glob("*.css")))
def test_every_stylesheet_with_motion_honours_reduced_motion(sheet):
    css = (STATIC / "css" / sheet).read_text(encoding="utf-8")
    if re.search(r"@keyframes|transition\s*:|animation\s*:", css):
        assert "prefers-reduced-motion" in css, sheet


@pytest.mark.parametrize("page", PAGES)
def test_the_design_tells_the_craft_floor_refuses_are_absent(page):
    text = (STATIC / page).read_text(encoding="utf-8")
    for sheet in (STATIC / "css").glob("*.css"):
        text += sheet.read_text(encoding="utf-8")
    assert "kicker" not in text, "no eyebrow label above headings"
    assert not re.search(r"border-(left|right)\s*:\s*[2-9]\d*px", text), "no thick side stripes"
    assert "background-clip: text" not in text and "-webkit-background-clip: text" not in text
    assert not re.search(r"transition\s*:\s*all\b", text), "name the properties"
    assert not re.search(r"scale\(0\)", text), "never animate from scale(0)"


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
@pytest.mark.parametrize("script", sorted(p.name for p in (STATIC / "js").glob("*.js")))
def test_scripts_are_syntactically_valid(script):
    result = subprocess.run(
        ["node", "--check", str(STATIC / "js" / script)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_the_app_page_wires_the_live_stream():
    js = (STATIC / "js" / "app.js").read_text(encoding="utf-8")
    assert "/api/stream" in js and "EventSource" in js
