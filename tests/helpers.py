"""Small helpers shared by the test modules (``tests/`` is on ``sys.path`` through conftest)."""

import os

import pytest


def skip_or_fail(reason: str) -> None:
    """A missing service skips a test locally but FAILS it in CI (``CI_REQUIRE_SERVICES=1``).

    Otherwise a CI job whose database or Java is broken would go green by skipping every test.
    """
    if os.environ.get("CI_REQUIRE_SERVICES") == "1":
        pytest.fail(f"required service unavailable in CI: {reason}")
    pytest.skip(reason)
