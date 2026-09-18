"""Startup smoke test.

Regression guard: the app module must import cleanly (FastAPI builds the
routes), and the two lease response models must serialise the same
``expires_at`` to the same string. A broken field serializer used to raise at
import time, which made the whole service fail to start.
"""

from __future__ import annotations

from datetime import datetime, timezone

from app.main import app
from app.schemas import AcquireResponse, LeaseStatusResponse


def test_app_module_imports_and_routes_are_registered():
    # FastAPI >= 0.124 includes routers lazily: app.routes may hold wrapper
    # objects instead of flattened routes. The OpenAPI schema is built from
    # the same routing table through a stable public API, so assert there.
    paths = set(app.openapi()["paths"].keys())
    assert "/leases" in paths
    assert "/leases/{lease_token}" in paths
    assert "/leases/{lease_token}/progress" in paths
    assert "/health" in paths


def test_both_response_models_serialise_timestamps_identically():
    instant = datetime(2026, 9, 12, 4, 0, 30, 123456, tzinfo=timezone.utc)
    common = {
        "antenna_id": "ANT-01",
        "controller": "ctrl",
        "lease_token": "x" * 43,
        "acquired_at": instant,
        "expires_at": instant,
    }

    acquired = AcquireResponse(**common, replay=False).model_dump(mode="json")
    status = LeaseStatusResponse(**common, active=True).model_dump(mode="json")

    expected = "2026-09-12T04:00:30.123456+00:00"
    assert acquired["expires_at"] == expected
    assert status["expires_at"] == expected
    assert acquired["expires_at"] == status["expires_at"]
    assert acquired["acquired_at"] == status["acquired_at"]
    # Explicit offset, never the bare "Z" shorthand.
    assert not acquired["expires_at"].endswith("Z")
    assert not status["expires_at"].endswith("Z")
