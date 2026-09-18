"""Command progress reporting against the real API + real PostgreSQL.

During a pass the control program issues its commands in order; operators
watch ``GET /leases/{token}`` to learn which sequence the current holder has
reached. Reporting must never extend the lease: ``expires_at`` is untouched.

Covered here: continuous reporting and lookup, replay stability for the same
sequence, regression rejection, concurrent reporters converging on the
maximum sequence, expired/unknown tokens refused without writes, and strict
request validation.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest

from conftest import (
    KNOWN_ANTENNA,
    acquire,
    count_rows,
    insert_expired_lease,
)


def _acquire(client, antenna=KNOWN_ANTENNA, duration=30):
    resp = acquire(client, antenna_id=antenna, duration_seconds=duration)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _report(client, token, sequence):
    return client.post(f"/leases/{token}/progress", json={"sequence": sequence})


def _progress_of(db_engine, token):
    """Committed progress columns for ``token`` straight from PostgreSQL."""
    from sqlalchemy import text

    with db_engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT last_command_sequence, last_progress_at "
                "FROM leases WHERE token = :token"
            ),
            {"token": token},
        ).mappings().one()


def test_fresh_lease_reports_null_progress(http_client):
    lease = _acquire(http_client)
    resp = http_client.get(f"/leases/{lease['lease_token']}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["last_command_sequence"] is None
    assert body["last_progress_at"] is None
    # The pre-existing fields are unaffected by the new columns.
    assert body["active"] is True
    assert body["expires_at"] == lease["expires_at"]
    assert body["acquired_at"] == lease["acquired_at"]


def test_historical_lease_without_progress_reads_null(http_client, db_engine):
    # Rows written before the progress feature existed have nothing to
    # backfill: both fields stay null.
    expired = insert_expired_lease(
        db_engine, antenna_id=KNOWN_ANTENNA, age_seconds=2
    )
    resp = http_client.get(f"/leases/{expired['token']}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["active"] is False
    assert body["last_command_sequence"] is None
    assert body["last_progress_at"] is None


def test_continuous_reporting_then_lookup_shows_latest(http_client, db_engine):
    lease = _acquire(http_client)
    token = lease["lease_token"]

    last_report = None
    for seq in (0, 1, 2, 3, 4):
        resp = _report(http_client, token, seq)
        assert resp.status_code == 200, resp.text
        last_report = resp.json()
        assert last_report["lease_token"] == token
        assert last_report["last_command_sequence"] == seq
        assert last_report["replay"] is False
        # Same ISO-8601-with-offset convention as every other timestamp.
        assert last_report["last_progress_at"].endswith("+00:00")
        assert not last_report["last_progress_at"].endswith("Z")

    status = http_client.get(f"/leases/{token}")
    assert status.status_code == 200
    body = status.json()
    assert body["last_command_sequence"] == 4
    # The record time is byte-identical between the report response and the
    # lease lookup.
    assert body["last_progress_at"] == last_report["last_progress_at"]
    # Reporting progress must not extend the occupation of the antenna.
    assert body["expires_at"] == lease["expires_at"]

    # The committed row agrees with the API.
    row = _progress_of(db_engine, token)
    assert row["last_command_sequence"] == 4
    assert row["last_progress_at"].isoformat() == last_report["last_progress_at"]


def test_same_sequence_replay_returns_original_timestamp(http_client, db_engine):
    lease = _acquire(http_client)
    token = lease["lease_token"]

    first = _report(http_client, token, 7)
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["replay"] is False

    time.sleep(0.05)  # a rewrite would move clock_timestamp() forward

    again = _report(http_client, token, 7)
    assert again.status_code == 200
    again_body = again.json()
    assert again_body["replay"] is True
    assert again_body["last_command_sequence"] == 7
    # Replay returns the ORIGINAL record time, byte-for-byte.
    assert again_body["last_progress_at"] == first_body["last_progress_at"]

    status = http_client.get(f"/leases/{token}").json()
    assert status["last_command_sequence"] == 7
    assert status["last_progress_at"] == first_body["last_progress_at"]


def test_smaller_sequence_is_a_stable_regression(http_client, db_engine):
    lease = _acquire(http_client)
    token = lease["lease_token"]

    ok = _report(http_client, token, 10)
    assert ok.status_code == 200
    recorded_at = ok.json()["last_progress_at"]

    reg = _report(http_client, token, 9)
    assert reg.status_code == 409
    body = reg.json()
    assert body["error"]["code"] == "PROGRESS_REGRESSION"
    assert body["error"]["details"]["last_command_sequence"] == 10
    assert body["error"]["details"]["sequence"] == 9

    # Stable: the same regression fails identically a second time.
    again = _report(http_client, token, 9)
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "PROGRESS_REGRESSION"

    # Nothing was written: the confirmed state is untouched.
    status = http_client.get(f"/leases/{token}").json()
    assert status["last_command_sequence"] == 10
    assert status["last_progress_at"] == recorded_at
    row = _progress_of(db_engine, token)
    assert row["last_command_sequence"] == 10
    assert row["last_progress_at"].isoformat() == recorded_at


def test_concurrent_reports_converge_on_max_sequence(http_client, db_engine):
    lease = _acquire(http_client, duration=60)
    token = lease["lease_token"]
    sequences = list(range(12))

    barrier = threading.Barrier(len(sequences))

    def one(seq):
        barrier.wait(timeout=10)
        return _report(http_client, token, seq)

    with ThreadPoolExecutor(max_workers=len(sequences)) as pool:
        responses = list(pool.map(one, sequences))

    # pool.map preserves submission order: responses[i] reported sequences[i].
    for seq, r in zip(sequences, responses):
        assert r.status_code in (200, 409), r.text
        if r.status_code == 409:
            # Serialised after a higher sequence: a well-formed regression.
            assert r.json()["error"]["code"] == "PROGRESS_REGRESSION"
        else:
            body = r.json()
            # All sequences are distinct, so a 200 is always a fresh write
            # confirming exactly the reported sequence.
            assert body["replay"] is False
            assert body["last_command_sequence"] == seq

    # Whatever the interleaving was, the maximum sequence survives.
    status = http_client.get(f"/leases/{token}").json()
    assert status["last_command_sequence"] == max(sequences)
    row = _progress_of(db_engine, token)
    assert row["last_command_sequence"] == max(sequences)
    assert row["last_progress_at"] is not None


def test_concurrent_identical_sequence_replays_share_one_timestamp(http_client):
    lease = _acquire(http_client, duration=60)
    token = lease["lease_token"]

    barrier = threading.Barrier(8)

    def one(_):
        barrier.wait(timeout=10)
        return _report(http_client, token, 42)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(one, range(8)))

    assert all(r.status_code == 200 for r in responses), [
        r.status_code for r in responses
    ]
    bodies = [r.json() for r in responses]
    # Exactly one reporter wrote the row; everyone else replayed it.
    assert [b["replay"] for b in bodies].count(False) == 1
    assert [b["replay"] for b in bodies].count(True) == 7
    # All of them observe the very same record time.
    assert len({b["last_progress_at"] for b in bodies}) == 1
    assert all(b["last_command_sequence"] == 42 for b in bodies)


def test_expired_token_is_rejected_and_data_is_unchanged(http_client, db_engine):
    # Minimum-length lease through the API, real progress, then wait for the
    # database-computed deadline to pass.
    lease = _acquire(http_client, antenna="ANT-03", duration=5)
    token = lease["lease_token"]

    ok = _report(http_client, token, 3)
    assert ok.status_code == 200
    recorded_at = ok.json()["last_progress_at"]

    deadline = datetime.fromisoformat(lease["expires_at"])
    while datetime.now(timezone.utc) <= deadline:
        time.sleep(0.1)
    time.sleep(0.3)  # boundary margin

    # A higher sequence is refused: the token no longer holds the antenna.
    resp = _report(http_client, token, 4)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "LEASE_EXPIRED"

    # Even a replay of the last confirmed sequence is refused after expiry.
    replay = _report(http_client, token, 3)
    assert replay.status_code == 409
    assert replay.json()["error"]["code"] == "LEASE_EXPIRED"

    # The lease row still carries the original progress, unchanged.
    status = http_client.get(f"/leases/{token}").json()
    assert status["active"] is False
    assert status["last_command_sequence"] == 3
    assert status["last_progress_at"] == recorded_at
    row = _progress_of(db_engine, token)
    assert row["last_command_sequence"] == 3
    assert row["last_progress_at"].isoformat() == recorded_at

    # The antenna handed over normally: the successor reports its own
    # progress from zero while the old token stays refused.
    successor = _acquire(http_client, antenna="ANT-03", duration=30)
    assert successor["lease_token"] != token
    fresh = _report(http_client, successor["lease_token"], 0)
    assert fresh.status_code == 200
    assert fresh.json()["last_command_sequence"] == 0
    stale = _report(http_client, token, 4)
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "LEASE_EXPIRED"


def test_unknown_token_is_404_and_writes_nothing(http_client, db_engine):
    resp = _report(http_client, "no-such-token", 1)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "LEASE_NOT_FOUND"

    # Stable on repetition and zero rows anywhere.
    again = _report(http_client, "no-such-token", 1)
    assert again.status_code == 404
    assert again.json()["error"]["code"] == "LEASE_NOT_FOUND"
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0


def test_progress_does_not_disturb_acquisition_state(http_client, db_engine):
    # Interleaved with reporting, the acquisition invariants still hold:
    # one active lease, contenders rejected, replay intact.
    lease = _acquire(http_client, duration=60)
    token = lease["lease_token"]
    assert _report(http_client, token, 1).status_code == 200

    contender = acquire(http_client, antenna_id=KNOWN_ANTENNA, duration_seconds=60)
    assert contender.status_code == 409
    assert contender.json()["error"]["code"] == "ANTENNA_BUSY"
    assert contender.json()["error"]["details"]["held_by_lease"] == token

    assert _report(http_client, token, 2).status_code == 200
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 1


@pytest.mark.parametrize("bad", [-1, -100, "3", 3.5, True, None])
def test_invalid_sequence_is_422_and_writes_nothing(http_client, db_engine, bad):
    lease = _acquire(http_client)
    token = lease["lease_token"]

    resp = _report(http_client, token, bad)
    assert resp.status_code == 422, bad
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"

    status = http_client.get(f"/leases/{token}").json()
    assert status["last_command_sequence"] is None
    assert status["last_progress_at"] is None
    row = _progress_of(db_engine, token)
    assert row["last_command_sequence"] is None
    assert row["last_progress_at"] is None


def test_missing_and_extra_fields_rejected(http_client):
    lease = _acquire(http_client)
    token = lease["lease_token"]

    missing = http_client.post(f"/leases/{token}/progress", json={})
    assert missing.status_code == 422
    assert missing.json()["error"]["code"] == "VALIDATION_ERROR"

    extra = http_client.post(
        f"/leases/{token}/progress", json={"sequence": 1, "controller": "x"}
    )
    assert extra.status_code == 422
    assert extra.json()["error"]["code"] == "VALIDATION_ERROR"

    status = http_client.get(f"/leases/{token}").json()
    assert status["last_command_sequence"] is None
