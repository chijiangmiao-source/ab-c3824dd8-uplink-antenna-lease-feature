"""Command progress reporting: POST /leases/{token}/progress.

During a pass the control program issues many commands; operators read
``GET /leases/{token}`` to learn how far the current holder has executed.
Reporting must never extend or shorten the lease's occupancy window, must
only move forward, and must treat an equal sequence as a replay of a report
whose response may have been lost.

Everything runs against the real API and the real PostgreSQL, like the rest
of the suite.
"""

from __future__ import annotations

import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import text

from conftest import acquire, count_rows, insert_expired_lease

# Same explicit-offset ISO-8601 shape the other timestamp fields use.
ISO_OFFSET = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{2}:\d{2}$"
)


def _report(client, token, sequence):
    return client.post(f"/leases/{token}/progress", json={"sequence": sequence})


def _stored_progress(db_engine, token):
    with db_engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT last_command_sequence, last_progress_at "
                "FROM leases WHERE token = :token"
            ),
            {"token": token},
        ).mappings().one()


def test_acquire_report_and_query_latest_progress(http_client, db_engine):
    acquired = acquire(http_client, antenna_id="ANT-01", duration_seconds=60)
    assert acquired.status_code == 200
    token = acquired.json()["lease_token"]

    # A fresh lease has never reported: both fields are null.
    status = http_client.get(f"/leases/{token}")
    assert status.status_code == 200
    assert status.json()["last_command_sequence"] is None
    assert status.json()["last_progress_at"] is None

    # Consecutive reports advance the confirmed sequence one by one.
    recorded = None
    for seq in (0, 1, 2, 3):
        resp = _report(http_client, token, seq)
        assert resp.status_code == 200, (seq, resp.text)
        body = resp.json()
        assert body["lease_token"] == token
        assert body["last_command_sequence"] == seq
        assert body["replay"] is False
        assert ISO_OFFSET.match(body["last_progress_at"]), body
        recorded = body["last_progress_at"]

    # The lookup endpoint reflects the latest confirmed progress...
    status = http_client.get(f"/leases/{token}")
    body = status.json()
    assert body["last_command_sequence"] == 3
    assert body["last_progress_at"] == recorded
    # ...and reporting did not alter the lease's occupancy window.
    assert body["acquired_at"] == acquired.json()["acquired_at"]
    assert body["expires_at"] == acquired.json()["expires_at"]
    assert body["active"] is True

    # Committed database state matches what the API reported.
    stored = _stored_progress(db_engine, token)
    assert stored.last_command_sequence == 3
    assert stored.last_progress_at.isoformat() == recorded


def test_same_sequence_replay_returns_original_record_time(
    http_client, db_engine
):
    acquired = acquire(http_client, antenna_id="ANT-02", duration_seconds=60)
    token = acquired.json()["lease_token"]

    first = _report(http_client, token, 7)
    assert first.status_code == 200
    assert first.json()["replay"] is False
    first_time = first.json()["last_progress_at"]

    time.sleep(0.2)  # a real rewrite would land on a later database clock tick

    replay = _report(http_client, token, 7)
    assert replay.status_code == 200
    body = replay.json()
    assert body["replay"] is True
    assert body["last_command_sequence"] == 7
    # Byte-identical record time: the row was not rewritten.
    assert body["last_progress_at"] == first_time

    stored = _stored_progress(db_engine, token)
    assert stored.last_command_sequence == 7
    assert stored.last_progress_at.isoformat() == first_time


def test_smaller_sequence_is_rejected_as_regression(http_client, db_engine):
    acquired = acquire(http_client, antenna_id="ANT-03", duration_seconds=60)
    token = acquired.json()["lease_token"]

    ok = _report(http_client, token, 5)
    assert ok.status_code == 200
    recorded = ok.json()["last_progress_at"]

    resp = _report(http_client, token, 4)
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"]["code"] == "PROGRESS_REGRESSION"
    assert body["error"]["details"]["last_command_sequence"] == 5
    assert body["error"]["details"]["requested_sequence"] == 4

    # The rejected report changed nothing.
    status = http_client.get(f"/leases/{token}")
    assert status.json()["last_command_sequence"] == 5
    assert status.json()["last_progress_at"] == recorded
    stored = _stored_progress(db_engine, token)
    assert stored.last_command_sequence == 5
    assert stored.last_progress_at.isoformat() == recorded


def test_concurrent_reports_converge_to_max_sequence(http_client, db_engine):
    acquired = acquire(http_client, antenna_id="ANT-04", duration_seconds=60)
    token = acquired.json()["lease_token"]

    sequences = list(range(12))
    barrier = threading.Barrier(len(sequences))

    def one(seq):
        barrier.wait(timeout=10)
        return _report(http_client, token, seq)

    with ThreadPoolExecutor(max_workers=len(sequences)) as pool:
        responses = list(pool.map(one, sequences))

    # Every report either advanced the sequence or lost the race to a larger
    # one; nothing else can happen.
    for resp in responses:
        assert resp.status_code in (200, 409), resp.text
        if resp.status_code == 409:
            assert resp.json()["error"]["code"] == "PROGRESS_REGRESSION"
    assert any(r.status_code == 200 for r in responses)

    # Whatever the interleaving, the largest sequence is what survives, both
    # through the API and in the committed row.
    status = http_client.get(f"/leases/{token}")
    assert status.json()["last_command_sequence"] == max(sequences)
    assert _stored_progress(db_engine, token).last_command_sequence == max(
        sequences
    )


def test_expired_token_is_rejected_and_writes_nothing(http_client, db_engine):
    expired = insert_expired_lease(
        db_engine, antenna_id="ANT-05", age_seconds=2, controller="ghost"
    )
    token = expired["token"]

    resp = _report(http_client, token, 0)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "LEASE_EXPIRED"

    # Nothing was recorded on the expired lease.
    stored = _stored_progress(db_engine, token)
    assert stored.last_command_sequence is None
    assert stored.last_progress_at is None


def test_expired_holder_keeps_prior_progress_but_cannot_advance(
    http_client, db_engine
):
    # A lease that reported progress before expiring keeps its history; new
    # reports are still rejected and change nothing.
    expired = insert_expired_lease(db_engine, antenna_id="ANT-06", age_seconds=2)
    token = expired["token"]
    with db_engine.begin() as conn:
        row = conn.execute(
            text(
                """
                UPDATE leases
                SET last_command_sequence = 9,
                    last_progress_at = expires_at - make_interval(secs => 3)
                WHERE token = :token
                RETURNING last_progress_at
                """
            ),
            {"token": token},
        ).mappings().one()

    resp = _report(http_client, token, 10)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "LEASE_EXPIRED"

    status = http_client.get(f"/leases/{token}")
    assert status.json()["active"] is False
    assert status.json()["last_command_sequence"] == 9
    assert status.json()["last_progress_at"] == row.last_progress_at.isoformat()


def test_superseded_token_cannot_report_after_handover(http_client, db_engine):
    expired = insert_expired_lease(db_engine, antenna_id="ANT-06", age_seconds=1)
    successor = acquire(http_client, antenna_id="ANT-06", duration_seconds=30)
    assert successor.status_code == 200
    new_token = successor.json()["lease_token"]

    # The old token is no longer the current holder.
    stale = _report(http_client, expired["token"], 0)
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "LEASE_EXPIRED"

    # The current holder reports normally.
    ok = _report(http_client, new_token, 0)
    assert ok.status_code == 200
    assert ok.json()["last_command_sequence"] == 0
    assert _stored_progress(db_engine, new_token).last_command_sequence == 0


def test_unknown_token_is_404_and_writes_nothing(http_client, db_engine):
    resp = _report(http_client, "no-such-token", 0)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "LEASE_NOT_FOUND"
    assert count_rows(db_engine, "SELECT count(*) FROM leases") == 0


def test_sequence_must_be_a_non_negative_integer(http_client, db_engine):
    acquired = acquire(http_client, antenna_id="ANT-01", duration_seconds=30)
    token = acquired.json()["lease_token"]

    for bad in (-1, -100, 1.5, "3", None, True):
        resp = _report(http_client, token, bad)
        assert resp.status_code == 422, (bad, resp.text)
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"

    # Missing field and unexpected fields are rejected too.
    assert http_client.post(f"/leases/{token}/progress", json={}).status_code == 422
    assert (
        http_client.post(
            f"/leases/{token}/progress", json={"sequence": 0, "extra": 1}
        ).status_code
        == 422
    )

    # All rejections were write-free.
    assert http_client.get(f"/leases/{token}").json()["last_command_sequence"] is None
    stored = _stored_progress(db_engine, token)
    assert stored.last_command_sequence is None
    assert stored.last_progress_at is None


def test_pre_existing_lease_without_progress_reads_null(http_client, db_engine):
    # Rows that predate progress reporting (inserted directly, as a migration
    # would have left them) expose null fields and need no backfill.
    expired = insert_expired_lease(db_engine, antenna_id="ANT-03", age_seconds=1)
    resp = http_client.get(f"/leases/{expired['token']}")
    assert resp.status_code == 200
    assert resp.json()["last_command_sequence"] is None
    assert resp.json()["last_progress_at"] is None
