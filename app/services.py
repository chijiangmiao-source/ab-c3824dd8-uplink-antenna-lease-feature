"""Lease acquisition domain logic.

Concurrency design (all inside one READ COMMITTED transaction):

1. ``pg_advisory_xact_lock(hashtext(:key))``
   Transactions that carry the same idempotency key are serialised, so a lost
   response followed by a retry can never create a second lease.
2. Look up the stored idempotency record. Same parameters -> replay the
   original token/expiry; different parameters -> stable ``IDEMPOTENCY_CONFLICT``.
3. ``SELECT ... FROM antennas WHERE id = :antenna_id FOR UPDATE``
   Serialises every contender for the same antenna. Unknown antenna raises
   ``ANTENNA_NOT_FOUND`` before any row is written.
4. Look up an active lease with ``expires_at > clock_timestamp()``. A lease
   whose ``expires_at`` has been reached (``expires_at <= clock_timestamp()``)
   is gone: the boundary belongs to the new request.
5. Insert the new lease (``expires_at = clock_timestamp() + make_interval``)
   and its idempotency record, then commit atomically.

Every timestamp originates from PostgreSQL; the host clock is never read.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.config import MAX_LEASE_SECONDS, MIN_LEASE_SECONDS
from app.errors import APIError


def _canonical_params(antenna_id: str, controller: str, duration_seconds: int) -> str:
    # Stable textual fingerprint; parameter names are part of it.
    return (
        f"antenna_id={antenna_id}\n"
        f"controller={controller}\n"
        f"duration_seconds={duration_seconds}"
    )


def acquire_lease(
    conn: Connection,
    *,
    antenna_id: str,
    controller: str,
    duration_seconds: int,
    idempotency_key: str,
) -> dict[str, Any]:
    # Defence in depth: Pydantic validates the HTTP boundary, the service
    # validates any internal caller as well. Rejections happen before any
    # write statement is issued.
    if not (
        isinstance(duration_seconds, int)
        and MIN_LEASE_SECONDS <= duration_seconds <= MAX_LEASE_SECONDS
    ):
        raise APIError(
            422,
            "LEASE_DURATION_OUT_OF_RANGE",
            f"租期必须为 {MIN_LEASE_SECONDS} 至 {MAX_LEASE_SECONDS} 秒之间的整数。",
            {
                "duration_seconds": duration_seconds,
                "min": MIN_LEASE_SECONDS,
                "max": MAX_LEASE_SECONDS,
            },
        )

    fingerprint = _canonical_params(antenna_id, controller, duration_seconds)

    # 1. Serialise transactions sharing one idempotency key.
    conn.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
        {"key": idempotency_key},
    )

    # 2. Replay or stable conflict.
    existing = conn.execute(
        text(
            """
            SELECT lease_id, request_params
            FROM idempotency_keys
            WHERE idempotency_key = :key
            """
        ),
        {"key": idempotency_key},
    ).mappings().first()

    if existing is not None:
        if existing.request_params != fingerprint:
            raise APIError(
                409,
                "IDEMPOTENCY_CONFLICT",
                "同一幂等键曾用于不同的请求参数，拒绝执行。",
                {
                    "idempotency_key": idempotency_key,
                    "original_params": existing.request_params,
                    "request_params": fingerprint,
                },
            )
        replay = conn.execute(
            text(
                """
                SELECT id AS lease_id, antenna_id, controller,
                       token AS lease_token, acquired_at, expires_at
                FROM leases
                WHERE id = :lease_id
                """
            ),
            {"lease_id": existing.lease_id},
        ).mappings().first()
        # lease_id is NOT NULL with an FK; the row always exists.
        return {**dict(replay), "replay": True}

    # 3. Lock the antenna row (also proves the antenna is provisioned).
    antenna = conn.execute(
        text("SELECT id FROM antennas WHERE id = :antenna_id FOR UPDATE"),
        {"antenna_id": antenna_id},
    ).first()
    if antenna is None:
        raise APIError(
            404,
            "ANTENNA_NOT_FOUND",
            f"未知天线：{antenna_id}",
            {"antenna_id": antenna_id},
        )

    # 4. An unexpired lease wins; expiry boundary (expires_at == now) goes
    #    to the new request because the predicate is strictly greater-than.
    active = conn.execute(
        text(
            """
            SELECT token, expires_at
            FROM leases
            WHERE antenna_id = :antenna_id
              AND expires_at > clock_timestamp()
            ORDER BY acquired_at DESC, id DESC
            LIMIT 1
            """
        ),
        {"antenna_id": antenna_id},
    ).mappings().first()
    if active is not None:
        raise APIError(
            409,
            "ANTENNA_BUSY",
            f"天线 {antenna_id} 已被未到期租约占用。",
            {
                "antenna_id": antenna_id,
                "held_by_lease": active.token,
                "expires_at": active.expires_at.isoformat(),
            },
        )

    # 5. Create lease + idempotency record atomically. Token comes from
    #    PostgreSQL's CSPRNG so it is unpredictable on the wire. Standard
    #    base64 contains '/', '+' and '=' which are unsafe in a single URL
    #    path segment, so emit the base64url alphabet with padding stripped
    #    (43 chars for 32 random bytes).
    row = conn.execute(
        text(
            """
            WITH new_lease AS (
                INSERT INTO leases (antenna_id, controller, token, acquired_at, expires_at)
                VALUES (
                    :antenna_id,
                    :controller,
                    rtrim(
                        replace(
                            replace(encode(gen_random_bytes(32), 'base64'), '+', '-'),
                            '/', '_'
                        ),
                        '='
                    ),
                    clock_timestamp(),
                    clock_timestamp() + make_interval(secs => :duration)
                )
                RETURNING id AS lease_id, antenna_id, controller,
                          token AS lease_token, acquired_at, expires_at
            ), recorded AS (
                INSERT INTO idempotency_keys (idempotency_key, lease_id, request_params)
                SELECT :key, lease_id, :params
                FROM new_lease
            )
            SELECT * FROM new_lease
            """
        ),
        {
            "antenna_id": antenna_id,
            "controller": controller,
            "duration": duration_seconds,
            "key": idempotency_key,
            "params": fingerprint,
        },
    ).mappings().one()
    return {**dict(row), "replay": False}


def get_lease_by_token(conn: Connection, token: str) -> dict[str, Any] | None:
    row = conn.execute(
        text(
            """
            SELECT id AS lease_id, antenna_id, controller,
                   token, acquired_at, expires_at,
                   last_command_sequence, last_progress_at,
                   (expires_at > clock_timestamp()) AS active
            FROM leases
            WHERE token = :token
            """
        ),
        {"token": token},
    ).mappings().first()
    return dict(row) if row is not None else None


def report_lease_progress(
    conn: Connection,
    *,
    token: str,
    sequence: int,
) -> dict[str, Any]:
    """Record how far the current holder of a lease has executed.

    Runs in the caller's READ COMMITTED transaction:

    1. Resolve the token (unknown token -> ``LEASE_NOT_FOUND``, no writes).
    2. ``SELECT ... FROM antennas ... FOR UPDATE`` on the lease's antenna —
       the same lock acquisition takes, so progress reports serialise against
       handovers and against each other.
    3. Re-read the lease under the lock and let the database clock decide
       whether it is still the current, unexpired holder. An expired token
       is rejected (``LEASE_EXPIRED``) without touching any row — even a
       replay of its last confirmed sequence.
    4. Monotonicity: a higher sequence is recorded with
       ``last_progress_at = clock_timestamp()``; the same sequence is a
       replay and returns the original record time unchanged; a smaller
       sequence is a stable ``PROGRESS_REGRESSION`` conflict.

    Reporting progress never modifies ``expires_at``: watching a pass cannot
    extend the occupation of the antenna.
    """
    # Defence in depth, mirroring acquire_lease: Pydantic enforces this at
    # the HTTP boundary, the service re-checks any internal caller before
    # issuing a write.
    if not (isinstance(sequence, int) and sequence >= 0):
        raise APIError(
            422,
            "SEQUENCE_OUT_OF_RANGE",
            "指令序号必须为非负整数。",
            {"sequence": sequence},
        )

    # 1. Resolve the token. No row is locked yet: everything that follows
    #    re-validates under the antenna lock.
    lease = conn.execute(
        text("SELECT id, antenna_id FROM leases WHERE token = :token"),
        {"token": token},
    ).mappings().first()
    if lease is None:
        raise APIError(
            404,
            "LEASE_NOT_FOUND",
            "未知租约令牌。",
            {"lease_token": token},
        )

    # 2. Serialise against acquisition and sibling progress reports for the
    #    same antenna (the FK guarantees the antenna row exists).
    conn.execute(
        text("SELECT id FROM antennas WHERE id = :antenna_id FOR UPDATE"),
        {"antenna_id": lease.antenna_id},
    )

    # 3. Under the lock, the database clock decides whether this token is
    #    still the current holder. At most one lease per antenna is unexpired
    #    at any moment, so an unexpired row IS the current one.
    row = conn.execute(
        text(
            """
            SELECT id, expires_at, last_command_sequence, last_progress_at,
                   (expires_at > clock_timestamp()) AS active
            FROM leases
            WHERE id = :lease_id
            FOR UPDATE
            """
        ),
        {"lease_id": lease.id},
    ).mappings().one()

    if not row.active:
        raise APIError(
            409,
            "LEASE_EXPIRED",
            "租约已到期，控制权已交接，拒绝上报进度。",
            {"lease_token": token, "expires_at": row.expires_at.isoformat()},
        )

    # 4. Monotonic update / replay / regression.
    last = row.last_command_sequence
    if last is not None:
        if sequence < last:
            raise APIError(
                409,
                "PROGRESS_REGRESSION",
                "指令序号不允许回退。",
                {
                    "lease_token": token,
                    "last_command_sequence": last,
                    "sequence": sequence,
                },
            )
        if sequence == last:
            # Replay: the original record time is returned as-is and no
            # write happens, so repeated reports are byte-identical.
            return {
                "lease_token": token,
                "last_command_sequence": last,
                "last_progress_at": row.last_progress_at,
                "replay": True,
            }

    updated = conn.execute(
        text(
            """
            UPDATE leases
            SET last_command_sequence = :sequence,
                last_progress_at = clock_timestamp()
            WHERE id = :lease_id
            RETURNING last_command_sequence, last_progress_at
            """
        ),
        {"sequence": sequence, "lease_id": lease.id},
    ).mappings().one()
    return {
        "lease_token": token,
        "last_command_sequence": updated.last_command_sequence,
        "last_progress_at": updated.last_progress_at,
        "replay": False,
    }
