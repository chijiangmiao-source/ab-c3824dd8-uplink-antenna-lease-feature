"""Lease acquisition and progress-reporting domain logic.

Acquisition concurrency design (all inside one READ COMMITTED transaction):

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

Progress reports serialise on the same antenna row lock (step 3 above), so
they are atomic against acquisition handover and against each other. Because
acquisition guarantees at most one unexpired lease per antenna, "this lease
is unexpired" and "this lease is the current holder" are the same fact.

Every timestamp originates from PostgreSQL; the host clock is never read.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.config import (
    MAX_COMMAND_SEQUENCE,
    MAX_LEASE_SECONDS,
    MIN_LEASE_SECONDS,
)
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


def report_progress(
    conn: Connection,
    *,
    token: str,
    sequence: int,
) -> dict[str, Any]:
    """Record how far the current holder has executed, without touching the
    lease's occupancy window.

    Transaction shape (READ COMMITTED, same as acquisition):

    1. Resolve the token; unknown tokens are rejected before any lock or
       write, exactly like the lookup endpoint.
    2. ``SELECT ... FROM antennas ... FOR UPDATE`` on the lease's antenna —
       the same lock acquisition takes, so reports serialise against
       handover and against concurrent reports for this antenna.
    3. A single guarded UPDATE writes ``(sequence, clock_timestamp())`` only
       while the lease is unexpired and the sequence sequence is strictly
       greater than the stored one. Keeping the clock predicate inside the
       UPDATE means an expiry landing mid-transaction cannot slip a write
       through.
    4. If the UPDATE matched nothing, the antenna lock is still held, so the
       stored progress is stable and only the database clock can have moved:
       re-read to classify the outcome as expired / replay / regression.
    """
    # Defence in depth, mirroring the duration check in acquire_lease: the
    # HTTP boundary validates first, but internal callers are checked too.
    # Rejections happen before any statement is issued.
    if (
        isinstance(sequence, bool)
        or not isinstance(sequence, int)
        or not 0 <= sequence <= MAX_COMMAND_SEQUENCE
    ):
        raise APIError(
            422,
            "SEQUENCE_OUT_OF_RANGE",
            f"指令序号必须为 0 至 {MAX_COMMAND_SEQUENCE} 之间的整数。",
            {"sequence": sequence, "min": 0, "max": MAX_COMMAND_SEQUENCE},
        )

    # 1. Resolve the token.
    lease = conn.execute(
        text(
            """
            SELECT id AS lease_id, antenna_id, token
            FROM leases
            WHERE token = :token
            """
        ),
        {"token": token},
    ).mappings().first()
    if lease is None:
        raise APIError(
            404,
            "LEASE_NOT_FOUND",
            "未知租约令牌。",
            {"lease_token": token},
        )

    # 2. Lock the antenna row (the row always exists via the lease's FK).
    conn.execute(
        text("SELECT id FROM antennas WHERE id = :antenna_id FOR UPDATE"),
        {"antenna_id": lease.antenna_id},
    ).one()

    # 3. Advance only while this lease is still the current unexpired one
    #    and the sequence is strictly increasing.
    updated = conn.execute(
        text(
            """
            UPDATE leases
            SET last_command_sequence = :sequence,
                last_progress_at = clock_timestamp()
            WHERE id = :lease_id
              AND expires_at > clock_timestamp()
              AND (last_command_sequence IS NULL
                   OR last_command_sequence < :sequence)
            RETURNING last_command_sequence, last_progress_at
            """
        ),
        {"lease_id": lease.lease_id, "sequence": sequence},
    ).mappings().first()
    if updated is not None:
        return {
            "lease_token": lease.token,
            "last_command_sequence": updated.last_command_sequence,
            "last_progress_at": updated.last_progress_at,
            "replay": False,
        }

    # 4. Nothing was written; classify why with the antenna lock still held.
    state = conn.execute(
        text(
            """
            SELECT (expires_at > clock_timestamp()) AS active,
                   expires_at, last_command_sequence, last_progress_at
            FROM leases
            WHERE id = :lease_id
            """
        ),
        {"lease_id": lease.lease_id},
    ).mappings().one()

    if not state.active:
        raise APIError(
            409,
            "LEASE_EXPIRED",
            "租约已到期，该令牌不再持有控制权，拒绝上报进度。",
            {
                "lease_token": token,
                "expires_at": state.expires_at.isoformat(),
            },
        )
    if state.last_command_sequence == sequence:
        # Same sequence as already recorded: a replay of a report whose
        # response may have been lost. Return the original record time and
        # leave the row untouched.
        return {
            "lease_token": lease.token,
            "last_command_sequence": state.last_command_sequence,
            "last_progress_at": state.last_progress_at,
            "replay": True,
        }
    raise APIError(
        409,
        "PROGRESS_REGRESSION",
        "指令序号必须递增，小于已确认序号的上报被拒绝。",
        {
            "lease_token": token,
            "last_command_sequence": state.last_command_sequence,
            "requested_sequence": sequence,
        },
    )


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
