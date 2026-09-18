"""Pydantic models for the antenna control lease API."""

from __future__ import annotations

from datetime import datetime

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    field_serializer,
    field_validator,
)

from app.config import (
    MAX_COMMAND_SEQUENCE,
    MAX_LEASE_SECONDS,
    MIN_LEASE_SECONDS,
)


class AcquireRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    antenna_id: str = Field(..., min_length=1, max_length=64)
    controller: str = Field(..., min_length=1, max_length=128)
    duration_seconds: StrictInt = Field(
        ...,
        ge=MIN_LEASE_SECONDS,
        le=MAX_LEASE_SECONDS,
        description=f"租约时长（必须是 JSON 整数，不接受文本数字），闭区间 [{MIN_LEASE_SECONDS}, {MAX_LEASE_SECONDS}] 秒。",
    )
    idempotency_key: str = Field(..., min_length=1, max_length=128)

    @field_validator("antenna_id", "controller", "idempotency_key")
    @classmethod
    def _reject_blank(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("不能为空或纯空白。")
        return value


class _LeaseBase(BaseModel):
    """Shared lease fields and timestamp serialisation.

    Pydantic renders a UTC datetime as ``...Z`` while FastAPI's plain-dict
    path (``jsonable_encoder``) renders it as ``...+00:00``; routing every
    lease response through one explicit serializer keeps the same
    ``expires_at`` byte-identical across acquisition and lookup.

    The serializer references ``acquired_at``/``expires_at``, so those fields
    MUST be declared on this same class: Pydantic v2 rejects a field
    serializer whose target fields are only defined in a subclass (it raises
    at import time, which would prevent the app from starting).
    """

    antenna_id: str
    controller: str
    lease_token: str
    acquired_at: datetime
    expires_at: datetime

    @field_serializer("acquired_at", "expires_at", when_used="always")
    def _serialize_iso8601(self, value: datetime) -> str:
        return value.isoformat()


class AcquireResponse(_LeaseBase):
    replay: bool = False


class LeaseStatusResponse(_LeaseBase):
    active: bool
    # Null until the holder's first progress report; historical leases
    # (pre-progress-migration) therefore also read as null.
    last_command_sequence: int | None = None
    last_progress_at: datetime | None = None

    @field_serializer("last_progress_at", when_used="always")
    def _serialize_optional_iso8601(self, value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None


class ProgressReportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sequence: StrictInt = Field(
        ...,
        ge=0,
        le=MAX_COMMAND_SEQUENCE,
        description="已执行到的指令序号（必须是 JSON 整数，不接受文本数字），非负。",
    )


class ProgressReportResponse(BaseModel):
    lease_token: str
    last_command_sequence: int
    last_progress_at: datetime
    replay: bool = False

    @field_serializer("last_progress_at", when_used="always")
    def _serialize_iso8601(self, value: datetime) -> str:
        return value.isoformat()
