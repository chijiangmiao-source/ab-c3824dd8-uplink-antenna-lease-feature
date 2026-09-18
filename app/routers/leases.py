"""HTTP routes for antenna control leases."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import engine
from app.errors import APIError
from app.schemas import AcquireRequest, AcquireResponse, LeaseStatusResponse
from app.services import acquire_lease, get_lease_by_token

router = APIRouter(tags=["leases"])


def get_session():
    # Plain transactional scope: the service layer decides commit/rollback
    # boundaries explicitly.
    with Session(engine, future=True) as session:
        yield session


@router.post(
    "/leases",
    response_model=AcquireResponse,
    status_code=200,
    summary="原子获取一副天线的控制租约",
)
def acquire(payload: AcquireRequest, session: Session = Depends(get_session)):
    try:
        result = acquire_lease(
            session.connection(),
            antenna_id=payload.antenna_id,
            controller=payload.controller,
            duration_seconds=payload.duration_seconds,
            idempotency_key=payload.idempotency_key,
        )
    except APIError:
        session.rollback()
        raise
    session.commit()
    return result


@router.get(
    "/leases/{lease_token}",
    response_model=LeaseStatusResponse,
    summary="按令牌查询租约状态",
)
def lease_status(lease_token: str, session: Session = Depends(get_session)):
    result = get_lease_by_token(session.connection(), lease_token)
    if result is None:
        session.rollback()
        raise APIError(
            404,
            "LEASE_NOT_FOUND",
            "未知租约令牌。",
            {"lease_token": lease_token},
        )
    session.commit()
    # Normalise to the response model field names (drop the internal id,
    # expose the token as ``lease_token``).
    result.pop("lease_id", None)
    result["lease_token"] = result.pop("token")
    return result
