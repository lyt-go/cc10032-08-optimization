"""回填恢复专用 HTTP 接口。

通用资源动作接口只能传 action 名称，无法表达窗口、切点和优先级，因此这里
提供一组薄路由；所有业务判断仍在 BackfillJobService/恢复引擎中。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from app.services import service_for
from app.services.snapshot_mgr import SnapshotMgrService

router = APIRouter(prefix="/api/backfill_job/recovery", tags=["回填恢复"])


def _service():
    return service_for("backfill_job")


@router.post("/snapshots")
def publish_snapshot(payload: dict[str, Any]):
    return SnapshotMgrService().publish_snapshot(**payload)


@router.get("/manifest")
def manifest(
    target_table: str | None = Query(default=None),
    job_id: str | None = Query(default=None),
    state: str | None = Query(default=None),
):
    return _service().manifest(target_table=target_table, job_id=job_id, state=state)


@router.get("/events")
def events(job_id: str | None = Query(default=None), window_id: str | None = Query(default=None)):
    return {"ok": True, "events": _service().events(job_id=job_id, window_id=window_id)}


@router.post("/plan")
def plan(payload: dict[str, Any]):
    return _service().plan_windows(**payload)


@router.post("/execute")
def execute(payload: dict[str, Any]):
    args = payload.pop("args", [])
    return _service().execute_window(*args, **payload)


@router.post("/interrupt")
def interrupt(payload: dict[str, Any]):
    args = payload.pop("args", [])
    return _service().interrupt_window(*args, **payload)


@router.post("/resume")
def resume(payload: dict[str, Any]):
    args = payload.pop("args", [])
    return _service().resume_window(*args, **payload)


@router.post("/recover")
def recover(payload: dict[str, Any] | None = None):
    payload = payload or {}
    args = payload.pop("args", [])
    return _service().recover_backfill(*args, **payload)


@router.post("/revoke")
def revoke(payload: dict[str, Any]):
    args = payload.pop("args", [])
    return _service().revoke_window(*args, **payload)


@router.post("/claim")
def claim(payload: dict[str, Any]):
    args = payload.pop("args", [])
    return _service().claim_window(*args, **payload)


@router.post("/commit")
def commit(payload: dict[str, Any]):
    args = payload.pop("args", [])
    return _service().commit_window(*args, **payload)


@router.post("/overlap")
def overlap(payload: dict[str, Any]):
    return _service().overlap_report(**payload)


@router.get("/delivery/{target_table}")
def delivery(target_table: str):
    return _service().verify_delivery(target_table)


@router.get("/source-records")
def source_records(
    job_id: str | None = Query(default=None),
    window_id: str | None = Query(default=None),
    source_table: str | None = Query(default=None),
):
    return _service().read_source_records(
        job_id=job_id, window_id=window_id, source_table=source_table)
