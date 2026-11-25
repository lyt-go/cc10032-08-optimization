"""backfill_job 领域服务：历史数据回填任务。

业务逻辑：
1. 回填任务：基于快照基准向目标表回填历史数据
2. 回填链路：snapshot_mgr → backfill_job（使用快照作为数据基准）
3. 失败场景：依赖检查未通过、并行度超限、回填水位不连续
4. 与上下游流水线：消费 snapshot_mgr 快照；输出回填结果

跨模块数据流：
  snapshot_mgr → backfill_job（快照数据驱动历史回填）
"""
from __future__ import annotations

import time
from typing import Any

from app.recovery import (
    WINDOW_PENDING,
    WINDOW_RECOVERING,
    WINDOW_REVOKED,
    WINDOW_WRITTEN,
    BackfillInterrupted,
    BackfillRecoveryEngine,
    BackfillRecoveryService,
    RecoveryManifest,
    RecoveryManager,
    BackfillRecoveryManager,
    WindowRecoveryManager,
    RECOVERY_EVENTS,
    RECOVERY_MANIFEST,
    WindowState,
    recovery_engine,
    recovery_service,
)
from app.services.base import BaseModuleService
from app.store import store


class BackfillJobService(BaseModuleService, BackfillRecoveryEngine):
    MODULE = "backfill_job"
    ENTITY = "回填任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "目标表", "回填起始时间", "回填结束时间", "源表列表", "依赖检查", "并行度", "回填水位"]
    REQUIRED_FIELDS = ["任务编号", "目标表", "回填起始时间"]
    STATUS_ORDER = ["待执行", "运行中", "已完成", "已失败"]
    ACTION_RULES = {"发起回填": "运行中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "backfill_pipeline"
    UPSTREAM_MODULES = ["snapshot_mgr"]
    PRODUCES_ARTIFACT = "回填执行记录"

    def __init__(self) -> None:
        BaseModuleService.__init__(self)
        BackfillRecoveryEngine.__init__(self, store)

    def execute_backfill(self, entry_id: int | str | None = None, windows: Any = None, **options: Any) -> dict[str, Any]:
        """执行回填。

        - 不传窗口参数时保留原有粗粒度回填行为和旧测试契约。
        - 传入 windows/dates/days 或中断/恢复参数时，走按时间窗口的恢复引擎。
        """
        rich_options = {"dates", "days", "start_date", "interrupt_window", "interrupt_after_window",
                        "partial_end", "cutoff", "fail_windows", "window_overrides", "recovery"}
        if windows is not None or any(k in options for k in rich_options):
            return self.run_backfill(entry_id, windows=windows, **options)

        entry = None
        if entry_id is not None:
            try:
                entry = store.find(self.MODULE, int(entry_id))
            except (TypeError, ValueError):
                entry = next((row for row in store.rows(self.MODULE)
                              if str(row.get("任务编号")) == str(entry_id)), None)
        if entry is None:
            return {"ok": False, "reason": f"任务 {entry_id} 不存在"}

        task_job_id = str(entry.get("任务编号") or f"BACKFILL-{entry.get('id')}")
        existing_windows = [
            record for record in store._recovery_manifest
            if record.get("entry_id") == entry.get("id") or record.get("job_id") == task_job_id
        ]
        if existing_windows and not options.get("fresh"):
            # 已建立窗口清单后，再次调用 execute_backfill 的语义是断点续做，
            # 而不是被作业行上的“运行中/已失败”状态挡住。
            recovered = self.recover_backfill(entry_id=entry.get("id"))
            recovered["rows_inserted"] = sum(
                int(item.get("rows_written") or 0)
                for item in recovered.get("results", []) if item.get("ok")
            )
            return recovered

        if entry.get("status") != "待执行":
            return {"ok": False, "reason": f"任务状态 {entry.get('status')} 非待执行"}

        target_table = str(entry.get("目标表", ""))
        source_tables = str(entry.get("源表列表", "")).split(",")
        parallelism = int(str(entry.get("并行度", "1")))

        # 依赖检查：验证源表有快照
        deps_ok = True
        dep_issues = []
        for st in source_tables:
            st = st.strip()
            if not st:
                continue
            has_snapshot = any(snap.get("table") == st for snap in store._snapshot_store.values())
            if not has_snapshot:
                dep_issues.append(f"源表 {st} 无可用快照")
                deps_ok = False
        if not deps_ok:
            entry["依赖检查"] = "失败: " + "; ".join(dep_issues)
            entry["status"] = "已失败"
            entry["abnormal"] = True
            return {"ok": False, "reason": "; ".join(dep_issues)}

        if parallelism > 10:
            return {"ok": False, "reason": f"并行度 {parallelism} 超限（最大10）"}

        # 模拟回填数据
        rows_to_insert = []
        for i in range(20):
            rows_to_insert.append({
                "table": target_table,
                "batch": entry.get("任务编号"),
                "row": i,
                "ts": time.time() - i * 3600,  # 历史时间
                "backfilled": True,
            })

        entry["依赖检查"] = "通过"
        entry["回填水位"] = str(time.time())
        entry["status"] = "运行中"
        entry["pending"] = True

        batch_id = f"BACKFILL_{entry_id}_{int(time.time())}"
        self.produce_artifact(rows_to_insert, batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "target_table": target_table,
            "rows_inserted": len(rows_to_insert),
            "source_tables": source_tables,
            "dependencies_passed": len(source_tables),
        }

    def run_backfill(self, job: Any = None, windows: Any = None, **options: Any) -> dict[str, Any]:
        """按天/时间窗口执行一段回填，可在指定窗口模拟写入中断。"""
        entry_id = options.pop("entry_id", None)
        if entry_id is None and isinstance(job, (int, str)) and not self.get_job(job):
            try:
                store.find(self.MODULE, int(job))
                entry_id = job
                job = None
            except (TypeError, ValueError):
                pass

        plan = self.plan_windows(
            job,
            windows,
            entry_id=entry_id,
            target_table=options.get("target_table"),
            dates=options.get("dates"),
            days=options.get("days"),
            start_date=options.get("start_date"),
            source_tables=options.get("source_tables"),
            priority=options.get("priority"),
            snapshot_versions=options.get("snapshot_versions"),
            snapshot_version=options.get("snapshot_version"),
            snapshot_id=options.get("snapshot_id"),
        )
        if not plan.get("ok"):
            return plan

        job_id = plan["job_id"]
        entry = store.find(self.MODULE, int(entry_id)) if entry_id is not None else None
        if entry is not None:
            entry["依赖检查"] = "通过"
            entry["status"] = "运行中"
            entry["pending"] = True

        interrupt_window = options.get("interrupt_window", options.get("interrupt_after_window"))
        fail_windows = set(options.get("fail_windows") or [])
        partial_end = options.get("partial_end") or options.get("cutoff")
        window_overrides = options.get("window_overrides") or {}
        results: list[dict[str, Any]] = []

        for index, record in enumerate(plan["windows"], 1):
            wid = record["window_id"]
            should_interrupt = (
                interrupt_window in (index, wid, record["id"])
                or index in fail_windows or wid in fail_windows or record["id"] in fail_windows
            )
            call_options: dict[str, Any] = dict(window_overrides.get(wid, {}))
            if should_interrupt:
                call_options["interrupt"] = True
                if partial_end is not None:
                    cut = partial_end.get(wid, partial_end.get(index)) if isinstance(partial_end, dict) else partial_end
                    if cut is not None:
                        call_options["partial_end"] = cut
                elif "partial_fraction" in options:
                    rng = record["range"]
                    frac = float(options["partial_fraction"])
                    call_options["partial_end"] = rng["lo"] + (rng["hi"] - rng["lo"]) * frac
                if options.get("raise_on_interrupt"):
                    call_options["raise_on_interrupt"] = True

            result = self.execute_window(job_id, record["id"], **call_options)
            results.append(result)
            if should_interrupt and not result.get("ok") and options.get("stop_on_interrupt", True):
                if entry is not None:
                    entry["status"] = "已失败"
                    entry["abnormal"] = True
                    entry["回填水位"] = "window_recovering"
                return self._ok(
                    job_id=job_id, entry_id=entry_id, results=results,
                    interrupted=True, recovery_required=True,
                    manifest=self.store.recovery_manifest_for(job_id=job_id),
                )

        if entry is not None and all(r.get("ok") for r in results):
            entry["status"] = "已完成"
            entry["pending"] = False
            entry["回填水位"] = str(time.time())
        return self._ok(
            job_id=job_id, entry_id=entry_id, results=results,
            manifest=self.store.recovery_manifest_for(job_id=job_id),
        )

    start_backfill = run_backfill

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        recovery_actions = {
            "续做回填": "recover",
            "恢复回填": "recover",
            "回填恢复": "recover",
            "撤销窗口": "revoke",
            "撤销回填窗口": "revoke",
        }
        if action in recovery_actions:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            if recovery_actions[action] == "recover":
                result = self.recover_backfill(entry_id=entry_id)
            else:
                windows = self.store.recovery_manifest_for(job_id=str(entry.get("任务编号") or ""))
                if not windows:
                    windows = [r for r in self.store._recovery_manifest if r.get("entry_id") == entry_id]
                if not windows:
                    return None, "没有可撤销的回填窗口"
                result = self.revoke_window(windows[-1]["id"])
            message = "；".join(str(r.get("reason") or r.get("state") or "完成")
                                for r in result.get("results", [])) or "恢复清单已更新"
            return entry if result.get("ok", True) else None, message
        return super().run_action(entry_id, action)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["snapshot_dependencies"] = len(store._snapshot_store)
        base["recovery_windows"] = len(store._recovery_manifest)
        base["recovery_events"] = len(store._recovery_events)
        base["valid_target_results"] = len(self.valid_results())
        return base

# 模块级清单别名，兼容 from app.services.backfill_job import recovery_manifest。
recovery_manifest = RECOVERY_MANIFEST
