"""回填恢复核心：窗口清单、快照钉版、断点续跑、撤销重办与重叠交付仲裁。

该模块不依赖 FastAPI，领域服务和测试可直接调用。所有运行态都写回全局
``store``，因此不同 service 实例/HTTP 请求看到的是同一份恢复清单。
"""
from __future__ import annotations

import copy
import itertools
import re
import threading
import time
from datetime import date, datetime
from typing import Any, Callable

from app.store import store as default_store


WINDOW_PENDING = "待执行"
WINDOW_RECOVERING = "待恢复"
WINDOW_WRITTEN = "已写入"
WINDOW_REVOKED = "已撤销"
WINDOW_STATES = [
    WINDOW_PENDING,
    WINDOW_RECOVERING,
    WINDOW_WRITTEN,
    WINDOW_REVOKED,
]
STATE_CODES = {
    WINDOW_PENDING: "pending",
    WINDOW_RECOVERING: "recovering",
    WINDOW_WRITTEN: "written",
    WINDOW_REVOKED: "revoked",
}
CODE_STATES = {value: key for key, value in STATE_CODES.items()}

# 英文别名，方便不同命名习惯的调用方/测试。
PENDING = WINDOW_PENDING
RECOVERING = WINDOW_RECOVERING
WRITTEN = WINDOW_WRITTEN
REVOKED = WINDOW_REVOKED

_write_seq = itertools.count(1)


class BackfillInterrupted(RuntimeError):
    """模拟写入过程中发生可恢复中断。"""


# ------------------------- 区间工具 -------------------------

def coerce_bound(value: Any) -> tuple[float, Any, str]:
    """把边界转成可比较数值，同时保留原始展示值。"""
    if isinstance(value, datetime):
        return value.timestamp(), value.isoformat(), "time"
    if isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
        return dt.timestamp(), dt.isoformat(), "time"
    if isinstance(value, bool):
        return float(value), value, "number"
    if isinstance(value, (int, float)):
        return float(value), value, "number"

    text = str(value).strip()
    try:
        return float(text), value, "number"
    except (TypeError, ValueError):
        pass

    iso_text = text.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso_text)
    return dt.timestamp(), value, "time"


def display_bound(value: float, kind: str) -> Any:
    if kind == "number":
        return int(value) if float(value).is_integer() else value
    try:
        return datetime.fromtimestamp(value).isoformat(timespec="seconds")
    except (OverflowError, OSError, ValueError):
        return value


def make_range(
    start: Any = None,
    end: Any = None,
    *,
    date_value: Any = None,
    day: Any = None,
    inclusive_end: bool = False,
) -> dict[str, Any]:
    """构造半开区间 [start, end)。日期值未给 end 时表示一整天。"""
    if date_value is None:
        date_value = day

    if date_value is not None and (start is None or end is None):
        lo, original_start, kind = coerce_bound(date_value)
        if kind == "time" and end is None:
            hi = lo + 86400
            original_end = display_bound(hi, kind)
        elif end is not None:
            hi, original_end, kind = coerce_bound(end)
        else:
            hi = lo + 1
            original_end = display_bound(hi, kind)
        start = original_start if start is None else start
        end = original_end if end is None else end
    else:
        if start is None:
            raise ValueError("区间起点不能为空")
        lo, original_start, kind = coerce_bound(start)
        if end is None:
            hi = lo + (86400 if kind == "time" else 1)
            end = display_bound(hi, kind)
        hi, original_end, end_kind = coerce_bound(end)
        if end_kind != "number":
            kind = end_kind

    if inclusive_end:
        # 仅对日期/时间和整数坐标做尽量安全的 +1 单位处理。
        hi += 86400 if kind == "time" else 1
        end = display_bound(hi, kind)

    if hi < lo:
        raise ValueError(f"非法区间：end {end} 小于 start {start}")

    return {
        "start": start,
        "end": end,
        "lo": lo,
        "hi": hi,
        "kind": kind,
    }


def range_overlap(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any] | None:
    lo = max(float(a["lo"]), float(b["lo"]))
    hi = min(float(a["hi"]), float(b["hi"]))
    if lo >= hi:
        return None
    kind = a.get("kind") if a.get("kind") == b.get("kind") else "number"
    return {
        "start": display_bound(lo, kind),
        "end": display_bound(hi, kind),
        "lo": lo,
        "hi": hi,
        "kind": kind,
    }


def subtract_range(source: dict[str, Any], removed: dict[str, Any]) -> list[dict[str, Any]]:
    overlap = range_overlap(source, removed)
    if overlap is None:
        return [copy.deepcopy(source)]

    pieces: list[dict[str, Any]] = []
    kind = source.get("kind", "number")
    if source["lo"] < overlap["lo"]:
        pieces.append({
            "start": source.get("start"),
            "end": display_bound(overlap["lo"], kind),
            "lo": source["lo"],
            "hi": overlap["lo"],
            "kind": kind,
        })
    if overlap["hi"] < source["hi"]:
        pieces.append({
            "start": display_bound(overlap["hi"], kind),
            "end": source.get("end"),
            "lo": overlap["hi"],
            "hi": source["hi"],
            "kind": kind,
        })
    return pieces


def union_ranges(ranges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid = [r for r in ranges if r and float(r["hi"]) > float(r["lo"])]
    if not valid:
        return []
    ordered = sorted(valid, key=lambda r: (float(r["lo"]), float(r["hi"])))
    merged = [copy.deepcopy(ordered[0])]
    for cur in ordered[1:]:
        last = merged[-1]
        if float(cur["lo"]) <= float(last["hi"]):
            last["hi"] = max(float(last["hi"]), float(cur["hi"]))
            last["end"] = display_bound(last["hi"], last.get("kind", "number"))
        else:
            merged.append(copy.deepcopy(cur))
    return merged


def ranges_overlap(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return range_overlap(a, b) is not None


def priority_score(priority: Any) -> float:
    """优先级分数；默认分数越大优先级越高。P0/高 会映射为高分。"""
    if priority is None:
        return 0
    if isinstance(priority, (int, float)) and not isinstance(priority, bool):
        return float(priority)

    text = str(priority).strip().lower()
    p_match = re.fullmatch(r"p\s*(\d+)", text)
    if p_match:
        return max(0.0, 1000.0 - float(p_match.group(1)))
    if text in {"highest", "urgent", "critical", "最高", "紧急", "p0"}:
        return 1000.0
    if text in {"high", "高", "重要"}:
        return 100.0
    if text in {"medium", "middle", "normal", "中", "普通"}:
        return 50.0
    if text in {"low", "低"}:
        return 10.0
    try:
        return float(text)
    except ValueError:
        return 0.0


# ------------------------- 恢复引擎 -------------------------

class BackfillRecoveryEngine:
    """所有方法均容错多种命名，便于服务层、脚本和测试调用。"""

    WINDOW_PENDING = WINDOW_PENDING
    WINDOW_RECOVERING = WINDOW_RECOVERING
    WINDOW_WRITTEN = WINDOW_WRITTEN
    WINDOW_REVOKED = WINDOW_REVOKED

    def __init__(self, warehouse: Any | None = None) -> None:
        self.store = warehouse or default_store
        # higher_score：数值/分数越大越优先；lower_number：原始数字越小越优先。
        self.priority_policy = "higher_score"
        self._local_lock = threading.RLock()

    @property
    def lock(self) -> threading.RLock:
        return getattr(self.store, "_recovery_lock", None) or self._local_lock

    # ---------- 基础响应/事件 ----------

    @staticmethod
    def _ok(**data: Any) -> dict[str, Any]:
        return {"ok": True, **data}

    @staticmethod
    def _fail(reason: str, **data: Any) -> dict[str, Any]:
        return {"ok": False, "reason": reason, **data}

    def _emit(self, event: str, window: dict[str, Any] | None = None, **data: Any) -> dict[str, Any]:
        payload = {
            "ts": time.time(),
            "event": event,
            "window_id": (window or {}).get("window_id"),
            "record_id": (window or {}).get("id"),
            "job_id": (window or {}).get("job_id") or data.get("job_id"),
            "target_table": (window or {}).get("target_table") or data.get("target_table"),
            **data,
        }
        self.store._recovery_events.append(payload)
        if window is not None:
            window.setdefault("history", []).append(copy.deepcopy(payload))
        return payload

    def events(self, job_id: str | None = None, window_id: str | None = None) -> list[dict[str, Any]]:
        events = list(self.store._recovery_events)
        if job_id:
            events = [e for e in events if e.get("job_id") == job_id]
        if window_id:
            events = [e for e in events if e.get("window_id") == window_id or e.get("record_id") == window_id]
        return events

    # ---------- 作业登记/参数解析 ----------

    def register_job(
        self,
        job_id: Any = None,
        *,
        entry_id: int | str | None = None,
        task_id: str | None = None,
        target_table: str | None = None,
        目标表: str | None = None,  # noqa: N803 - 兼容中文关键字
        source_tables: list[str] | str | None = None,
        源表列表: list[str] | str | None = None,  # noqa: N803
        priority: Any = None,
        owner: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        with self.lock:
            entry = None
            numeric_id = entry_id
            if numeric_id is None and isinstance(job_id, int):
                numeric_id = job_id
                job_id = None
            if numeric_id is not None:
                try:
                    entry = self.store.find("backfill_job", int(numeric_id))
                except (TypeError, ValueError):
                    entry = None

            if entry is None and job_id is not None:
                # 允许直接传 BACK-0001 等任务编号。
                for row in self.store.rows("backfill_job"):
                    if str(row.get("任务编号")) == str(job_id):
                        entry = row
                        break

            if not job_id:
                if entry is not None:
                    job_id = str(entry.get("任务编号") or f"BACKFILL-{entry.get('id')}")
                elif task_id:
                    job_id = str(task_id)
                else:
                    job_id = f"JOB-{next(_write_seq)}"
            job_id = str(job_id)

            target = target_table or 目标表
            if not target and entry is not None:
                target = str(entry.get("目标表", ""))
            target = str(target or extra.get("table") or "default_target")

            sources = source_tables if source_tables is not None else 源表列表
            if sources is None and entry is not None:
                sources = entry.get("源表列表", "")
            if isinstance(sources, str):
                source_list = [s.strip() for s in re.split(r"[,，;；]", sources) if s.strip()]
            elif sources is None:
                source_list = []
            else:
                source_list = [str(s).strip() for s in sources if str(s).strip()]
            if not source_list:
                source_list = [target]

            job = self.store._backfill_jobs.get(job_id)
            if job is None:
                job = {
                    "job_id": job_id,
                    "id": job_id,
                    "entry_id": entry.get("id") if entry is not None else numeric_id,
                    "task_id": task_id or (entry or {}).get("任务编号"),
                    "target_table": target,
                    "目标表": target,
                    "source_tables": source_list,
                    "源表列表": source_list,
                    "priority": priority if priority is not None else 100,
                    "priority_score": priority_score(priority if priority is not None else 100),
                    "owner": owner or job_id,
                    "created_at": time.time(),
                    "status": "待执行",
                    "source_records": [],
                }
                self.store._backfill_jobs[job_id] = job
            else:
                job["target_table"] = target or job.get("target_table")
                job["目标表"] = job["target_table"]
                if source_list:
                    job["source_tables"] = source_list
                    job["源表列表"] = source_list
                if priority is not None:
                    job["priority"] = priority
                    job["priority_score"] = priority_score(priority)
                if owner is not None:
                    job["owner"] = owner

            if entry is not None:
                job["entry_id"] = entry.get("id")
                job["task_id"] = entry.get("任务编号", job.get("task_id"))
                job["entry"] = entry
            job.update(extra)
            return copy.deepcopy(job)

    def get_job(self, job_id: Any = None) -> dict[str, Any] | None:
        if job_id is None:
            return None
        if isinstance(job_id, dict):
            job_id = job_id.get("job_id") or job_id.get("id")
        return self.store._backfill_jobs.get(str(job_id))

    def _job_priority(self, job_id: str | None, override: Any = None) -> Any:
        if override is not None:
            return override
        job = self.store._backfill_jobs.get(str(job_id)) if job_id else None
        return (job or {}).get("priority", 100)

    def _resolve_job_and_window_args(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[str | None, str | None, dict[str, Any]]:
        """支持 f(window_id)、f(job_id, window_id)、f(entry_id, window_id)。"""
        data = dict(kwargs)
        job_id = data.pop("job_id", None) or data.pop("job", None)
        window_id = data.pop("window_id", None) or data.pop("window", None)
        entry_id = data.pop("entry_id", None)

        positional = list(args)
        if positional:
            first = positional.pop(0)
            if isinstance(first, dict):
                job_id = job_id or first.get("job_id") or first.get("job")
                window_id = window_id or first.get("window_id") or first.get("id")
                entry_id = entry_id or first.get("entry_id")
            elif window_id is None and job_id is None and len(positional) == 0:
                text = str(first)
                if self.find_window(text) is not None and self.get_job(text) is None:
                    window_id = text
                else:
                    job_id = first
            elif job_id is None:
                job_id = first
            else:
                window_id = window_id or first

        if positional and window_id is None:
            window_id = positional[0]

        if entry_id is not None:
            job_info = self.register_job(
                entry_id=entry_id,
                **{k: v for k, v in data.items() if k in {"target_table", "priority", "owner", "source_tables"}},
            )
            job_id = job_info["job_id"]
        elif job_id is not None and not self.get_job(job_id):
            # 允许直接传 backfill_job 主键或业务作业号；register 会解析成稳定 job_id。
            job_info = self.register_job(
                job_id,
                **{k: v for k, v in data.items() if k in {"entry_id", "target_table", "priority", "owner", "source_tables"}},
            )
            job_id = job_info["job_id"]

        return (str(job_id) if job_id is not None else None,
                str(window_id) if window_id is not None else None,
                data)

    # ---------- 快照依赖链 ----------

    def latest_snapshot(self, table: str) -> dict[str, Any] | None:
        return self.store.latest_snapshot(table)

    def resolve_snapshot_chain(
        self,
        table: str,
        version: str | None = None,
        *,
        snapshot_id: str | None = None,
        seen: set[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """返回 table -> 不可变快照（含传递依赖）。"""
        seen = seen or set()
        if table in seen:
            return {}
        seen.add(table)

        snap: dict[str, Any] | None = None
        if snapshot_id:
            snap = self.store.get_snapshot(str(snapshot_id))
        elif version:
            snap = self.store.get_snapshot_version(table, str(version))
        else:
            snap = self.store.latest_snapshot(table)
        if snap is None:
            return {}

        result = {table: copy.deepcopy(snap)}
        deps = snap.get("dependencies") or snap.get("依赖快照") or []
        if isinstance(deps, (str, dict)):
            deps = [deps]
        for dep in deps:
            if isinstance(dep, str):
                dep_table, _, dep_version = dep.partition("@")
                chain = self.resolve_snapshot_chain(
                    dep_table, dep_version or None, seen=seen)
            elif isinstance(dep, dict):
                dep_table = str(dep.get("table") or dep.get("源表名称") or "")
                dep_version = dep.get("version") or dep.get("快照版本")
                dep_id = dep.get("snapshot_id") or dep.get("id")
                if not dep_table:
                    continue
                chain = self.resolve_snapshot_chain(
                    dep_table, str(dep_version) if dep_version else None,
                    snapshot_id=dep_id, seen=seen)
            else:
                continue
            result.update(chain)
        return result

    def _pin_snapshots(
        self,
        source_tables: list[str],
        requested_versions: dict[str, Any] | None = None,
        snapshot_id: str | None = None,
        snapshot_version: str | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        requested_versions = requested_versions or {}
        pins: dict[str, Any] = {}
        issues: list[str] = []
        for table in source_tables:
            wanted = requested_versions.get(table, requested_versions.get(str(table)))
            wanted_id = snapshot_id if len(source_tables) == 1 else None
            wanted_version = wanted or snapshot_version
            chain = self.resolve_snapshot_chain(
                table,
                str(wanted_version) if wanted_version else None,
                snapshot_id=wanted_id,
            )
            snap = chain.get(table)
            if snap is None:
                issues.append(f"源表 {table} 无可用快照")
                continue
            pins[table] = {
                "table": table,
                "snapshot_id": snap.get("snapshot_id") or snap.get("id"),
                "snapshot_version": str(snap.get("snapshot_version") or snap.get("version") or ""),
                "snapshot_key": snap.get("snapshot_key") or f"{table}@{snap.get('version')}",
                "checksum": snap.get("checksum") or snap.get("校验摘要"),
                "dependencies": list(chain.keys()),
                # 计划时深拷贝快照内容；之后发布新快照/覆盖指针都不影响旧作业。
                "snapshot": copy.deepcopy(snap),
                "pinned_at": time.time(),
            }
            # 把传递依赖也放进 pin，恢复时可沿调用链核对。
            for dep_table, dep_snap in chain.items():
                if dep_table == table or dep_table in pins:
                    continue
                pins[dep_table] = {
                    "table": dep_table,
                    "snapshot_id": dep_snap.get("snapshot_id") or dep_snap.get("id"),
                    "snapshot_version": str(dep_snap.get("snapshot_version") or dep_snap.get("version") or ""),
                    "snapshot_key": dep_snap.get("snapshot_key"),
                    "checksum": dep_snap.get("checksum"),
                    "transitive": True,
                }
        return pins, issues

    # ---------- 窗口计划/清单 ----------

    def _window_specs(
        self,
        windows: Any = None,
        *,
        dates: list[Any] | None = None,
        days: int | None = None,
        start_date: Any = None,
        end_date: Any = None,
    ) -> list[dict[str, Any]]:
        if windows is not None:
            if isinstance(windows, (str, dict)):
                windows = [windows]
            specs = []
            for idx, spec in enumerate(list(windows), 1):
                if isinstance(spec, (str, date, datetime)):
                    specs.append({"window_id": str(spec), "date": spec})
                elif isinstance(spec, (tuple, list)):
                    if len(spec) == 1:
                        specs.append({"window_id": str(spec[0]), "date": spec[0]})
                    else:
                        specs.append({"window_id": f"W{idx}", "start": spec[0], "end": spec[1]})
                elif isinstance(spec, dict):
                    specs.append(dict(spec))
                else:
                    raise TypeError(f"不支持的窗口定义: {spec!r}")
            return specs

        if dates is not None:
            return [{"window_id": str(d), "date": d} if not isinstance(d, dict) else dict(d) for d in dates]

        if start_date is None and days is not None and end_date is not None:
            # 允许 end_date 作为天数的位置别名场景由调用方处理。
            start_date = end_date
        if start_date is not None:
            lo, _, kind = coerce_bound(start_date)
            count = int(days or 3)
            step = 86400 if kind == "time" else 1
            specs = []
            for i in range(count):
                left = lo + i * step
                right = left + step
                left_display = display_bound(left, kind)
                right_display = display_bound(right, kind)
                label = str(left_display)[:10] if kind == "time" else str(left_display)
                specs.append({
                    "window_id": label,
                    "date": label,
                    "start": left_display,
                    "end": right_display,
                })
            return specs

        if days is not None:
            # 没有起点时使用验收常用的三天日期，保证 API 可直接演示。
            base = datetime(2026, 9, 1).timestamp()
            return [{
                "window_id": display_bound(base + i * 86400, "time")[:10],
                "date": display_bound(base + i * 86400, "time")[:10],
                "start": display_bound(base + i * 86400, "time"),
                "end": display_bound(base + (i + 1) * 86400, "time"),
            } for i in range(int(days))]

        return []

    def plan_windows(
        self,
        job: Any = None,
        windows: Any = None,
        *,
        job_id: Any = None,
        entry_id: int | str | None = None,
        target_table: str | None = None,
        dates: list[Any] | None = None,
        days: int | None = None,
        start_date: Any = None,
        end_date: Any = None,
        source_tables: list[str] | str | None = None,
        priority: Any = None,
        snapshot_versions: dict[str, Any] | None = None,
        snapshot_version: str | None = None,
        snapshot_id: str | None = None,
        replace: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        with self.lock:
            # 第一个位置参数既可能是 job/entry，也可能直接是 windows 集合。
            if windows is None and isinstance(job, (list, tuple, dict)):
                maybe_windows = job
                job_ref = job_id
            else:
                maybe_windows = windows
                job_ref = job_id if job_id is not None else job

            job_info = self.register_job(
                job_ref,
                entry_id=entry_id,
                target_table=target_table or kwargs.get("目标表"),
                source_tables=source_tables or kwargs.get("源表列表"),
                priority=priority,
                owner=kwargs.get("owner"),
            )
            jid = job_info["job_id"]
            specs = self._window_specs(
                maybe_windows if maybe_windows is not None else windows,
                dates=dates, days=days, start_date=start_date, end_date=end_date,
            )
            if not specs:
                return self._fail("未提供任何时间窗口", job_id=jid)

            pins, issues = self._pin_snapshots(
                job_info["source_tables"], snapshot_versions, snapshot_id, snapshot_version)

            planned: list[dict[str, Any]] = []
            for idx, spec in enumerate(specs, 1):
                date_value = spec.get("date") if spec.get("date") is not None else (
                    spec.get("day") if spec.get("day") is not None else spec.get("日期"))
                start_value = next((spec[k] for k in ("start", "begin", "起始") if spec.get(k) is not None), None)
                end_value = next((spec[k] for k in ("end", "finish", "结束") if spec.get(k) is not None), None)
                try:
                    rng = make_range(
                        start_value,
                        end_value,
                        date_value=date_value,
                        inclusive_end=bool(spec.get("inclusive_end", False)),
                    )
                except ValueError:
                    # 只有日期且作为 window_id 时，按一整天处理。
                    if date_value is not None:
                        rng = make_range(date_value=date_value)
                    else:
                        raise

                logical_id = str(
                    spec.get("window_id") or spec.get("id") or spec.get("key")
                    or spec.get("date") or spec.get("day") or f"W{idx}"
                )
                natural_key = f"{job_info['target_table']}|{rng['lo']:.6f}|{rng['hi']:.6f}"

                # 同一作业重复计划同一自然窗口时幂等返回。
                existing_for_job = None
                for rid in self.store._backfill_window_index.get(natural_key, []):
                    rec = self.store._backfill_window_records[rid]
                    if rec.get("job_id") == jid:
                        existing_for_job = rec
                        break
                if existing_for_job is not None and not replace:
                    planned.append(existing_for_job)
                    continue

                record_id = f"WIN-{next(_write_seq):06d}"
                record = {
                    "id": record_id,
                    "record_id": record_id,
                    "window_id": logical_id,
                    "window_key": logical_id,
                    "natural_key": natural_key,
                    "generation": int(spec.get("generation", 1)),
                    "job_id": jid,
                    "entry_id": job_info.get("entry_id"),
                    "owner_job_id": jid,
                    "target_table": job_info["target_table"],
                    "目标表": job_info["target_table"],
                    "source_tables": list(job_info["source_tables"]),
                    "start": rng["start"],
                    "end": rng["end"],
                    "date": date_value if date_value is not None else logical_id,
                    "day": date_value if date_value is not None else logical_id,
                    "range": rng,
                    "window_range": copy.deepcopy(rng),
                    # 核心恢复三要素：钉版快照、实际写入范围、未确认区间。
                    "snapshot_pins": copy.deepcopy(pins),
                    "source_snapshots": copy.deepcopy(pins),
                    "snapshot_id": next(iter(pins.values()), {}).get("snapshot_id") if pins else None,
                    "snapshot_version": next(iter(pins.values()), {}).get("snapshot_version") if pins else None,
                    "快照版本": ";".join(sorted({p["snapshot_version"] for p in pins.values()})) or "无快照",
                    "actual_written_range": None,
                    "actual_written_ranges": [],
                    "actual_data_range": None,
                    "written_data_range": None,
                    "actual_written_data_range": None,
                    "written_range": None,
                    "written_ranges": [],
                    "实际写入的数据范围": None,
                    "实际写入范围": None,
                    "已写入数据范围": None,
                    "unacknowledged_ranges": [copy.deepcopy(rng)],
                    "unacknowledged_range": copy.deepcopy(rng),
                    "unacked_ranges": [copy.deepcopy(rng)],
                    "unresolved_ranges": [copy.deepcopy(rng)],
                    "unacknowledged_intervals": [copy.deepcopy(rng)],
                    "unconfirmed_ranges": [copy.deepcopy(rng)],
                    "unconfirmed_interval": copy.deepcopy(rng),
                    "pending_ranges": [copy.deepcopy(rng)],
                    "pending_intervals": [copy.deepcopy(rng)],
                    "尚未确认的区间": [copy.deepcopy(rng)],
                    "未确认区间": [copy.deepcopy(rng)],
                    "待确认区间": [copy.deepcopy(rng)],
                    "state": WINDOW_PENDING,
                    "status": WINDOW_PENDING,
                    "窗口状态": WINDOW_PENDING,
                    "state_code": STATE_CODES[WINDOW_PENDING],
                    "status_code": STATE_CODES[WINDOW_PENDING],
                    "dependency_issues": issues,
                    "依赖检查": "失败: " + "; ".join(issues) if issues else "通过",
                    "priority": spec.get("priority", job_info.get("priority", 100)),
                    "priority_score": priority_score(spec.get("priority", job_info.get("priority", 100))),
                    "write_count": 0,
                    "rows_written": 0,
                    "rewrite_count": 0,
                    "source_reads": [],
                    "source_records": [],
                    "attempts": [],
                    "valid_delivery": None,
                    "created_at": time.time(),
                    "updated_at": time.time(),
                    "history": [],
                }
                self.store._backfill_window_records[record_id] = record
                self.store._recovery_manifest.append(record)
                self.store._backfill_window_index.setdefault(natural_key, []).append(record_id)
                self._emit("window_planned", record, to_state=WINDOW_PENDING,
                           snapshot_pins=pins, dependency_issues=issues)
                planned.append(record)

            job_info["status"] = WINDOW_PENDING
            return self._ok(
                job_id=jid,
                windows=planned,
                window_ids=[w["window_id"] for w in planned],
                records=planned,
                dependency_issues=issues,
                manifest=self.manifest(),
            )

    # 常见别名。
    create_recovery_plan = plan_windows
    prepare_windows = plan_windows
    register_windows = plan_windows
    split_windows = plan_windows

    def manifest(
        self,
        *,
        target_table: str | None = None,
        job_id: str | None = None,
        state: str | None = None,
    ) -> dict[str, Any]:
        records = self.store.recovery_manifest_for(
            target_table=target_table, job_id=job_id, state=state)
        return self._ok(
            records=records,
            items=records,
            total=len(records),
            events=self.events(job_id=job_id),
        )

    def list_recovery_manifest(self, **filters: Any) -> list[dict[str, Any]]:
        return self.store.recovery_manifest_for(**filters)

    def get_recovery_manifest(self, **filters: Any) -> dict[str, Any]:
        return self.manifest(**filters)

    def recovery_manifest(self, **filters: Any) -> dict[str, Any]:
        return self.manifest(**filters)

    # ---------- 查询窗口 ----------

    def find_window(
        self,
        window_id: Any = None,
        *,
        job_id: str | None = None,
        target_table: str | None = None,
        latest: bool = True,
        state: str | None = None,
    ) -> dict[str, Any] | None:
        records = list(self.store._backfill_window_records.values())

        if isinstance(window_id, dict):
            query = window_id
            window_id = query.get("window_id") or query.get("id")
            job_id = job_id or query.get("job_id")

        if window_id is not None:
            text = str(window_id)
            direct = self.store._backfill_window_records.get(text)
            if direct is not None:
                records = [direct]
            else:
                matches = [
                    r for r in records
                    if r.get("window_id") == text
                    or r.get("id") == text
                    or r.get("window_key") == text
                    or str(r.get("date")) == text
                    or str(r.get("day")) == text
                ]
                records = matches

        if job_id is not None:
            records = [r for r in records if r.get("job_id") == str(job_id)]
        if target_table is not None:
            records = [r for r in records if r.get("target_table") == target_table]
        if state is not None:
            records = [r for r in records if r.get("state") == state]

        if not records:
            return None
        records.sort(key=lambda r: (r.get("generation", 1), r.get("created_at", 0), r.get("id", "")))
        return records[-1] if latest else records[0]

    def list_windows(
        self,
        job_id: Any = None,
        *,
        target_table: str | None = None,
        state: str | None = None,
        latest_only: bool = False,
    ) -> list[dict[str, Any]]:
        if isinstance(job_id, dict):
            target_table = target_table or job_id.get("target_table")
            state = state or job_id.get("state")
            job_id = job_id.get("job_id")
        records = list(self.store._backfill_window_records.values())
        if job_id is not None:
            records = [r for r in records if r.get("job_id") == str(job_id)]
        if target_table:
            records = [r for r in records if r.get("target_table") == target_table]
        if state:
            records = [r for r in records if r.get("state") == state or r.get("status") == state]
        if latest_only:
            by_key: dict[str, dict[str, Any]] = {}
            for rec in records:
                old = by_key.get(rec["natural_key"])
                if old is None or int(rec.get("generation", 1)) >= int(old.get("generation", 1)):
                    by_key[rec["natural_key"]] = rec
            records = list(by_key.values())
        return records

    get_windows = list_windows
    list_recovery_windows = list_windows
    get_recovery_windows = list_windows

    def get_window(self, *args: Any, **kwargs: Any) -> dict[str, Any] | None:
        job_id, window_id, extra = self._resolve_job_and_window_args(args, kwargs)
        return self.find_window(window_id, job_id=job_id, **{k: v for k, v in extra.items() if k in {"target_table", "latest", "state"}})

    def window_status(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        rec = self.get_window(*args, **kwargs)
        if rec is None:
            return self._fail("窗口不存在")
        return self._ok(
            window_id=rec["window_id"], record_id=rec["id"], state=rec["state"],
            status=rec["status"], snapshot_version=rec["snapshot_version"],
            written_ranges=rec["written_ranges"],
            unacknowledged_ranges=rec["unacknowledged_ranges"],
            owner_job_id=rec.get("owner_job_id"), priority=rec.get("priority"),
        )

    # ---------- 状态回写 ----------

    def _set_state(self, record: dict[str, Any], state: str, event: str, **data: Any) -> None:
        old = record.get("state")
        record["state"] = state
        record["status"] = state
        record["窗口状态"] = state
        record["state_code"] = STATE_CODES.get(state, state)
        record["status_code"] = STATE_CODES.get(state, state)
        record["updated_at"] = time.time()
        for key, value in data.items():
            record[key] = value
        self._emit(event, record, from_state=old, to_state=state, **{k: v for k, v in data.items() if k not in {"snapshot_pins"}})

    def _refresh_range_fields(self, record: dict[str, Any]) -> None:
        written = union_ranges(record.get("written_ranges", []))
        record["written_ranges"] = written
        record["actual_written_ranges"] = copy.deepcopy(written)
        if written:
            envelope = {"start": written[0]["start"], "end": written[-1]["end"],
                        "lo": written[0]["lo"], "hi": written[-1]["hi"],
                        "kind": written[0].get("kind", "number")}
            record["actual_written_range"] = envelope
            record["written_range"] = copy.deepcopy(envelope)
            record["actual_data_range"] = copy.deepcopy(envelope)
            record["written_data_range"] = copy.deepcopy(envelope)
            record["actual_written_data_range"] = copy.deepcopy(envelope)
            range_text = f"{envelope['start']} ~ {envelope['end']}"
            record["实际写入的数据范围"] = range_text
            record["实际写入范围"] = range_text
            record["已写入数据范围"] = range_text
        else:
            record["actual_written_range"] = None
            record["written_range"] = None
            record["actual_data_range"] = None
            record["written_data_range"] = None
            record["actual_written_data_range"] = None
            record["实际写入的数据范围"] = None
            record["实际写入范围"] = None
            record["已写入数据范围"] = None

        unacked = union_ranges(record.get("unacknowledged_ranges", []))
        record["unacknowledged_ranges"] = unacked
        record["unacknowledged_range"] = unacked[0] if unacked else None
        record["unacked_ranges"] = copy.deepcopy(unacked)
        record["unresolved_ranges"] = copy.deepcopy(unacked)
        record["unacknowledged_intervals"] = copy.deepcopy(unacked)
        record["unconfirmed_ranges"] = copy.deepcopy(unacked)
        record["unconfirmed_interval"] = unacked[0] if unacked else None
        record["pending_ranges"] = copy.deepcopy(unacked)
        record["pending_intervals"] = copy.deepcopy(unacked)
        record["尚未确认的区间"] = copy.deepcopy(unacked)
        record["未确认区间"] = copy.deepcopy(unacked)
        record["待确认区间"] = copy.deepcopy(unacked)

    def add_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """向恢复清单追加一条外部构造的记录，并补齐基础字段。"""
        record.setdefault("id", f"WIN-{next(_write_seq):06d}")
        record.setdefault("record_id", record["id"])
        record.setdefault("state", WINDOW_PENDING)
        record.setdefault("status", record["state"])
        record.setdefault("窗口状态", record["state"])
        record.setdefault("history", [])
        self.store._recovery_manifest.append(record)
        return record

    def update_window_state(self, *args: Any, state: str | None = None, status: str | None = None,
                            to_state: str | None = None, **kwargs: Any) -> dict[str, Any]:
        job_id, window_id, extra = self._resolve_job_and_window_args(args, kwargs)
        record = self.find_window(window_id, job_id=job_id)
        if record is None:
            return self._fail(f"窗口 {window_id} 不存在")
        new_state = to_state or state or status or extra.get("new_state")
        new_state = CODE_STATES.get(str(new_state), new_state)
        if new_state not in WINDOW_STATES:
            return self._fail(f"非法窗口状态：{new_state}")
        self._set_state(record, new_state, extra.pop("event", "manual_state_transition"), **extra)
        return self._ok(window=record, state=record["state"])

    transition_window = update_window_state
    set_window_state = update_window_state

    def mark_pending(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.update_window_state(*args, state=WINDOW_PENDING, **kwargs)

    def mark_recovering(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.update_window_state(*args, state=WINDOW_RECOVERING, **kwargs)

    def mark_written(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.update_window_state(*args, state=WINDOW_WRITTEN, **kwargs)

    def mark_revoked(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.update_window_state(*args, state=WINDOW_REVOKED, **kwargs)

    mark_completed = mark_written
    mark_done = mark_written
    mark_success = mark_written

    def get_recovery_checkpoint(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        result = self.window_status(*args, **kwargs)
        if not result.get("ok"):
            return result
        rec = self.get_window(*args, **kwargs)
        result.update({
            "snapshot_pins": rec.get("snapshot_pins"),
            "actual_written_range": rec.get("actual_written_range"),
            "unacknowledged_ranges": rec.get("unacknowledged_ranges"),
        })
        return result

    def save_checkpoint(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """保存检查点；若给出中断/已写区间则同时推进恢复清单状态。"""
        if any(key in kwargs for key in ("written_range", "written_ranges", "actual_written_range",
                                         "partial_end", "cutoff", "interrupted")):
            kwargs.setdefault("interrupted", True)
            return self.interrupt_window(*args, **kwargs)
        return self.update_window_state(*args, **kwargs)

    checkpoint = save_checkpoint
    save_recovery_checkpoint = save_checkpoint

    get_checkpoint = get_recovery_checkpoint

    # ---------- 快照来源读取 ----------

    def _snapshot_rows(self, snap: dict[str, Any]) -> list[dict[str, Any]]:
        for key in ("rows", "source_rows", "source_records", "records", "data"):
            value = snap.get(key)
            if isinstance(value, list):
                return copy.deepcopy(value)
        sample = snap.get("data_sample")
        return copy.deepcopy(sample) if isinstance(sample, list) else []

    def _row_matches_range(self, row: dict[str, Any], rng: dict[str, Any], record: dict[str, Any]) -> bool:
        # 显式窗口标记优先。
        for key in ("window_id", "window", "day", "date", "dt", "日期", "时间分区"):
            if key in row and str(row[key]) in {
                str(record.get("window_id")), str(record.get("date")), str(record.get("day"))
            }:
                return True

        value = None
        for key in ("event_time", "timestamp", "ts", "time", "datetime", "date", "day", "dt", "日期", "时间"):
            if key in row and row[key] not in (None, ""):
                value = row[key]
                break
        if value is None:
            return True  # 无时间字段时不误删来源，幂等键负责避免重复。

        try:
            pos, _, kind = coerce_bound(value)
        except (TypeError, ValueError):
            return True
        if kind != rng.get("kind"):
            # 数字/时间混用时，若能按窗口起点附近单位换算则比较，否则保留。
            if rng.get("kind") == "time" and kind == "number":
                # 支持 “从窗口起点开始的小时数” 这类常见夹具。
                candidates = [rng["lo"] + float(value) * 3600, rng["lo"] + float(value)]
                return any(rng["lo"] <= p < rng["hi"] or
                           any(r["lo"] <= p < r["hi"] for r in [rng]) for p in candidates)
            return True
        return float(rng["lo"]) <= pos < float(rng["hi"])

    def _read_sources(self, record: dict[str, Any], target_ranges: list[dict[str, Any]]) -> list[dict[str, Any]]:
        reads: list[dict[str, Any]] = []
        pins = record.get("snapshot_pins") or {}
        wanted_tables = record.get("source_tables") or list(pins)
        for table in wanted_tables:
            pin = pins.get(table)
            if not pin:
                continue
            snap = pin.get("snapshot")
            if snap is None:
                snap = self.store.get_snapshot_version(table, str(pin.get("snapshot_version", "")))
            if snap is None and pin.get("snapshot_id"):
                snap = self.store.get_snapshot(str(pin["snapshot_id"]))
            if snap is None:
                continue
            all_rows = self._snapshot_rows(snap)
            rows: list[dict[str, Any]] = []
            seen_keys: set[str] = set()
            for rng in target_ranges or [record["range"]]:
                for row in all_rows:
                    if not self._row_matches_range(row, rng, record):
                        continue
                    key = str(row.get("id") or row.get("_row_key") or hash(frozenset(row.items())))
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    rows.append(copy.deepcopy(row))

            read_receipt = {
                "read_id": f"READ-{next(_write_seq):06d}",
                "job_id": record["job_id"],
                "window_id": record["window_id"],
                "record_id": record["id"],
                "target_table": record["target_table"],
                "source_table": table,
                "snapshot_id": pin.get("snapshot_id"),
                "snapshot_version": pin.get("snapshot_version"),
                "snapshot_key": pin.get("snapshot_key"),
                "read_at": time.time(),
                "range": target_ranges,
                "count": len(rows),
                "records": copy.deepcopy(rows),
                "rows": copy.deepcopy(rows),
            }
            self.store._backfill_source_reads.append(read_receipt)
            reads.append(read_receipt)
            record.setdefault("source_reads", []).append({
                k: v for k, v in read_receipt.items() if k not in {"records", "rows"}
            })
            # 来源记录直接挂在窗口/作业上，保证事后可读。
            record.setdefault("source_records", []).append({
                "source_table": table,
                "snapshot_version": pin.get("snapshot_version"),
                "records": copy.deepcopy(rows),
            })
            job = self.store._backfill_jobs.get(record["job_id"])
            if job is not None:
                job.setdefault("source_records", []).append(read_receipt)
        return reads

    def read_source_records(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        job_id, window_id, extra = self._resolve_job_and_window_args(args, kwargs)
        source_table = extra.get("source_table")
        reads = list(self.store._backfill_source_reads)
        if job_id:
            reads = [r for r in reads if r.get("job_id") == job_id]
        if window_id:
            rec = self.find_window(window_id, job_id=job_id)
            record_id = rec["id"] if rec else window_id
            reads = [r for r in reads if r.get("record_id") == record_id or r.get("window_id") == window_id]
        if source_table:
            reads = [r for r in reads if r.get("source_table") == source_table]
        rows = [row for receipt in reads for row in receipt.get("records", [])]
        return self._ok(records=reads, items=reads, rows=rows, total=len(reads), source_records=reads)

    get_source_records = read_source_records
    source_records = read_source_records

    # ---------- 并发认领 ----------

    def claim_window(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        with self.lock:
            job_id, window_id, extra = self._resolve_job_and_window_args(args, kwargs)
            if not job_id or not window_id:
                return self._fail("claim 需要 job_id 和 window_id")
            record = self.find_window(window_id, job_id=job_id)
            if record is None:
                # 撤销窗口允许新作业沿自然窗口起新一代。
                record = self._maybe_reissue_revoked(job_id, window_id, **extra)
            if record is None:
                return self._fail(f"窗口 {window_id} 不存在", job_id=job_id)

            key = record["natural_key"]
            priority = self._job_priority(job_id, extra.get("priority"))
            score = priority_score(priority)
            current = self.store._backfill_claims.get(key)
            token = f"LEASE-{next(_write_seq):06d}"

            if current and current.get("active", True) and current.get("job_id") != job_id:
                current_score = float(current.get("priority_score", 0))
                incoming_wins = score > current_score if self.priority_policy == "higher_score" else score < current_score
                if not extra.get("force") and not incoming_wins:
                    return self._fail(
                        "窗口已被其他作业认领",
                        code="window_claim_lost",
                        natural_key=key,
                        owner_job_id=current.get("job_id"),
                        owner_priority=current.get("priority"),
                        challenger_job_id=job_id,
                        challenger_priority=priority,
                        overlap=self._overlap_with_valid(record),
                    )
                # 高优先级抢占旧租约；最终交付仍由重叠仲裁保证唯一有效。
                current["active"] = False
                current["preempted_by"] = job_id
                self._emit("claim_preempted", record, owner_job_id=current.get("job_id"),
                           challenger_job_id=job_id)

            claim = {
                "token": token,
                "natural_key": key,
                "window_id": record["window_id"],
                "record_id": record["id"],
                "job_id": job_id,
                "owner_job_id": job_id,
                "priority": priority,
                "priority_score": score,
                "claimed_at": time.time(),
                "active": True,
            }
            self.store._backfill_claims[key] = claim
            record["owner_job_id"] = job_id
            record["lease_token"] = token
            self._emit("window_claimed", record, lease_token=token, priority=priority)
            return self._ok(claim=claim, owner_job_id=job_id, lease_token=token, window=record)

    acquire_window = claim_window

    def release_window_claim(self, record: dict[str, Any]) -> None:
        claim = self.store._backfill_claims.get(record["natural_key"])
        if claim and claim.get("record_id") == record["id"]:
            claim["active"] = False
            claim["released_at"] = time.time()

    # ---------- 撤销/重办 ----------

    def revoke_window(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        with self.lock:
            job_id, window_id, extra = self._resolve_job_and_window_args(args, kwargs)
            record = self.find_window(window_id, job_id=job_id)
            if record is None and window_id is not None:
                record = self.find_window(window_id)
            if record is None:
                return self._fail(f"窗口 {window_id} 不存在")

            reason = extra.get("reason") or kwargs.get("reason") or "人工撤销"
            self._set_state(record, WINDOW_REVOKED, "window_revoked",
                            revoke_reason=reason,
                            revoked_by=extra.get("revoked_by") or job_id or "operator",
                            valid_delivery=False)
            # 撤销已交付区间：保留全部结果记录，但从有效索引移除/标记失效。
            for result in self.store._backfill_target_results.get(record["target_table"], []):
                if result.get("record_id") == record["id"]:
                    result["valid"] = False
                    result["status"] = WINDOW_REVOKED
                    result["invalid_reason"] = reason
            self._rebuild_valid_ranges(record["target_table"])
            self._sync_target_row_validity(record["target_table"])
            self.release_window_claim(record)
            return self._ok(window=record, window_id=record["window_id"], state=record["state"])

    cancel_window = revoke_window
    invalidate_window = revoke_window
    revoke = revoke_window

    def _maybe_reissue_revoked(self, job_id: str, window_id: str, **kwargs: Any) -> dict[str, Any] | None:
        old = self.find_window(window_id)
        if old is None or old.get("state") != WINDOW_REVOKED:
            return None
        # 新作业/新一代重新解析最新快照；旧记录和旧快照均保留。
        job = self.register_job(job_id, target_table=old["target_table"],
                                source_tables=old["source_tables"],
                                priority=kwargs.get("priority"))
        pins, issues = self._pin_snapshots(job["source_tables"])
        record_id = f"WIN-{next(_write_seq):06d}"
        rng = copy.deepcopy(old["range"])
        record = copy.deepcopy({
            **{k: v for k, v in old.items() if k not in {"history", "source_reads", "source_records", "attempts"}},
            "id": record_id,
            "record_id": record_id,
            "generation": int(old.get("generation", 1)) + 1,
            "job_id": job_id,
            "owner_job_id": job_id,
            "entry_id": job.get("entry_id"),
            "snapshot_pins": pins,
            "source_snapshots": pins,
            "snapshot_version": next(iter(pins.values()), {}).get("snapshot_version") if pins else None,
            "快照版本": ";".join(sorted({p["snapshot_version"] for p in pins.values()})) or "无快照",
            "actual_written_range": None,
            "actual_written_ranges": [],
            "written_ranges": [],
            "unacknowledged_ranges": [copy.deepcopy(rng)],
            "unconfirmed_ranges": [copy.deepcopy(rng)],
            "pending_ranges": [copy.deepcopy(rng)],
            "state": WINDOW_PENDING,
            "status": WINDOW_PENDING,
            "窗口状态": WINDOW_PENDING,
            "state_code": STATE_CODES[WINDOW_PENDING],
            "status_code": STATE_CODES[WINDOW_PENDING],
            "write_count": 0,
            "rows_written": 0,
            "rewrite_count": 0,
            "source_reads": [],
            "source_records": [],
            "attempts": [],
            "valid_delivery": None,
            "created_at": time.time(),
            "updated_at": time.time(),
            "history": [],
            "reissued_from": old["id"],
            "dependency_issues": issues,
        })
        self._refresh_range_fields(record)
        self.store._backfill_window_records[record_id] = record
        self.store._recovery_manifest.append(record)
        self.store._backfill_window_index.setdefault(record["natural_key"], []).append(record_id)
        self._emit("window_reissued", record, from_record=old["id"], previous_job_id=old.get("job_id"))
        return record

    def reprocess_revoked_window(self, old_job: Any, window_id: str, new_job_id: str, **kwargs: Any) -> dict[str, Any]:
        record = self._maybe_reissue_revoked(str(new_job_id), window_id, **kwargs)
        if record is None:
            return self._fail("只能重办已撤销窗口或窗口不存在")
        return self.execute_window(new_job_id, record["id"], **kwargs)

    # ---------- 实际写入模拟 ----------

    def _physical_write(self, table: str, envelopes: list[dict[str, Any]]) -> None:
        """物理 sink 钩子；测试可 monkeypatch 此方法制造真实异常。"""
        self.store._backfill_target_rows.setdefault(table, []).extend(envelopes)

    def _write_ranges(
        self,
        record: dict[str, Any],
        ranges: list[dict[str, Any]],
        reads: list[dict[str, Any]],
        *,
        rewrite: bool = False,
    ) -> dict[str, Any]:
        rows_by_table = {r["source_table"]: r.get("records", []) for r in reads}
        total_rows = 0
        for rng in ranges:
            envelopes: list[dict[str, Any]] = []
            for table, rows in rows_by_table.items():
                for row in rows:
                    # 用区间过滤一次；无法解析时间的行已在读取阶段保留。
                    if self._row_matches_range(row, rng, record):
                        envelopes.append({
                            "target_table": record["target_table"],
                            "job_id": record["job_id"],
                            "window_id": record["window_id"],
                            "record_id": record["id"],
                            "source_table": table,
                            "snapshot_version": record["snapshot_pins"].get(table, {}).get("snapshot_version"),
                            "row_key": str(row.get("id") or hash(frozenset(row.items()))),
                            "row": copy.deepcopy(row),
                            "range": copy.deepcopy(rng),
                            "written_at": time.time(),
                            "valid": False,  # commit 仲裁后才会置为有效。
                        })
            if not envelopes:
                # 即使该区间在快照中没有记录，也允许推进空区间，避免窗口永久待恢复。
                pass
            self._physical_write(record["target_table"], envelopes)
            audit = {
                "audit_id": f"WRITE-{next(_write_seq):06d}",
                "target_table": record["target_table"],
                "job_id": record["job_id"],
                "window_id": record["window_id"],
                "record_id": record["id"],
                "range": copy.deepcopy(rng),
                "rows": len(envelopes),
                "rewrite": rewrite,
                "ts": time.time(),
            }
            self.store._backfill_write_audit.append(audit)
            record["write_count"] += 1
            record["rows_written"] += len(envelopes)
            if rewrite:
                record["rewrite_count"] += 1
            record.setdefault("written_ranges", []).append(copy.deepcopy(rng))
            # 写入成功并留下检查点后，从未确认集合移除该区间。
            remaining: list[dict[str, Any]] = []
            for unacked in record.get("unacknowledged_ranges", []):
                if range_overlap(unacked, rng):
                    remaining.extend(subtract_range(unacked, rng))
                else:
                    remaining.append(unacked)
            record["unacknowledged_ranges"] = union_ranges(remaining)
            total_rows += len(envelopes)
        self._refresh_range_fields(record)
        return self._ok(rows_written=total_rows, write_count=record["write_count"])

    def write_audit(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        job_id, window_id, _ = self._resolve_job_and_window_args(args, kwargs)
        audits = list(self.store._backfill_write_audit)
        if job_id:
            audits = [a for a in audits if a.get("job_id") == job_id]
        if window_id:
            rec = self.find_window(window_id, job_id=job_id)
            rid = rec["id"] if rec else window_id
            audits = [a for a in audits if a.get("record_id") == rid or a.get("window_id") == window_id]
        return audits

    # ---------- 执行/续跑 ----------

    def _coerce_range_input(self, value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        if isinstance(value, dict) and "lo" in value and "hi" in value:
            return copy.deepcopy(value)
        if isinstance(value, dict):
            return make_range(value.get("start") or value.get("lo"),
                              value.get("end") or value.get("hi"),
                              date_value=value.get("date"))
        if isinstance(value, (tuple, list)) and len(value) >= 2:
            return make_range(value[0], value[1])
        return None

    def interrupt_window(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """显式把窗口置为“待恢复”，可回写已落盘范围和未确认区间。"""
        with self.lock:
            job_id, window_id, extra = self._resolve_job_and_window_args(args, kwargs)
            record = self.find_window(window_id, job_id=job_id)
            if record is None:
                return self._fail(f"窗口 {window_id} 不存在")

            written_input = (
                extra.pop("written_ranges", None)
                or extra.pop("written_range", None)
                or extra.pop("actual_written_ranges", None)
                or extra.pop("actual_written_range", None)
                or kwargs.pop("written_ranges", None)
                or kwargs.pop("written_range", None)
                or kwargs.pop("actual_written_range", None)
            )
            cutoff = (
                extra.pop("written_end", None)
                or extra.pop("partial_end", None)
                or extra.pop("cutoff", None)
                or kwargs.pop("written_end", None)
                or kwargs.pop("partial_end", None)
            )
            if written_input is not None:
                values = written_input if isinstance(written_input, list) else [written_input]
                ranges = [r for item in values if (r := self._coerce_range_input(item)) is not None]
                record["written_ranges"] = union_ranges([*record.get("written_ranges", []), *ranges])
            elif cutoff is not None:
                cut_lo, _, _ = coerce_bound(cutoff)
                full = record["range"]
                prefix = copy.deepcopy(full)
                prefix["hi"] = min(float(full["hi"]), cut_lo)
                if prefix["hi"] > prefix["lo"]:
                    prefix["end"] = display_bound(prefix["hi"], prefix.get("kind", "number"))
                    record.setdefault("written_ranges", []).append(prefix)

            unacked_input = (
                extra.pop("unacknowledged_ranges", None)
                or extra.pop("unconfirmed_ranges", None)
                or extra.pop("pending_ranges", None)
                or kwargs.pop("unacknowledged_ranges", None)
            )
            if unacked_input is not None:
                values = unacked_input if isinstance(unacked_input, list) else [unacked_input]
                record["unacknowledged_ranges"] = union_ranges([
                    r for item in values if (r := self._coerce_range_input(item)) is not None
                ])
            else:
                remaining = [copy.deepcopy(record["range"])]
                for written in record.get("written_ranges", []):
                    next_remaining: list[dict[str, Any]] = []
                    for rng in remaining:
                        next_remaining.extend(subtract_range(rng, written))
                    remaining = next_remaining
                record["unacknowledged_ranges"] = union_ranges(remaining)

            self._refresh_range_fields(record)
            reason = extra.pop("reason", None) or kwargs.pop("reason", None) or "写入中断，等待恢复"
            self._set_state(record, WINDOW_RECOVERING, "window_marked_interrupted",
                            last_error=reason, interrupted=True, recoverable=True)
            return self._ok(window=record, state=record["state"], interrupted=True,
                            written_ranges=record["written_ranges"],
                            unacknowledged_ranges=record["unacknowledged_ranges"])

    mark_window_interrupted = interrupt_window
    simulate_interruption = interrupt_window
    fail_window = interrupt_window

    def execute_window(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        with self.lock:
            job_id, window_id, extra = self._resolve_job_and_window_args(args, kwargs)
            if not job_id and extra.get("job_id"):
                job_id = extra["job_id"]
            record = self.find_window(window_id, job_id=job_id)
            if record is None and job_id and window_id:
                record = self._maybe_reissue_revoked(job_id, str(window_id), **extra)
            if record is None and job_id and window_id and (
                extra.get("start") is not None or extra.get("date") is not None
                or re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(window_id))
            ):
                spec = {
                    "window_id": window_id,
                    "date": extra.get("date", window_id),
                }
                if extra.get("start") is not None:
                    spec["start"] = extra["start"]
                    spec["end"] = extra.get("end")
                self.plan_windows(
                    job_id, [spec],
                    target_table=extra.get("target_table"),
                    source_tables=extra.get("source_tables"),
                    priority=extra.get("priority"),
                    snapshot_version=extra.get("snapshot_version"),
                )
                record = self.find_window(window_id, job_id=job_id)
            if record is None:
                return self._fail(f"窗口 {window_id} 不存在或不属于作业 {job_id}")

            # 已写入窗口绝不重写；恢复调用直接幂等成功。
            if record["state"] == WINDOW_WRITTEN and not extra.get("force"):
                return self._ok(
                    skipped=True, rewritten=False, already_written=True,
                    window=record, state=record["state"],
                    newly_written_ranges=[], rewrite_count=record.get("rewrite_count", 0),
                    snapshot_version=record.get("snapshot_version"),
                )
            if record["state"] == WINDOW_REVOKED:
                return self._fail(
                    "窗口已撤销；请由新作业重新认领/重办",
                    code="window_revoked", window=record,
                )

            # 续跑严禁重新解析 latest；record 中 plan 时的 pin 是唯一基准。
            if not record.get("snapshot_pins"):
                pins, issues = self._pin_snapshots(record["source_tables"])
                record["snapshot_pins"] = pins
                record["source_snapshots"] = pins
                record["dependency_issues"] = issues
                if issues:
                    self._set_state(record, WINDOW_RECOVERING, "window_dependency_failed")
                    return self._fail("; ".join(issues), window=record)

            # 显式认领（若并发测试使用 claim API）；执行本身采用乐观交付仲裁。
            if extra.get("exclusive_claim"):
                claim_result = self.claim_window(job_id, record["id"], **extra)
                if not claim_result.get("ok"):
                    return claim_result

            unacked = union_ranges(record.get("unacknowledged_ranges", [record["range"]]))
            if record["state"] == WINDOW_PENDING:
                self._set_state(record, WINDOW_RECOVERING, "window_execution_started")
            else:
                self._emit("window_resume_started", record)

            # 中断夹具：partial_end/interrupt_after/cutoff 之前的前缀成功写入后崩溃。
            cutoff = extra.get("partial_end", extra.get("interrupt_after", extra.get("cutoff", extra.get("stop_after"))))
            attempt_ranges = copy.deepcopy(unacked)
            interrupted = bool(extra.get("interrupt", False) or cutoff is not None or extra.get("fail_after_rows") is not None)
            interrupt_payload: dict[str, Any] = {}
            if cutoff is not None:
                cut_lo, cut_value, _ = coerce_bound(cutoff)
                limited: list[dict[str, Any]] = []
                for rng in attempt_ranges:
                    if cut_lo <= rng["lo"]:
                        continue
                    piece = copy.deepcopy(rng)
                    piece["hi"] = min(rng["hi"], cut_lo)
                    if piece["hi"] > piece["lo"]:
                        piece["end"] = display_bound(piece["hi"], piece.get("kind", "number"))
                        limited.append(piece)
                attempt_ranges = limited
                interrupt_payload["cutoff"] = cut_value

            try:
                reads = self._read_sources(record, attempt_ranges)
                write_result = self._write_ranges(record, attempt_ranges, reads)
            except Exception as exc:  # 物理 sink 故障也要回写待恢复，而不是让窗口悬空。
                self._refresh_range_fields(record)
                self._set_state(
                    record, WINDOW_RECOVERING, "window_write_failed",
                    last_error=str(exc), error_type=type(exc).__name__, recoverable=True)
                self.release_window_claim(record)
                if extra.get("raise_on_error"):
                    raise
                return self._fail(
                    f"写入失败，窗口已进入待恢复：{exc}",
                    code="write_failed", exception=type(exc).__name__,
                    recoverable=True, window=record, state=record["state"],
                    written_ranges=record["written_ranges"],
                    unacknowledged_ranges=record["unacknowledged_ranges"],
                )

            fail_after_rows = extra.get("fail_after_rows")
            if fail_after_rows is not None and write_result.get("rows_written", 0) >= int(fail_after_rows):
                interrupted = True

            attempt = {
                "attempt_id": f"ATTEMPT-{next(_write_seq):06d}",
                "job_id": job_id,
                "at": time.time(),
                "ranges": copy.deepcopy(attempt_ranges),
                "rows_written": write_result.get("rows_written", 0),
                "interrupted": interrupted,
                "snapshot_version": record.get("snapshot_version"),
            }
            record.setdefault("attempts", []).append(attempt)

            if interrupted:
                self._set_state(record, WINDOW_RECOVERING, "window_write_interrupted",
                                last_error=extra.get("error", "simulated write interruption"),
                                **interrupt_payload)
                self.release_window_claim(record)
                result = self._fail(
                    "写入中断，窗口已进入待恢复",
                    code="write_interrupted", interrupted=True, recoverable=True,
                    window=record, state=record["state"],
                    snapshot_version=record.get("snapshot_version"),
                    written_ranges=record["written_ranges"],
                    unacknowledged_ranges=record["unacknowledged_ranges"],
                    source_reads=reads,
                )
                if extra.get("raise_on_interrupt"):
                    raise BackfillInterrupted(result["reason"])
                return result

            # 全部 unacked 已清空，提交目标表并做重叠仲裁。
            commit = self.commit_window(record=record, priority=extra.get("priority"))
            if not commit.get("ok"):
                return commit
            if commit.get("valid") is False:
                self._set_state(
                    record, WINDOW_REVOKED, "window_conflict_lost",
                    valid_delivery=False, conflict_lost=True,
                    revoke_reason="重叠区间归属更高优先级/在先交付",
                )
                return self._fail(
                    "重叠窗口交付未生效：已有更高优先级/在先属主",
                    code="conflict_lost", conflict=True,
                    window=record, state=record.get("state"), commit=commit,
                    written_ranges=record.get("written_ranges"),
                    source_reads=reads,
                )
            self._set_state(record, WINDOW_WRITTEN, "window_committed",
                            valid_delivery=commit.get("valid_result"))
            self.release_window_claim(record)
            return self._ok(
                skipped=False, rewritten=False, already_written=False,
                window=record, state=record["state"], commit=commit,
                valid_result=commit.get("valid_result"),
                newly_written_ranges=attempt_ranges,
                written_ranges=record["written_ranges"],
                unacknowledged_ranges=record["unacknowledged_ranges"],
                snapshot_version=record.get("snapshot_version"),
                source_reads=reads,
                rows_written=write_result.get("rows_written", 0),
            )

    run_window = execute_window
    process_window = execute_window
    write_window = execute_window

    def resume_window(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        # 续跑不接受“重新解析最新快照”的隐含行为。
        kwargs.pop("use_latest_snapshot", None)
        return self.execute_window(*args, **kwargs)

    continue_window = resume_window
    recover_window = resume_window

    def recover_backfill(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        with self.lock:
            job_id, _, extra = self._resolve_job_and_window_args(args, kwargs)
            if not job_id and kwargs.get("entry_id") is not None:
                info = self.register_job(entry_id=kwargs["entry_id"])
                job_id = info["job_id"]
            if not job_id and args and isinstance(args[0], int):
                info = self.register_job(entry_id=args[0])
                job_id = info["job_id"]

            records = [r for r in self.store._recovery_manifest if r.get("job_id") == job_id]
            if extra.get("target_table"):
                records = [r for r in records if r.get("target_table") == extra["target_table"]]

            results = []
            for rec in records:
                if rec["state"] == WINDOW_WRITTEN:
                    results.append(self._ok(
                        window_id=rec["window_id"], record_id=rec["id"],
                        skipped=True, rewritten=False,
                        state=rec["state"], snapshot_version=rec.get("snapshot_version")))
                elif rec["state"] == WINDOW_REVOKED:
                    results.append(self._fail(
                        "已撤销窗口不由原作业恢复，需另起作业",
                        window_id=rec["window_id"], record_id=rec["id"],
                        code="window_revoked", state=rec["state"]))
                else:
                    result = self.resume_window(job_id, rec["id"])
                    result.setdefault("window_id", rec["window_id"])
                    result.setdefault("record_id", rec["id"])
                    result.setdefault("state", rec.get("state"))
                    results.append(result)

            recovered = sum(1 for r in results if r.get("ok") and not r.get("skipped"))
            skipped = sum(1 for r in results if r.get("skipped"))
            failed = [r for r in results if not r.get("ok")]
            return self._ok(
                job_id=job_id, results=results, recovered=recovered,
                skipped=skipped, failed=failed,
                manifest=self.store.recovery_manifest_for(job_id=job_id),
            )

    resume_backfill = recover_backfill
    continue_backfill = recover_backfill
    resume_job = recover_backfill
    recover_backfill_job = recover_backfill
    resume_backfill_job = recover_backfill
    continue_backfill_job = recover_backfill
    recover = recover_backfill
    resume = recover_backfill

    # ---------- 目标表交付与重叠仲裁 ----------

    def _overlap_with_valid(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        return self.overlap_report(target_table=record["target_table"], window=record).get("overlaps", [])

    def commit_window(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        with self.lock:
            record = kwargs.pop("record", None)
            if record is None:
                job_id, window_id, extra = self._resolve_job_and_window_args(args, kwargs)
                record = self.find_window(window_id, job_id=job_id)
                kwargs.update(extra)
            if record is None:
                return self._fail("提交失败：窗口不存在")

            ranges = kwargs.get("ranges") or record.get("written_ranges") or []
            if not ranges:
                return self._fail("提交失败：没有已写入区间", window=record)

            priority = kwargs.get("priority")
            if priority is None:
                priority = record.get("priority", 100)
            result = {
                "result_id": f"RESULT-{next(_write_seq):06d}",
                "target_table": record["target_table"],
                "job_id": record["job_id"],
                "owner_job_id": record["job_id"],
                "window_id": record["window_id"],
                "record_id": record["id"],
                "generation": record.get("generation", 1),
                "priority": priority,
                "priority_score": priority_score(priority),
                "snapshot_version": record.get("snapshot_version"),
                "ranges": copy.deepcopy(ranges),
                "range": union_ranges(ranges)[0] if len(union_ranges(ranges)) == 1 else {
                    "start": ranges[0].get("start"), "end": ranges[-1].get("end")
                },
                "committed_at": time.time(),
                "valid": True,
                "status": WINDOW_WRITTEN,
            }
            self.store._backfill_target_results.setdefault(record["target_table"], []).append(result)
            self._arbitrate(result)
            self._rebuild_valid_ranges(record["target_table"])
            self._sync_target_row_validity(record["target_table"])
            self._sync_delivery_states(record["target_table"])
            valid_result = self._result_by_id(result["result_id"])
            return self._ok(result=result, valid_result=valid_result,
                            valid=bool(valid_result and valid_result.get("valid")))

    def _sync_target_row_validity(self, target_table: str) -> None:
        """按仲裁后的有效区间回写物理行信封的有效标记。"""
        segments = self.store._backfill_valid_ranges.get(target_table, [])
        for row in self.store._backfill_target_rows.get(target_table, []):
            row_range = row.get("range")
            owned = [seg["range"] for seg in segments if seg.get("record_id") == row.get("record_id")]
            if not row_range or not owned:
                row["valid"] = False
                continue

            covered = 0.0
            for seg in owned:
                overlap = range_overlap(row_range, seg)
                if overlap:
                    covered += max(0.0, float(overlap["hi"]) - float(overlap["lo"]))
            total = max(1e-9, float(row_range["hi"]) - float(row_range["lo"]))
            row["valid"] = covered + 1e-9 >= total
            if not row["valid"] and covered > 0:
                row["partially_valid"] = True
                row["covered_length"] = covered

    def _sync_delivery_states(self, target_table: str) -> None:
        """目标结果被高优先级作业完全取代时，把对应窗口回写为已撤销。"""
        for result in self.store._backfill_target_results.get(target_table, []):
            rec = self.store._backfill_window_records.get(result.get("record_id"))
            if rec is None:
                continue
            if not result.get("valid") and rec.get("state") == WINDOW_WRITTEN:
                self._set_state(
                    rec, WINDOW_REVOKED, "window_delivery_rejected",
                    valid_delivery=False,
                    revoke_reason=result.get("invalid_reason") or "overlap conflict lost",
                    conflict_lost=True,
                )

    deliver_window = commit_window
    complete_window = commit_window
    acknowledge_window = commit_window
    ack_window = commit_window

    def _result_by_id(self, result_id: str) -> dict[str, Any] | None:
        for results in self.store._backfill_target_results.values():
            for result in results:
                if result.get("result_id") == result_id:
                    return result
        return None

    def _priority_wins(self, incoming: dict[str, Any], existing: dict[str, Any]) -> bool:
        # 如果调用方显式给了 rank（数字越小越优先），优先尊重该字段。
        if "priority_rank" in incoming or "priority_rank" in existing:
            ir = incoming.get("priority_rank", 10**9)
            er = existing.get("priority_rank", 10**9)
            if ir != er:
                return float(ir) < float(er)
        if self.priority_policy == "lower_number":
            return float(incoming.get("priority_score", 0)) < float(existing.get("priority_score", 0))
        return float(incoming.get("priority_score", 0)) > float(existing.get("priority_score", 0))

    def _arbitrate(self, incoming: dict[str, Any]) -> None:
        """把 incoming 与既有结果的重叠段拆开，保证每个坐标只有一个有效属主。"""
        table = incoming["target_table"]
        for existing in self.store._backfill_target_results.get(table, []):
            if existing is incoming or not existing.get("valid", True):
                continue
            if existing.get("record_id") == incoming.get("record_id"):
                continue

            new_pieces: list[dict[str, Any]] = []
            for ir in incoming.get("ranges", []):
                overlap = None
                for er in existing.get("ranges", []):
                    overlap = range_overlap(ir, er)
                    if overlap:
                        break
                if not overlap:
                    new_pieces.append(copy.deepcopy(ir))
                    continue

                if self._priority_wins(incoming, existing):
                    # 新结果赢重叠：旧结果区间被切走并标记失效。
                    existing["ranges"] = self._remove_overlap_from_ranges(existing["ranges"], overlap)
                    existing["overridden_by"] = incoming["job_id"]
                    if not existing["ranges"]:
                        existing["valid"] = False
                        existing["status"] = WINDOW_REVOKED
                        existing["invalid_reason"] = f"overridden by higher priority job {incoming['job_id']}"
                    new_pieces.append(copy.deepcopy(ir))
                    incoming.setdefault("won_overlaps", []).append({
                        "range": overlap, "loser_job_id": existing["job_id"],
                        "priority": existing.get("priority"),
                    })
                else:
                    # 旧结果赢重叠：新结果的重叠部分无效，只保留不重叠片段。
                    incoming.setdefault("lost_overlaps", []).append({
                        "range": overlap, "winner_job_id": existing["job_id"],
                        "winner_priority": existing.get("priority"),
                    })
                    new_pieces.extend(subtract_range(ir, overlap))
            incoming["ranges"] = union_ranges(new_pieces)
        incoming["ranges"] = union_ranges(incoming.get("ranges", []))
        if not incoming["ranges"]:
            incoming["valid"] = False
            incoming["status"] = WINDOW_REVOKED
            incoming["invalid_reason"] = "all delivered ranges conflicted with an equal/higher priority owner"

    @staticmethod
    def _remove_overlap_from_ranges(ranges: list[dict[str, Any]], overlap: dict[str, Any]) -> list[dict[str, Any]]:
        pieces: list[dict[str, Any]] = []
        for rng in ranges:
            ov = range_overlap(rng, overlap)
            if ov:
                pieces.extend(subtract_range(rng, ov))
            else:
                pieces.append(rng)
        return union_ranges(pieces)

    def _rebuild_valid_ranges(self, table: str) -> None:
        segments: list[dict[str, Any]] = []
        for result in self.store._backfill_target_results.get(table, []):
            if not result.get("valid"):
                continue
            for rng in result.get("ranges", []):
                segments.append({
                    "target_table": table,
                    "range": copy.deepcopy(rng),
                    "start": rng.get("start"),
                    "end": rng.get("end"),
                    "lo": rng.get("lo"),
                    "hi": rng.get("hi"),
                    "kind": rng.get("kind"),
                    "owner_job_id": result["job_id"],
                    "window_id": result["window_id"],
                    "record_id": result["record_id"],
                    "result_id": result["result_id"],
                    "priority": result.get("priority"),
                    "priority_score": result.get("priority_score"),
                    "snapshot_version": result.get("snapshot_version"),
                    "valid": True,
                })
        segments.sort(key=lambda s: (float(s["lo"]), float(s["hi"])))
        self.store._backfill_valid_ranges[table] = segments

    def commit_result(self, target_table: str, owner_job_id: str, ranges: list[dict[str, Any]] | dict[str, Any],
                      priority: Any = 100, **kwargs: Any) -> dict[str, Any]:
        if isinstance(ranges, dict) and "lo" in ranges:
            range_list = [ranges]
        elif isinstance(ranges, dict):
            range_list = [make_range(ranges.get("start"), ranges.get("end"),
                                     date_value=ranges.get("date"))]
        else:
            range_list = []
            for r in ranges:
                if "lo" in r:
                    range_list.append(r)
                else:
                    range_list.append(make_range(r[0], r[1]) if isinstance(r, (tuple, list)) else make_range(r.get("start"), r.get("end")))
        pseudo_record = {
            "target_table": target_table, "job_id": owner_job_id,
            "window_id": kwargs.get("window_id", owner_job_id),
            "id": kwargs.get("record_id", owner_job_id),
            "generation": 1, "snapshot_version": kwargs.get("snapshot_version"),
        }
        return self.commit_window(record=pseudo_record, ranges=range_list, priority=priority)

    deliver_result = commit_result
    register_result = commit_result

    # ---------- 重叠核对/目标表验证 ----------

    def overlap_report(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        target_table = kwargs.get("target_table")
        window_a = kwargs.get("window_a") or kwargs.get("a")
        window_b = kwargs.get("window_b") or kwargs.get("b")

        def pseudo_window(range_value: Any, owner: Any = None, priority: Any = None) -> dict[str, Any] | None:
            if range_value is None:
                return None
            if isinstance(range_value, dict) and ("written_ranges" in range_value or "range" in range_value and "job_id" in range_value):
                return range_value
            coerced = self._coerce_range_input(range_value)
            if coerced is None:
                return None
            return {"job_id": owner, "window_id": owner, "priority": priority,
                    "written_ranges": [coerced]}

        raw_a = kwargs.get("range_a") or kwargs.get("range1") or kwargs.get("ranges_a")
        raw_b = kwargs.get("range_b") or kwargs.get("range2") or kwargs.get("ranges_b")
        if window_a is None and raw_a is not None:
            window_a = pseudo_window(raw_a, kwargs.get("owner_a") or kwargs.get("job_a"), kwargs.get("priority_a"))
        if window_b is None and raw_b is not None:
            window_b = pseudo_window(raw_b, kwargs.get("owner_b") or kwargs.get("job_b"), kwargs.get("priority_b"))

        # 支持 overlap_report(job_a, window_a, job_b, window_b) 的简化位置形式。
        pos = list(args)
        if len(pos) >= 4 and not window_a:
            ja, wa, jb, wb = pos[:4]
            window_a = self.find_window(wa, job_id=ja) or pseudo_window(wa, ja, kwargs.get("priority_a"))
            window_b = self.find_window(wb, job_id=jb) or pseudo_window(wb, jb, kwargs.get("priority_b"))
        elif len(pos) >= 3 and isinstance(pos[0], str) and isinstance(pos[1], (dict, tuple, list)):
            target_table = target_table or pos[0]
            window_a = pseudo_window(pos[1], kwargs.get("owner_a"), kwargs.get("priority_a"))
            window_b = pseudo_window(pos[2], kwargs.get("owner_b"), kwargs.get("priority_b"))
        elif len(pos) >= 2 and window_a is None:
            window_a = self.find_window(pos[0]) or pseudo_window(pos[0])
            window_b = self.find_window(pos[1]) or pseudo_window(pos[1])

        if isinstance(window_a, str):
            window_a = self.find_window(window_a)
        if isinstance(window_b, str):
            window_b = self.find_window(window_b)

        if window_a is not None and not target_table:
            target_table = window_a.get("target_table")

        ranges_a = (window_a or {}).get("written_ranges") or (window_a or {}).get("unacknowledged_ranges") or []
        ranges_b = (window_b or {}).get("written_ranges") or (window_b or {}).get("unacknowledged_ranges") or []
        pair_overlaps: list[dict[str, Any]] = []
        for ra in ranges_a:
            for rb in ranges_b:
                ov = range_overlap(ra, rb)
                if ov:
                    a_priority = (window_a or {}).get("priority")
                    b_priority = (window_b or {}).get("priority")
                    a_wins = priority_score(a_priority) >= priority_score(b_priority)
                    pair_overlaps.append({
                        "range": ov,
                        "window_a": (window_a or {}).get("window_id"),
                        "window_b": (window_b or {}).get("window_id"),
                        "owner_a": (window_a or {}).get("job_id"),
                        "owner_b": (window_b or {}).get("job_id"),
                        "priority_a": a_priority,
                        "priority_b": b_priority,
                        "winner_job_id": (window_a or {}).get("job_id") if a_wins else (window_b or {}).get("job_id"),
                    })

        valid_segments = self.store._backfill_valid_ranges.get(str(target_table), []) if target_table else []
        valid_overlaps: list[dict[str, Any]] = []
        # 有效区间由仲裁器切分，理论上不会重叠；这里仍提供核对结果。
        for i, sa in enumerate(valid_segments):
            for sb in valid_segments[i + 1:]:
                ov = range_overlap(sa["range"], sb["range"])
                if ov and sa.get("owner_job_id") != sb.get("owner_job_id"):
                    valid_overlaps.append({"range": ov, "owners": [sa.get("owner_job_id"), sb.get("owner_job_id")]})

        ownership: list[dict[str, Any]] = []
        if window_a is not None:
            ownership.extend(self._ownership_for_window(window_a, valid_segments))
        if window_b is not None:
            ownership.extend(self._ownership_for_window(window_b, valid_segments))

        return self._ok(
            target_table=target_table,
            pair_overlaps=pair_overlaps,
            overlaps=pair_overlaps or [
                s for s in valid_segments
                if window_a is None or s.get("record_id") == window_a.get("id")
            ],
            valid_overlaps=valid_overlaps,
            ownership=ownership,
            unique_valid_owner=not valid_overlaps,
            valid_ranges=valid_segments,
        )

    check_overlap = overlap_report
    verify_overlap = overlap_report
    resolve_overlap = overlap_report
    check_overlap_ownership = overlap_report

    def _ownership_for_window(self, window: dict[str, Any], segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result = []
        for seg in segments:
            if seg.get("record_id") != window.get("id"):
                # 也报告与该窗口范围相交的其他属主。
                for rng in window.get("written_ranges", []):
                    ov = range_overlap(rng, seg["range"])
                    if ov:
                        result.append({"overlap": ov, **seg})
                        break
                continue
            result.append({"overlap": seg["range"], **seg})
        return result

    def verify_delivery(self, target_table: str) -> dict[str, Any]:
        self._rebuild_valid_ranges(target_table)
        segments = self.store._backfill_valid_ranges.get(target_table, [])
        conflicts = []
        for i, a in enumerate(segments):
            for b in segments[i + 1:]:
                ov = range_overlap(a["range"], b["range"])
                if ov and a["owner_job_id"] != b["owner_job_id"]:
                    conflicts.append({"range": ov, "owners": [a["owner_job_id"], b["owner_job_id"]]})
        rows = self.store._backfill_target_rows.get(target_table, [])
        return self._ok(
            target_table=target_table,
            ok=not conflicts,
            valid=not conflicts,
            conflicts=conflicts,
            valid_ranges=segments,
            valid_results=[r for r in self.store._backfill_target_results.get(target_table, []) if r.get("valid")],
            all_results=self.store._backfill_target_results.get(target_table, []),
            target_rows=rows,
            valid_row_count=sum(1 for row in rows if row.get("valid")),
        )

    validate_delivery = verify_delivery
    delivery_report = verify_delivery

    def valid_results(self, target_table: str | None = None) -> list[dict[str, Any]]:
        if target_table:
            return [r for r in self.store._backfill_target_results.get(target_table, []) if r.get("valid")]
        return [r for rs in self.store._backfill_target_results.values() for r in rs if r.get("valid")]

    # ---------- 命名兼容别名 ----------
    create_backfill_windows = plan_windows
    create_windows = plan_windows
    build_windows = plan_windows
    generate_windows = plan_windows
    create_recovery_manifest = plan_windows
    register_recovery_windows = plan_windows
    record_window = add_record
    execute_with_recovery = execute_window
    run_with_recovery = execute_window
    backfill_window = execute_window
    process_time_window = execute_window
    get_manifest = get_recovery_manifest
    recover_backfill_job = recover_backfill
    resume_backfill_job = recover_backfill
    continue_backfill_job = recover_backfill
    revoke_backfill_window = revoke_window
    cancel_backfill_window = revoke_window
    acquire_window_lock = claim_window
    lock_window = claim_window
    commit_window_result = commit_window
    deliver_result_to_target = commit_window
    find_overlaps = overlap_report
    check_conflicts = overlap_report
    resolve_overlap_conflict = overlap_report
    get_overlap_ownership = overlap_report
    validate_target_delivery = verify_delivery

    def reset(self) -> dict[str, Any]:
        self.store.reset_backfill_recovery()
        return self._ok(reset=True)


recovery_engine = BackfillRecoveryEngine(default_store)
recovery_service = recovery_engine

# 与 store 中清单是同一个 list；reset 使用 clear()，引用不会失效。
RECOVERY_MANIFEST = default_store._recovery_manifest
RECOVERY_EVENTS = default_store._recovery_events


class WindowState:
    """窗口四态的显式常量集合。"""

    PENDING = WINDOW_PENDING
    RECOVERING = WINDOW_RECOVERING
    WRITTEN = WINDOW_WRITTEN
    REVOKED = WINDOW_REVOKED
    ALL = WINDOW_STATES


# 语义别名：恢复清单本身也是一个可查询/流转的领域引擎。
RecoveryManifest = BackfillRecoveryEngine
RecoveryManager = BackfillRecoveryEngine
BackfillRecoveryManager = BackfillRecoveryEngine
BackfillRecoveryService = BackfillRecoveryEngine
WindowRecoveryManager = BackfillRecoveryEngine
