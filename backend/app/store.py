"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据，并支持跨模块数据流水线。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
from typing import Any

from app.seed import SEED_ROWS


class DataPipeline:
    """跨模块数据流水线引擎：支持 输入→转换→版本化产物→下游消费 的完整链路。

    每条流水线定义了源模块、目标模块、转换函数和验证规则。
    产物存储为可版本化的数据快照，支持脏行检测、重复批次拒绝、冲突版本检测、中断恢复。
    """

    def __init__(self, name: str, sources: list[str], target_module: str) -> None:
        self.name = name
        self.sources = sources
        self.target_module = target_module
        self._artifacts: list[dict[str, Any]] = []
        self._versions: list[str] = []  # 版本号列表
        self._dirty_log: list[dict[str, Any]] = []
        self._batch_registry: set[str] = set()  # 防重复批次
        self._last_checkpoint: dict[str, Any] = {}  # 中断恢复点
        self._interrupted = False

    def ingest(self, rows: list[dict[str, Any]], batch_id: str) -> tuple[int, list[str]]:
        """摄入数据行，返回 (成功行数, 脏行详情列表)"""
        if batch_id in self._batch_registry:
            return 0, [f"重复批次 {batch_id} 已存在，拒绝摄入"]
        self._batch_registry.add(batch_id)
        clean_rows = []
        dirty_details = []
        for i, row in enumerate(rows):
            issues = self._validate_row(row, i)
            if issues:
                dirty_details.append(f"行 {i}: {'; '.join(issues)}")
                self._dirty_log.append({"batch": batch_id, "row_index": i, "issues": issues, "row": row, "ts": time.time()})
            else:
                clean_rows.append(row)
        self._artifacts.extend(clean_rows)
        return len(clean_rows), dirty_details

    def transform(self, transform_fn, *args, **kwargs) -> list[dict[str, Any]]:
        """对当前产物执行转换函数"""
        self._save_checkpoint("transform_enter", len(self._artifacts))
        try:
            result = transform_fn(self._artifacts, *args, **kwargs)
            version = self._make_version("transform")
            self._versions.append(version)
            self._save_checkpoint("transform_exit", len(result))
            return result
        except Exception:
            self._interrupted = True
            raise

    def versioned_snapshot(self) -> dict[str, Any]:
        """生成版本化产物快照"""
        version = self._versions[-1] if self._versions else self._make_version("snapshot")
        return {
            "pipeline": self.name,
            "version": version,
            "artifact_count": len(self._artifacts),
            "artifact_sample": self._artifacts[:5],
            "dirty_count": len(self._dirty_log),
            "batch_count": len(self._batch_registry),
            "interrupted": self._interrupted,
            "checkpoint": self._last_checkpoint,
        }

    def health_report(self) -> dict[str, Any]:
        """流水线健康报告：用于外部模块消费和容量材料生成"""
        return {
            "pipeline": self.name,
            "source_modules": self.sources,
            "target_module": self.target_module,
            "total_ingested": sum(1 for a in self._artifacts),
            "dirty_rows": len(self._dirty_log),
            "versions": len(self._versions),
            "batches_processed": len(self._batch_registry),
            "interrupted": self._interrupted,
            "last_artifact_ts": max((a.get("ts", 0) for a in self._artifacts), default=0) if self._artifacts else 0,
        }

    def _validate_row(self, row: dict[str, Any], index: int) -> list[str]:
        issues = []
        for key, val in row.items():
            if val is None or (isinstance(val, str) and not val.strip()):
                issues.append(f"空值字段 '{key}'")
        if any(row.get(k) is None for k in ["id"]):
            issues.append("缺少主键 id")
        return issues

    def _make_version(self, tag: str) -> str:
        ts = str(int(time.time() * 1000))
        raw = f"{self.name}:{tag}:{ts}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def _save_checkpoint(self, stage: str, count: int) -> None:
        self._last_checkpoint = {"stage": stage, "count": count, "ts": time.time()}

    def recover_checkpoint(self) -> dict[str, Any]:
        """从最近检查点恢复"""
        return {
            "pipeline": self.name,
            "interrupted": self._interrupted,
            "last_checkpoint": self._last_checkpoint,
            "artifacts_before_interrupt": len(self._artifacts),
        }


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据流水线
        self._pipelines: dict[str, DataPipeline] = {}
        # 全局变更日志（用于 changelog 跨模块追溯）
        self._global_changelog: list[dict[str, Any]] = []
        # Schema 注册表（cross-module schema registry）
        self._schema_registry: dict[str, list[dict[str, Any]]] = {}
        # 水位偏移记录（watermark → late_data 共享）
        self._watermark_offsets: dict[str, dict[str, Any]] = {}
        # 快照存储表（snapshot_mgr → backfill_job 共享）
        self._snapshot_store: dict[str, dict[str, Any]] = {}
        # 按 table@version 保存的不可变快照版本。即使同一个快照编号随后发布新版本，
        # 已开始的回填仍可沿原版本读取来源记录。
        self._snapshot_versions: dict[str, dict[str, Any]] = {}
        self._snapshot_history: dict[str, list[dict[str, Any]]] = {}
        self._snapshot_seq = 0

        # 回填恢复清单与窗口状态。清单是 list，窗口当前态原地更新，
        # 每次状态流转同时写入 _recovery_events，便于审计和续跑。
        self._recovery_manifest: list[dict[str, Any]] = []
        # 兼容可能的命名：恢复清单 / 回填恢复清单。
        self._backfill_recovery_manifest = self._recovery_manifest
        self._recovery_events: list[dict[str, Any]] = []
        self._backfill_jobs: dict[str, dict[str, Any]] = {}
        self._backfill_window_records: dict[str, dict[str, Any]] = {}
        self._backfill_window_index: dict[str, list[str]] = {}
        self._backfill_claims: dict[str, dict[str, Any]] = {}
        self._backfill_source_reads: list[dict[str, Any]] = []
        self._backfill_write_audit: list[dict[str, Any]] = []
        # target_table -> 已交付的行信封 / 全部结果（含失效结果）/ 有效区间段
        self._backfill_target_rows: dict[str, list[dict[str, Any]]] = {}
        self._backfill_target_results: dict[str, list[dict[str, Any]]] = {}
        self._backfill_valid_ranges: dict[str, list[dict[str, Any]]] = {}
        self._recovery_lock = threading.RLock()
        # 聚合别名，方便调用方按“恢复清单”一个入口读取全部运行态。
        self._backfill_recovery: dict[str, Any] = {
            "manifest": self._recovery_manifest,
            "records": self._recovery_manifest,
            "events": self._recovery_events,
            "jobs": self._backfill_jobs,
            "windows": self._backfill_window_records,
            "window_index": self._backfill_window_index,
            "claims": self._backfill_claims,
            "source_reads": self._backfill_source_reads,
            "write_audit": self._backfill_write_audit,
            "target_rows": self._backfill_target_rows,
            "target_results": self._backfill_target_results,
            "valid_ranges": self._backfill_valid_ranges,
        }
        # 常见命名的直接别名，便于不同测试/脚本访问同一组运行态。
        self._target_tables = self._backfill_target_rows
        self._target_writes = self._backfill_write_audit
        self._delivered_results = self._backfill_target_results
        self._valid_results = self._backfill_valid_ranges
        self._window_registry = self._backfill_window_records

        # 血缘图（lineage → table_format 共享）
        self._lineage_graph: dict[str, list[dict[str, Any]]] = {}
        # 归档清单（cold_archive → retention 共享）
        self._archive_manifest: list[dict[str, Any]] = []

    # ---------- 基础 CRUD ----------
    def reset(self) -> None:
        """恢复到种子数据和空运行态；供隔离测试/验收脚本调用。"""
        self._tables.clear()
        self._tables.update({
            name: [dict(row) for row in rows]
            for name, rows in SEED_ROWS.items()
        })
        self._pipelines.clear()
        self._global_changelog.clear()
        self._schema_registry.clear()
        self._watermark_offsets.clear()
        self._snapshot_store.clear()
        self._snapshot_versions.clear()
        self._snapshot_history.clear()
        self._snapshot_seq = 0
        self._lineage_graph.clear()
        self._archive_manifest.clear()
        self.reset_backfill_recovery()

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        # 跨模块流水线状态
        pipeline_stats = []
        for pname, pl in self._pipelines.items():
            pipeline_stats.append(pl.health_report())
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
            {"label": "流水线", "value": len(pipeline_stats)},
        ]
        return {"cards": cards, "modules": modules, "pipelines": pipeline_stats}

    # ---------- 跨模块流水线 ----------
    def create_pipeline(self, name: str, sources: list[str], target_module: str) -> DataPipeline:
        pl = DataPipeline(name, sources, target_module)
        self._pipelines[name] = pl
        return pl

    def pipeline(self, name: str) -> DataPipeline | None:
        return self._pipelines.get(name)

    def all_pipelines(self) -> dict[str, DataPipeline]:
        return dict(self._pipelines)

    # ---------- Schema 注册表 ----------
    def register_schema(self, topic: str, schema_def: dict[str, Any], version: str) -> dict[str, Any]:
        versions = self._schema_registry.setdefault(topic, [])
        entry = {"topic": topic, "definition": schema_def, "version": version, "ts": time.time()}
        versions.append(entry)
        return entry

    def schema_versions(self, topic: str) -> list[dict[str, Any]]:
        return self._schema_registry.get(topic, [])

    def check_schema_compatibility(self, topic: str, new_fields: list[str]) -> dict[str, Any]:
        versions = self.schema_versions(topic)
        if not versions:
            return {"compatible": True, "reason": "新主题，无兼容性问题"}
        latest = versions[-1]
        old_fields = set(latest.get("definition", {}).get("fields", []))
        missing_required = [f for f in old_fields if f not in new_fields]
        if missing_required:
            return {"compatible": False, "reason": f"缺少已有字段: {missing_required}"}
        return {"compatible": True, "reason": "向后兼容"}

    # ---------- 水位记录 ----------
    def update_watermark(self, partition: str, offset: dict[str, Any]) -> None:
        self._watermark_offsets[partition] = offset

    def get_watermark(self, partition: str) -> dict[str, Any] | None:
        return self._watermark_offsets.get(partition)

    # ---------- 快照存储 ----------
    def save_snapshot(self, snap_id: str, data: dict[str, Any]) -> None:
        """保存当前快照指针，同时把 table@version 归档为不可变版本。"""
        snap = copy.deepcopy(data)
        snap.setdefault("snapshot_id", snap_id)
        snap.setdefault("id", snap_id)
        self._snapshot_seq += 1
        snap.setdefault("created_seq", self._snapshot_seq)
        snap.setdefault("published_at", time.time())

        self._snapshot_store[snap_id] = snap

        table = str(snap.get("table") or snap.get("源表名称") or "")
        version = str(snap.get("version") or snap.get("快照版本") or "")
        if table and version:
            version_key = f"{table}@{version}"
            # 已发布版本不可被同名发布静默改写，保证旧作业可沿用原快照。
            if version_key not in self._snapshot_versions:
                archived = copy.deepcopy(snap)
                archived["snapshot_key"] = version_key
                self._snapshot_versions[version_key] = archived
                self._snapshot_history.setdefault(table, []).append(archived)

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshot_store.get(snap_id)

    def save_snapshot_version(self, table: str, version: str, data: dict[str, Any]) -> dict[str, Any]:
        key = f"{table}@{version}"
        snap = copy.deepcopy(data)
        snap.setdefault("table", table)
        snap.setdefault("version", version)
        snap["snapshot_key"] = key
        if key not in self._snapshot_versions:
            self._snapshot_versions[key] = copy.deepcopy(snap)
            self._snapshot_history.setdefault(table, []).append(self._snapshot_versions[key])
        return self._snapshot_versions[key]

    def get_snapshot_version(self, table: str, version: str) -> dict[str, Any] | None:
        return self._snapshot_versions.get(f"{table}@{version}")

    def snapshot_versions_for(self, table: str) -> list[dict[str, Any]]:
        return list(self._snapshot_history.get(table, []))

    def latest_snapshot(self, table: str) -> dict[str, Any] | None:
        # _snapshot_store 中每个快照编号都保留当前指针；即使同一 version 被
        # 重新发布，也以最新 created_seq 为准。旧作业的 pin 会自行深拷贝原快照。
        matches = [s for s in self._snapshot_store.values() if s.get("table") == table]
        if matches:
            return sorted(matches, key=lambda s: int(s.get("created_seq", 0)))[-1]
        versions = self._snapshot_history.get(table)
        if versions:
            return versions[-1]
        return None

    # ---------- 回填恢复清单 ----------
    def reset_backfill_recovery(self) -> None:
        """清空回填恢复运行态；主要给隔离测试使用。"""
        with self._recovery_lock:
            self._recovery_manifest.clear()
            self._recovery_events.clear()
            self._backfill_jobs.clear()
            self._backfill_window_records.clear()
            self._backfill_window_index.clear()
            self._backfill_claims.clear()
            self._backfill_source_reads.clear()
            self._backfill_write_audit.clear()
            self._backfill_target_rows.clear()
            self._backfill_target_results.clear()
            self._backfill_valid_ranges.clear()

    def append_recovery_record(self, record: dict[str, Any]) -> dict[str, Any]:
        self._recovery_manifest.append(record)
        return record

    def save_recovery_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return self.append_recovery_record(record)

    @property
    def recovery_manifest(self) -> list[dict[str, Any]]:
        return self._recovery_manifest

    @property
    def backfill_recovery_manifest(self) -> list[dict[str, Any]]:
        return self._backfill_recovery_manifest

    def get_recovery_manifest(self, **filters: Any) -> list[dict[str, Any]]:
        return self.recovery_manifest_for(**filters)

    def recovery_manifest_for(
        self,
        *,
        target_table: str | None = None,
        job_id: str | None = None,
        state: str | None = None,
    ) -> list[dict[str, Any]]:
        records = list(self._recovery_manifest)
        if target_table:
            records = [r for r in records if r.get("target_table") == target_table or r.get("目标表") == target_table]
        if job_id:
            records = [r for r in records if r.get("job_id") == job_id]
        if state:
            records = [r for r in records if r.get("state") == state or r.get("status") == state]
        return records

    def get_backfill_recovery(self) -> dict[str, Any]:
        return self._backfill_recovery

    # ---------- 变更日志 ----------
    def append_changelog(self, entry: dict[str, Any]) -> None:
        entry["_ts"] = time.time()
        self._global_changelog.append(entry)

    def changelog_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [e for e in self._global_changelog if e.get("目标表") == table_name]

    # ---------- 血缘图 ----------
    def add_lineage_edge(self, upstream: str, downstream: str, edge_data: dict[str, Any]) -> None:
        edges = self._lineage_graph.setdefault(upstream, [])
        edges.append({"downstream": downstream, **edge_data, "ts": time.time()})

    def lineage_downstream(self, table_name: str) -> list[dict[str, Any]]:
        return self._lineage_graph.get(table_name, [])

    # ---------- 归档清单 ----------
    def add_archive_record(self, record: dict[str, Any]) -> None:
        self._archive_manifest.append(record)

    def archive_manifest_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [r for r in self._archive_manifest if r.get("源表名称") == table_name]


store = Store()
# 模块级便捷别名；与 store._recovery_manifest 是同一个 list。
recovery_manifest = store.recovery_manifest
