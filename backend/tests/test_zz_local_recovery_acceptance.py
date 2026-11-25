"""回填恢复链路验收：三天数据、中断续跑、撤销重办、快照钉版与重叠仲裁。"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta

import pytest

from app.services.backfill_job import BackfillJobService
from app.services.snapshot_mgr import SnapshotMgrService
from app.store import store


@pytest.fixture
def recovery_context():
    store.reset_backfill_recovery()
    suffix = "acceptance"
    target = f"recovery_target_{suffix}"
    source = f"recovery_source_{suffix}"
    dates = ["2026-09-01", "2026-09-02", "2026-09-03"]
    base = datetime(2026, 9, 1)
    rows_v1 = []
    row_id = 0
    for day in range(3):
        for hour in (0, 6, 12, 18):
            row_id += 1
            rows_v1.append({
                "id": row_id,
                "event_time": (base + timedelta(days=day, hours=hour)).isoformat(),
                "day": dates[day],
                "value": f"v1-{day + 1}-{hour}",
            })

    snapshots = SnapshotMgrService()
    publish_v1 = snapshots.publish_snapshot(
        source, "v1", rows_v1, snapshot_id="SNAP-RECOVERY-V1")
    assert publish_v1["ok"]

    jobs = BackfillJobService()
    plan = jobs.plan_windows(
        "job-a", dates=dates, target_table=target,
        source_tables=[source], priority=10)
    assert plan["ok"]

    yield {
        "jobs": jobs,
        "snapshots": snapshots,
        "target": target,
        "source": source,
        "dates": dates,
        "rows_v1": rows_v1,
    }

    store.reset_backfill_recovery()


def test_three_day_interrupt_revoke_new_snapshot_and_recover(recovery_context):
    ctx = recovery_context
    jobs: BackfillJobService = ctx["jobs"]
    snapshots: SnapshotMgrService = ctx["snapshots"]
    target = ctx["target"]
    source = ctx["source"]
    d1, d2, d3 = ctx["dates"]

    day1 = jobs.execute_window("job-a", d1)
    assert day1["ok"] is True
    assert day1["state"] == "已写入"
    assert day1["snapshot_version"] == "v1"

    interrupted = jobs.execute_window(
        "job-a", d2, partial_end="2026-09-02T12:00:00")
    assert interrupted["ok"] is False
    assert interrupted["code"] == "write_interrupted"
    assert interrupted["state"] == "待恢复"
    assert len(interrupted["written_ranges"]) == 1
    assert len(interrupted["unacknowledged_ranges"]) == 1

    revoked = jobs.revoke_window("job-a", d3, reason="source late")
    assert revoked["ok"] is True
    assert revoked["window"]["state"] == "已撤销"

    rows_v2 = [dict(row, value=row["value"] + "-updated") for row in ctx["rows_v1"]]
    published = snapshots.publish_snapshot(
        source, "v2", rows_v2, snapshot_id="SNAP-RECOVERY-V2")
    assert published["ok"] is True

    recovered = jobs.recover_backfill("job-a")
    assert recovered["ok"] is True
    by_day = {item.get("window_id"): item for item in recovered["results"]}

    assert by_day[d1]["skipped"] is True
    assert by_day[d1]["state"] == "已写入"

    assert by_day[d2]["ok"] is True
    assert by_day[d2]["state"] == "已写入"
    assert by_day[d2]["snapshot_version"] == "v1"

    assert by_day[d3]["ok"] is False
    assert by_day[d3]["code"] == "window_revoked"

    d1_audits = jobs.write_audit("job-a", d1)
    d2_audits = jobs.write_audit("job-a", d2)
    assert len(d1_audits) == 1
    assert d1_audits[0]["rewrite"] is False
    assert len(d2_audits) == 2
    assert sum(audit["rows"] for audit in d2_audits) == 8

    # 撤销窗口允许另一个作业重办，并使用新发布的 v2。
    plan_b = jobs.plan_windows(
        "job-b", dates=[d3], target_table=target,
        source_tables=[source], priority=20)
    assert plan_b["ok"]
    day3_b = jobs.execute_window("job-b", d3)
    assert day3_b["ok"] is True
    assert day3_b["state"] == "已写入"
    assert day3_b["snapshot_version"] == "v2"

    source_a = jobs.read_source_records("job-a")
    source_b = jobs.read_source_records("job-b")
    assert source_a["total"] > 0
    assert source_b["total"] > 0
    assert any(receipt["snapshot_version"] == "v1" for receipt in source_a["records"])
    assert any(receipt["snapshot_version"] == "v2" for receipt in source_b["records"])
    assert source_a["rows"] and source_b["rows"]

    delivery = jobs.verify_delivery(target)
    assert delivery["valid"] is True
    owners = [(segment["window_id"], segment["owner_job_id"])
              for segment in delivery["valid_ranges"]]
    assert (d1, "job-a") in owners
    assert (d2, "job-a") in owners
    assert (d3, "job-b") in owners

    manifest = jobs.get_recovery_manifest(target_table=target)
    assert manifest["total"] >= 4  # A 的三个窗口 + B 重办的新一代窗口
    states = {record["state"] for record in manifest["records"]}
    assert "已写入" in states
    assert "已撤销" in states


def test_same_window_concurrent_jobs_have_one_valid_delivery():
    store.reset_backfill_recovery()
    target = "concurrent_target"
    source = "concurrent_source"
    snapshots = SnapshotMgrService()
    snapshots.publish_snapshot(
        source, "cv1", [{"id": 1, "ts": 1}, {"id": 2, "ts": 2}],
        snapshot_id="SNAP-CONCURRENT")
    jobs = BackfillJobService()
    window = {"window_id": "same-window", "start": 0, "end": 10}
    jobs.plan_windows("low-job", [window], target_table=target,
                      source_tables=[source], priority=1)
    jobs.plan_windows("high-job", [window], target_table=target,
                      source_tables=[source], priority=10)

    barrier = threading.Barrier(2)
    outcomes = {}

    def run(job_id: str) -> None:
        barrier.wait(timeout=5)
        outcomes[job_id] = jobs.execute_window(job_id, "same-window")

    threads = [threading.Thread(target=run, args=(job,))
               for job in ("low-job", "high-job")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()

    assert outcomes["high-job"]["ok"] is True
    assert outcomes["low-job"]["ok"] is False
    assert outcomes["low-job"]["code"] == "conflict_lost"

    delivery = jobs.verify_delivery(target)
    assert delivery["valid"] is True
    assert len(delivery["valid_ranges"]) == 1
    winner = delivery["valid_ranges"][0]
    assert winner["owner_job_id"] == "high-job"
    assert winner["priority"] == 10

    report = jobs.overlap_report(
        window_a=jobs.find_window("same-window", job_id="low-job"),
        window_b=jobs.find_window("same-window", job_id="high-job"),
    )
    assert report["pair_overlaps"]
    assert report["pair_overlaps"][0]["winner_job_id"] == "high-job"
    assert report["pair_overlaps"][0]["priority_a"] == 1
    assert report["pair_overlaps"][0]["priority_b"] == 10

    # 失败/失效作业的来源读取记录仍保留。
    assert jobs.read_source_records("low-job")["total"] > 0
    assert jobs.read_source_records("high-job")["total"] > 0
    store.reset_backfill_recovery()
