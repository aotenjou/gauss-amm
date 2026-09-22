#!/usr/bin/env python3
"""Five-stage native/AMM low-memory evaluation.

The runner deliberately uses cgroup v1 when it is the only memory controller
available on the host.  It never initializes a database and treats the
existing PGDATA as read-only test input.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import random
import re
import shlex
import signal
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

HERE = Path(__file__).resolve().parent
WRITE_LOCK = threading.Lock()


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def as_number(value: Any) -> float | None:
    try:
        value = float(value)
        return value if value == value and value not in (float("inf"), float("-inf")) else None
    except (TypeError, ValueError):
        return None


def percentile(values: Iterable[float], ratio: float) -> float | None:
    values = sorted(values)
    if not values:
        return None
    pos = (len(values) - 1) * ratio
    low, high = int(pos), min(len(values) - 1, int(pos) + 1)
    return values[low] + (values[high] - values[low]) * (pos - low)


def parse_status(value: Any) -> dict[str, Any]:
    """Parse gs_amm_status()'s key=value text without assuming every key exists."""
    if not isinstance(value, str):
        return {}
    result: dict[str, Any] = {}
    for token in value.split():
        if "=" not in token:
            continue
        key, raw = token.split("=", 1)
        if raw.lower() in {"true", "false"}:
            result[key] = raw.lower() == "true"
        else:
            number = as_number(raw)
            result[key] = int(number) if number is not None and number.is_integer() else number if number is not None else raw
    return result


class MemoryCgroupV1:
    """Small, recoverable v1 memory cgroup manager."""

    def __init__(self, root: Path, name: str, limit_bytes: int):
        self.root = Path(root)
        self.path = self.root / name
        self.limit_bytes = limit_bytes
        self.created = False

    @property
    def available(self) -> bool:
        return (self.root / "memory.limit_in_bytes").exists()

    def create(self) -> None:
        if not self.available:
            raise RuntimeError(f"v1 memory controller not found under {self.root}")
        self.path.mkdir(mode=0o755)
        (self.path / "memory.limit_in_bytes").write_text(str(self.limit_bytes))
        swap = self.path / "memory.memsw.limit_in_bytes"
        if swap.exists():
            try:
                swap.write_text(str(self.limit_bytes))
            except OSError:
                pass
        self.created = True

    def add_pid(self, pid: int) -> None:
        if not self.created:
            raise RuntimeError("cgroup is not created")
        tasks = self.path / "tasks"
        tasks.write_text(str(pid))

    def sample(self) -> dict[str, Any]:
        def read(name: str) -> int | None:
            try:
                return int((self.path / name).read_text().strip())
            except (OSError, ValueError):
                return None

        result = {"cgroup_path": str(self.path), "cgroup_limit_bytes": read("memory.limit_in_bytes"),
                  "cgroup_usage_bytes": read("memory.usage_in_bytes"),
                  "cgroup_peak_bytes": read("memory.max_usage_in_bytes"),
                  "cgroup_failcnt": read("memory.failcnt")}
        try:
            result["cgroup_oom_control"] = (self.path / "memory.oom_control").read_text().strip()
        except OSError:
            result["cgroup_oom_control"] = None
        try:
            result["cgroup_stat"] = dict(line.split()[:2] for line in (self.path / "memory.stat").read_text().splitlines())
        except OSError:
            result["cgroup_stat"] = {}
        return result

    def remove(self) -> None:
        if not self.created:
            return
        try:
            self.path.rmdir()
        except OSError:
            # A non-empty cgroup is evidence that cleanup was incomplete.
            pass
        self.created = False


def connect(args: argparse.Namespace, application: str):
    try:
        import psycopg2
    except ImportError as exc:
        raise SystemExit("psycopg2 is required for lowmem.py") from exc
    return psycopg2.connect(host=args.host, port=args.port, dbname=args.dbname,
                            user=args.user, password=os.environ.get("PGPASSWORD", ""),
                            application_name=application, connect_timeout=10)


def sql_value(cur: Any, sql: str) -> Any:
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        cur.connection.rollback()
        return None


def sample_db(conn: Any, cgroup: MemoryCgroupV1 | None, server_pids: list[int]) -> dict[str, Any]:
    row: dict[str, Any] = {"ts": now(), "epoch": time.time()}
    with conn.cursor() as cur:
        for key, sql in {
            "xact_commit": "select coalesce(sum(xact_commit),0) from pg_stat_database",
            "xact_total": "select coalesce(sum(xact_commit+xact_rollback),0) from pg_stat_database",
            "blks_hit": "select coalesce(sum(blks_hit),0) from pg_stat_database",
            "blks_read": "select coalesce(sum(blks_read),0) from pg_stat_database",
            "temp_bytes": "select coalesce(sum(temp_bytes),0) from pg_stat_database",
            "temp_files": "select coalesce(sum(temp_files),0) from pg_stat_database",
            "ap_active": "select count(*) from pg_stat_activity where application_name like 'amm_lowmem_ap_%'",
            "ap_exec_ms": "select coalesce(sum(extract(epoch from now()-query_start)*1000),0) from pg_stat_activity where application_name like 'amm_lowmem_ap_%' and state='active'",
            "amm_status": "select pg_catalog.gs_amm_status()",
        }.items():
            row[key] = sql_value(cur, sql)
        for name in ("gs_amm_enabled", "gs_amm_workload_role", "shared_buffers", "work_mem"):
            row["guc_" + name] = sql_value(cur, "show " + name)
    row["amm_status_fields"] = parse_status(row.get("amm_status"))
    rss = 0
    for pid in server_pids:
        try:
            rss += int(Path(f"/proc/{pid}/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        except (OSError, IndexError, ValueError):
            pass
    row["server_rss_bytes"] = rss or None
    if cgroup:
        row.update(cgroup.sample())
    return row


def tp_worker(args: argparse.Namespace, index: int, output: Path, stop: threading.Event,
              duration: float, warmup: float) -> None:
    try:
        conn = connect(args, f"amm_lowmem_tp_{index}")
    except Exception as exc:
        append_json(output, {"ts": now(), "worker": index, "status": "connect_error", "error": str(exc)})
        return
    conn.autocommit = False
    deadline = time.monotonic() + warmup + duration
    rng = random.Random(index * 1009 + int(time.time()))
    try:
        with output.open("a", encoding="utf-8") as handle:
            while not stop.is_set() and time.monotonic() < deadline:
                started = time.monotonic()
                record: dict[str, Any] = {"ts": now(), "worker": index}
                try:
                    key = rng.randint(1, 3_477_459)
                    with conn.cursor() as cur:
                        cur.execute("begin")
                        cur.execute(f"select balance from {args.schema}.accounts where id=%s", (key,))
                        cur.execute(f"update {args.schema}.accounts set balance=balance+0.01, updated_at=current_timestamp where id=%s", (key,))
                        cur.execute("commit")
                    elapsed = (time.monotonic() - started) * 1000
                    record.update({"status": "ok", "latency_ms": elapsed,
                                   "warmup": time.monotonic() < deadline - duration})
                except Exception as exc:
                    conn.rollback()
                    record.update({"status": "error", "error": str(exc),
                                   "latency_ms": (time.monotonic() - started) * 1000})
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                handle.flush()
    finally:
        conn.close()


def inspect_plan(node: Any) -> dict[str, Any]:
    result = {"spill": False, "external_merge": False, "disk_kb": 0,
              "temp_read_blocks": 0, "temp_written_blocks": 0, "sort_methods": []}
    if isinstance(node, dict):
        method = str(node.get("Sort Method", ""))
        space = str(node.get("Sort Space Type", "")).lower()
        result["sort_methods"].append(method) if method else None
        result["external_merge"] |= "external merge" in method.lower()
        result["spill"] |= result["external_merge"] or space == "disk"
        result["disk_kb"] += int(node.get("Sort Space Used") or 0) if space == "disk" else 0
        result["temp_read_blocks"] += int(node.get("Temp Read Blocks") or 0)
        result["temp_written_blocks"] += int(node.get("Temp Written Blocks") or 0)
        for child in node.get("Plans", []):
            child_data = inspect_plan(child)
            for key in ("spill", "external_merge"):
                result[key] |= child_data[key]
            for key in ("disk_kb", "temp_read_blocks", "temp_written_blocks"):
                result[key] += child_data[key]
            result["sort_methods"].extend(child_data["sort_methods"])
    return result


def ap_once(args: argparse.Namespace, index: int, sql: str, work_mem: str, output: Path,
            stop: threading.Event | None = None, role: str | None = None) -> dict[str, Any]:
    started = time.monotonic()
    record: dict[str, Any] = {"ts": now(), "worker": index}
    try:
        conn = connect(args, f"amm_lowmem_ap_{index}")
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("set local work_mem = %s", (work_mem,))
            if role:
                cur.execute("set local gs_amm_workload_role = %s", (role,))
            cur.execute("explain (analyze, buffers, format json) " + sql)
            value = cur.fetchone()[0]
        conn.commit()
        if isinstance(value, str):
            value = json.loads(value)
        plan = value[0] if isinstance(value, list) else value
        details = inspect_plan(plan.get("Plan", plan))
        record.update({"status": "ok", "execution_ms": plan.get("Execution Time"),
                       "planning_ms": plan.get("Planning Time"), **details})
        conn.close()
    except Exception as exc:
        record.update({"status": "error", "error": str(exc)})
    record["wall_ms"] = (time.monotonic() - started) * 1000
    append_json(output, record)
    return record


def explain_text(args: argparse.Namespace, sql: str, path: Path, work_mem: str,
                 role: str | None) -> dict[str, Any]:
    """Keep textual external-merge/Disk evidence for the Stage 2 control gate."""
    result: dict[str, Any] = {"external_merge": False, "disk": False}
    try:
        conn = connect(args, "amm_lowmem_gate")
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("set local work_mem = %s", (work_mem,))
            if role:
                cur.execute("set local gs_amm_workload_role = %s", (role,))
            cur.execute("explain (analyze, buffers) " + sql)
            text = "\n".join(str(row[0]) for row in cur.fetchall())
        conn.rollback()
        conn.close()
        path.write_text(text + "\n", encoding="utf-8")
        lower = text.lower()
        result.update({"external_merge": "external merge" in lower,
                       "disk": bool(re.search(r"disk\\s*:", lower))})
    except Exception as exc:
        result["error"] = str(exc)
        path.write_text("EXPLAIN ERROR: " + str(exc) + "\n", encoding="utf-8")
    return result


def ap_worker(args: argparse.Namespace, index: int, sql: str, work_mem: str, output: Path,
              stop: threading.Event, duration: float, warmup: float, role: str | None) -> None:
    deadline = time.monotonic() + warmup + duration
    while not stop.is_set() and time.monotonic() < deadline:
        ap_once(args, index, sql, work_mem, output, stop, role)


def append_json(path: Path, value: dict[str, Any]) -> None:
    # TP/AP workers share a stage log; serialize records to preserve JSONL.
    with WRITE_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str) + "\n")


@dataclass(frozen=True)
class Stage:
    name: str
    ap_workers: int
    tp_workers: int
    ap_duration: float
    warmup: float


STAGES = (
    Stage("stage1_tp_baseline", 0, 2, 60, 15),
    Stage("stage2_single_ap_spill", 1, 0, 60, 15),
    Stage("stage3_ap_pressure", 4, 2, 60, 15),
    Stage("stage4_admission_pressure", 10, 2, 60, 15),
    Stage("stage5_tp8_with_ap", 10, 8, 60, 15),
    Stage("stage5_recovery_no_ap", 0, 8, 60, 15),
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-root", type=Path, default=Path("lowmem-runs"))
    p.add_argument("--run-id")
    p.add_argument("--kernel", choices=("native", "amm"), required=True)
    p.add_argument("--host", default=os.environ.get("PGHOST", "/tmp"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "15439")))
    p.add_argument("--dbname", default="postgres")
    p.add_argument("--user", default=os.environ.get("PGUSER", "ammdb"))
    p.add_argument("--schema", default="gsbench_20g")
    p.add_argument("--ap-sql", default="select id, group_id, sort_key, payload from {schema}.sort_data order by payload, sort_key desc, id")
    p.add_argument("--ap-work-mem", default="512MB")
    p.add_argument("--warmup", type=float, default=15)
    p.add_argument("--duration", type=float, default=60)
    p.add_argument("--memory-limit", type=int, default=1024 * 1024 * 1024)
    p.add_argument("--cgroup-root", type=Path, default=Path("/sys/fs/cgroup/memory"))
    p.add_argument("--server-pid", type=int, action="append", default=[])
    p.add_argument("--server-pid-file", type=Path)
    p.add_argument("--start-command", help="shell command used only after the fresh cgroup is created")
    p.add_argument("--stop-command", help="shell command run before an optional kernel start")
    p.add_argument("--skip-cgroup", action="store_true")
    p.add_argument("--report-only", type=Path)
    return p.parse_args()


def descendants(pids: list[int]) -> list[int]:
    result = set(pids)
    changed = True
    while changed:
        changed = False
        for line in Path("/proc").glob("[0-9]*/stat"):
            try:
                fields = line.read_text().split()
                pid, ppid = int(fields[0]), int(fields[3])
                if ppid in result and pid not in result:
                    result.add(pid); changed = True
            except (OSError, ValueError, IndexError):
                continue
    return sorted(result)


def readiness(args: argparse.Namespace, timeout: float = 30) -> None:
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            conn = connect(args, "amm_lowmem_ready")
            conn.close()
            return
        except Exception as exc:
            last = exc
            time.sleep(1)
    raise RuntimeError(f"database did not become ready: {last}")


def run_stage(args: argparse.Namespace, run_dir: Path, stage: Stage, cgroup: MemoryCgroupV1 | None) -> dict[str, Any]:
    stage_dir = run_dir / stage.name
    stage_dir.mkdir()
    sql = args.ap_sql.format(schema=args.schema)
    samples = stage_dir / "samples.jsonl"
    tp_log, ap_log = stage_dir / "tp.jsonl", stage_dir / "ap.jsonl"
    stop = threading.Event()
    gate = None
    if stage.name == "stage2_single_ap_spill":
        gate = explain_text(args, sql, stage_dir / "explain.txt", args.ap_work_mem,
                            "ap" if args.kernel == "amm" else None)
    server_pids = descendants(args.server_pid)
    if cgroup:
        for pid in server_pids:
            try:
                cgroup.add_pid(pid)
            except OSError:
                pass
    try:
        conn = connect(args, "amm_lowmem_sampler")
        conn.autocommit = True
    except Exception as exc:
        append_json(samples, {"ts": now(), "error": str(exc)})
        conn = None
    def sample_loop() -> None:
        with samples.open("a", encoding="utf-8"):
            while not stop.is_set():
                if conn:
                    append_json(samples, sample_db(conn, cgroup, server_pids))
                elif cgroup:
                    append_json(samples, {"ts": now(), "epoch": time.time(), **cgroup.sample()})
                stop.wait(1)
    sampler = threading.Thread(target=sample_loop, daemon=True); sampler.start()
    threads: list[threading.Thread] = []
    for index in range(1, stage.tp_workers + 1):
        thread = threading.Thread(target=tp_worker, args=(args, index, tp_log, stop, stage.ap_duration, stage.warmup), daemon=True)
        threads.append(thread); thread.start()
    role = "ap" if args.kernel == "amm" else None
    for index in range(1, stage.ap_workers + 1):
        thread = threading.Thread(target=ap_worker, args=(args, index, sql, args.ap_work_mem, ap_log, stop, stage.ap_duration, stage.warmup, role), daemon=True)
        threads.append(thread); thread.start()
    for thread in threads:
        thread.join()
    stop.set(); sampler.join(timeout=3)
    if conn:
        conn.close()
    summary = summarize_stage(stage_dir, stage)
    if gate is not None:
        summary["explain_gate"] = gate
    return summary


def summarize_stage(stage_dir: Path, stage: Stage) -> dict[str, Any]:
    def rows(path: Path) -> list[dict[str, Any]]:
        result = []
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                try: result.append(json.loads(line))
                except json.JSONDecodeError: pass
        return result
    tp_rows = rows(stage_dir / "tp.jsonl")
    tp = [r for r in tp_rows if not r.get("warmup")]
    ap = rows(stage_dir / "ap.jsonl")
    latencies = [as_number(r.get("latency_ms")) for r in tp if r.get("status") == "ok"]
    latencies = [v for v in latencies if v is not None]
    spill = [r for r in ap if r.get("spill")]
    samples = rows(stage_dir / "samples.jsonl")
    temp_values = [as_number(r.get("temp_bytes")) for r in samples]
    temp_values = [v for v in temp_values if v is not None]
    sample_epochs = [as_number(r.get("epoch")) for r in samples]
    sample_epochs = [v for v in sample_epochs if v is not None]
    recovery = None
    if stage.name == "stage5_recovery_no_ap" and samples:
        active = [as_number(r.get("ap_active")) or 0 for r in samples]
        rss = [as_number(r.get("server_rss_bytes")) for r in samples]
        rss = [v for v in rss if v is not None]
        recovery = {"ap_zero_sec": None, "rss_floor_sec": None, "tp_90pct_sec": None}
        if sample_epochs:
            first = sample_epochs[0]
            for epoch, count in zip(sample_epochs, active):
                if count == 0:
                    recovery["ap_zero_sec"] = epoch - first
                    break
            if rss:
                floor = min(rss) * 1.1
                for epoch, value in zip(sample_epochs, [as_number(r.get("server_rss_bytes")) for r in samples]):
                    if value is not None and value <= floor:
                        recovery["rss_floor_sec"] = epoch - first
                        break
        qps_by_second: dict[int, int] = {}
        for row in tp_rows:
            if row.get("status") != "ok":
                continue
            try:
                second = int(datetime.fromisoformat(str(row["ts"])).timestamp())
            except (KeyError, ValueError):
                continue
            qps_by_second[second] = qps_by_second.get(second, 0) + 1
        if qps_by_second:
            seconds = list(range(min(qps_by_second), max(qps_by_second) + 1))
            values = [qps_by_second.get(second, 0) for second in seconds]
            steady = statistics.median(values[-min(20, len(values)):])
            threshold = steady * .9
            for index in range(max(0, len(values) - 4)):
                if len(values[index:index + 5]) == 5 and all(value >= threshold for value in values[index:index + 5]):
                    recovery["tp_90pct_sec"] = seconds[index] - seconds[0]
                    break
    oom = any(("oom_kill 1" in str(r.get("cgroup_oom_control")))
              or re.search(r"(?:^|\\s)oom_kill(?:\\s|=)1", str(r.get("cgroup_oom_control")))
              or (as_number(r.get("cgroup_failcnt")) or 0) > 0 for r in samples)
    return {"name": stage.name, "tp_workers": stage.tp_workers, "ap_workers": stage.ap_workers,
            "tp": {"count": len(tp), "ok": len(latencies), "errors": sum(r.get("status") != "ok" for r in tp),
                   "qps": len(latencies) / max(stage.ap_duration, 1), "p95_ms": percentile(latencies, .95), "p99_ms": percentile(latencies, .99)},
            "ap": {"count": len(ap), "ok": sum(r.get("status") == "ok" for r in ap), "errors": sum(r.get("status") == "error" for r in ap),
                   "spill_queries": len(spill), "spill_rate": len(spill) / len(ap) if ap else None},
            "memory": {"rss_peak_bytes": max((as_number(r.get("server_rss_bytes")) or 0 for r in samples), default=None),
                       "cgroup_peak_bytes": max((as_number(r.get("cgroup_peak_bytes")) or 0 for r in samples), default=None),
                       "cgroup_failcnt_peak": max((as_number(r.get("cgroup_failcnt")) or 0 for r in samples), default=None), "oom": oom},
            "temp_bytes_delta": max(0, temp_values[-1] - temp_values[0]) if len(temp_values) > 1 else None,
            "native_spill_gate": None, "recovery": recovery}


def run(args: argparse.Namespace) -> int:
    if args.report_only:
        return write_report(args.report_only)
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_root / f"{run_id}-{args.kernel}"
    run_dir.mkdir(parents=True, exist_ok=False)
    args.warmup = args.warmup
    manifest = {"schema_version": 1, "run_id": run_id, "kernel": args.kernel, "start": now(),
                "platform": platform.platform(), "memory_limit_bytes": args.memory_limit, "memory_controller": "v1",
                "database": {"host": args.host, "port": args.port, "dbname": args.dbname, "user": args.user},
                "schema": args.schema, "ap_sql": args.ap_sql.format(schema=args.schema), "ap_work_mem": args.ap_work_mem,
                "warmup_sec": args.warmup, "duration_sec": args.duration, "command_line": sys.argv}
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    cgroup = None
    server_process = None
    try:
        if args.stop_command:
            subprocess.run(args.stop_command, shell=True, check=False)
        if not args.skip_cgroup:
            cgroup = MemoryCgroupV1(args.cgroup_root, f"amm-lowmem-{run_id}-{args.kernel}", args.memory_limit)
            cgroup.create()
        if args.start_command:
            server_process = subprocess.Popen(args.start_command, shell=True, start_new_session=True)
            args.server_pid = [server_process.pid]
            if cgroup:
                cgroup.add_pid(server_process.pid)
        elif args.server_pid_file and args.server_pid_file.exists():
            args.server_pid = [int(args.server_pid_file.read_text().splitlines()[0])]
        readiness(args)
        summaries = []
        for base in STAGES:
            stage = Stage(base.name, base.ap_workers, base.tp_workers, args.duration, args.warmup)
            summary = run_stage(args, run_dir, stage, cgroup)
            summaries.append(summary)
            if summary["memory"]["oom"]:
                manifest["terminal_failure"] = {"stage": stage.name, "reason": "cgroup_oom_or_failcnt"}
                break
        (run_dir / "stages.json").write_text(json.dumps(summaries, indent=2) + "\n")
        manifest["stages"] = summaries
        manifest["end"] = now()
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        write_report(run_dir)
        return 1 if manifest.get("terminal_failure") else 0
    except KeyboardInterrupt:
        return 130
    finally:
        if server_process and server_process.poll() is None:
            os.killpg(server_process.pid, signal.SIGTERM)
            try:
                server_process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(server_process.pid, signal.SIGKILL)
        if cgroup:
            cgroup.remove()


def write_report(run_dir: Path) -> int:
    manifest = json.loads((run_dir / "manifest.json").read_text())
    stages = manifest.get("stages") or json.loads((run_dir / "stages.json").read_text()) if (run_dir / "stages.json").exists() else []
    native_gate = None
    for stage in stages:
        if stage.get("name") == "stage2_single_ap_spill":
            records = []
            path = run_dir / stage["name"] / "ap.jsonl"
            if path.exists():
                records = [json.loads(x) for x in path.read_text().splitlines() if x.strip()]
            explain_gate = stage.get("explain_gate") or {}
            temp_ok = (as_number(stage.get("temp_bytes_delta")) or 0) > 0
            native_gate = (any(r.get("external_merge") and r.get("spill") for r in records)
                           and explain_gate.get("external_merge") and explain_gate.get("disk") and temp_ok) if manifest.get("kernel") == "native" else None
            stage["native_spill_gate"] = native_gate
    lines = ["# Low-memory AMM evaluation", "", f"- Kernel: `{manifest.get('kernel')}`", f"- Memory controller: `{manifest.get('memory_controller')}`", f"- Memory limit: `{manifest.get('memory_limit_bytes')}` bytes", "", "| Stage | TP QPS | TP P95 ms | TP P99 ms | AP spill rate | RSS peak | cgroup peak | OOM |", "|---|---:|---:|---:|---:|---:|---:|---|"]
    for stage in stages:
        tp, ap, mem = stage["tp"], stage["ap"], stage["memory"]
        lines.append(f"| {stage['name']} | {tp.get('qps', 0):.2f} | {tp.get('p95_ms') or 0:.2f} | {tp.get('p99_ms') or 0:.2f} | {(ap.get('spill_rate') or 0) * 100:.1f}% | {mem.get('rss_peak_bytes') or 0:.0f} | {mem.get('cgroup_peak_bytes') or 0:.0f} | {'yes' if mem.get('oom') else 'no'} |")
    if manifest.get("kernel") == "native":
        lines += ["", f"Native Stage 2 spill gate: **{'PASS' if native_gate else 'FAIL'}** (requires external merge/Disk evidence)."]
    else:
        lines += ["", "AMM spill-control comparison is valid only when the native Stage 2 gate passes."]
    (run_dir / "report.md").write_text("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
