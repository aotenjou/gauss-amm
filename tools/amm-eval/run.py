#!/usr/bin/env python3
"""Run one AMM evaluation and emit a standard evidence directory."""
from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from report import generate_report  # noqa: E402


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def json_default(value: Any) -> Any:
    """Keep driver Decimal/numeric values JSON compatible without losing nulls."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-root", type=Path, default=Path("evaluation-runs"))
    p.add_argument("--run-id", help="directory name; defaults to UTC timestamp")
    p.add_argument("--gauss-home", default=os.environ.get("GAUSSHOME", ""))
    p.add_argument("--gsbench", type=Path, help="gsbench checkout, used as cwd")
    p.add_argument("--gsbench-bin", default="gsbench")
    p.add_argument("--scenario", default="101")
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--duration", type=float, default=60.0)
    p.add_argument("--workload", help="explicit workload command; overrides --scenario")
    p.add_argument("--gsbench-config", type=Path)
    p.add_argument("--scale", default="unknown", help="gsbench scale, e.g. SF1, 20GB, size20")
    p.add_argument("--workload-name", default="gsbench")
    p.add_argument("--host", default=os.environ.get("PGHOST", "/tmp"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "15438")))
    p.add_argument("--dbname", default=os.environ.get("PGDATABASE", "postgres"))
    p.add_argument("--user", default=os.environ.get("PGUSER", os.environ.get("USER", "")))
    p.add_argument("--schema", default=os.environ.get("GSBENCH_SCHEMA", "gsbench"))
    p.add_argument("--sample-interval", type=float, default=1.0)
    p.add_argument("--ap-sql", help="representative AP SQL; collected with EXPLAIN ANALYZE")
    p.add_argument("--ap-concurrency", type=int, default=1)
    p.add_argument("--ap-work-mem", help="optional AP work_mem, for example 512MB")
    p.add_argument("--no-gsbench", action="store_true", help="only collect DB/host samples")
    p.add_argument("--report-only", type=Path, help="regenerate report for an existing run")
    return p.parse_args()


def connect(args: argparse.Namespace):
    try:
        import psycopg2
    except ImportError as exc:
        raise SystemExit("psycopg2 is required for run.py; use report.py for offline reports") from exc
    return psycopg2.connect(host=args.host, port=args.port, dbname=args.dbname,
                            user=args.user, password=os.environ.get("PGPASSWORD", ""),
                            application_name="amm_eval_sampler", connect_timeout=10)


def sql_value(cur: Any, sql: str) -> Any:
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return None if row is None else row[0]
    except Exception:
        cur.connection.rollback()
        return None


def sample_db(conn: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"ts": now(), "epoch": time.time()}
    with conn.cursor() as cur:
        values = {
            "xact_commit": "select coalesce(sum(xact_commit),0) from pg_stat_database",
            "xact_total": "select coalesce(sum(xact_commit+xact_rollback),0) from pg_stat_database",
            "blks_hit": "select coalesce(sum(blks_hit),0) from pg_stat_database",
            "blks_read": "select coalesce(sum(blks_read),0) from pg_stat_database",
            "temp_bytes": "select coalesce(sum(temp_bytes),0) from pg_stat_database",
            "temp_files": "select coalesce(sum(temp_files),0) from pg_stat_database",
            "tup_returned": "select coalesce(sum(tup_returned),0) from pg_stat_database",
            "ap_active": "select count(*) from pg_stat_activity where application_name like 'amm_eval_ap%' or application_name like 'gsbench_disturbance_ap%'",
            "ap_exec_ms": "select coalesce(sum(extract(epoch from now()-query_start)*1000),0) from pg_stat_activity where (application_name like 'amm_eval_ap%' or application_name like 'gsbench_disturbance_ap%') and state='active'",
        }
        for key, sql in values.items():
            row[key] = sql_value(cur, sql)
        row["amm_status"] = sql_value(cur, "select pg_catalog.gs_amm_status()")
        for name in ("gs_amm_enabled", "gs_amm_native_auto_mode", "shared_buffers", "work_mem"):
            row["guc_" + name] = sql_value(cur, "show " + name)
        io_sql = "select coalesce(sum(read_bytes),0), coalesce(sum(write_bytes),0), coalesce(sum(read_time),0), coalesce(sum(write_time),0) from pg_stat_io"
        try:
            cur.execute(io_sql)
            io = cur.fetchone()
            row.update({"io_read_bytes": io[0], "io_write_bytes": io[1],
                        "io_read_ms": io[2], "io_write_ms": io[3]})
        except Exception:
            conn.rollback()
            row.update({"io_read_bytes": None, "io_write_bytes": None,
                        "io_read_ms": None, "io_write_ms": None})
    return row


def host_sample() -> dict[str, Any]:
    row: dict[str, Any] = {"cpu_count": os.cpu_count(), "load1": os.getloadavg()[0]}
    try:
        text = Path("/proc/stat").read_text(encoding="utf-8")
        cpu = next(line for line in text.splitlines() if line.startswith("cpu ")).split()
        row["cpu_user"] = int(cpu[1])
        row["cpu_system"] = int(cpu[3])
        row["cpu_idle"] = int(cpu[4])
        row["cpu_iowait"] = int(cpu[5])
    except (OSError, StopIteration, ValueError):
        row.update({"cpu_user": None, "cpu_system": None, "cpu_idle": None, "cpu_iowait": None})
    try:
        mem = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines() if ":" in line)
        row["mem_available_kb"] = int(mem["MemAvailable"].split()[0])
    except (OSError, KeyError, ValueError):
        row["mem_available_kb"] = None
    try:
        read_sectors = write_sectors = busy_ms = 0
        for line in Path("/proc/diskstats").read_text().splitlines():
            fields = line.split()
            if len(fields) < 14:
                continue
            device = fields[2]
            if not (device.startswith(("sd", "nvme", "vd", "xvd", "mmc", "md"))):
                continue
            read_sectors += int(fields[5])
            write_sectors += int(fields[9])
            busy_ms += int(fields[12])
        row.update({"disk_read_sectors": read_sectors, "disk_write_sectors": write_sectors,
                    "disk_busy_ms": busy_ms})
    except (OSError, ValueError):
        row.update({"disk_read_sectors": None, "disk_write_sectors": None, "disk_busy_ms": None})
    return row


def sampler(args: argparse.Namespace, path: Path, stop: threading.Event) -> None:
    try:
        conn = connect(args)
    except Exception as exc:
        path.write_text(json.dumps({"error": str(exc), "ts": now()}, default=json_default) + "\n", encoding="utf-8")
        return
    conn.autocommit = True
    previous_host: dict[str, Any] | None = None
    with path.open("w", encoding="utf-8") as out:
        while not stop.is_set():
            record = sample_db(conn)
            host = host_sample()
            if previous_host:
                total = sum(host.get(k) or 0 for k in ("cpu_user", "cpu_system", "cpu_idle", "cpu_iowait")) - sum(previous_host.get(k) or 0 for k in ("cpu_user", "cpu_system", "cpu_idle", "cpu_iowait"))
                row_delta = (host.get("cpu_iowait") or 0) - (previous_host.get("cpu_iowait") or 0)
                record["cpu_iowait_pct"] = round(100 * row_delta / total, 3) if total > 0 else None
            record.update(host)
            out.write(json.dumps(record, ensure_ascii=False, sort_keys=True, default=json_default) + "\n")
            out.flush()
            previous_host = host
            stop.wait(max(args.sample_interval, 0.1))
    conn.close()


def run() -> int:
    args = parse_args()
    if args.report_only:
        generate_report(args.report_only)
        return 0
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": 1, "run_id": run_id, "start": now(),
        "host": socket.gethostname(), "platform": platform.platform(),
        "scale": args.scale, "workload": args.workload_name, "schema": args.schema,
        "database": {"host": args.host, "port": args.port, "dbname": args.dbname, "user": args.user},
        "sample_interval_sec": args.sample_interval, "duration_sec": args.duration,
        "gsbench": args.workload or f"{args.gsbench_bin} run {args.scenario} --workers {args.workers} --duration {args.duration}s",
        "gauss_home": args.gauss_home, "command_line": sys.argv,
        "ap": {"sql_supplied": bool(args.ap_sql), "concurrency": args.ap_concurrency,
               "work_mem": args.ap_work_mem},
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    stop = threading.Event()
    thread = threading.Thread(target=sampler, args=(args, run_dir / "samples.jsonl", stop), daemon=True)
    thread.start()
    ap_process: subprocess.Popen[str] | None = None
    if args.ap_sql:
        ap_process = subprocess.Popen(
            [sys.executable, str(HERE / "ap.py"), "--sql", args.ap_sql,
             "--output", str(run_dir / "ap-metrics.jsonl"), "--duration", str(args.duration),
             "--concurrency", str(args.ap_concurrency), "--host", args.host,
             "--port", str(args.port), "--dbname", args.dbname, "--user", args.user]
            + (["--work-mem", args.ap_work_mem] if args.ap_work_mem else []),
            env=os.environ.copy())
    process: subprocess.Popen[str] | None = None
    try:
        if not args.no_gsbench:
            command = args.workload or f"{shlex.quote(args.gsbench_bin)} run {shlex.quote(args.scenario)} --workers {args.workers} --duration {args.duration}s"
            env = os.environ.copy()
            if args.gsbench_config:
                env["GSBENCH_CONFIG"] = str(args.gsbench_config.resolve())
            log = (run_dir / "gsbench.log").open("w")
            process = subprocess.Popen(command, shell=True, cwd=str(args.gsbench) if args.gsbench else None,
                                       stdout=log, stderr=subprocess.STDOUT, env=env, text=True)
            process.wait()
            log.close()
        else:
            stop.wait(args.duration)
    except KeyboardInterrupt:
        if process:
            process.send_signal(signal.SIGINT)
    finally:
        stop.set()
        thread.join(timeout=5)
        if ap_process and ap_process.poll() is None:
            ap_process.terminate()
            ap_process.wait(timeout=10)
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
    manifest["end"] = now()
    manifest["gsbench_exit_code"] = process.returncode if process else None
    (run_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    generate_report(run_dir)
    return 0 if not process or process.returncode == 0 else process.returncode


if __name__ == "__main__":
    raise SystemExit(run())
