#!/usr/bin/env python3
"""Run a repeatable AP query and record execution/spill evidence."""
from __future__ import annotations

import argparse
import json
import os
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def percentile(values: list[float], ratio: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * ratio))]


def parse_plan(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if isinstance(value, list):
        value = value[0]
    return value if isinstance(value, dict) else {}


def inspect_plan(node: Any) -> dict[str, Any]:
    result = {"spill": False, "temp_read_blocks": 0, "temp_written_blocks": 0, "sort_methods": []}
    if isinstance(node, dict):
        sort_type = str(node.get("Sort Space Type", "")).lower()
        method = node.get("Sort Method")
        if method:
            result["sort_methods"].append(str(method))
        if sort_type == "disk" or "external" in str(method).lower():
            result["spill"] = True
        result["temp_read_blocks"] += int(node.get("Temp Read Blocks") or 0)
        result["temp_written_blocks"] += int(node.get("Temp Written Blocks") or 0)
        for child in node.get("Plans", []):
            child_data = inspect_plan(child)
            result["spill"] = result["spill"] or child_data["spill"]
            result["temp_read_blocks"] += child_data["temp_read_blocks"]
            result["temp_written_blocks"] += child_data["temp_written_blocks"]
            result["sort_methods"].extend(child_data["sort_methods"])
    return result


def connect(args: argparse.Namespace, worker: int):
    try:
        import psycopg2
    except ImportError as exc:
        raise SystemExit("psycopg2 is required for AP collection") from exc
    return psycopg2.connect(host=args.host, port=args.port, dbname=args.dbname,
                            user=args.user, password=os.environ.get("PGPASSWORD", ""),
                            application_name=f"amm_eval_ap_{worker}", connect_timeout=10)


def worker(args: argparse.Namespace, index: int, output: Path, stop: threading.Event) -> None:
    try:
        conn = connect(args, index)
    except Exception as exc:
        with output.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"ts": iso_now(), "worker": index, "status": "error", "error": str(exc)}) + "\n")
        return
    conn.autocommit = False
    deadline = time.monotonic() + args.duration
    query = args.sql.strip().rstrip(";")
    with output.open("a", encoding="utf-8") as handle:
        while not stop.is_set() and time.monotonic() < deadline:
            started = time.monotonic()
            record: dict[str, Any] = {"ts": iso_now(), "worker": index}
            try:
                with conn.cursor() as cur:
                    if args.work_mem:
                        cur.execute("set local work_mem = %s", (args.work_mem,))
                    cur.execute("explain (analyze, buffers, format json) " + query)
                    payload = parse_plan(cur.fetchone()[0])
                conn.commit()
                plan = payload.get("Plan", payload)
                details = inspect_plan(plan)
                record.update({"status": "ok", "execution_ms": payload.get("Execution Time"),
                               "planning_ms": payload.get("Planning Time"), **details,
                               "wall_ms": (time.monotonic() - started) * 1000})
            except Exception as exc:
                conn.rollback()
                record.update({"status": "error", "error": str(exc),
                               "wall_ms": (time.monotonic() - started) * 1000})
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()
    conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sql", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--work-mem")
    parser.add_argument("--host", default=os.environ.get("PGHOST", "/tmp"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "15438")))
    parser.add_argument("--dbname", default=os.environ.get("PGDATABASE", "postgres"))
    parser.add_argument("--user", default=os.environ.get("PGUSER", os.environ.get("USER", "")))
    args = parser.parse_args()
    if args.concurrency < 1 or args.duration <= 0:
        raise SystemExit("concurrency and duration must be positive")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("", encoding="utf-8")
    stop = threading.Event()
    threads = [threading.Thread(target=worker, args=(args, index, args.output, stop), daemon=True)
               for index in range(1, args.concurrency + 1)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
