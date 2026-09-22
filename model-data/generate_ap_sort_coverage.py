#!/usr/bin/env python3
"""Collect native bounds for the three-key GSBench Sort AP shape.

The generic Sort coverage set intentionally spans width and plan-depth.  It
did not contain a 532-byte Sort with three sort keys, which is the shape used
by the published GSBench AP.  This collector keeps that shape fixed and varies
the indexed ``dist_key`` interval on every database size.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any

import memory_bounds
from generate_sort_coverage import append_jsonl, read_jsonl, scalar_two_bound_sample


ROOT = Path(__file__).resolve().parent
SIZES = (1, 2, 5, 10, 15, 20)
BASE_DIST_SPAN = 112_346
DEFAULT_MULTIPLIERS = (0.20, 0.35, 0.55, 0.80, 1.20, 1.60)


def ap_shape_sql(start: int, span: int) -> str:
    end = max(start, start + span)
    return (
        "SELECT id, sort_key, payload FROM gsbench.sort_data "
        f"WHERE dist_key BETWEEN {start} AND {end} "
        "ORDER BY payload, sort_key DESC, id"
    )


def connect(instance: dict[str, Any], database: str, args: argparse.Namespace):
    return memory_bounds.connect({
        "host": args.host or instance.get("socket_dir") or instance.get("host", "127.0.0.1"),
        "port": args.port or instance["port"],
        "user": instance["user"],
        "dbname": database,
    })


def dist_key_range(conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT coalesce(min(dist_key), 1), coalesce(max(dist_key), 1) FROM gsbench.sort_data")
        low, high = cur.fetchone()
    return int(low), int(high)


def planned_input_mb(conn, sql: str, timeout_ms: int) -> tuple[dict[str, Any], float]:
    payload = memory_bounds.explain_static(conn, sql, timeout_ms)
    static = memory_bounds.plan_features(payload)
    required = {
        "sort_nodes": static.get("sort_nodes"),
        "sort_tuple_width": static.get("sort_tuple_width"),
        "sort_key_count": static.get("sort_key_count"),
    }
    if required["sort_nodes"] != 1 or required["sort_tuple_width"] != 532 or required["sort_key_count"] != 3:
        raise ValueError(f"unexpected_ap_shape:{required}")
    bytes_value = float(static.get("sort_input_bytes") or 0.0)
    if bytes_value <= 0:
        raise ValueError("missing_sort_input_bytes")
    return payload, bytes_value / (1024 * 1024)


def candidate_specs(low: int, high: int, count: int, multipliers: tuple[float, ...]):
    """Emit shifted intervals so the exact AP text remains held out."""
    domain = max(1, high - low)
    offset_stride = max(1, domain // 13)
    for index in range(count * 4):
        multiplier = multipliers[index % len(multipliers)]
        span = max(64, min(domain // 2, int(BASE_DIST_SPAN * multiplier)))
        max_start = max(low, high - span)
        start = low + ((index + 1) * offset_stride) % max(1, max_start - low + 1)
        # Avoid collecting the published AP itself, even when the wrap-around
        # chooses the first range.
        if start == 1 and span == BASE_DIST_SPAN:
            start = min(max_start, start + offset_stride)
        yield index, start, span, multiplier


def collect_size(size: int, instance: dict[str, Any], args: argparse.Namespace,
                 multipliers: tuple[float, ...]) -> dict[str, Any]:
    database = instance["databases"][str(size)]
    root = args.output_root / f"gsbench-{size}gb"
    root.mkdir(parents=True, exist_ok=True)
    bounds_path = root / "bounds.jsonl"
    rejected_path = root / "rejected.jsonl"
    accepted = read_jsonl(bounds_path) if args.resume else []
    rejected = read_jsonl(rejected_path) if args.resume else []
    if not args.resume and (accepted or rejected):
        raise RuntimeError(f"{root} already contains rows; use --resume or a new output root")

    conn = connect(instance, database, args)
    try:
        preflight = memory_bounds.session_preflight(conn, expected_database=database, expected_schema="gsbench")
        low, high = dist_key_range(conn)
        seen = {str(row.get("sql_sha256")) for row in accepted + rejected}
        for ordinal, start, span, multiplier in candidate_specs(low, high, args.target_count, multipliers):
            if len(accepted) >= args.target_count:
                break
            sql = ap_shape_sql(start, span)
            digest = hashlib.sha256(sql.encode()).hexdigest()
            if digest in seen:
                continue
            seen.add(digest)
            record: dict[str, Any] = {
                "schema_version": memory_bounds.SCHEMA_VERSION,
                "label_protocol": memory_bounds.LABEL_PROTOCOL,
                "label_semantics": "cache_and_one_pass",
                "label_mode": "two_bound_native",
                "collection_mode": "native",
                "target": "gsbench_ap_shape_coverage",
                "dataset": "gsbench",
                "workload": "gsbench",
                "size_gb": size,
                "dbname": database,
                "query_id": f"gsbench_ap_shape_{size}gb_{ordinal:03d}",
                "sql": sql,
                "sql_sha256": digest,
                "coverage_target": {"dist_key_start": start, "dist_key_span": span, "multiplier": multiplier},
            }
            started = time.monotonic()
            try:
                static_payload, input_mb = planned_input_mb(conn, sql, args.statement_timeout_ms)
                # This only gives binary search a bracket hint. It never
                # supplies a label and is always checked on the live query.
                collection = {
                    "min_work_mem_kb": args.min_work_mem_kb,
                    "max_work_mem_mb": args.max_work_mem_mb,
                    "upper_max_work_mem_mb": args.upper_max_work_mem_mb,
                    "statement_timeout_ms": args.statement_timeout_ms,
                    "candidate_timeout_ms": args.candidate_timeout_ms,
                    "one_pass_max_batches": args.one_pass_max_batches,
                    "one_pass_temp_read_write_ratio": args.one_pass_temp_read_write_ratio,
                    "label_protocol": memory_bounds.LABEL_PROTOCOL,
                    "collection_mode": "native",
                    "boundary_hints_mb": {"cache": max(1.0, input_mb * 2.05), "one_pass": 0.0625},
                }
                bounds, error = memory_bounds.collect_cache_one_bounds(conn, sql, collection)
                if bounds is None:
                    raise RuntimeError(error or "native_bound_collection_failed")
                actual = memory_bounds.plan_features(static_payload)
                record.update(bounds)
                record["static_plan_payload"] = bounds["static_plan_payload"]
                record["coverage_actual"] = {
                    "plan_node_count": actual.get("plan_node_count"),
                    "sort_tuple_width": actual.get("sort_tuple_width"),
                    "sort_key_count": actual.get("sort_key_count"),
                    "sort_input_bytes": actual.get("sort_input_bytes"),
                    "sort_input_mb": input_mb,
                }
                record["collection_preflight"] = preflight
                record["elapsed_sec"] = round(time.monotonic() - started, 3)
                append_jsonl(bounds_path, record)
                accepted.append(record)
                print(
                    f"ap-shape-{size}gb: accepted {len(accepted)}/{args.target_count} "
                    f"input_mb={input_mb:.2f} cache={bounds['mem_cache_mb']} one={bounds['mem_one_pass_mb']}",
                    flush=True,
                )
            except Exception as exc:
                record.update({"error": str(exc), "elapsed_sec": round(time.monotonic() - started, 3)})
                append_jsonl(rejected_path, record)
                rejected.append(record)
                print(f"ap-shape-{size}gb: reject {ordinal}: {exc}", flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
    finally:
        conn.close()

    sft_path = args.output_root / "sft" / "by_target" / f"gsbench_ap_shape_{size}gb.jsonl"
    sft_path.parent.mkdir(parents=True, exist_ok=True)
    feature_conn = connect(instance, database, args)
    try:
        sft_path.write_text(
            "".join(json.dumps(scalar_two_bound_sample(feature_conn, row, row["static_plan_payload"]), ensure_ascii=False) + "\n"
                    for row in accepted[:args.target_count]),
            encoding="utf-8",
        )
    finally:
        feature_conn.close()
    return {"size_gb": size, "database": database, "accepted": len(accepted), "rejected": len(rejected),
            "output": str(sft_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instance", type=Path, default=ROOT / "runtime" / "instance.json")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--output-root", type=Path, default=ROOT / "output-memory-ap-shape-coverage")
    parser.add_argument("--sizes", nargs="+", type=int, choices=SIZES, default=list(SIZES))
    parser.add_argument("--target-count", type=int, default=6)
    parser.add_argument("--multipliers", nargs="+", type=float, default=list(DEFAULT_MULTIPLIERS))
    parser.add_argument("--min-work-mem-kb", type=int, default=64)
    parser.add_argument("--max-work-mem-mb", type=int, default=2048)
    parser.add_argument("--upper-max-work-mem-mb", type=int, default=4096)
    parser.add_argument("--statement-timeout-ms", type=int, default=180000)
    parser.add_argument("--candidate-timeout-ms", type=int, default=420000)
    parser.add_argument("--one-pass-max-batches", type=int, default=2)
    parser.add_argument("--one-pass-temp-read-write-ratio", type=float, default=1.25)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.target_count < 1:
        parser.error("target-count must be positive")
    multipliers = tuple(value for value in args.multipliers if value > 0)
    if not multipliers:
        parser.error("at least one positive multiplier is required")
    instance = json.loads(args.instance.read_text(encoding="utf-8"))
    args.output_root.mkdir(parents=True, exist_ok=True)
    reports = [collect_size(size, instance, args, multipliers) for size in args.sizes]
    all_rows: list[dict[str, Any]] = []
    for report in reports:
        all_rows.extend(read_jsonl(Path(report["output"])))
    all_path = args.output_root / "sft" / "all.jsonl"
    all_path.parent.mkdir(parents=True, exist_ok=True)
    all_path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in all_rows), encoding="utf-8")
    (args.output_root / "manifest.json").write_text(json.dumps({"rows": len(all_rows), "per_size": reports}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"rows": len(all_rows), "output": str(args.output_root)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
