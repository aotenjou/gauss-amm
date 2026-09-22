#!/usr/bin/env python3
"""Collect immutable native labels for the two AMM five-stage Sort APs."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import memory_bounds


FIVE_STAGE_APS = (
    {
        "id": "five_stage_sort_128mb",
        "range_start": 1,
        "range_end": 28_086,
        "requested_work_mem_mb": 128,
        "stage_scope": "stage1",
    },
    {
        "id": "five_stage_sort_512mb",
        "range_start": 1,
        "range_end": 112_347,
        "requested_work_mem_mb": 512,
        "stage_scope": "stage2_to_stage5",
    },
)


def five_stage_sql(range_start: int, range_end: int) -> str:
    return (
        "SELECT id, sort_key, payload FROM gsbench.sort_data "
        f"WHERE dist_key BETWEEN {range_start} AND {range_end} "
        "ORDER BY payload, sort_key DESC, id"
    )


def validate_ap_shape(payload: dict[str, Any]) -> dict[str, Any]:
    features = memory_bounds.plan_features(payload)
    expected = {"sort_nodes": 1.0, "sort_tuple_width": 532.0, "sort_key_count": 3.0}
    actual = {name: features.get(name) for name in expected}
    if actual != expected:
        raise RuntimeError(f"unexpected_five_stage_plan_shape: expected={expected} actual={actual}")
    if float(features.get("sort_input_rows") or 0.0) <= 0:
        raise RuntimeError("five_stage_plan_has_no_sort_input_rows")
    return features


def collection_settings(args: argparse.Namespace, static_features: dict[str, Any]) -> dict[str, Any]:
    input_mb = float(static_features["sort_input_bytes"]) / (1024.0 * 1024.0)
    return {
        "min_work_mem_kb": args.min_work_mem_kb,
        "max_work_mem_mb": args.max_work_mem_mb,
        "upper_max_work_mem_mb": args.upper_max_work_mem_mb,
        "statement_timeout_ms": args.statement_timeout_ms,
        "candidate_timeout_ms": args.candidate_timeout_ms,
        "one_pass_max_batches": args.one_pass_max_batches,
        "one_pass_temp_read_write_ratio": args.one_pass_temp_read_write_ratio,
        "label_protocol": memory_bounds.LABEL_PROTOCOL,
        "collection_mode": "native",
        # The hint is only a live-probed search bracket, never a copied label.
        "boundary_hints_mb": {"cache": max(1.0, input_mb * 2.1), "one_pass": 0.0625},
    }


def collect_case(conn, case: dict[str, Any], args: argparse.Namespace,
                 preflight: dict[str, Any]) -> dict[str, Any]:
    sql = five_stage_sql(int(case["range_start"]), int(case["range_end"]))
    static_payload = memory_bounds.explain_static(conn, sql, args.statement_timeout_ms)
    static_features = validate_ap_shape(static_payload)
    bounds, error = memory_bounds.collect_cache_one_bounds(
        conn, sql, collection_settings(args, static_features)
    )
    if bounds is None:
        raise RuntimeError(f"{case['id']}: {error or 'native_bound_collection_failed'}")
    if bounds.get("static_plan_payload") != static_payload:
        # Static EXPLAIN is deliberately repeated inside the generic collector.
        # Reject plan drift instead of silently mixing labels and features.
        repeated_features = validate_ap_shape(bounds["static_plan_payload"])
        if repeated_features != static_features:
            raise RuntimeError(f"{case['id']}: static_plan_changed_during_collection")
    cache_mb = float(bounds["mem_cache_mb"])
    one_pass_mb = float(bounds["mem_one_pass_mb"])
    return {
        "schema_version": memory_bounds.SCHEMA_VERSION,
        "label_protocol": memory_bounds.LABEL_PROTOCOL,
        "label_semantics": "native_executor_boundaries",
        "collection_mode": "native",
        "target": "five_stage_ap_holdout",
        "frozen": True,
        "query_id": case["id"],
        "id": case["id"],
        "stage_scope": case["stage_scope"],
        "requested_work_mem_mb": case["requested_work_mem_mb"],
        "range": {"start": case["range_start"], "end": case["range_end"]},
        "dataset": "gsbench",
        "workload": "gsbench",
        "dbname": args.database,
        "sql": sql,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "static_plan_payload": bounds["static_plan_payload"],
        "static_plan_hash": hashlib.sha256(
            json.dumps(bounds["static_plan_payload"], sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "features": {"static_plan_features": memory_bounds.plan_features(bounds["static_plan_payload"])},
        "collection_preflight": preflight,
        "diagnostics": bounds["diagnostics"],
        "bounds": {
            "cache_mb": cache_mb,
            "one_pass_mb": one_pass_mb,
            "multi_pass_mb": None,
            "label_protocol": memory_bounds.LABEL_PROTOCOL,
            "label_semantics": "cache_and_one_pass",
        },
        # Keep the collector-native top-level names for existing tooling.
        "mem_cache_mb": cache_mb,
        "mem_one_pass_mb": one_pass_mb,
        "mem_multi_pass_mb": None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="/tmp/amm-stage5")
    parser.add_argument("--port", type=int, default=15436)
    parser.add_argument("--user", default="baiyutao")
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--schema", default="gsbench")
    parser.add_argument("--output", type=Path,
                        default=ROOT / "output-memory-five-stage-ap" / "frozen_native_bounds.jsonl")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--min-work-mem-kb", type=int, default=64)
    parser.add_argument("--max-work-mem-mb", type=int, default=2048)
    parser.add_argument("--upper-max-work-mem-mb", type=int, default=4096)
    parser.add_argument("--statement-timeout-ms", type=int, default=180000)
    parser.add_argument("--candidate-timeout-ms", type=int, default=420000)
    parser.add_argument("--one-pass-max-batches", type=int, default=2)
    parser.add_argument("--one-pass-temp-read-write-ratio", type=float, default=1.25)
    args = parser.parse_args()
    if args.output.exists() and not args.overwrite:
        raise SystemExit(f"refusing to overwrite frozen manifest: {args.output}; use --overwrite after review")
    if args.min_work_mem_kb < 1 or args.max_work_mem_mb < 1 or args.upper_max_work_mem_mb < args.max_work_mem_mb:
        parser.error("invalid work_mem search range")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    conn = memory_bounds.connect({
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "dbname": args.database,
        "connect_timeout": 10,
    })
    try:
        preflight = memory_bounds.session_preflight(conn, expected_database=args.database, expected_schema=args.schema)
        rows = [collect_case(conn, case, args, preflight) for case in FIVE_STAGE_APS]
    finally:
        conn.close()

    args.output.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    manifest = {
        "rows": len(rows),
        "output": str(args.output),
        "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
        "query_ids": [row["query_id"] for row in rows],
        "labels_mb": {row["query_id"]: row["bounds"] for row in rows},
    }
    manifest_path = args.output.with_suffix(".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
