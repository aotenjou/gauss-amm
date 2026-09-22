#!/usr/bin/env python3
"""Collect filtered native one-pass intervals for all gsbench sizes.

Candidates are read from the latest joint dataset first.  Deterministic Sort
coverage candidates are appended when a size lacks enough usable SQL.  Every
boundary is measured on the official non-AMM openGauss instance; old labels
are never used as results.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import time
from itertools import chain
from pathlib import Path
from typing import Any, Iterable

import memory_bounds
from generate_sort_coverage import candidate_stream, make_sql, normalize_sql, query_max_id


ROOT = Path(__file__).resolve().parent
SIZES = (1, 2, 5, 10, 15, 20)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def deduplicate_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the first record for each SQL hash after an interrupted append."""
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        sql = str(row.get("sql") or "").strip().rstrip(";")
        key = str(row.get("sql_sha256") or digest_sql(sql))
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()


def digest_sql(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def size_value(row: dict[str, Any]) -> int | None:
    value = row.get("scale_factor")
    if value is None:
        value = row.get("size_gb")
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def template_name(row: dict[str, Any]) -> str:
    ident = str(row.get("id") or row.get("query_id") or "")
    if "sort_coverage" in ident:
        return "sort_coverage"
    if "ap_shape" in ident:
        return "ap_shape"
    if "_q" in ident:
        return "wide_template"
    return str(row.get("target") or row.get("workload") or "joint")


def joint_candidates(path: Path, size: int) -> Iterable[dict[str, Any]]:
    rows = [row for row in read_jsonl(path) if row.get("workload") == "gsbench" and size_value(row) == size]
    # Stable interleaving preserves representation from each source family.
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        sql = str(row.get("sql") or "").strip()
        if not sql:
            continue
        buckets.setdefault(template_name(row), []).append(row)
    while buckets:
        for key in list(buckets):
            bucket = buckets[key]
            if not bucket:
                buckets.pop(key)
                continue
            row = bucket.pop(0)
            yield {"sql": row["sql"], "template": key, "source": row.get("id") or row.get("query_id")}


def sort_candidates(conn, size: int, seed: int) -> Iterable[dict[str, Any]]:
    max_id = query_max_id(conn)
    for target in candidate_stream(max_id, 200, seed + size, sort_key_count=1):
        rows = min(int(target["rows_target"]), max_id)
        sql = normalize_sql(make_sql(target["width_target"], rows, target["node_target"],
                                     target["start"], target["variant"], sort_key_count=1))
        if sql:
            yield {"sql": sql, "template": "sort_coverage", "coverage_target": target, "source": "deterministic_sort"}


def write_sft(path: Path, rows: list[dict[str, Any]], db_config: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = memory_bounds.connect(db_config)
    try:
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                features, input_text = memory_bounds.build_feature_payload(conn, row["sql"], row["static_plan_payload"])
                sample = {
                    "schema_version": memory_bounds.SCHEMA_VERSION,
                    "label_protocol": memory_bounds.LABEL_PROTOCOL,
                    "label_semantics": "one_pass_execution_interval",
                    "id": row["query_id"],
                    "instruction": "Estimate [one_pass_lower_mb,one_pass_upper_mb] and return exactly two positive numeric values.",
                    "input": input_text,
                    "output": json.dumps([row["one_pass_lower_mb"], row["one_pass_upper_mb"]], separators=(",", ":")),
                    "sql": row["sql"], "dataset": "gsbench", "workload": "gsbench",
                    "size_gb": row["size_gb"], "dbname": row["dbname"], "query_id": row["query_id"],
                    "features": features, "plan_payload": row["static_plan_payload"],
                    "bounds": {"one_pass_lower_mb": row["one_pass_lower_mb"], "one_pass_upper_mb": row["one_pass_upper_mb"],
                               "cache_mb": row["mem_cache_mb"], "one_pass_mb": row["mem_one_pass_mb"],
                               "label_semantics": "one_pass_execution_interval"},
                }
                handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
    finally:
        conn.close()


def collect_size(size: int, args: argparse.Namespace, instance: dict[str, Any], joint_path: Path) -> None:
    database = instance["databases"][str(size)]
    output = args.output_root / f"gsbench-{size}gb"
    output.mkdir(parents=True, exist_ok=True)
    bounds_path, rejected_path = output / "bounds.jsonl", output / "rejected.jsonl"
    accepted = deduplicate_records(read_jsonl(bounds_path)) if args.resume else []
    rejected = read_jsonl(rejected_path) if args.resume else []
    if args.resume and bounds_path.exists():
        original_count = sum(1 for line in bounds_path.open(encoding="utf-8") if line.strip())
        if original_count != len(accepted):
            bounds_path.write_text("".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in accepted), encoding="utf-8")
    if not args.resume and (accepted or rejected):
        raise RuntimeError(f"{output} contains data; use --resume or a new output root")
    seen = {row.get("sql_sha256") for row in accepted + rejected if row.get("sql_sha256")}
    db_config = {"host": args.host or instance.get("host") or instance.get("socket_dir", "127.0.0.1"),
                 "port": args.port or instance["port"], "user": instance["user"], "dbname": database}
    pre_conn = memory_bounds.connect(db_config)
    try:
        preflight = memory_bounds.session_preflight(pre_conn, expected_database=database,
                                                     expected_schema=instance.get("schema", "gsbench"))
        if not args.skip_analyze:
            preflight.update(memory_bounds.analyze_database(pre_conn))
        if preflight.get("gs_amm_native_auto_mode") not in {"off", "false", "0", "unavailable_native_baseline"}:
            raise RuntimeError(f"non-native collection refused: AMM mode={preflight.get('gs_amm_native_auto_mode')}")
    finally:
        pre_conn.close()
    collection = {"min_work_mem_kb": args.min_work_mem_kb, "max_work_mem_mb": args.max_work_mem_mb,
                  "upper_max_work_mem_mb": args.upper_max_work_mem_mb, "statement_timeout_ms": args.statement_timeout_ms,
                  "candidate_timeout_ms": args.candidate_timeout_ms, "one_pass_max_batches": args.one_pass_max_batches,
                  "one_pass_temp_read_write_ratio": args.one_pass_temp_read_write_ratio,
                  "label_protocol": memory_bounds.LABEL_PROTOCOL, "collection_mode": "native"}
    conn = memory_bounds.connect(db_config)
    try:
        candidates = chain(joint_candidates(joint_path, size), sort_candidates(conn, size, args.seed))
        index = len(accepted) + len(rejected)
        for candidate in candidates:
            if len(accepted) >= args.target_count or index >= args.max_candidates:
                break
            sql = str(candidate["sql"]).strip()
            digest = digest_sql(sql.rstrip(";"))
            if digest in seen:
                continue
            seen.add(digest)
            index += 1
            started = time.monotonic()
            bounds, error = memory_bounds.collect_onepass_bounds(conn, sql, collection)
            elapsed = round(time.monotonic() - started, 3)
            base = {"schema_version": memory_bounds.SCHEMA_VERSION, "label_protocol": memory_bounds.LABEL_PROTOCOL,
                    "label_semantics": "one_pass_execution_interval", "label_mode": "one_pass_bounds_native",
                    "collection_mode": "native", "target": f"gsbench_{size}gb", "workload": "gsbench",
                    "size_gb": size, "dbname": database, "schema": instance.get("schema", "gsbench"),
                    "sql": sql, "sql_sha256": digest, "template": candidate.get("template"),
                    "source_query_id": candidate.get("source"), "coverage_target": candidate.get("coverage_target"),
                    "elapsed_sec": elapsed, "collection_preflight": preflight}
            if bounds is None:
                base.update({"error": error or "unknown", "stage": "bounds"})
                append_jsonl(rejected_path, base)
                rejected.append(base)
                continue
            lower = float(bounds["one_pass_lower_mb"])
            if not math.isfinite(lower) or lower < args.min_one_pass_mb:
                base.update({"error": f"one_pass_lower_below_{args.min_one_pass_mb:g}mb", "stage": "filter",
                             "one_pass_lower_mb": lower, "one_pass_upper_mb": bounds["one_pass_upper_mb"]})
                append_jsonl(rejected_path, base)
                rejected.append(base)
                continue
            base.update(bounds)
            base["query_id"] = f"gsbench_{size}gb_onepass_q{len(accepted)+1:03d}"
            base["selection_rank"] = len(accepted) + 1
            append_jsonl(bounds_path, base)
            accepted.append(base)
            print(f"gsbench-{size}gb accepted {len(accepted)}/{args.target_count} template={base['template']} lower={lower:.3f} upper={base['one_pass_upper_mb']:.3f}", flush=True)
    finally:
        conn.close()
    if len(accepted) < args.target_count:
        raise RuntimeError(f"{database}: accepted {len(accepted)}/{args.target_count}; candidates={len(accepted)+len(rejected)}")
    accepted = accepted[:args.target_count]
    generated_path = output / "generated.sql"
    generated_path.write_text(
        "-- Native openGauss one-pass interval samples\n\n" +
        "\n\n".join(f"-- Query {i}\n{row['sql'].rstrip(';')};" for i, row in enumerate(accepted, 1)) +
        "\n", encoding="utf-8")
    write_sft(args.output_root / "sft" / "by_target" / f"gsbench_{size}gb.jsonl", accepted, db_config)
    (output / "manifest.json").write_text(json.dumps({"size_gb": size, "database": database, "target_count": args.target_count,
        "valid_sql_count": len(accepted), "rejected_candidate_count": len(rejected), "min_one_pass_mb": args.min_one_pass_mb,
        "label_mode": "one_pass_bounds_native", "label_semantics": "one_pass_execution_interval",
        "collection_preflight": preflight}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instance", type=Path, default=ROOT / "runtime" / "instance.json")
    parser.add_argument("--joint-data", type=Path, default=ROOT / "output-memory-xgb-combined-v6/data/combined.jsonl")
    parser.add_argument("--output-root", type=Path, default=ROOT / "output-memory-onepass-gsbench-v1")
    parser.add_argument("--sizes", nargs="+", type=int, choices=SIZES, default=list(SIZES))
    parser.add_argument("--target-count", type=int, default=200)
    parser.add_argument("--min-one-pass-mb", type=float, default=4.0)
    parser.add_argument("--max-candidates", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=20260912)
    parser.add_argument("--host", default=None); parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--min-work-mem-kb", type=int, default=64); parser.add_argument("--max-work-mem-mb", type=int, default=1024)
    parser.add_argument("--upper-max-work-mem-mb", type=int, default=4096); parser.add_argument("--statement-timeout-ms", type=int, default=200000)
    parser.add_argument("--candidate-timeout-ms", type=int, default=200000); parser.add_argument("--one-pass-max-batches", type=int, default=2)
    parser.add_argument("--one-pass-temp-read-write-ratio", type=float, default=1.25)
    parser.add_argument("--skip-analyze", action="store_true"); parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    args.output_root.mkdir(parents=True, exist_ok=True)
    instance = json.loads(args.instance.read_text(encoding="utf-8"))
    for size in args.sizes:
        collect_size(size, args, instance, args.joint_data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
