#!/usr/bin/env python3
"""Re-collect native-v2 bounds for an existing SQL candidate set.

This deliberately reuses SQL text only. Labels, plans, relation sizes and
runtime provenance are measured again on the selected v2 instance, so an old
v1 bounds file is never treated as ground truth.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any

import memory_bounds


ROOT = Path(__file__).resolve().parent
SIZES = (1, 2, 5, 10, 15, 20)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.open(encoding="utf-8") if line.strip()]


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()


def target_name(size: int) -> str:
    return f"gsbench_{size}gb"


def write_generated_sql(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        handle.write(f"-- Re-collected {len(records)} memory-bound SQL queries\n")
        handle.write("-- Labels and plans were freshly measured under native-v2.\n\n")
        for number, record in enumerate(records, start=1):
            handle.write(f"-- Query {number}\n{record['sql'].rstrip(';')};\n\n")


def write_sft(path: Path, records: list[dict[str, Any]], db_config: dict[str, Any]) -> None:
    conn = memory_bounds.connect(db_config)
    try:
        with path.open("w", encoding="utf-8") as handle:
            for index, record in enumerate(records, start=1):
                payload = record.get("static_plan_payload")
                if not payload:
                    raise RuntimeError(f"missing static plan payload for {record.get('query_id')}")
                sample = memory_bounds.build_sft_sample(conn, record, payload)
                handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
                if index % 10 == 0 or index == len(records):
                    print(f"{record['target']}: wrote SFT {index}/{len(records)}", flush=True)
    finally:
        conn.close()


def write_manifest(
    path: Path,
    size: int,
    instance: dict[str, Any],
    source_path: Path,
    accepted: int,
    rejected: int,
    args: argparse.Namespace,
    preflight: dict[str, Any],
) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": memory_bounds.SCHEMA_VERSION,
                "label_protocol": memory_bounds.LABEL_PROTOCOL,
                "collection_mode": "native",
                "size_gb": size,
                "database": instance["databases"][str(size)],
                "schema": instance.get("schema", "gsbench"),
                "port": instance["port"],
                "server_version": instance.get("version"),
                "target_count": args.target_count,
                "source_candidates": str(source_path),
                "valid_sql_count": accepted,
                "rejected_candidate_count": rejected,
                "label_mode": "three_bound",
                "label_semantics": memory_bounds.LABEL_SEMANTICS,
                "multi_pass_definition": memory_bounds.MULTI_PASS_DEFINITION,
                "collection_preflight": preflight,
                "min_work_mem_kb": args.min_work_mem_kb,
                "max_work_mem_mb": args.max_work_mem_mb,
                "upper_max_work_mem_mb": args.upper_max_work_mem_mb,
                "statement_timeout_ms": args.statement_timeout_ms,
                "candidate_timeout_ms": args.candidate_timeout_ms,
                "hint_probe_step_kb": args.hint_probe_step_kb,
                "source_labels_reused": False,
                "repeat_count": args.repeat_count,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def recollect(size: int, instance: dict[str, Any], args: argparse.Namespace) -> None:
    database = instance["databases"][str(size)]
    source_path = Path(args.source_root) / f"gsbench-{size}gb" / "bounds.jsonl"
    source_rows = read_jsonl(source_path)
    if len(source_rows) < args.target_count:
        raise RuntimeError(f"{source_path}: found {len(source_rows)} candidates, need {args.target_count}")
    source_rows = source_rows[: args.target_count]

    output_dir = Path(args.output_root) / f"gsbench-{size}gb"
    output_dir.mkdir(parents=True, exist_ok=True)
    bounds_path = output_dir / "bounds.jsonl"
    rejected_path = output_dir / "rejected.jsonl"
    generated_path = output_dir / "generated.sql"
    sft_path = Path(args.output_root) / "sft" / "by_target" / f"{target_name(size)}.jsonl"
    sft_path.parent.mkdir(parents=True, exist_ok=True)

    accepted = read_jsonl(bounds_path) if args.resume else []
    rejected = read_jsonl(rejected_path) if args.resume else []
    if not args.resume and (accepted or rejected):
        raise RuntimeError(f"{output_dir} already contains data; use --resume or a new output root")
    seen = {row.get("sql_sha256") for row in accepted + rejected if row.get("sql_sha256")}

    db_config = {
        "host": instance.get("socket_dir", "127.0.0.1"),
        "port": instance["port"],
        "user": instance["user"],
        "dbname": database,
    }
    preflight_conn = memory_bounds.connect(db_config)
    try:
        preflight = memory_bounds.session_preflight(
            preflight_conn,
            expected_database=database,
            expected_schema=instance.get("schema", "gsbench"),
        )
        if args.skip_analyze:
            preflight.update({"analyze_status": "skipped_resume"})
        else:
            preflight.update(memory_bounds.analyze_database(preflight_conn))
            if preflight.get("analyze_status") != "completed":
                raise RuntimeError(f"ANALYZE failed: {preflight.get('analyze_error', 'unknown error')}")
    finally:
        preflight_conn.close()

    collection = {
        "min_work_mem_kb": args.min_work_mem_kb,
        "max_work_mem_mb": args.max_work_mem_mb,
        "upper_max_work_mem_mb": args.upper_max_work_mem_mb,
        "statement_timeout_ms": args.statement_timeout_ms,
        "candidate_timeout_ms": args.candidate_timeout_ms,
        "one_pass_max_batches": args.one_pass_max_batches,
        "one_pass_temp_read_write_ratio": args.one_pass_temp_read_write_ratio,
        "hint_probe_step_kb": args.hint_probe_step_kb,
        "label_protocol": memory_bounds.LABEL_PROTOCOL,
        "collection_mode": "native",
    }
    conn = memory_bounds.connect(db_config)
    try:
        print(
            f"{target_name(size)}: source={len(source_rows)} accepted={len(accepted)} "
            f"rejected={len(rejected)}",
            flush=True,
        )
        for source_index, source in enumerate(source_rows, start=1):
            sql = str(source.get("sql", "")).strip()
            digest = source.get("sql_sha256") or hashlib.sha256(sql.encode("utf-8")).hexdigest()
            if digest in seen:
                continue
            started = time.monotonic()
            probe_collection = dict(collection)
            # Prior v1 values narrow the search only; every hint is probed and
            # the returned labels come solely from the current v2 instance.
            probe_collection["boundary_hints_mb"] = {
                "cache": source.get("mem_cache_mb"),
                "one_pass": source.get("mem_one_pass_mb"),
            }
            bounds, error = memory_bounds.collect_three_bounds_repeated(
                conn, sql, probe_collection, repeat_count=args.repeat_count
            )
            elapsed = round(time.monotonic() - started, 3)
            candidate = {
                "schema_version": memory_bounds.SCHEMA_VERSION,
                "label_protocol": memory_bounds.LABEL_PROTOCOL,
                "label_semantics": memory_bounds.LABEL_SEMANTICS,
                "multi_pass_definition": memory_bounds.MULTI_PASS_DEFINITION,
                "collection_mode": "native",
                "target": target_name(size),
                "workload": "gsbench",
                "sf": None,
                "size_gb": size,
                "dbname": database,
                "schema": instance.get("schema", "gsbench"),
                "source_v1_query_id": source.get("query_id"),
                "source_query_id": f"source_{source_index:03d}",
                "query_id": f"{target_name(size)}_q{len(accepted) + 1:03d}",
                "selection_rank": len(accepted) + 1,
                "sql": sql,
                "sql_sha256": digest,
                "elapsed_sec": elapsed,
                "collection_preflight": preflight,
            }
            if bounds is None:
                candidate.update({"error": error, "stage": "bounds", "requirement": source.get("requirement")})
                append_jsonl(rejected_path, candidate)
                rejected.append(candidate)
                print(f"{target_name(size)}: reject source {source_index}: {error}", flush=True)
                continue
            candidate.update(bounds)
            candidate["requirement"] = source.get("requirement")
            append_jsonl(bounds_path, candidate)
            accepted.append(candidate)
            seen.add(digest)
            print(
                f"{target_name(size)}: accepted {len(accepted)}/{args.target_count} "
                f"({elapsed}s) cache={bounds['mem_cache_mb']} one={bounds['mem_one_pass_mb']} "
                f"multi={bounds['mem_multi_pass_mb']}",
                flush=True,
            )
            if len(accepted) >= args.target_count:
                break
    finally:
        conn.close()

    if len(accepted) != args.target_count:
        raise RuntimeError(
            f"{target_name(size)}: reached {len(accepted)}/{args.target_count}; "
            f"rejected={len(rejected)}"
        )
    write_generated_sql(generated_path, accepted)
    write_sft(sft_path, accepted, db_config)
    write_manifest(output_dir / "manifest.json", size, instance, source_path, len(accepted), len(rejected), args, preflight)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instance", type=Path, required=True)
    parser.add_argument("--port", type=int, default=None,
                        help="Override the instance port when the service moved after restart")
    parser.add_argument("--source-root", type=Path, default=ROOT / "output-memory-bounds")
    parser.add_argument("--output-root", type=Path, default=ROOT / "output-memory-bounds-v2")
    parser.add_argument("--size", type=int, choices=SIZES, required=True)
    parser.add_argument("--target-count", type=int, default=200)
    parser.add_argument("--min-work-mem-kb", type=int, default=64)
    parser.add_argument("--max-work-mem-mb", type=int, default=1024)
    parser.add_argument("--upper-max-work-mem-mb", type=int, default=4096)
    parser.add_argument("--statement-timeout-ms", type=int, default=200000)
    parser.add_argument("--candidate-timeout-ms", type=int, default=200000)
    parser.add_argument("--one-pass-max-batches", type=int, default=2)
    parser.add_argument("--one-pass-temp-read-write-ratio", type=float, default=1.25)
    parser.add_argument("--repeat-count", type=int, default=3,
                        help="Repeat each native boundary search and take the median")
    parser.add_argument(
        "--hint-probe-step-kb",
        type=int,
        default=256,
        help="Initial KB bracket around a prior boundary hint (labels are always re-probed)",
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument(
        "--skip-analyze",
        action="store_true",
        help="Skip the statistics scan when resuming a target already analyzed",
    )
    args = parser.parse_args()
    if args.target_count < 1 or args.repeat_count < 1:
        parser.error("--target-count and --repeat-count must be positive")
    instance = json.loads(args.instance.read_text(encoding="utf-8"))
    if args.port is not None:
        instance["port"] = args.port
    recollect(args.size, instance, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
