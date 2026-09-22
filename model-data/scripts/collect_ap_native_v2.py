#!/usr/bin/env python3
"""Collect a native-v2 AP sort label and emit XGBoost evaluation JSONL.

The default statement mirrors the 400k-row sort used by the AMM acceptance
report.  Bounds are measured on the supplied native instance; the legacy AMM
grant labels are never reused.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import memory_bounds


DEFAULT_SQL = (
    "SELECT id, sort_key, payload FROM gsbench.sort_data "
    "WHERE dist_key BETWEEN 1 AND 112347 "
    "ORDER BY payload, sort_key DESC, id"
)


def scalar_instruction(bound: str) -> str:
    labels = {"cache": "cache-mode", "one_pass": "one-pass", "multi_pass": "multi-pass"}
    return (
        "You are an expert database memory-bound prediction assistant.\n\n"
        f"Estimate the work_mem boundary (in MB) for {labels[bound]} for the provided SQL query "
        "under the current database and system state.\n\n"
        "Return exactly one positive numeric value and no other text."
    )


def load_instance(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def feature_input(sql: str, features: dict[str, Any]) -> str:
    return (
        f"# SQL\n{sql}\n\n# Static plan features\n"
        f"{json.dumps(features['static_plan_features'], ensure_ascii=False, indent=2)}\n\n"
        f"# System and session state\n{json.dumps(features['runtime_state'], ensure_ascii=False, indent=2)}\n\n"
        f"# Related table and index size\n{json.dumps(features['relation_profile'], ensure_ascii=False, indent=2)}\n"
    )


def write_outputs(out_dir: Path, record: dict[str, Any], features: dict[str, Any], input_text: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for bound, key in (("cache", "mem_cache_mb"), ("one_pass", "mem_one_pass_mb"), ("multi_pass", "mem_multi_pass_mb")):
        value = record.get(key)
        if value is None:
            continue
        row = {
            "id": f"{record['query_id']}_{bound}",
            "instruction": scalar_instruction(bound),
            "input": input_text,
            "output": f"{float(value):.6f}",
            "sql": record["sql"],
            "dataset": record["workload"],
            "dbname": record["dbname"],
            "query_id": record["query_id"],
            "bound": bound,
            "gold_mb": float(value),
            "target": record["target"],
            "features": features,
            "plan_payload": record["static_plan_payload"],
            "schema_version": record["schema_version"],
            "label_protocol": record["label_protocol"],
            "label_semantics": record["label_semantics"],
        }
        (out_dir / f"ap_{bound}.jsonl").write_text(
            json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    (out_dir / "record.json").write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out_dir / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": record["schema_version"],
                "label_protocol": record["label_protocol"],
                "label_semantics": record["label_semantics"],
                "label_mode": record.get("label_mode", "three_bound_native"),
                "multi_pass_definition": record["multi_pass_definition"],
                "available_bounds": [
                    bound for bound, key in (("cache", "mem_cache_mb"), ("one_pass", "mem_one_pass_mb"), ("multi_pass", "mem_multi_pass_mb"))
                    if record.get(key) is not None
                ],
                "query_id": record["query_id"],
                "target": record["target"],
                "database": record["dbname"],
                "port": record["collection_preflight"].get("port", record.get("port")),
                "sql": record["sql"],
                "source": "gauss-amm-analysis-report.md",
                "legacy_amm_labels_excluded": True,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def collect(args: argparse.Namespace) -> dict[str, Any]:
    instance = load_instance(args.instance)
    size = str(args.size)
    database = args.database or instance["databases"][size]
    sql = args.sql.rstrip(";").strip()
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
            expected_schema=args.schema,
        )
        if args.analyze:
            preflight.update(memory_bounds.analyze_database(preflight_conn))
    finally:
        preflight_conn.close()
    preflight["port"] = instance["port"]

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
    started = time.monotonic()
    try:
        collector = memory_bounds.collect_three_bounds if args.label_mode == "three_bound" else memory_bounds.collect_cache_one_bounds
        bounds, error = collector(conn, sql, collection)
    finally:
        conn.close()
    if bounds is None:
        raise RuntimeError(f"AP bound collection failed: {error}")

    record = {
        "schema_version": memory_bounds.SCHEMA_VERSION,
        "label_protocol": memory_bounds.LABEL_PROTOCOL,
        "label_semantics": memory_bounds.LABEL_SEMANTICS,
        "multi_pass_definition": memory_bounds.MULTI_PASS_DEFINITION if args.label_mode == "three_bound" else None,
        "label_mode": args.label_mode,
        "collection_mode": "native",
        "target": args.target,
        "workload": "gsbench",
        "sf": None,
        "size_gb": int(args.size),
        "dbname": database,
        "schema": args.schema,
        "port": instance["port"],
        "query_id": args.query_id,
        "sql": sql,
        "elapsed_sec": round(time.monotonic() - started, 3),
        "collection_preflight": preflight,
    }
    record.update(bounds)
    # Features derive from the exact static plan used for the label search.
    feature_conn = memory_bounds.connect(db_config)
    try:
        features, input_text = memory_bounds.build_feature_payload(feature_conn, sql, record["static_plan_payload"])
    finally:
        feature_conn.close()
    record["features"] = features
    write_outputs(Path(args.output_dir), record, features, input_text)
    print(
        json.dumps(
            {
                "query_id": record["query_id"],
                "elapsed_sec": record["elapsed_sec"],
                "cache_mb": record["mem_cache_mb"],
                "one_pass_mb": record["mem_one_pass_mb"],
                "multi_pass_mb": record["mem_multi_pass_mb"],
                "output_dir": str(args.output_dir),
            },
            indent=2,
        )
    )
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instance", type=Path, required=True)
    parser.add_argument("--size", type=int, choices=(1, 2, 5, 10, 15, 20), default=20)
    parser.add_argument("--database")
    parser.add_argument("--schema", default="gsbench")
    parser.add_argument("--target", default="gsbench_report_native_v2")
    parser.add_argument("--query-id", default="gsbench_report_ap_native_v2")
    parser.add_argument("--sql", default=DEFAULT_SQL)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output-memory-xgb-v2" / "ap")
    parser.add_argument("--reuse-record", type=Path, help="Rewrite evaluation JSONL from an already collected record")
    parser.add_argument(
        "--label-mode",
        choices=("two_bound", "three_bound"),
        default="two_bound",
        help="Use two_bound for Sort-only APs that are one-pass even at min work_mem",
    )
    parser.add_argument("--analyze", action="store_true")
    parser.add_argument("--min-work-mem-kb", type=int, default=64)
    parser.add_argument("--max-work-mem-mb", type=int, default=1024)
    parser.add_argument("--upper-max-work-mem-mb", type=int, default=4096)
    parser.add_argument("--statement-timeout-ms", type=int, default=300000)
    parser.add_argument("--candidate-timeout-ms", type=int, default=600000)
    parser.add_argument("--one-pass-max-batches", type=int, default=2)
    parser.add_argument("--one-pass-temp-read-write-ratio", type=float, default=1.25)
    parser.add_argument("--hint-probe-step-kb", type=int, default=256)
    args = parser.parse_args()
    if args.min_work_mem_kb < 1 or args.hint_probe_step_kb < 1:
        parser.error("memory and hint probe values must be positive")
    if args.reuse_record:
        record = json.loads(args.reuse_record.read_text(encoding="utf-8"))
        if record.get("label_protocol") != memory_bounds.LABEL_PROTOCOL or not record.get("features"):
            parser.error("--reuse-record must contain native-v2 labels and features")
        write_outputs(args.output_dir, record, record["features"], feature_input(record["sql"], record["features"]))
        print(json.dumps({"query_id": record["query_id"], "output_dir": str(args.output_dir), "reused": True}, indent=2))
        return 0
    collect(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
