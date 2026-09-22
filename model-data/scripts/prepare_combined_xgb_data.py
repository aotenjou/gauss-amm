#!/usr/bin/env python3
"""Normalize prior benchmark labels and gsbench labels for joint XGBoost training."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import memory_bounds

OLD_ROOT = ROOT / "data" / "legacy"
GSBENCH = ROOT / "output-memory-bounds-v2" / "sft" / "all.jsonl"
SORT_COVERAGE = ROOT / "output-memory-sort-coverage-v3" / "sft" / "all.jsonl"
WORKLOADS = ("tpch", "tpcds", "dsb", "job", "gsbench")


def optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if value == value and abs(value) != float("inf") else None


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.open(encoding="utf-8") if line.strip()]


def workload_for(row: dict[str, Any], fallback: str) -> str:
    value = str(row.get("workload") or row.get("dataset") or fallback).lower()
    for name in WORKLOADS:
        if name in value:
            return name
    return fallback


def scale_for(row: dict[str, Any]) -> float:
    for key in ("scale_factor", "sf", "size_gb"):
        value = optional_float(row.get(key))
        if value is not None and value > 0:
            return value
    text = str(row.get("dbname") or row.get("target") or "").lower()
    match = re.search(r"(?:^|[_-])s(?:f)?(\d+(?:\.\d+)?)(?:gb)?(?:$|[_-])", text)
    if match:
        return float(match.group(1))
    match = re.search(r"s(\d+(?:\.\d+)?)gb", text)
    return float(match.group(1)) if match else 1.0


def normalize_row(row: dict[str, Any], source_file: Path, fallback_workload: str) -> tuple[dict[str, Any] | None, str | None]:
    bounds = dict(row.get("bounds") or {})
    if not bounds:
        bounds = {
            "cache_mb": row.get("mem_cache_mb"),
            "one_pass_mb": row.get("mem_one_pass_mb"),
            "multi_pass_mb": row.get("mem_multi_pass_mb"),
        }
    cache = optional_float(bounds.get("cache_mb"))
    one = optional_float(bounds.get("one_pass_mb"))
    multi = optional_float(bounds.get("multi_pass_mb"))
    if cache is None or one is None or cache <= 0 or one <= 0:
        return None, "missing_or_invalid_cache_one_pass"
    if cache < one:
        return None, "invalid_bound_order"
    if multi is not None and (multi <= 0 or one < multi):
        return None, "invalid_multi_pass_bound"
    sql = str(row.get("sql") or "").strip()
    if not sql:
        return None, "missing_sql"
    workload = workload_for(row, fallback_workload)
    scale = scale_for(row)
    source_id = str(row.get("query_id") or row.get("id") or hashlib.sha1(sql.encode()).hexdigest()[:16])
    query_id = str(row.get("query_id") or row.get("id") or source_id)
    normalized = dict(row)
    # Older SFT files contain the EXPLAIN payload but predate the P0 feature
    # schema. Recompute the static block while the payload is still available,
    # then keep only the normalized features in the joint training file.
    plan_payload = row.get("plan_payload") or row.get("static_plan_payload")
    if isinstance(plan_payload, dict):
        features = dict(normalized.get("features") or {})
        static = dict(features.get("static_plan_features") or {})
        static.update(memory_bounds.plan_features(plan_payload))
        features["static_plan_features"] = static
        normalized["features"] = features
    source_protocol = str(row.get("label_protocol") or bounds.get("label_protocol") or "native-legacy")
    source_semantics = str(row.get("label_semantics") or bounds.get("label_semantics") or "")
    # A numeric multi-pass field is only an observed boundary when it came
    # from the native-v2 three-bound collector. Legacy rows often contain a
    # compatibility value derived from one_pass_mb and must remain marked as
    # derived even though the numeric field is present.
    multi_observed = bool(
        multi is not None
        and source_protocol == "native-v2"
        and source_semantics == "native_executor_boundaries"
    )
    multi_status = "observed" if multi_observed else "derived" if multi is not None else "missing"
    normalized.update({
        "query_id": query_id,
        "id": normalized.get("id", query_id),
        "sql": sql,
        "dataset": workload,
        "workload": workload,
        "scale_factor": scale,
        "source_file": str(source_file),
        "source_query_id": source_id,
        "source_label_protocol": source_protocol,
        "source_label_semantics": source_semantics or None,
        "label_protocol": "native-v2",
        "schema_version": "memory-bounds-v2",
        "label_semantics": "native_executor_boundaries" if multi_observed else "cache_and_one_pass",
        "multi_pass_status": multi_status,
        "bounds": {
            "cache_mb": cache,
            "one_pass_mb": one,
            "multi_pass_mb": multi,
            "label_protocol": "native-v2",
            "label_semantics": "native_executor_boundaries" if multi_observed else "cache_and_one_pass",
        },
    })
    normalized["_sql_sha256"] = str(row.get("sql_sha256") or hashlib.sha256(sql.encode()).hexdigest())
    normalized["_template"] = hashlib.sha1(re.sub(r"\s+", " ", sql.lower()).strip().encode()).hexdigest()[:16]
    normalized["_multi_pass_observed"] = multi_observed
    normalized["_multi_pass_derived"] = bool(multi is not None and not multi_observed)
    return normalized, None


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def split_groups(rows: list[dict[str, Any]], seed: int) -> dict[str, list[dict[str, Any]]]:
    import numpy as np

    groups = sorted({row["_template"] for row in rows})
    rng = np.random.default_rng(seed)
    rng.shuffle(groups)
    n = len(groups)
    train_end = max(1, int(round(n * 0.70)))
    valid_end = max(train_end + 1, int(round(n * 0.85))) if n > 2 else train_end
    assignment = {group: "train" if i < train_end else "validation" if i < valid_end else "test" for i, group in enumerate(groups)}
    return {name: [row for row in rows if assignment[row["_template"]] == name] for name in ("train", "validation", "test")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-root", type=Path, default=OLD_ROOT)
    parser.add_argument("--gsbench", type=Path, default=GSBENCH)
    parser.add_argument("--coverage", type=Path, action="append", default=[],
                        help="Optional native two-bound coverage JSONL; repeat to combine independent coverage sets")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output-memory-xgb-combined-v2" / "data")
    parser.add_argument("--seed", type=int, default=20260906)
    parser.add_argument("--observed-only", action="store_true",
                        help="Keep only native-v2 rows with an observed multi-pass boundary")
    args = parser.parse_args()
    sources = [
        ("tpch", args.old_root / "tpch_sf1.jsonl"),
        ("tpcds", args.old_root / "tpcds_sf1.jsonl"),
        ("dsb", args.old_root / "dsb_sf1.jsonl"),
        ("job", args.old_root / "job.jsonl"),
        ("gsbench", args.gsbench),
    ]
    for coverage_path in args.coverage:
        sources.append(("gsbench", coverage_path))
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    source_stats: dict[str, Counter[str]] = defaultdict(Counter)
    for workload, path in sources:
        if not path.exists():
            raise FileNotFoundError(path)
        for index, row in enumerate(read_jsonl(path)):
            normalized, reason = normalize_row(row, path, workload)
            if normalized is None:
                rejected.append({"source_file": str(path), "row_index": index, "reason": reason})
                source_stats[workload][f"rejected:{reason}"] += 1
                continue
            accepted.append(normalized)
            source_stats[workload]["accepted"] += 1

    # Deduplicate exact SQL/workload/scale points while preferring gsbench and
    # complete three-bound records over legacy two-bound records.
    best: dict[tuple[str, str, float], dict[str, Any]] = {}
    duplicates: list[dict[str, Any]] = []
    for row in accepted:
        key = (row["_sql_sha256"], row["workload"], float(row["scale_factor"]))
        current = best.get(key)
        rank = (row["workload"] == "gsbench", row["_multi_pass_observed"])
        current_rank = ((current or {}).get("workload") == "gsbench", (current or {}).get("_multi_pass_observed", False))
        if current is None or rank > current_rank:
            if current is not None:
                duplicates.append({"kept": row["query_id"], "dropped": current["query_id"], "key": list(key)})
            best[key] = row
        else:
            duplicates.append({"kept": current["query_id"], "dropped": row["query_id"], "key": list(key)})
    rows = list(best.values())
    if args.observed_only:
        rows = [row for row in rows if row.get("_multi_pass_observed")]
    rows.sort(key=lambda row: (row["workload"], float(row["scale_factor"]), row["query_id"]))
    counts = Counter(row["workload"] for row in rows)
    n_workloads = max(len(counts), 1)
    for row in rows:
        row["sample_weight"] = len(rows) / (n_workloads * counts[row["workload"]])
        row.pop("_template", None)
        row.pop("_sql_sha256", None)
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    write_jsonl(output / "combined.jsonl", rows)
    write_jsonl(output / "combined_three_bound.jsonl", [row for row in rows if row["bounds"].get("multi_pass_mb") is not None])
    splits = split_groups([{**row, "_template": hashlib.sha1(re.sub(r"\s+", " ", row["sql"].lower()).strip().encode()).hexdigest()[:16]} for row in rows], args.seed)
    for name, split_rows in splits.items():
        write_jsonl(output / f"{name}.jsonl", split_rows)
    (output / "rejected_rows.jsonl").write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rejected), encoding="utf-8")
    (output / "duplicate_rows.jsonl").write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in duplicates), encoding="utf-8")
    manifest = {
        "schema_version": "memory-bounds-v2",
        "label_protocol": "native-v2",
        "rows": len(rows),
        "three_bound_rows": sum(row["bounds"].get("multi_pass_mb") is not None for row in rows),
        "observed_three_bound_rows": sum(bool(row.get("_multi_pass_observed")) for row in rows),
        "derived_multi_pass_rows": sum(bool(row.get("_multi_pass_derived")) for row in rows),
        "missing_multi_pass_rows": sum(row["bounds"].get("multi_pass_mb") is None for row in rows),
        "observed_only": bool(args.observed_only),
        "split_rows": {name: len(value) for name, value in splits.items()},
        "workload_counts": dict(sorted(counts.items())),
        "source_stats": {key: dict(value) for key, value in sorted(source_stats.items())},
        "rejected_rows": len(rejected),
        "duplicate_rows": len(duplicates),
        "sources": [str(path) for _, path in sources],
        "seed": args.seed,
        "sample_weight": "inverse_workload_frequency_normalized_to_mean_one",
        "sha256": hashlib.sha256((output / "combined.jsonl").read_bytes()).hexdigest(),
    }
    (output / "dataset_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
