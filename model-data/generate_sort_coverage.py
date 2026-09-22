#!/usr/bin/env python3
"""Generate and label large-sort SQLs with explicit plan-feature coverage.

The original memory generator intentionally accepted only a wide hash-join
shape.  This entry point targets the complementary sort regime: it creates
deterministic SQL candidates, measures their actual EXPLAIN features, and
collects native cache/one-pass boundaries.  Candidate targets are hints only;
the recorded feature values always come from the database plan.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import memory_bounds


ROOT = Path(__file__).resolve().parent
SIZES = (1, 2, 5, 10, 15, 20)
WIDTH_TARGETS = (64, 128, 256, 384, 512, 768, 1024)
BYTE_TARGET_MB = (1, 8, 32, 64, 128, 256, 512, 1024)
NODE_TARGETS = tuple(range(1, 11))
BYTE_BINS = ((0, 1), (1, 16), (16, 64), (64, 128), (128, 256), (256, 512), (512, 1024), (1024, math.inf))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()


def load_api_key(path: Path) -> None:
    if not path.exists():
        return
    first = next((line.strip() for line in path.read_text(encoding="utf-8").splitlines()
                  if line.strip() and not line.lstrip().startswith("#")), "")
    if "=" in first:
        name, value = first.split("=", 1)
        os.environ.setdefault(name.strip(), value.strip().strip("'\""))
    elif first:
        os.environ.setdefault("OPENAI_API_KEY", first)


def normalize_sql(sql: str) -> str | None:
    text = str(sql or "").strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    if not text or ";" in text or "{{" in text or "}}" in text:
        return None
    if not re.match(r"^(?:with\b.*?\bselect\b|select\b)", text, re.IGNORECASE | re.DOTALL):
        return None
    return text + ";"


def payload_expr(width: int) -> str:
    """Build a planner-visible projection for the requested sort width.

    openGauss estimates ``varchar(512)`` at 516 bytes and prunes repeated
    expressions, so duplicate aliases cannot widen a Sort node.  Distinct
    base columns are retained here; large targets add a second payload via a
    self-join in :func:`make_sql` (see ``wide_join``).
    """
    target = max(8, min(int(width), 1024))
    if target > 544:
        return ("sd.payload AS sort_payload, sd.sort_key AS sort_key, sd.id AS row_id, "
                "sd.dist_key AS dist_key, sd.group_id AS group_id, "
                "sd2.payload AS sort_payload_extra")
    if target > 512:
        # Three projected columns reproduce the AP shape: 516-byte payload
        # plus two fixed-width keys, yielding a 532-byte Sort tuple.
        return "sd.payload AS sort_payload, sd.sort_key AS sort_key, sd.id AS row_id"
    if target > 32:
        return "sd.payload AS sort_payload"
    return "sd.sort_key AS sort_payload"


def make_sql(width: int, rows: int, layers: int, start: int, variant: int,
             sort_key_count: int = 1) -> str:
    expression = payload_expr(width)
    end = max(start + 1, start + int(rows))
    wide_join = (
        " JOIN gsbench.sort_data sd2 ON sd2.id = sd.id"
        if int(width) > 544 else ""
    )
    base = (
        f"SELECT {expression} FROM gsbench.sort_data sd{wide_join} "
        f"WHERE sd.id >= {start} AND sd.id < {end}"
    )
    order_by = "sort_payload"
    if sort_key_count >= 3 and width > 512:
        order_by = "sort_payload, sort_key DESC, row_id"
    if layers <= 2:
        return f"{base} ORDER BY {order_by}"
    # MATERIALIZED CTEs are used deliberately to preserve plan nodes. The
    # fallback behavior is handled by coverage auditing if a server inlines a
    # particular layer.
    ctes = [f"c1 AS MATERIALIZED ({base})"]
    for index in range(2, layers - 1):
        ctes.append(f"c{index} AS MATERIALIZED (SELECT * FROM c{index - 1})")
    source = f"c{layers - 2}"
    if variant % 3 == 1:
        body = f"SELECT *, row_number() OVER (ORDER BY sort_payload) AS rn FROM {source}"
    elif variant % 3 == 2:
        body = f"SELECT * FROM (SELECT DISTINCT * FROM {source}) d"
    else:
        body = f"SELECT * FROM {source}"
    return f"WITH {', '.join(ctes)} {body} ORDER BY {order_by}"


def byte_bin(value_mb: float) -> str:
    for low, high in BYTE_BINS:
        if low <= value_mb < high:
            high_text = "inf" if math.isinf(high) else str(high)
            return f"{low}-{high_text}"
    return "unknown"


def scalar_two_bound_sample(conn, record: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    features, input_text = memory_bounds.build_feature_payload(conn, record["sql"], payload)
    return {
        "schema_version": memory_bounds.SCHEMA_VERSION,
        "label_protocol": memory_bounds.LABEL_PROTOCOL,
        "label_semantics": "cache_and_one_pass",
        "id": record["query_id"],
        "instruction": (
            "You are an expert database memory-bound prediction assistant.\n\n"
            "Estimate [cache_mb,one_pass_mb] for the provided SQL under the current "
            "database and system state. Return exactly two positive numeric values."
        ),
        "input": input_text,
        "output": json.dumps([record["mem_cache_mb"], record["mem_one_pass_mb"]], separators=(",", ":")),
        "sql": record["sql"],
        "dataset": "gsbench",
        "workload": "gsbench",
        "target": record.get("target", "gsbench_sort_coverage"),
        "sf": None,
        "size_gb": record["size_gb"],
        "dbname": record["dbname"],
        "query_id": record["query_id"],
        "features": features,
        "plan_payload": payload,
        "bounds": {
            "cache_mb": record["mem_cache_mb"],
            "one_pass_mb": record["mem_one_pass_mb"],
            "multi_pass_mb": None,
            "label_protocol": memory_bounds.LABEL_PROTOCOL,
            "label_semantics": "cache_and_one_pass",
        },
        "coverage_target": record["coverage_target"],
        "coverage_actual": record["coverage_actual"],
    }


def query_max_id(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT coalesce(max(id), 0) FROM gsbench.sort_data")
        return int(cur.fetchone()[0] or 0)


def query_static(conn, sql: str, timeout_ms: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = %s", (f"{timeout_ms}ms",))
        cur.execute("EXPLAIN (FORMAT JSON, COSTS TRUE, VERBOSE FALSE) " + sql)
        return memory_bounds.normalize_plan(cur.fetchone()[0])


def candidate_stream(max_id: int, count: int, seed: int, sort_key_count: int = 1):
    rng = random.Random(seed)
    # A broad Cartesian grid is shuffled so interruption/resume still fills
    # all requested dimensions instead of spending the first batch on one bin.
    candidates = []
    width_targets = (532,) if sort_key_count >= 3 else WIDTH_TARGETS
    for width in width_targets:
        for bytes_mb in BYTE_TARGET_MB:
            rows = max(1, int(bytes_mb * 1024 * 1024 / max(width, 1)))
            for node_target in NODE_TARGETS:
                start_max = max(1, max_id - min(rows, max_id) + 1)
                start = rng.randint(1, start_max)
                candidates.append({
                    "width_target": width,
                    "bytes_target_mb": bytes_mb,
                    "node_target": node_target,
                    "rows_target": rows,
                    "start": start,
                    "variant": rng.randrange(3),
                })
    rng.shuffle(candidates)
    for item in candidates:
        yield item
    # Continue with random repeats if accepted count exceeds the base grid.
    while True:
        width = rng.choice(width_targets)
        bytes_mb = rng.choice(BYTE_TARGET_MB)
        rows = max(1, int(bytes_mb * 1024 * 1024 / max(width, 1)))
        start_max = max(1, max_id - min(rows, max_id) + 1)
        yield {
            "width_target": width,
            "bytes_target_mb": bytes_mb,
            "node_target": rng.choice(NODE_TARGETS),
            "rows_target": rows,
            "start": rng.randint(1, start_max),
            "variant": rng.randrange(3),
        }


def collect_size(size: int, instance: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    database = instance["databases"][str(size)]
    output_dir = Path(args.output_root) / f"gsbench-{size}gb"
    output_dir.mkdir(parents=True, exist_ok=True)
    bounds_path = output_dir / "bounds.jsonl"
    rejected_path = output_dir / "rejected.jsonl"
    sft_path = Path(args.output_root) / "sft" / "by_target" / f"gsbench_sort_coverage_{size}gb.jsonl"
    sft_path.parent.mkdir(parents=True, exist_ok=True)
    accepted = read_jsonl(bounds_path) if args.resume else []
    rejected = read_jsonl(rejected_path) if args.resume else []
    if not args.resume and (accepted or rejected):
        raise RuntimeError(f"{output_dir} already contains data; use --resume or a new output root")
    # Prefer an explicit TCP host when the instance metadata points at a
    # stale/missing Unix socket directory.  This also makes remote/local
    # collection reproducible without editing instance.json.
    db_config = {"host": args.host or instance.get("host") or instance.get("socket_dir", "127.0.0.1"),
                 "port": args.port or instance["port"],
                 "user": instance["user"], "dbname": database}
    preflight_conn = memory_bounds.connect(db_config)
    try:
        preflight = memory_bounds.session_preflight(preflight_conn, expected_database=database,
                                                     expected_schema=instance.get("schema", "gsbench"))
        if not args.skip_analyze:
            preflight.update(memory_bounds.analyze_database(preflight_conn))
        max_id = query_max_id(preflight_conn)
    finally:
        preflight_conn.close()
    if max_id < 1:
        raise RuntimeError(f"{database}: gsbench.sort_data is empty")
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
    }
    conn = memory_bounds.connect(db_config)
    seen = {row.get("sql_sha256") for row in accepted + rejected if row.get("sql_sha256")}
    candidate_index = len(accepted) + len(rejected)
    try:
        for target in candidate_stream(max_id, args.target_count, args.seed + size,
                                       sort_key_count=args.sort_key_count):
            if len(accepted) >= args.target_count or candidate_index >= args.max_candidates:
                break
            candidate_index += 1
            # The wide self-join used to expose >1 KiB Sort tuples, but it
            # contributes three extra plan nodes.  Cap its CTE depth so the
            # recorded plan remains inside the model's 1..10 node envelope.
            layer_target = target["node_target"]
            if target["width_target"] > 544:
                layer_target = min(layer_target, 7)
            rows_target = target["rows_target"]
            if target["width_target"] > 544:
                # The self-join needed for >544-byte tuples multiplies scan
                # work. Keep these rare OOD samples bounded; ordinary 516/544
                # byte sorts carry the 1 GiB input-size coverage.
                rows_target = min(rows_target, 262144)
            raw_sql = make_sql(target["width_target"], min(rows_target, max_id),
                               layer_target, target["start"], target["variant"],
                               sort_key_count=args.sort_key_count)
            sql = normalize_sql(raw_sql)
            digest = hashlib.sha256((sql or raw_sql).encode()).hexdigest()
            if digest in seen:
                continue
            seen.add(digest)
            candidate = {
                "schema_version": memory_bounds.SCHEMA_VERSION,
                "label_protocol": memory_bounds.LABEL_PROTOCOL,
                "label_semantics": "cache_and_one_pass",
                "collection_mode": "native",
                "label_mode": "two_bound_native",
                "target": "gsbench_sort_coverage",
                "workload": "gsbench",
                "dataset": "gsbench",
                "size_gb": size,
                "sf": None,
                "dbname": database,
                "schema": instance.get("schema", "gsbench"),
                "source_query_id": f"candidate_{candidate_index:06d}",
                "query_id": f"gsbench_sort_coverage_{size}gb_candidate_{candidate_index:06d}",
                "sql": sql or raw_sql,
                "sql_sha256": digest,
                "selection_rank": candidate_index,
                "coverage_target": target,
            }
            started = time.monotonic()
            if sql is None:
                bounds, error = None, "invalid_sql"
                static_payload = None
            else:
                try:
                    static_payload = query_static(conn, sql, args.statement_timeout_ms)
                    static_features = memory_bounds.plan_features(static_payload)
                    # A coverage row is useful only when the measured plan
                    # actually contains a Sort and exposes the two sort
                    # dimensions.  Index-order plans and DISTINCT plans that
                    # collapse to a single row must not become fake zero-byte
                    # coverage samples.
                    if (static_features.get("sort_nodes", 0) < 1
                            or static_features.get("sort_tuple_width") is None
                            or static_features.get("sort_input_bytes") is None
                            or static_features.get("plan_node_count", 0) > 10):
                        raise ValueError(
                            "coverage_plan_invalid: "
                            f"sort_nodes={static_features.get('sort_nodes')} "
                            f"width={static_features.get('sort_tuple_width')} "
                            f"bytes={static_features.get('sort_input_bytes')} "
                            f"nodes={static_features.get('plan_node_count')}"
                        )
                    bounds, error = memory_bounds.collect_cache_one_bounds(conn, sql, collection)
                except Exception as exc:
                    bounds, error, static_payload = None, str(exc), None
            elapsed = round(time.monotonic() - started, 3)
            if bounds is None:
                candidate.update({"error": error, "elapsed_sec": elapsed, "stage": "bounds",
                                  "collection_preflight": preflight})
                append_jsonl(rejected_path, candidate)
                rejected.append(candidate)
                print(f"gsbench-sort-{size}gb: reject {candidate_index}: {error}", flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
                continue
            actual = memory_bounds.plan_features(static_payload or bounds["static_plan_payload"])
            candidate.update(bounds)
            candidate["static_plan_payload"] = bounds["static_plan_payload"]
            candidate["coverage_actual"] = {
                "plan_node_count": actual.get("plan_node_count"),
                "sort_tuple_width": actual.get("sort_tuple_width"),
                "sort_input_bytes": actual.get("sort_input_bytes"),
                "sort_input_bytes_mb": (actual.get("sort_input_bytes") or 0.0) / (1024 * 1024),
                "sort_input_bytes_bin": byte_bin((actual.get("sort_input_bytes") or 0.0) / (1024 * 1024)),
            }
            candidate.update({"elapsed_sec": elapsed, "collection_preflight": preflight})
            append_jsonl(bounds_path, candidate)
            accepted.append(candidate)
            print(f"gsbench-sort-{size}gb: accepted {len(accepted)}/{args.target_count} "
                  f"nodes={actual.get('plan_node_count')} width={actual.get('sort_tuple_width')} "
                  f"bytes_mb={candidate['coverage_actual']['sort_input_bytes_mb']:.2f} "
                  f"cache={bounds['mem_cache_mb']} one={bounds['mem_one_pass_mb']}", flush=True)
    finally:
        conn.close()
    if len(accepted) < args.target_count:
        raise RuntimeError(f"{database}: accepted {len(accepted)}/{args.target_count}; candidates={candidate_index}")
    with sft_path.open("w", encoding="utf-8") as handle:
        feature_conn = memory_bounds.connect(db_config)
        try:
            for row in accepted[:args.target_count]:
                sample = scalar_two_bound_sample(feature_conn, row, row["static_plan_payload"])
                handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
        finally:
            feature_conn.close()
    manifest = {
        "schema_version": memory_bounds.SCHEMA_VERSION,
        "label_protocol": memory_bounds.LABEL_PROTOCOL,
        "label_mode": "two_bound_native",
        "size_gb": size,
        "database": database,
        "port": db_config["port"],
        "target_count": args.target_count,
        "valid_sql_count": len(accepted[:args.target_count]),
        "rejected_candidate_count": len(rejected),
        "coverage_targets": {"plan_node_count": [1, 10], "sort_tuple_width": [1, 1024],
                              "sort_input_bytes_mb": [0, 1024]},
        "actual_ranges": {
            key: {"min": min((float(row["coverage_actual"].get(key) or 0) for row in accepted), default=0),
                  "max": max((float(row["coverage_actual"].get(key) or 0) for row in accepted), default=0)}
            for key in ("plan_node_count", "sort_tuple_width", "sort_input_bytes_mb")
        },
        "node_counts": dict(sorted(Counter(str(row["coverage_actual"].get("plan_node_count")) for row in accepted).items())),
        "width_bins": dict(sorted(Counter(str(row["coverage_actual"].get("sort_tuple_width")) for row in accepted).items())),
        "byte_bins": dict(sorted(Counter(row["coverage_actual"].get("sort_input_bytes_bin", "unknown") for row in accepted).items())),
        "collection_preflight": preflight,
        "source": "deterministic_sort_coverage_grid",
    }
    (output_dir / "coverage_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instance", type=Path, default=ROOT / "runtime" / "instance.json")
    parser.add_argument("--host", default=None, help="database host or Unix socket directory (defaults to instance metadata)")
    parser.add_argument("--port", type=int, default=None, help="database port (defaults to instance metadata)")
    parser.add_argument("--output-root", type=Path, default=ROOT / "output-memory-sort-coverage-v3")
    parser.add_argument("--sizes", nargs="+", type=int, choices=SIZES, default=list(SIZES))
    parser.add_argument("--target-count", type=int, default=400, help="accepted rows per database")
    parser.add_argument("--max-candidates", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=20260907)
    parser.add_argument("--min-work-mem-kb", type=int, default=64)
    parser.add_argument("--max-work-mem-mb", type=int, default=4096)
    parser.add_argument("--upper-max-work-mem-mb", type=int, default=8192)
    parser.add_argument("--statement-timeout-ms", type=int, default=300000)
    parser.add_argument("--candidate-timeout-ms", type=int, default=900000)
    parser.add_argument("--one-pass-max-batches", type=int, default=2)
    parser.add_argument("--one-pass-temp-read-write-ratio", type=float, default=1.25)
    parser.add_argument("--skip-analyze", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--sort-key-count", type=int, choices=(1, 3), default=1,
                        help="Sort key count target; 3 with width 532 reproduces the AP multi-key shape")
    args = parser.parse_args()
    if args.target_count < 1 or args.max_candidates < args.target_count:
        parser.error("target-count must be positive and max-candidates must cover it")
    instance = json.loads(args.instance.read_text(encoding="utf-8"))
    load_api_key(ROOT / "api.env")
    args.output_root.mkdir(parents=True, exist_ok=True)
    manifests = [collect_size(size, instance, args) for size in args.sizes]
    all_rows = []
    for size in args.sizes:
        all_rows.extend(read_jsonl(args.output_root / f"sft/by_target/gsbench_sort_coverage_{size}gb.jsonl"))
    all_path = args.output_root / "sft" / "all.jsonl"
    all_path.parent.mkdir(parents=True, exist_ok=True)
    all_path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in all_rows), encoding="utf-8")
    (args.output_root / "coverage_manifest.json").write_text(
        json.dumps({"rows": len(all_rows), "per_size": manifests, "seed": args.seed,
                    "label_mode": "two_bound_native"}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"rows": len(all_rows), "output": str(args.output_root), "sizes": args.sizes}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
