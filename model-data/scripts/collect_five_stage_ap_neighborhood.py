#!/usr/bin/env python3
"""Collect nearby, non-identical Sort intervals around the frozen five-stage APs.

These rows improve feature coverage for the AMM instance but never include the
two exact frozen SQL strings.  They are explicitly marked with a fixed train
or validation split for the one-pass-only refit.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

try:
    from scripts.collect_five_stage_ap_bounds import FIVE_STAGE_APS, collect_case, five_stage_sql
except ModuleNotFoundError:
    from collect_five_stage_ap_bounds import FIVE_STAGE_APS, collect_case, five_stage_sql

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import memory_bounds


RATIOS = (0.30, 0.50, 0.70, 0.85)


def interval_cases(low: int, high: int) -> list[dict[str, object]]:
    """Build four below-anchor intervals per AP, with different starting keys."""
    result: list[dict[str, object]] = []
    frozen_sql = {
        five_stage_sql(int(case["range_start"]), int(case["range_end"]))
        for case in FIVE_STAGE_APS
    }
    for anchor_index, anchor in enumerate(FIVE_STAGE_APS):
        anchor_span = int(anchor["range_end"]) - int(anchor["range_start"]) + 1
        for ratio_index, ratio in enumerate(RATIOS):
            span = max(64, int(round(anchor_span * ratio)))
            max_start = max(low, high - span + 1)
            # Even rows begin near the anchor; odd rows move to an independent
            # part of the relation.  Neither can reproduce the frozen SQL.
            if ratio_index % 2 == 0:
                start = min(max_start, low + max(1, anchor_span // 11))
            else:
                start = min(max_start, low + max(1, (high - low - span + 1) // 3))
            end = start + span - 1
            sql = five_stage_sql(start, end)
            if sql in frozen_sql:
                raise RuntimeError("neighborhood generator produced frozen AP SQL")
            result.append({
                "id": f"five_stage_neighbor_{anchor_index}_{ratio_index}",
                "range_start": start,
                "range_end": end,
                "requested_work_mem_mb": anchor["requested_work_mem_mb"],
                "stage_scope": f"neighbor_of_{anchor['id']}",
                "xgb_split": "train" if ratio_index in {0, 2} else "validation",
            })
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="/tmp/amm-stage5")
    parser.add_argument("--port", type=int, default=15436)
    parser.add_argument("--user", default="baiyutao")
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--schema", default="gsbench")
    parser.add_argument("--output", type=Path,
                        default=ROOT / "output-memory-five-stage-ap" / "neighborhood_native_bounds.jsonl")
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
        raise SystemExit(f"refusing to overwrite neighborhood manifest: {args.output}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    conn = memory_bounds.connect({
        "host": args.host, "port": args.port, "user": args.user,
        "dbname": args.database, "connect_timeout": 10,
    })
    try:
        preflight = memory_bounds.session_preflight(conn, expected_database=args.database, expected_schema=args.schema)
        with conn.cursor() as cur:
            cur.execute("SELECT min(dist_key), max(dist_key) FROM gsbench.sort_data")
            low, high = (int(value) for value in cur.fetchone())
        rows = []
        for case in interval_cases(low, high):
            row = collect_case(conn, case, args, preflight)
            row.update({
                "target": "five_stage_ap_neighborhood",
                "frozen": False,
                "xgb_split": case["xgb_split"],
                "neighbor_of": case["stage_scope"],
            })
            rows.append(row)
    finally:
        conn.close()
    args.output.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8"
    )
    manifest = {
        "rows": len(rows), "output": str(args.output),
        "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
        "split_counts": {name: sum(row["xgb_split"] == name for row in rows) for name in ("train", "validation")},
        "query_ids": [row["query_id"] for row in rows],
    }
    args.output.with_suffix(".manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
