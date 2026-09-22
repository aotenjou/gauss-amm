#!/usr/bin/env python3
"""Validate and merge completed native-v2 recollection targets into SFT data."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import memory_bounds

SIZES = (1, 2, 5, 10, 15, 20)


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.open(encoding="utf-8") if line.strip()]


def validate_bounds(rows: list[dict], size: int, expected_count: int) -> None:
    target = f"gsbench_{size}gb"
    if len(rows) != expected_count:
        raise ValueError(f"{target}: found {len(rows)} labels, expected {expected_count}")
    hashes = [str(row.get("sql_sha256", "")) for row in rows]
    if not all(hashes) or len(set(hashes)) != len(hashes):
        raise ValueError(f"{target}: SQL hashes are missing or duplicated")
    query_ids = [str(row.get("query_id", "")) for row in rows]
    if len(set(query_ids)) != len(query_ids):
        raise ValueError(f"{target}: query ids are duplicated")
    for row in rows:
        if row.get("target") != target:
            raise ValueError(f"{target}: unexpected target in {row.get('query_id')}")
        if row.get("schema_version") != memory_bounds.SCHEMA_VERSION:
            raise ValueError(f"{target}: non-v2 schema in {row.get('query_id')}")
        if row.get("label_protocol") != memory_bounds.LABEL_PROTOCOL:
            raise ValueError(f"{target}: non-native-v2 label in {row.get('query_id')}")
        values = [row.get(key) for key in ("mem_cache_mb", "mem_one_pass_mb", "mem_multi_pass_mb")]
        if not all(isinstance(value, (int, float)) and value > 0 for value in values):
            raise ValueError(f"{target}: incomplete bounds in {row.get('query_id')}")
        if not (values[0] >= values[1] >= values[2]):
            raise ValueError(f"{target}: invalid bound order in {row.get('query_id')}")
        if not row.get("static_plan_payload"):
            raise ValueError(f"{target}: missing static plan in {row.get('query_id')}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, default=ROOT / "output-memory-bounds-v2")
    parser.add_argument("--output", type=Path, default=ROOT / "output-memory-bounds-v2" / "sft" / "all.jsonl")
    parser.add_argument("--expected-per-size", type=int, default=200)
    args = parser.parse_args()

    all_rows: list[dict] = []
    for size in SIZES:
        target = f"gsbench_{size}gb"
        bounds_path = args.input_root / f"gsbench-{size}gb" / "bounds.jsonl"
        sft_path = args.input_root / "sft" / "by_target" / f"{target}.jsonl"
        if not bounds_path.exists() or not sft_path.exists():
            raise FileNotFoundError(f"{target}: expected completed bounds and SFT files")
        bounds = load_jsonl(bounds_path)
        sft = load_jsonl(sft_path)
        validate_bounds(bounds, size, args.expected_per_size)
        if len(sft) != args.expected_per_size:
            raise ValueError(f"{target}: found {len(sft)} SFT samples, expected {args.expected_per_size}")
        by_id = {str(row.get("query_id")): row for row in bounds}
        for sample in sft:
            query_id = str(sample.get("query_id"))
            bound = by_id.get(query_id)
            if not bound or sample.get("label_protocol") != memory_bounds.LABEL_PROTOCOL:
                raise ValueError(f"{target}: SFT protocol/id mismatch for {query_id}")
            if sample.get("plan_payload") != bound.get("static_plan_payload"):
                raise ValueError(f"{target}: SFT plan mismatch for {query_id}")
        all_rows.extend(sft)

    ids = [str(row["query_id"]) for row in all_rows]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate query ids across targets")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in all_rows),
        encoding="utf-8",
    )
    digest = hashlib.sha256(args.output.read_bytes()).hexdigest()
    manifest = {
        "schema_version": memory_bounds.SCHEMA_VERSION,
        "label_protocol": memory_bounds.LABEL_PROTOCOL,
        "targets": [f"gsbench_{size}gb" for size in SIZES],
        "rows": len(all_rows),
        "sha256": digest,
    }
    (args.output.parent / "all.manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
