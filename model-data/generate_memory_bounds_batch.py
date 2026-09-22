"""Generate gsbench SQL and collect complete cache/one-pass/multi-pass labels."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import memory_bounds
import sqlGEM


ROOT = Path(__file__).resolve().parent
SIZES = (1, 2, 5, 10, 15, 20)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()


def normalize_sql(sql: str) -> str | None:
    statement = sql.strip()
    while statement.endswith(";"):
        statement = statement[:-1].rstrip()
    if not statement or not re.match(r"^(?:with\b.*?\bselect\b|select\b)", statement, re.IGNORECASE | re.DOTALL):
        return None
    if ";" in statement or "{{" in statement or "}}" in statement:
        return None
    return statement + ";"


def has_required_memory_shape(sql: str) -> bool:
    """Reject LLM drift before expensive EXPLAIN ANALYZE boundary probes."""
    compact = re.sub(r"\s+", " ", sql.lower())
    required_fragments = (
        "from gsbench.fact_sales fs",
        "join gsbench.accounts a on",
        "fs.customer_id = a.customer_id",
        "fs.dist_key = a.dist_key",
        "fs.id >= ",
        "fs.id < ",
        "a.customer_id < 100000",
    )
    return all(fragment in compact for fragment in required_fragments)


def load_api_key(api_file: Path) -> None:
    if not api_file.exists():
        raise RuntimeError(f"API key file not found: {api_file}")
    first = next((line.strip() for line in api_file.read_text(encoding="utf-8").splitlines()
                  if line.strip() and not line.lstrip().startswith("#")), "")
    if "=" in first:
        name, key = first.split("=", 1)
        os.environ[name.strip()] = key.strip().strip("'\"")
    else:
        os.environ["OPENAI_API_KEY"] = first
    if not os.environ.get("OPENAI_API_KEY") and not os.environ.get("DEEPSEEK_API_KEY"):
        raise RuntimeError("api.env does not contain a usable API key")


def target_name(size: int) -> str:
    return f"gsbench_{size}gb"


def write_generated_sql(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        handle.write(f"-- Generated {len(records)} memory-bound SQL queries\n")
        handle.write("-- Every query has complete native-v2 cache/one-pass/multi-pass labels.\n\n")
        for number, record in enumerate(records, start=1):
            handle.write(f"-- Query {number}\n{record['sql'].rstrip(';')};\n\n")


def static_plan(conn, sql: str, timeout_ms: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = %s", (f"{timeout_ms}ms",))
        cur.execute("EXPLAIN (FORMAT JSON, COSTS TRUE, VERBOSE FALSE) " + sql)
        return memory_bounds.normalize_plan(cur.fetchone()[0])


def write_sft(path: Path, records: list[dict[str, Any]], db_config: dict[str, Any]) -> list[dict[str, Any]]:
    conn = memory_bounds.connect(db_config)
    try:
        with path.open("w", encoding="utf-8") as handle:
            samples = []
            for index, record in enumerate(records, start=1):
                payload = record.get("static_plan_payload") or static_plan(conn, record["sql"], 200000)
                sample = memory_bounds.build_sft_sample(conn, record, payload)
                handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
                samples.append(sample)
                print(f"{record['target']}: wrote SFT {index}/{len(records)}", flush=True)
            return samples
    finally:
        conn.close()


def write_manifest(path: Path, size: int, database: str, instance: dict[str, Any], accepted: int, rejected: int,
                   args: argparse.Namespace, preflight: dict[str, Any]) -> None:
    path.write_text(json.dumps({
        "schema_version": memory_bounds.SCHEMA_VERSION,
        "label_protocol": memory_bounds.LABEL_PROTOCOL,
        "collection_mode": "native",
        "size_gb": size,
        "database": database,
        "schema": instance.get("schema", "gsbench"),
        "port": instance["port"],
        "server_version": instance.get("version"),
        "target_count": args.target_count,
        "valid_sql_count": accepted,
        "rejected_candidate_count": rejected,
        "label_mode": "three_bound",
        "label_semantics": memory_bounds.LABEL_SEMANTICS,
        "multi_pass_definition": memory_bounds.MULTI_PASS_DEFINITION,
        "collection_preflight": preflight,
        "min_work_mem_kb": args.min_work_mem_kb,
        "max_work_mem_mb": args.max_work_mem_mb,
        "upper_max_work_mem_mb": args.upper_max_work_mem_mb,
        "repeat_count": args.repeat_count,
        "model": "deepseek-chat",
        "base_url": "https://api.deepseek.com",
    }, indent=2), encoding="utf-8")


def generate_for_size(size: int, instance: dict[str, Any], args: argparse.Namespace, output_root: Path) -> list[dict[str, Any]]:
    database = instance["databases"][str(size)]
    output_dir = output_root / f"gsbench-{size}gb"
    output_dir.mkdir(parents=True, exist_ok=True)
    bounds_path = output_dir / "bounds.jsonl"
    rejected_path = output_dir / "rejected.jsonl"
    generated_path = output_dir / "generated.sql"
    sft_path = output_root / "sft" / "by_target" / f"{target_name(size)}.jsonl"
    sft_path.parent.mkdir(parents=True, exist_ok=True)
    accepted = read_jsonl(bounds_path)
    rejected = read_jsonl(rejected_path)
    if not args.resume and (accepted or rejected):
        raise RuntimeError(f"{output_dir} already contains data; use --resume or select a new --output-root")
    if len(accepted) > args.target_count:
        raise RuntimeError(f"{output_dir} contains {len(accepted)} accepted rows, above target {args.target_count}")

    config_path = output_root / ".configs" / f"memory-bounds-{size}gb.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config = {
        "database": {
            "host": instance.get("socket_dir", "127.0.0.1"), "port": instance["port"],
            "user": instance["user"], "dbname": database, "schema": instance.get("schema", "gsbench"),
        },
        "model": {"api_key_env": "OPENAI_API_KEY", "base_url": "https://api.deepseek.com", "name": "deepseek-chat"},
        "api_env_file": str(ROOT / "api.env"),
        "spec_file": str(ROOT / "specs_memory.json"),
        "prompt_dir": str(ROOT / "prompts-memory"),
    }
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    db_config = dict(config["database"])
    db_config.pop("schema")
    sqlGEM.load_resources(config_path)
    sqlGEM.DB_SCHEMA = instance.get("schema", "gsbench")
    schema_excerpt = sqlGEM.get_schema_excerpt()
    preflight_conn = memory_bounds.connect(db_config)
    try:
        preflight = memory_bounds.session_preflight(
            preflight_conn,
            expected_database=database,
            expected_schema=instance.get("schema", "gsbench"),
        )
        preflight.update(memory_bounds.analyze_database(preflight_conn))
        if preflight.get("analyze_status") != "completed":
            raise RuntimeError(f"ANALYZE failed for {database}: {preflight.get('analyze_error', 'unknown error')}")
    finally:
        preflight_conn.close()
    if preflight["current_database"] != database:
        raise RuntimeError(f"connected to {preflight['current_database']}, expected {database}")

    seen_hashes = {record.get("sql_sha256") for record in accepted + rejected if record.get("sql_sha256")}
    candidate_index = len(accepted) + len(rejected)
    collection = {
        "min_work_mem_kb": args.min_work_mem_kb,
        "max_work_mem_mb": args.max_work_mem_mb,
        "upper_max_work_mem_mb": args.upper_max_work_mem_mb,
        "statement_timeout_ms": args.statement_timeout_ms,
        "candidate_timeout_ms": args.candidate_timeout_ms,
        "one_pass_max_batches": args.one_pass_max_batches,
        "one_pass_temp_read_write_ratio": args.one_pass_temp_read_write_ratio,
        "repeat_count": args.repeat_count,
        "label_protocol": memory_bounds.LABEL_PROTOCOL,
        "collection_mode": "native",
    }
    specs = list(dict.fromkeys(sqlGEM.SPECS))[:args.max_specs]
    print(f"{target_name(size)}: resume accepted={len(accepted)} rejected={len(rejected)} specs={len(specs)}", flush=True)
    conn = memory_bounds.connect(db_config)
    try:
        for round_number in range(1, args.max_rounds + 1):
            if len(accepted) >= args.target_count or candidate_index >= args.max_candidates:
                break
            with ThreadPoolExecutor(max_workers=args.llm_workers) as executor:
                futures = {executor.submit(sqlGEM.generate_sql_template, schema_excerpt, spec): spec for spec in specs}
                templates = []
                for future in as_completed(futures):
                    spec = futures[future]
                    try:
                        sql, placeholders_json = future.result()
                        if sql and placeholders_json:
                            templates.append((spec, sql, placeholders_json))
                    except Exception as exc:
                        print(f"{target_name(size)}: template error for {spec}: {exc}", flush=True)
            print(f"{target_name(size)}: round {round_number} received {len(templates)} templates", flush=True)
            for spec, template, placeholders_json in templates:
                if len(accepted) >= args.target_count or candidate_index >= args.max_candidates:
                    break
                try:
                    placeholders = json.loads(placeholders_json)["placeholders"]
                except (KeyError, TypeError, json.JSONDecodeError) as exc:
                    print(f"{target_name(size)}: skipped malformed placeholders: {exc}", flush=True)
                    continue
                for _ in range(args.instances_per_template):
                    if len(accepted) >= args.target_count or candidate_index >= args.max_candidates:
                        break
                    candidate_index += 1
                    raw_sql = sqlGEM.instantiate_query(template, placeholders)
                    sql = normalize_sql(raw_sql)
                    digest = hashlib.sha256((sql or raw_sql).encode("utf-8")).hexdigest()
                    candidate = {
                        "schema_version": memory_bounds.SCHEMA_VERSION,
                        "label_protocol": memory_bounds.LABEL_PROTOCOL,
                        "label_semantics": memory_bounds.LABEL_SEMANTICS,
                        "multi_pass_definition": memory_bounds.MULTI_PASS_DEFINITION,
                        "collection_mode": "native",
                        "target": target_name(size), "workload": "gsbench", "sf": None, "size_gb": size,
                        "dbname": database, "schema": instance.get("schema", "gsbench"),
                        "source_file": str(generated_path.resolve()),
                        "source_query_id": f"candidate_{candidate_index:06d}",
                        "query_id": f"{target_name(size)}_candidate_{candidate_index:06d}",
                        "selection_rank": candidate_index, "sql": sql or raw_sql, "sql_sha256": digest,
                    }
                    if digest in seen_hashes:
                        continue
                    seen_hashes.add(digest)
                    started = time.monotonic()
                    if sql is None:
                        error = "invalid_or_uninstantiated_select"
                        bounds = None
                    elif not has_required_memory_shape(sql):
                        error = "missing_required_wide_bounded_hash_join_shape"
                        bounds = None
                    else:
                        bounds, error = memory_bounds.collect_three_bounds_repeated(
                            conn, sql, collection, repeat_count=args.repeat_count
                        )
                    elapsed = round(time.monotonic() - started, 3)
                    if bounds is None:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        candidate.update({"error": error, "elapsed_sec": elapsed, "stage": "bounds", "requirement": spec})
                        append_jsonl(rejected_path, candidate)
                        rejected.append(candidate)
                        print(f"{target_name(size)}: reject {candidate_index}: {error}", flush=True)
                        if getattr(conn, "closed", 0):
                            conn.close()
                            conn = memory_bounds.connect(db_config)
                            preflight = memory_bounds.session_preflight(
                                conn,
                                expected_database=database,
                                expected_schema=instance.get("schema", "gsbench"),
                            )
                        continue
                    accepted_index = len(accepted) + 1
                    candidate.update({
                        "source_query_id": f"query_{accepted_index:03d}",
                        "query_id": f"{target_name(size)}_q{accepted_index:03d}",
                        "selection_rank": accepted_index,
                        "elapsed_sec": elapsed,
                        "collection_preflight": preflight,
                        "requirement": spec,
                    })
                    # Keep the exact static EXPLAIN used for the boundary
                    # label. Re-running EXPLAIN during SFT preparation can
                    # change with planner statistics or session state and
                    # would silently desynchronise features from labels.
                    candidate.update(bounds)
                    append_jsonl(bounds_path, candidate)
                    accepted.append(candidate)
                    print(f"{target_name(size)}: accepted {len(accepted)}/{args.target_count} ({elapsed}s)", flush=True)
    finally:
        conn.close()

    write_generated_sql(generated_path, accepted)
    sft_samples = write_sft(sft_path, accepted, db_config)
    write_manifest(output_dir / "manifest.json", size, database, instance, len(accepted), len(rejected), args, preflight)
    if len(accepted) != args.target_count:
        raise RuntimeError(f"{target_name(size)}: reached {len(accepted)}/{args.target_count}; candidates checked={candidate_index}")
    return sft_samples


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instance", default=str(ROOT / "runtime" / "instance.json"))
    parser.add_argument("--output-root", default=str(ROOT / "output-memory-bounds-v2"))
    parser.add_argument("--target-count", type=int, default=200)
    parser.add_argument("--sizes", nargs="+", type=int, choices=SIZES, default=list(SIZES))
    parser.add_argument("--max-specs", type=int, default=12)
    parser.add_argument("--instances-per-template", type=int, default=8)
    parser.add_argument("--max-rounds", type=int, default=250)
    parser.add_argument("--max-candidates", type=int, default=6000)
    parser.add_argument("--llm-workers", type=int, default=4)
    parser.add_argument("--min-work-mem-kb", type=int, default=64)
    parser.add_argument("--max-work-mem-mb", type=int, default=1024)
    parser.add_argument("--upper-max-work-mem-mb", type=int, default=4096)
    parser.add_argument("--statement-timeout-ms", type=int, default=200000)
    parser.add_argument("--candidate-timeout-ms", type=int, default=200000)
    parser.add_argument("--one-pass-max-batches", type=int, default=2)
    parser.add_argument("--one-pass-temp-read-write-ratio", type=float, default=1.25)
    parser.add_argument("--repeat-count", type=int, default=3,
                        help="Repeat each native three-bound search and take the median boundary")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.target_count < 1 or args.llm_workers < 1 or args.instances_per_template < 1 or args.max_rounds < 1 or args.repeat_count < 1:
        parser.error("target counts, workers, instances, and rounds must be positive")
    if args.max_candidates < args.target_count:
        parser.error("--max-candidates must be at least --target-count")
    instance = json.loads(Path(args.instance).read_text(encoding="utf-8"))
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    load_api_key(ROOT / "api.env")
    for size in args.sizes:
        generate_for_size(size, instance, args, output_root)
    all_path = output_root / "sft" / "all.jsonl"
    all_samples = []
    for size in SIZES:
        all_samples.extend(read_jsonl(output_root / "sft" / "by_target" / f"{target_name(size)}.jsonl"))
    with all_path.open("w", encoding="utf-8") as handle:
        for sample in all_samples:
            handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
    print(f"completed {len(all_samples)} labeled SFT samples: {all_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
