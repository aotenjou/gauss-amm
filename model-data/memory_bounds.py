"""openGauss cache/one-pass/multi-pass label collection helpers.

The classification and boundary-search semantics intentionally match
``memory-bound-collection/scripts/collect_bounds.py``.
"""

from __future__ import annotations

import math
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any

import psycopg2


BATCH_FIELDS = ("Hash Batches", "Original Hash Batches", "HashAgg Batches", "Batches")
TEMP_FIELDS = ("Temp Read Blocks", "Temp Written Blocks", "Temp Space Read", "Temp Space Written", "Disk Usage")
TABLE_RE = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)", re.IGNORECASE)
SCHEMA_VERSION = "memory-bounds-v2"
LABEL_PROTOCOL = "native-v2"
LABEL_SEMANTICS = "native_executor_boundaries"
MULTI_PASS_DEFINITION = "one_pass_boundary_kb_minus_1"
THREE_VALUE_INSTRUCTION = (
    "You are an expert database memory-bound prediction assistant.\n\n"
    "Estimate the work_mem thresholds (in MB) at which the provided SQL query reaches "
    "cache mode, one-pass mode, and multi-pass mode under the current database and system state.\n\n"
    "Use native executor semantics: cache is the no-spill boundary, one-pass is the "
    "one-pass boundary, and multi-pass is the upper edge immediately below one-pass.\n\n"
    "Return exactly [cache_mb,one_pass_mb,multi_pass_mb] with three numeric values and no other text."
)


def normalize_plan(payload: Any) -> dict[str, Any]:
    if isinstance(payload, list) and payload:
        return payload[0]
    if isinstance(payload, dict):
        return payload
    raise ValueError(f"unexpected EXPLAIN JSON payload: {type(payload).__name__}")


def walk_plan(plan: dict[str, Any]):
    yield plan
    for child in plan.get("Plans", []) or []:
        if isinstance(child, dict):
            yield from walk_plan(child)


def as_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip().split()[0])
    except (ValueError, IndexError):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip().split()[0])
    except (ValueError, IndexError):
        return 0.0


def optional_float(value: Any) -> float | None:
    """Convert a value without erasing missing/non-finite state."""
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def inspect_mode(plan_payload: dict[str, Any], one_pass_max_batches: int, ratio: float) -> dict[str, Any]:
    """Classify a JSON plan with the existing collector's exact rules."""
    root = plan_payload.get("Plan", plan_payload)
    max_batches = 1
    temp_read = 0.0
    temp_written = 0.0
    spills: list[str] = []
    for node in walk_plan(root):
        node_type = str(node.get("Node Type", "Unknown"))
        for field in BATCH_FIELDS:
            if field in node:
                batches = int(as_float(node[field]))
                max_batches = max(max_batches, batches)
                if batches > 1:
                    spills.append(f"{node_type}.{field}={batches}")
        sort_type = str(node.get("Sort Space Type", "")).lower()
        sort_method = str(node.get("Sort Method", "")).lower()
        if sort_type == "disk" or "external" in sort_method:
            spills.append(f"{node_type}.SortSpace={sort_type or sort_method}")
        for field in TEMP_FIELDS:
            value = as_float(node.get(field))
            if value <= 0:
                continue
            if "Read" in field:
                temp_read += value
            elif "Written" in field:
                temp_written += value
            spills.append(f"{node_type}.{field}={value:g}")
    cache_ok = not spills and max_batches <= 1
    one_pass_ok = cache_ok or (
        max_batches <= one_pass_max_batches
        and (temp_written <= 0 or temp_read <= max(temp_written, 1.0) * ratio)
    )
    return {
        "mode": "cache" if cache_ok else "one_pass" if one_pass_ok else "multi_pass",
        "cache_ok": cache_ok,
        "one_pass_ok": one_pass_ok,
        "max_batches": max_batches,
        "temp_read": temp_read,
        "temp_written": temp_written,
        "spill_reasons": spills,
    }


def connect(db_config: dict[str, Any]):
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    return conn


def session_preflight(
    conn,
    allow_amm: bool = False,
    expected_database: str | None = None,
    expected_schema: str | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_schema(), version()")
        current_database, current_schema, server_version = cur.fetchone()
        if expected_database and str(current_database) != str(expected_database):
            raise RuntimeError(f"connected to {current_database}, expected {expected_database}")
        schema_exists = True
        if expected_schema and str(current_schema) != str(expected_schema):
            cur.execute("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %s)", (expected_schema,))
            schema_exists = bool(cur.fetchone()[0])
            if not schema_exists:
                raise RuntimeError(f"schema {expected_schema} does not exist")
        elif expected_schema:
            schema_exists = True
        try:
            cur.execute("SET gs_amm_native_auto_mode = off")
            cur.execute("SHOW gs_amm_native_auto_mode")
            amm_mode = str(cur.fetchone()[0]).lower()
        except Exception as exc:
            conn.rollback()
            if "unrecognized configuration parameter" in str(exc).lower():
                amm_mode = "unavailable_native_baseline"
            elif not allow_amm:
                raise RuntimeError("cannot force gs_amm_native_auto_mode=off") from exc
            else:
                amm_mode = "unavailable"
        if not allow_amm and amm_mode not in {"off", "false", "0", "unavailable_native_baseline"}:
            raise RuntimeError(f"AMM native auto mode is {amm_mode}, expected off")
    return {
        "current_database": current_database,
        "current_schema": current_schema,
        "expected_schema": expected_schema,
        "schema_exists": schema_exists,
        "server_version": server_version,
        "gs_amm_native_auto_mode": amm_mode,
        "collection_mode": (
            "native" if amm_mode in {"off", "false", "0", "unavailable_native_baseline"}
            else "amm" if amm_mode in {"on", "true", "1"}
            else "unknown"
        ),
        "label_protocol": LABEL_PROTOCOL,
        "schema_version": SCHEMA_VERSION,
        "analyze_status": "not_requested",
    }


def analyze_database(conn) -> dict[str, Any]:
    """Refresh planner statistics once and retain the outcome in provenance."""
    started = time.time()
    try:
        with conn.cursor() as cur:
            cur.execute("ANALYZE")
        return {"analyze_status": "completed", "analyze_elapsed_sec": round(time.time() - started, 3)}
    except Exception as exc:
        conn.rollback()
        return {
            "analyze_status": "failed",
            "analyze_error": str(exc),
            "analyze_elapsed_sec": round(time.time() - started, 3),
        }


def explain_static(conn, sql: str, timeout_ms: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = %s", (f"{timeout_ms}ms",))
        cur.execute("EXPLAIN (FORMAT JSON, COSTS TRUE, VERBOSE FALSE) " + sql)
        return normalize_plan(cur.fetchone()[0])


def explain_analyze(conn, sql: str, work_mem_kb: int, timeout_ms: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = %s", (f"{timeout_ms}ms",))
        cur.execute("SET work_mem = %s", (f"{work_mem_kb}kB",))
        cur.execute("EXPLAIN (ANALYZE, FORMAT JSON, BUFFERS, TIMING OFF) " + sql)
        return normalize_plan(cur.fetchone()[0])


def binary_search(low: int, high: int, predicate) -> int | None:
    if not predicate(high):
        return None
    while low < high:
        middle = (low + high) // 2
        if predicate(middle):
            high = middle
        else:
            low = middle + 1
    return low


def hinted_binary_search(
    low: int,
    high: int,
    predicate,
    hint: int | None = None,
    probe_step: int = 256,
) -> int | None:
    """Find a monotone true boundary, using a prior run only as a bracket hint.

    The hint never supplies a label: it is always probed on the current
    database first, then expanded until a false/true bracket is observed.
    Falling back to the ordinary full-range search keeps this safe for OOD or
    materially changed workloads.
    """
    if hint is None or hint < low or hint > high:
        return binary_search(low, high, predicate)
    if predicate(hint):
        upper = hint
        # Most prior labels are stable to the nearest KB. Check the adjacent
        # value before opening a wider bracket so unchanged boundaries need
        # only two current-instance probes.
        if upper <= low or not predicate(upper - 1):
            return upper
        step = probe_step
        lower = max(low, upper - step)
        while lower > low and predicate(lower):
            upper = lower
            step = min(max(step * 2, probe_step), high - low or step)
            lower = max(low, upper - step)
        if predicate(lower):
            return low
        return binary_search(lower, upper, predicate)
    lower = hint
    if lower < high and predicate(lower + 1):
        return lower + 1
    step = probe_step
    upper = min(high, lower + step)
    while upper < high and not predicate(upper):
        lower = upper
        step = min(max(step * 2, probe_step), high - low or step)
        upper = min(high, lower + step)
    if not predicate(upper):
        return None
    return binary_search(lower, upper, predicate)


def collect_three_bounds(conn, sql: str, collection: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """Collect a complete three-bound label, or a collector-compatible reason."""
    if collection.get("label_protocol", LABEL_PROTOCOL) != LABEL_PROTOCOL:
        return None, "unsupported_label_protocol"
    if collection.get("collection_mode", "native") != "native":
        return None, "native_collection_requires_amm_off"
    low = int(collection["min_work_mem_kb"])
    primary_high = int(collection["max_work_mem_mb"]) * 1024
    upper_high = int(collection["upper_max_work_mem_mb"]) * 1024
    timeout_ms = int(collection["statement_timeout_ms"])
    candidate_timeout_ms = int(collection["candidate_timeout_ms"])
    one_pass_max_batches = int(collection["one_pass_max_batches"])
    ratio = float(collection["one_pass_temp_read_write_ratio"])
    hints = collection.get("boundary_hints_mb") or {}
    hint_probe_step_kb = max(1, int(collection.get("hint_probe_step_kb", 256)))
    probes: dict[int, dict[str, Any]] = {}
    payloads: dict[int, dict[str, Any]] = {}
    started = time.monotonic()

    try:
        static_payload = explain_static(conn, sql, timeout_ms)
    except Exception as exc:
        return None, f"static_explain_failed: {exc}"

    def result(memory_kb: int) -> dict[str, Any]:
        if memory_kb not in probes:
            if candidate_timeout_ms > 0 and (time.monotonic() - started) * 1000 >= candidate_timeout_ms:
                raise TimeoutError(f"candidate_timeout: exceeded {candidate_timeout_ms}ms across boundary probes")
            payload = explain_analyze(conn, sql, memory_kb, timeout_ms)
            payloads[memory_kb] = payload
            probes[memory_kb] = inspect_mode(payload, one_pass_max_batches, ratio)
        return probes[memory_kb]

    try:
        minimum = result(low)
        if minimum["mode"] != "multi_pass":
            return None, f"multi_pass_not_observed_at_min_work_mem: {minimum['mode']}"
        cache_hint = optional_float(hints.get("cache"))
        one_pass_hint = optional_float(hints.get("one_pass"))
        # Establish the smallest known cache-success upper bracket from the
        # prior hint. This avoids an unconditional 1GB execution for every
        # candidate while still probing all values on the current instance.
        high = primary_high
        primary_high_checked = False
        if cache_hint is not None:
            hint_kb = int(cache_hint * 1024)
            if low <= hint_kb <= primary_high:
                if result(hint_kb)["cache_ok"]:
                    high = hint_kb
                else:
                    lower = hint_kb
                    step = hint_probe_step_kb
                    upper = min(primary_high, lower + step)
                    while upper < primary_high and not result(upper)["cache_ok"]:
                        lower = upper
                        step = min(max(step * 2, hint_probe_step_kb), primary_high - low or step)
                        upper = min(primary_high, lower + step)
                    if result(upper)["cache_ok"]:
                        high = upper
                    else:
                        primary_high_checked = True
        if high == primary_high and not primary_high_checked and not result(high)["cache_ok"]:
            high = upper_high
        cache_kb = hinted_binary_search(
            low,
            high,
            lambda value: bool(result(value)["cache_ok"]),
            int(cache_hint * 1024) if cache_hint is not None else None,
            probe_step=hint_probe_step_kb,
        )
        one_pass_kb = hinted_binary_search(
            low,
            high,
            lambda value: bool(result(value)["one_pass_ok"]),
            int(one_pass_hint * 1024) if one_pass_hint is not None else None,
            probe_step=hint_probe_step_kb,
        )
    except Exception as exc:
        return None, str(exc)

    if cache_kb is None:
        return None, "upper_censored: cache mode not observed at upper max work_mem"
    if one_pass_kb is None:
        return None, "one_pass_not_observed: inconsistent cache/one-pass classification"
    if cache_kb < one_pass_kb:
        return None, "invalid_bound_order"
    multi_pass_kb = one_pass_kb - 1
    if multi_pass_kb < low:
        return None, "multi_pass_collapsed_at_min"
    multi_pass_probe = result(multi_pass_kb)
    if multi_pass_probe["mode"] != "multi_pass":
        return None, "multi_pass_upper_probe_not_observed"

    cache_mb = round(cache_kb / 1024.0, 6)
    one_pass_mb = round(one_pass_kb / 1024.0, 6)
    multi_pass_mb = round(multi_pass_kb / 1024.0, 6)
    if not all(math.isfinite(value) and value > 0 for value in (cache_mb, one_pass_mb, multi_pass_mb)):
        return None, "non_finite_or_non_positive_bound"
    diagnostics = {
        "schema_version": SCHEMA_VERSION,
        "label_protocol": LABEL_PROTOCOL,
        "label_semantics": LABEL_SEMANTICS,
        "multi_pass_definition": MULTI_PASS_DEFINITION,
        "multi_pass_lower_probe_mb": round(low / 1024.0, 6),
        "multi_pass_upper_mb": multi_pass_mb,
        "multi_pass_upper_probe": multi_pass_probe,
        "max_tested_mb": round(high / 1024.0, 6),
        "one_pass_max_batches": one_pass_max_batches,
        "one_pass_temp_read_write_ratio": ratio,
        "hint_probe_step_kb": hint_probe_step_kb,
        "boundary_hints_mb": {key: value for key, value in hints.items() if optional_float(value) is not None},
        "probes": {str(round(memory / 1024.0, 6)): value for memory, value in sorted(probes.items())},
    }
    root = static_payload.get("Plan", static_payload)
    static_hash = hashlib.sha256(json.dumps(static_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    return {
        "schema_version": SCHEMA_VERSION,
        "label_protocol": LABEL_PROTOCOL,
        "label_semantics": LABEL_SEMANTICS,
        "multi_pass_definition": MULTI_PASS_DEFINITION,
        "mem_cache_mb": cache_mb,
        "mem_one_pass_mb": one_pass_mb,
        "mem_multi_pass_mb": multi_pass_mb,
        "multi_pass_lower_mb": round(low / 1024.0, 6),
        "multi_pass_upper_mb": multi_pass_mb,
        "diagnostics": diagnostics,
        "source_estimated_cost": optional_float(root.get("Total Cost")),
        "static_plan_hash": static_hash,
        "static_plan_payload": static_payload,
    }, None


def collect_three_bounds_repeated(
    conn,
    sql: str,
    collection: dict[str, Any],
    repeat_count: int = 3,
) -> tuple[dict[str, Any] | None, str | None]:
    """Repeat native boundary collection and return median stable labels.

    Each repetition runs a fresh boundary search on the current instance. A
    failed repetition invalidates the aggregate so the dataset never silently
    treats a partial stability measurement as a three-run label.
    """
    if repeat_count < 1:
        return None, "repeat_count_must_be_positive"
    single_collection = dict(collection)
    single_collection.pop("repeat_count", None)
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for _ in range(repeat_count):
        result, error = collect_three_bounds(conn, sql, single_collection)
        if result is None:
            errors.append(str(error))
        else:
            results.append(result)
    if errors:
        return None, f"repeat_failed:{'; '.join(errors)}"
    if len(results) != repeat_count:
        return None, "repeat_count_mismatch"
    keys = ("mem_cache_mb", "mem_one_pass_mb", "mem_multi_pass_mb")
    medians = {key: float(sorted(result[key] for result in results)[len(results) // 2]) for key in keys}
    stability = {
        key.removeprefix("mem_").removesuffix("_mb"): round(
            max(result[key] for result in results) - min(result[key] for result in results), 6
        )
        for key in keys
    }
    aggregate = dict(results[0])
    aggregate.update(medians)
    aggregate["repeat_count"] = repeat_count
    aggregate["repeat_bounds"] = [
        {key.removeprefix("mem_"): result[key] for key in keys} for result in results
    ]
    aggregate["boundary_stability_mb"] = stability
    aggregate["diagnostics"] = dict(aggregate.get("diagnostics") or {})
    aggregate["diagnostics"].update({
        "repeat_count": repeat_count,
        "repeat_bounds": aggregate["repeat_bounds"],
        "boundary_stability_mb": stability,
    })
    return aggregate, None


def collect_cache_one_bounds(conn, sql: str, collection: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """Collect native cache/one-pass bounds when a true multi-pass interval is absent.

    A Sort-only statement can be an external one-pass operation even at the
    minimum work_mem.  Such a query has no native-v2 multi-pass boundary, but
    its cache and one-pass thresholds remain meaningful and must not be mixed
    with AMM grant labels.
    """
    if collection.get("label_protocol", LABEL_PROTOCOL) != LABEL_PROTOCOL:
        return None, "unsupported_label_protocol"
    if collection.get("collection_mode", "native") != "native":
        return None, "native_collection_requires_amm_off"
    low = int(collection["min_work_mem_kb"])
    primary_high = int(collection["max_work_mem_mb"]) * 1024
    upper_high = int(collection["upper_max_work_mem_mb"]) * 1024
    timeout_ms = int(collection["statement_timeout_ms"])
    candidate_timeout_ms = int(collection["candidate_timeout_ms"])
    one_pass_max_batches = int(collection["one_pass_max_batches"])
    ratio = float(collection["one_pass_temp_read_write_ratio"])
    hints = collection.get("boundary_hints_mb") or {}
    hint_probe_step_kb = max(1, int(collection.get("hint_probe_step_kb", 256)))
    probes: dict[int, dict[str, Any]] = {}
    started = time.monotonic()

    try:
        static_payload = explain_static(conn, sql, timeout_ms)
    except Exception as exc:
        return None, f"static_explain_failed: {exc}"

    def result(memory_kb: int) -> dict[str, Any]:
        if memory_kb not in probes:
            if candidate_timeout_ms > 0 and (time.monotonic() - started) * 1000 >= candidate_timeout_ms:
                raise TimeoutError(f"candidate_timeout: exceeded {candidate_timeout_ms}ms across boundary probes")
            payload = explain_analyze(conn, sql, memory_kb, timeout_ms)
            probes[memory_kb] = inspect_mode(payload, one_pass_max_batches, ratio)
        return probes[memory_kb]

    try:
        cache_hint = optional_float(hints.get("cache"))
        one_pass_hint = optional_float(hints.get("one_pass"))
        high = primary_high
        primary_high_checked = False
        if cache_hint is not None:
            hint_kb = int(cache_hint * 1024)
            if low <= hint_kb <= primary_high:
                if result(hint_kb)["cache_ok"]:
                    high = hint_kb
                else:
                    lower = hint_kb
                    step = hint_probe_step_kb
                    upper = min(primary_high, lower + step)
                    while upper < primary_high and not result(upper)["cache_ok"]:
                        lower = upper
                        step = min(max(step * 2, hint_probe_step_kb), primary_high - low or step)
                        upper = min(primary_high, lower + step)
                    if result(upper)["cache_ok"]:
                        high = upper
                    else:
                        primary_high_checked = True
        if high == primary_high and not primary_high_checked and not result(high)["cache_ok"]:
            high = upper_high
        cache_kb = hinted_binary_search(
            low, high, lambda value: bool(result(value)["cache_ok"]),
            int(cache_hint * 1024) if cache_hint is not None else None, probe_step=hint_probe_step_kb,
        )
        one_pass_kb = hinted_binary_search(
            low, high, lambda value: bool(result(value)["one_pass_ok"]),
            int(one_pass_hint * 1024) if one_pass_hint is not None else None, probe_step=hint_probe_step_kb,
        )
    except Exception as exc:
        return None, str(exc)

    if cache_kb is None:
        return None, "upper_censored: cache mode not observed at upper max work_mem"
    if one_pass_kb is None:
        return None, "one_pass_not_observed: inconsistent cache/one-pass classification"
    if cache_kb < one_pass_kb:
        return None, "invalid_bound_order"
    cache_mb = round(cache_kb / 1024.0, 6)
    one_pass_mb = round(one_pass_kb / 1024.0, 6)
    if not all(math.isfinite(value) and value > 0 for value in (cache_mb, one_pass_mb)):
        return None, "non_finite_or_non_positive_bound"
    root = static_payload.get("Plan", static_payload)
    static_hash = hashlib.sha256(json.dumps(static_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    diagnostics = {
        "schema_version": SCHEMA_VERSION,
        "label_protocol": LABEL_PROTOCOL,
        "label_semantics": LABEL_SEMANTICS,
        "label_mode": "two_bound_native",
        "multi_pass_status": "not_observed_at_min_work_mem",
        "max_tested_mb": round(high / 1024.0, 6),
        "one_pass_max_batches": one_pass_max_batches,
        "one_pass_temp_read_write_ratio": ratio,
        "hint_probe_step_kb": hint_probe_step_kb,
        "boundary_hints_mb": {key: value for key, value in hints.items() if optional_float(value) is not None},
        "probes": {str(round(memory / 1024.0, 6)): value for memory, value in sorted(probes.items())},
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "label_protocol": LABEL_PROTOCOL,
        "label_semantics": LABEL_SEMANTICS,
        "label_mode": "two_bound_native",
        "multi_pass_definition": None,
        "mem_cache_mb": cache_mb,
        "mem_one_pass_mb": one_pass_mb,
        "mem_multi_pass_mb": None,
        "diagnostics": diagnostics,
        "source_estimated_cost": optional_float(root.get("Total Cost")),
        "static_plan_hash": static_hash,
        "static_plan_payload": static_payload,
    }, None


def collect_onepass_bounds(conn, sql: str, collection: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """Collect the 1pass execution interval on a native executor.

    The lower edge is the first work_mem value classified as one-pass and the
    upper edge is the first value classified as cache/no-spill.  The existing
    cache/one-pass collector performs the same current-instance binary search;
    this wrapper gives the result explicit one-pass semantics for downstream
    training and keeps the legacy fields available for compatibility.
    """
    record, error = collect_cache_one_bounds(conn, sql, collection)
    if record is None:
        return None, error
    lower = record.get("mem_one_pass_mb")
    upper = record.get("mem_cache_mb")
    if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
        return None, "one_pass_bound_missing"
    if upper < lower:
        return None, "invalid_one_pass_interval"
    diagnostics = dict(record.get("diagnostics") or {})
    diagnostics["label_semantics"] = "one_pass_execution_interval"
    diagnostics["one_pass_lower_mb"] = lower
    diagnostics["one_pass_upper_mb"] = upper
    record.update({
        "label_semantics": "one_pass_execution_interval",
        "label_mode": "one_pass_bounds_native",
        "one_pass_lower_mb": lower,
        "one_pass_upper_mb": upper,
        "one_pass_interval_mb": round(float(upper) - float(lower), 6),
        "diagnostics": diagnostics,
    })
    return record, None


def plan_features(payload: dict[str, Any]) -> dict[str, Any]:
    root = payload.get("Plan", payload)
    nodes = list(walk_plan(root))
    types = [str(node.get("Node Type", "")).lower() for node in nodes]
    row_values = [value for node in nodes if (value := optional_float(node.get("Plan Rows"))) is not None]
    width_values = [value for node in nodes if (value := optional_float(node.get("Plan Width"))) is not None]
    sort_nodes = [node for node in nodes if str(node.get("Node Type", "")).lower() == "sort"]
    sort_rows = [value for node in sort_nodes if (value := optional_float(node.get("Plan Rows"))) is not None]
    sort_widths = [value for node in sort_nodes if (value := optional_float(node.get("Plan Width"))) is not None]
    sort_keys = []
    for node in sort_nodes:
        if "Sort Key" not in node:
            continue
        keys = node.get("Sort Key") or []
        sort_keys.append(len(keys) if isinstance(keys, (list, tuple)) else 1)
    max_rows = max(row_values) if row_values else None
    max_width = max(width_values) if width_values else None
    sort_input_rows = max(sort_rows) if sort_rows else None
    sort_tuple_width = max(sort_widths) if sort_widths else None
    sort_input_bytes = (sort_input_rows * sort_tuple_width) if sort_input_rows is not None and sort_tuple_width is not None else None
    missing: list[str] = []

    def node_bytes(node: dict[str, Any]) -> float | None:
        rows = optional_float(node.get("Plan Rows"))
        width = optional_float(node.get("Plan Width"))
        if rows is None or width is None:
            return None
        return max(0.0, rows * width)

    def depth(node: dict[str, Any]) -> int:
        children = [child for child in node.get("Plans", []) or [] if isinstance(child, dict)]
        return 1 + max((depth(child) for child in children), default=0)

    plan_bytes = [value for node in nodes if (value := node_bytes(node)) is not None]
    plan_depth = depth(root) if isinstance(root, dict) and nodes else None
    plan_bytes_max = max(plan_bytes) if plan_bytes else None
    plan_bytes_sum = sum(plan_bytes) if plan_bytes else None

    join_nodes = [node for node in nodes if "join" in str(node.get("Node Type", "")).lower()]
    join_input_bytes: list[float] = []
    join_output_bytes: list[float] = []
    join_build_rows: list[float] = []
    join_build_width: list[float] = []
    join_build_bytes: list[float] = []
    for node in join_nodes:
        children = [child for child in node.get("Plans", []) or [] if isinstance(child, dict)]
        child_bytes = [value for child in children if (value := node_bytes(child)) is not None]
        if child_bytes:
            join_input_bytes.append(sum(child_bytes))
        output_bytes = node_bytes(node)
        if output_bytes is not None:
            join_output_bytes.append(output_bytes)
        # Hash Join's Hash child is the build side. Fall back to the smaller
        # direct input for other join implementations where no explicit build
        # marker is present.
        build = next((child for child in children
                      if str(child.get("Node Type", "")).lower() == "hash"), None)
        if build is None and children:
            build = min(children, key=lambda child: node_bytes(child) if node_bytes(child) is not None else float("inf"))
        if build is not None:
            rows = optional_float(build.get("Plan Rows"))
            width = optional_float(build.get("Plan Width"))
            if rows is not None:
                join_build_rows.append(rows)
            if width is not None:
                join_build_width.append(width)
            if rows is not None and width is not None:
                join_build_bytes.append(max(0.0, rows * width))

    aggregate_nodes = [node for node in nodes
                       if str(node.get("Node Type", "")).lower() in {"aggregate", "hashaggregate", "groupaggregate"}]
    aggregate_input_bytes: list[float] = []
    for node in aggregate_nodes:
        children = [child for child in node.get("Plans", []) or [] if isinstance(child, dict)]
        aggregate_input_bytes.extend(value for child in children if (value := node_bytes(child)) is not None)

    materialize_nodes = [node for node in nodes
                         if str(node.get("Node Type", "")).lower() in {"materialize", "memoize"}]
    materialize_bytes = [value for node in materialize_nodes if (value := node_bytes(node)) is not None]

    # Static EXPLAIN exposes estimated rows/width only.  For operators with
    # children, output/input cardinality is a safe estimate of selectivity;
    # scans without a parent cardinality remain missing rather than guessed.
    filter_selectivities: list[float] = []
    for node in nodes:
        children = [child for child in node.get("Plans", []) or [] if isinstance(child, dict)]
        output_rows = optional_float(node.get("Plan Rows"))
        input_rows = sum(value for child in children
                         if (value := optional_float(child.get("Plan Rows"))) is not None)
        if output_rows is not None and input_rows > 0:
            filter_selectivities.append(max(0.0, min(1.0, output_rows / input_rows)))

    seq_scan_nodes = sum(node_type == "seq scan" for node_type in types)
    index_scan_nodes = sum(node_type in {"index scan", "index only scan", "bitmap index scan", "bitmap heap scan"}
                           for node_type in types)
    derived_values = {
        "join_build_rows_max": max(join_build_rows) if join_build_rows else None,
        "join_build_width_max": max(join_build_width) if join_build_width else None,
        "join_build_bytes_max": max(join_build_bytes) if join_build_bytes else None,
        "join_input_bytes_max": max(join_input_bytes) if join_input_bytes else None,
        "join_output_bytes_max": max(join_output_bytes) if join_output_bytes else None,
        "plan_depth": plan_depth,
        "plan_bytes_max": plan_bytes_max,
        "plan_bytes_sum": plan_bytes_sum,
        "aggregate_input_bytes_max": max(aggregate_input_bytes) if aggregate_input_bytes else None,
        "materialize_bytes_max": max(materialize_bytes) if materialize_bytes else None,
        "seq_scan_nodes": float(seq_scan_nodes),
        "index_scan_nodes": float(index_scan_nodes),
        "filter_selectivity_min": min(filter_selectivities) if filter_selectivities else None,
    }
    for name, value in derived_values.items():
        if value is None:
            missing.append(name)
    if not row_values:
        missing.append("max_plan_rows_log10")
    if not width_values:
        missing.append("max_plan_width")
    if "Total Cost" not in root or root.get("Total Cost") is None:
        missing.append("total_cost")
    if sort_nodes and (sort_input_rows is None or sort_tuple_width is None):
        missing.append("sort_input_bytes")
    if not nodes:
        missing.append("plan_node_count")
    if sort_nodes and not sort_keys:
        missing.append("sort_key_count")
    return {
        "hash_join_nodes": sum(node == "hash join" for node in types),
        "sort_nodes": sum(node == "sort" for node in types),
        "aggregate_nodes": sum(node in {"aggregate", "hashaggregate", "groupaggregate"} for node in types),
        "window_nodes": sum(node == "windowagg" for node in types),
        "max_plan_rows_log10": round(math.log10(max_rows + 1.0), 6) if max_rows is not None else None,
        "max_plan_width": max_width,
        "total_cost": optional_float(root.get("Total Cost")),
        "parallel_workers_planned": max((int(node.get("Workers Planned", 0) or 0) for node in nodes), default=0),
        "parallel_aware_nodes": sum(bool(node.get("Parallel Aware", False)) for node in nodes),
        "plan_node_count": len(nodes),
        "sort_input_rows": sort_input_rows,
        "sort_tuple_width": sort_tuple_width,
        "sort_input_bytes": sort_input_bytes,
        "sort_key_count": max(sort_keys) if sort_keys else (0 if not sort_nodes else None),
        **derived_values,
        "plan_feature_missing": missing,
    }


def system_memory() -> dict[str, Any]:
    total = available = None
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                total = int(line.split()[1]) // 1024
            elif line.startswith("MemAvailable:"):
                available = int(line.split()[1]) // 1024
    except OSError:
        pass
    pressure = None if total is None or available is None or total <= 0 else round(max(0.0, min(1.0, (total - available) / total)), 4)
    missing = []
    if total is None:
        missing.append("system_total_memory_mb")
    if available is None:
        missing.append("system_available_memory_mb")
    return {
        "system_total_memory_mb": total,
        "system_available_memory_mb": available,
        "memory_pressure_score": pressure,
        "runtime_feature_missing": missing,
    }


def runtime_state(conn) -> dict[str, Any]:
    state = {"active_sessions": None, "current_session_private_memory_mb": None, "runtime_feature_missing": []}
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND pid != pg_backend_pid()")
            state["active_sessions"] = int(cur.fetchone()[0] or 0)
        except Exception:
            conn.rollback()
            state["runtime_feature_missing"].append("active_sessions")
        try:
            cur.execute("SELECT coalesce(sum(total_bytes) / (1024*1024)::float, 0) FROM pg_backend_memory_contexts")
            state["current_session_private_memory_mb"] = round(float(cur.fetchone()[0] or 0), 3)
        except Exception:
            conn.rollback()
            state["runtime_feature_missing"].append("current_session_private_memory_mb")
    system = system_memory()
    state["runtime_feature_missing"].extend(system.pop("runtime_feature_missing", []))
    return state | system


def relation_profile(conn, sql: str) -> dict[str, Any]:
    tables = sorted(dict.fromkeys(match.group(1).strip('"') for match in TABLE_RE.finditer(sql)))
    rows: list[dict[str, Any]] = []
    table_total = index_total = 0.0
    errors: list[str] = []
    with conn.cursor() as cur:
        for table in tables:
            try:
                cur.execute(
                    "SELECT pg_total_relation_size(%s::regclass) / (1024*1024)::float, "
                    "pg_indexes_size(%s::regclass) / (1024*1024)::float",
                    (table, table),
                )
                table_mb, index_mb = cur.fetchone()
                table_mb = optional_float(table_mb)
                index_mb = optional_float(index_mb)
                # A referenced relation must have a positive catalog size. A
                # zero/NULL result usually means the catalog lookup was
                # unavailable or the wrong database/schema was used; retaining
                # it as a valid zero would hide this provenance failure.
                if table_mb is None or table_mb <= 0:
                    raise ValueError("relation size is zero or unavailable")
                if index_mb is None or index_mb < 0:
                    raise ValueError("index size is unavailable")
                table_total += table_mb
                index_total += index_mb
                rows.append({"table": table, "table_size_mb": round(table_mb, 3), "index_size_mb": round(index_mb, 3)})
            except Exception:
                conn.rollback()
                error = f"{table}: relation lookup failed"
                errors.append(error)
                rows.append({"table": table, "table_size_mb": None, "index_size_mb": None, "error": error})
    status = "no_relations" if not tables else "ok" if not errors else "missing"
    return {
        "tables": rows,
        "status": status,
        "errors": errors,
        "involved_table_total_size_mb": round(table_total, 3) if status == "ok" else None,
        "involved_index_total_size_mb": round(index_total, 3) if status == "ok" else None,
    }


def build_feature_payload(conn, sql: str, static_plan_payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Build model features from the static plan that accompanied a label."""
    features = {
        "static_plan_features": plan_features(static_plan_payload),
        "runtime_state": runtime_state(conn),
        "relation_profile": relation_profile(conn, sql),
    }
    missing = list(features["static_plan_features"].get("plan_feature_missing", []))
    missing.extend(features["runtime_state"].get("runtime_feature_missing", []))
    relation = features["relation_profile"]
    if relation.get("status") != "ok":
        missing.extend(["involved_table_total_size_mb", "involved_index_total_size_mb"])
    features["feature_quality"] = {
        "missing": sorted(set(missing)),
        "relation_status": relation.get("status"),
        "plan_source": "fresh_explain_json",
        "relation_source": "catalog_query",
    }
    text = (
        f"# SQL\n{sql}\n\n# Static plan features\n"
        f"{__import__('json').dumps(features['static_plan_features'], ensure_ascii=False, indent=2)}\n\n"
        f"# System and session state\n{__import__('json').dumps(features['runtime_state'], ensure_ascii=False, indent=2)}\n\n"
        f"# Related table and index size\n{__import__('json').dumps(features['relation_profile'], ensure_ascii=False, indent=2)}\n"
    )
    return features, text


def build_sft_sample(conn, record: dict[str, Any], static_plan_payload: dict[str, Any]) -> dict[str, Any]:
    sql = record["sql"]
    features, text = build_feature_payload(conn, sql, static_plan_payload)
    return {
        "schema_version": record.get("schema_version", SCHEMA_VERSION),
        "label_protocol": record.get("label_protocol", LABEL_PROTOCOL),
        "label_semantics": record.get("label_semantics", LABEL_SEMANTICS),
        "id": record["query_id"],
        "instruction": THREE_VALUE_INSTRUCTION,
        "input": text,
        "output": "[{},{},{}]".format(record["mem_cache_mb"], record["mem_one_pass_mb"], record["mem_multi_pass_mb"]),
        "sql": sql,
        "dataset": record["workload"],
        "sf": record["sf"],
        "dbname": record["dbname"],
        "query_id": record["query_id"],
        "features": features,
        "plan_payload": static_plan_payload,
        "bounds": {
            "cache_mb": record["mem_cache_mb"],
            "one_pass_mb": record["mem_one_pass_mb"],
            "multi_pass_mb": record["mem_multi_pass_mb"],
            "multi_pass_lower_mb": record.get("multi_pass_lower_mb"),
            "multi_pass_upper_mb": record.get("multi_pass_upper_mb", record["mem_multi_pass_mb"]),
            "label_protocol": record.get("label_protocol", LABEL_PROTOCOL),
            "label_semantics": record.get("label_semantics", LABEL_SEMANTICS),
            "multi_pass_definition": record.get("multi_pass_definition", MULTI_PASS_DEFINITION),
            "multi_pass_status": record.get("multi_pass_status", "observed"),
        },
        "multi_pass_status": record.get("multi_pass_status", "observed"),
        "repeat_count": record.get("repeat_count", 1),
        "repeat_bounds": record.get("repeat_bounds"),
        "boundary_stability_mb": record.get("boundary_stability_mb"),
        "collection_preflight": record["collection_preflight"],
    }
