#!/usr/bin/env python3
"""Train and evaluate versioned XGBoost memory-bound regressors.

The input is the three-bound JSONL emitted by the memory-bound collector. SQL
text is used only to form leakage-safe template groups; the model consumes
planner and relation features. Two regressors are trained (cache and
one-pass); multi-pass is the upper edge immediately below one-pass and is
derived at prediction time.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import warnings
from pathlib import Path
from typing import Any, Iterable

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import memory_bounds


MIN_MB = 0.0625
MULTI_PASS_DELTA_MB = 1.0 / 1024.0
TARGETS = ("cache_mb", "one_pass_mb")
SCHEMA_VERSION = "memory-bounds-v2"
LABEL_PROTOCOL = "native-v2"
LABEL_SEMANTICS = "native_executor_boundaries"
MODEL_ABI_VERSION = "xgb-memory-v2"

# The v1 ABI is kept for reproducing the existing artifact only. New training
# must use FEATURE_NAMES_V2 and native-v2 records.
FEATURE_NAMES_V1 = (
    "hash_join_nodes",
    "sort_nodes",
    "aggregate_nodes",
    "window_nodes",
    "max_plan_rows_log10",
    "max_plan_width",
    "total_cost",
    "parallel_workers_planned",
    "parallel_aware_nodes",
    "system_available_memory_mb",
    "memory_pressure_score",
    "involved_table_total_size_mb",
    "involved_index_total_size_mb",
    "table_count",
    "index_table_ratio",
    "total_cost_log1p",
    "table_size_log1p",
    "index_size_log1p",
    "rows_width_log1p",
)

FEATURE_NAMES_V2_BASE = FEATURE_NAMES_V1 + (
    "active_sessions",
    "current_session_private_memory_mb",
    "system_total_memory_mb",
    "plan_node_count",
    "sort_input_rows",
    "sort_tuple_width",
    "sort_input_bytes",
    "sort_key_count",
    "sort_input_rows_log1p",
    "sort_input_bytes_log1p",
)

MISSING_FEATURE_BASES = (
    "max_plan_rows_log10",
    "max_plan_width",
    "total_cost",
    "system_available_memory_mb",
    "memory_pressure_score",
    "involved_table_total_size_mb",
    "involved_index_total_size_mb",
    "table_count",
    "index_table_ratio",
    "sort_input_rows",
    "sort_tuple_width",
    "sort_input_bytes",
    "active_sessions",
    "current_session_private_memory_mb",
    "system_total_memory_mb",
    "plan_node_count",
    "sort_key_count",
)

FEATURE_NAMES_V2 = FEATURE_NAMES_V2_BASE + tuple(f"{name}_missing" for name in MISSING_FEATURE_BASES)

P0_PLAN_FEATURES = (
    "join_build_rows_max",
    "join_build_width_max",
    "join_build_bytes_max",
    "join_input_bytes_max",
    "join_output_bytes_max",
    "plan_depth",
    "plan_bytes_max",
    "plan_bytes_sum",
    "aggregate_input_bytes_max",
    "materialize_bytes_max",
    "seq_scan_nodes",
    "index_scan_nodes",
    "filter_selectivity_min",
)

MIXED_WORKLOADS = ("tpch", "tpcds", "dsb", "job", "gsbench")
FEATURE_NAMES_MIXED = FEATURE_NAMES_V2 + tuple(
    f"workload_{name}" for name in MIXED_WORKLOADS
) + ("scale_factor", "scale_factor_log1p")

FEATURE_NAMES_V3_BASE = FEATURE_NAMES_V2_BASE + P0_PLAN_FEATURES
MISSING_FEATURES_V3 = MISSING_FEATURE_BASES + P0_PLAN_FEATURES
FEATURE_NAMES_V3 = FEATURE_NAMES_V3_BASE + tuple(f"{name}_missing" for name in MISSING_FEATURES_V3)
FEATURE_NAMES_MIXED_V3 = FEATURE_NAMES_V3 + tuple(
    f"workload_{name}" for name in MIXED_WORKLOADS
) + ("scale_factor", "scale_factor_log1p")

# Environment-independent mixed ABI used for feature-removal experiments.
# Keep FEATURE_NAMES_MIXED unchanged so the existing artifact remains
# reproducible and loadable.
FEATURE_NAMES_MIXED_CLEAN = tuple(
    name for name in FEATURE_NAMES_MIXED
    if name not in {
        "system_available_memory_mb",
        "memory_pressure_score",
        "active_sessions",
        "current_session_private_memory_mb",
        "system_total_memory_mb",
        "scale_factor",
        "scale_factor_log1p",
    }
    and not name.startswith("workload_")
    and not name.endswith("_missing")
)

FEATURE_NAMES_MIXED_CLEAN_P0 = tuple(
    name for name in FEATURE_NAMES_MIXED_V3
    if name not in {
        "system_available_memory_mb",
        "memory_pressure_score",
        "active_sessions",
        "current_session_private_memory_mb",
        "system_total_memory_mb",
        "scale_factor",
        "scale_factor_log1p",
    }
    and not name.startswith("workload_")
    and not name.endswith("_missing")
)

# Candidate-only ABI for the GSBench sort coverage work.  All fields are
# derived from a static EXPLAIN and are intentionally absent from the deployed
# mixed_clean ABI until a candidate passes the locked GSBench and AMM gates.
SORT_SHAPE_FEATURES = (
    "sort_tuple_width_log1p",
    "sort_key_count_log1p",
    "sort_nodes_log1p",
    "plan_node_count_log1p",
    "sort_bytes_per_plan_node_log1p",
    "sort_bytes_per_sort_node_log1p",
    "non_sort_plan_nodes",
)
FEATURE_NAMES_MIXED_CLEAN_SORT = FEATURE_NAMES_MIXED_CLEAN + SORT_SHAPE_FEATURES

# Public compatibility alias used by the original v1 evaluator.
FEATURE_NAMES = FEATURE_NAMES_V1

_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9_])\d+(?:\.\d+)?(?![A-Za-z0-9_])")
_SPACE_RE = re.compile(r"\s+")


def features_from_input(text: str) -> dict[str, Any]:
    """Decode feature JSON blocks used by the earlier scalar AP datasets."""
    decoded: dict[str, Any] = {}
    decoder = json.JSONDecoder()
    sections = (
        ("# Static plan features", "static_plan_features"),
        ("# System and session state", "runtime_state"),
        ("# Related table and index size", "relation_profile"),
    )
    for marker, name in sections:
        marker_pos = str(text or "").find(marker)
        if marker_pos < 0:
            continue
        start = str(text).find("{", marker_pos + len(marker))
        if start < 0:
            continue
        try:
            value, _ = decoder.raw_decode(str(text)[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            decoded[name] = value
    return decoded


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def normalize_sql(sql: str) -> str:
    """Normalize literals while retaining SQL structure for group splitting."""
    text = str(sql or "").lower()
    text = re.sub(r"'(?:''|[^'])*'", "'?'", text)
    text = _NUMBER_RE.sub("?", text)
    return _SPACE_RE.sub(" ", text).strip()


def template_group(record: dict[str, Any]) -> str:
    """Return a stable structural group without trusting row identifiers.

    New generators persist ``template_family`` so independently instantiated
    parameter sets cannot cross a split boundary.  Historical data falls back
    to literal-normalized SQL, preserving backwards-compatible reproducibility.
    """
    family = str(record.get("template_family") or "").strip()
    if family:
        return f"family:{family}"
    return f"sql:{normalize_sql(record.get('sql', ''))}"


def walk_plan(node: dict[str, Any]) -> Iterable[dict[str, Any]]:
    yield node
    for child in node.get("Plans", []) or []:
        if isinstance(child, dict):
            yield from walk_plan(child)


def feature_row_v1(record: dict[str, Any]) -> dict[str, float]:
    features = record.get("features") or {}
    if not features and record.get("input"):
        features = features_from_input(str(record["input"]))
    static = features.get("static_plan_features") or {}
    runtime = features.get("runtime_state") or {}
    relation = features.get("relation_profile") or {}
    table_size = finite(relation.get("involved_table_total_size_mb"))
    index_size = finite(relation.get("involved_index_total_size_mb"))
    total_cost = finite(static.get("total_cost"))
    rows_log10 = finite(static.get("max_plan_rows_log10"))
    width = finite(static.get("max_plan_width"))
    tables = relation.get("tables") or []
    values = {
        "hash_join_nodes": finite(static.get("hash_join_nodes")),
        "sort_nodes": finite(static.get("sort_nodes")),
        "aggregate_nodes": finite(static.get("aggregate_nodes")),
        "window_nodes": finite(static.get("window_nodes")),
        "max_plan_rows_log10": rows_log10,
        "max_plan_width": width,
        "total_cost": total_cost,
        "parallel_workers_planned": finite(static.get("parallel_workers_planned")),
        "parallel_aware_nodes": finite(static.get("parallel_aware_nodes")),
        "system_available_memory_mb": finite(runtime.get("system_available_memory_mb")),
        "memory_pressure_score": finite(runtime.get("memory_pressure_score")),
        "involved_table_total_size_mb": table_size,
        "involved_index_total_size_mb": index_size,
        "table_count": float(len(tables)) if isinstance(tables, list) else 0.0,
        "index_table_ratio": index_size / max(table_size, 1e-6),
        "total_cost_log1p": math.log1p(max(total_cost, 0.0)),
        "table_size_log1p": math.log1p(max(table_size, 0.0)),
        "index_size_log1p": math.log1p(max(index_size, 0.0)),
        "rows_width_log1p": math.log1p(max(rows_log10 * width, 0.0)),
    }
    return {name: finite(values.get(name)) for name in FEATURE_NAMES}


def feature_row_v2(record: dict[str, Any]) -> tuple[dict[str, float], list[str]]:
    """Build v2 features while retaining missingness as an explicit signal."""
    features = record.get("features") or {}
    if not features and record.get("input"):
        features = features_from_input(str(record["input"]))
    static = features.get("static_plan_features") or {}
    runtime = features.get("runtime_state") or {}
    relation = features.get("relation_profile") or {}
    tables = relation.get("tables")
    table_size = optional_float(relation.get("involved_table_total_size_mb"))
    index_size = optional_float(relation.get("involved_index_total_size_mb"))
    total_cost = optional_float(static.get("total_cost"))
    rows_log10 = optional_float(static.get("max_plan_rows_log10"))
    width = optional_float(static.get("max_plan_width"))
    available = optional_float(runtime.get("system_available_memory_mb"))
    pressure = optional_float(runtime.get("memory_pressure_score"))
    active_sessions = optional_float(runtime.get("active_sessions"))
    private_memory = optional_float(runtime.get("current_session_private_memory_mb"))
    total_memory = optional_float(runtime.get("system_total_memory_mb"))
    node_count = optional_float(static.get("plan_node_count"))
    sort_rows = optional_float(static.get("sort_input_rows"))
    sort_width = optional_float(static.get("sort_tuple_width"))
    sort_bytes = optional_float(static.get("sort_input_bytes"))
    sort_keys = optional_float(static.get("sort_key_count"))
    if sort_bytes is None and sort_rows is not None and sort_width is not None:
        sort_bytes = sort_rows * sort_width
    table_count = float(len(tables)) if isinstance(tables, list) else None
    index_ratio = index_size / table_size if table_size and table_size > 0 and index_size is not None else None
    values: dict[str, float | None] = {
        "hash_join_nodes": optional_float(static.get("hash_join_nodes")),
        "sort_nodes": optional_float(static.get("sort_nodes")),
        "aggregate_nodes": optional_float(static.get("aggregate_nodes")),
        "window_nodes": optional_float(static.get("window_nodes")),
        "max_plan_rows_log10": rows_log10,
        "max_plan_width": width,
        "total_cost": total_cost,
        "parallel_workers_planned": optional_float(static.get("parallel_workers_planned")),
        "parallel_aware_nodes": optional_float(static.get("parallel_aware_nodes")),
        "system_available_memory_mb": available,
        "memory_pressure_score": pressure,
        "involved_table_total_size_mb": table_size,
        "involved_index_total_size_mb": index_size,
        "table_count": table_count,
        "index_table_ratio": index_ratio,
        "total_cost_log1p": math.log1p(max(total_cost, 0.0)) if total_cost is not None else None,
        "table_size_log1p": math.log1p(max(table_size, 0.0)) if table_size is not None else None,
        "index_size_log1p": math.log1p(max(index_size, 0.0)) if index_size is not None else None,
        "rows_width_log1p": math.log1p(max(rows_log10 * width, 0.0)) if rows_log10 is not None and width is not None else None,
        "active_sessions": active_sessions,
        "current_session_private_memory_mb": private_memory,
        "system_total_memory_mb": total_memory,
        "plan_node_count": node_count,
        "sort_input_rows": sort_rows,
        "sort_tuple_width": sort_width,
        "sort_input_bytes": sort_bytes,
        "sort_key_count": sort_keys,
        "sort_input_rows_log1p": math.log1p(max(sort_rows, 0.0)) if sort_rows is not None else None,
        "sort_input_bytes_log1p": math.log1p(max(sort_bytes, 0.0)) if sort_bytes is not None else None,
    }
    missing = set(features.get("feature_quality", {}).get("missing", []) or [])
    missing.update(static.get("plan_feature_missing", []) or [])
    missing.update(runtime.get("runtime_feature_missing", []) or [])
    if relation.get("status") and relation.get("status") != "ok":
        missing.update(("involved_table_total_size_mb", "involved_index_total_size_mb"))
    for name in MISSING_FEATURE_BASES:
        if values.get(name) is None:
            missing.add(name)
    for name in MISSING_FEATURE_BASES:
        values[f"{name}_missing"] = 1.0 if name in missing else 0.0
    return {name: finite(values.get(name)) for name in FEATURE_NAMES_V2}, sorted(missing)


def feature_row_v3(record: dict[str, Any]) -> tuple[dict[str, float], list[str]]:
    """Build v2 features plus plan-derived P0 operator/resource features."""
    values_v2, missing = feature_row_v2(record)
    features = record.get("features") or {}
    static = dict(features.get("static_plan_features") or {})
    plan_payload = record.get("plan_payload")
    if isinstance(plan_payload, dict):
        # SFT records were created before P0 was introduced. Recompute the
        # static plan feature block from the retained EXPLAIN payload.
        derived = memory_bounds.plan_features(plan_payload)
        for name in P0_PLAN_FEATURES:
            if name not in static or static.get(name) is None:
                static[name] = derived.get(name)
        missing.extend(derived.get("plan_feature_missing", []))
    for name in P0_PLAN_FEATURES:
        value = optional_float(static.get(name))
        values_v2[name] = finite(value)
        if value is None:
            missing.append(name)
    for name in P0_PLAN_FEATURES:
        values_v2[f"{name}_missing"] = 1.0 if name in set(missing) else 0.0
    return {name: finite(values_v2.get(name)) for name in FEATURE_NAMES_V3}, sorted(set(missing))


def record_workload(record: dict[str, Any]) -> str:
    value = str(record.get("workload") or record.get("dataset") or "").strip().lower()
    if value in MIXED_WORKLOADS:
        return value
    dbname = str(record.get("dbname") or record.get("target") or "").lower()
    if "gsbench" in value or "gsbench" in dbname or "llm4sqlgen_s" in dbname:
        return "gsbench"
    for workload in MIXED_WORKLOADS:
        if workload in value or workload in dbname:
            return workload
    return "unknown"


def record_scale_factor(record: dict[str, Any]) -> float:
    for key in ("scale_factor", "sf", "size_gb"):
        value = optional_float(record.get(key))
        if value is not None and value > 0:
            return value
    text = str(record.get("dbname") or record.get("target") or "")
    match = re.search(r"(?:^|[_-])s(?:f)?(\d+(?:\.\d+)?)(?:gb)?(?:$|[_-])", text.lower())
    if match:
        return float(match.group(1))
    match = re.search(r"s(\d+(?:\.\d+)?)gb", text.lower())
    return float(match.group(1)) if match else 1.0


def feature_row_mixed(record: dict[str, Any]) -> tuple[dict[str, float], list[str]]:
    values, missing = feature_row_v2(record)
    workload = record_workload(record)
    scale = record_scale_factor(record)
    for name in MIXED_WORKLOADS:
        values[f"workload_{name}"] = 1.0 if workload == name else 0.0
    values["scale_factor"] = scale
    values["scale_factor_log1p"] = math.log1p(max(scale, 0.0))
    return {name: finite(values.get(name)) for name in FEATURE_NAMES_MIXED}, missing


def feature_row_mixed_clean(record: dict[str, Any]) -> tuple[dict[str, float], list[str]]:
    """Build the mixed feature row with environment/identity signals removed."""
    values, missing = feature_row_mixed(record)
    return {name: finite(values.get(name)) for name in FEATURE_NAMES_MIXED_CLEAN}, missing


def feature_row_mixed_clean_sort(record: dict[str, Any]) -> tuple[dict[str, float], list[str]]:
    """Add static Sort-shape features to the portable clean feature set."""
    values, missing = feature_row_mixed_clean(record)
    sort_width = optional_float(values.get("sort_tuple_width"))
    sort_keys = optional_float(values.get("sort_key_count"))
    sort_nodes = optional_float(values.get("sort_nodes"))
    plan_nodes = optional_float(values.get("plan_node_count"))
    sort_bytes = optional_float(values.get("sort_input_bytes"))

    # Missing Sort statistics are represented by the existing median
    # imputation path.  Do not add the historical *_missing features back to
    # this deployment-compatible candidate ABI.
    values.update({
        "sort_tuple_width_log1p": math.log1p(max(sort_width, 0.0)) if sort_width is not None else None,
        "sort_key_count_log1p": math.log1p(max(sort_keys, 0.0)) if sort_keys is not None else None,
        "sort_nodes_log1p": math.log1p(max(sort_nodes, 0.0)) if sort_nodes is not None else None,
        "plan_node_count_log1p": math.log1p(max(plan_nodes, 0.0)) if plan_nodes is not None else None,
        "sort_bytes_per_plan_node_log1p": (
            math.log1p(max(sort_bytes, 0.0) / max(plan_nodes, 1.0))
            if sort_bytes is not None and plan_nodes is not None else None
        ),
        "sort_bytes_per_sort_node_log1p": (
            math.log1p(max(sort_bytes, 0.0) / max(sort_nodes, 1.0))
            if sort_bytes is not None and sort_nodes is not None else None
        ),
        "non_sort_plan_nodes": max(plan_nodes - sort_nodes, 0.0)
        if plan_nodes is not None and sort_nodes is not None else None,
    })
    for name in SORT_SHAPE_FEATURES:
        if values.get(name) is None:
            missing.append(name)
    return {name: finite(values.get(name)) for name in FEATURE_NAMES_MIXED_CLEAN_SORT}, sorted(set(missing))


def feature_row_mixed_v3(record: dict[str, Any]) -> tuple[dict[str, float], list[str]]:
    values, missing = feature_row_v3(record)
    workload = record_workload(record)
    scale = record_scale_factor(record)
    for name in MIXED_WORKLOADS:
        values[f"workload_{name}"] = 1.0 if workload == name else 0.0
    values["scale_factor"] = scale
    values["scale_factor_log1p"] = math.log1p(max(scale, 0.0))
    return {name: finite(values.get(name)) for name in FEATURE_NAMES_MIXED_V3}, missing


def feature_row_mixed_clean_p0(record: dict[str, Any]) -> tuple[dict[str, float], list[str]]:
    values, missing = feature_row_mixed_v3(record)
    return {name: finite(values.get(name)) for name in FEATURE_NAMES_MIXED_CLEAN_P0}, missing


def feature_row(record: dict[str, Any], feature_names: tuple[str, ...] = FEATURE_NAMES) -> dict[str, float]:
    if feature_names == FEATURE_NAMES_MIXED:
        return feature_row_mixed(record)[0]
    if feature_names == FEATURE_NAMES_MIXED_CLEAN:
        return feature_row_mixed_clean(record)[0]
    if feature_names == FEATURE_NAMES_MIXED_CLEAN_SORT:
        return feature_row_mixed_clean_sort(record)[0]
    if feature_names == FEATURE_NAMES_MIXED_CLEAN_P0:
        return feature_row_mixed_clean_p0(record)[0]
    if feature_names == FEATURE_NAMES_MIXED_V3:
        return feature_row_mixed_v3(record)[0]
    if feature_names == FEATURE_NAMES_V3:
        return feature_row_v3(record)[0]
    if feature_names == FEATURE_NAMES_V2:
        return feature_row_v2(record)[0]
    return feature_row_v1(record)


def row_label_protocol(row: dict[str, Any]) -> str | None:
    bounds = row.get("bounds") or {}
    return row.get("label_protocol") or bounds.get("label_protocol")


def load_rows(
    path: Path,
    protocol: str = "v2",
    allow_legacy_v1: bool = False,
    allow_missing_multi_pass: bool = False,
    observed_multi_pass_only: bool = False,
    feature_abi: str = "auto",
) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in path.open(encoding="utf-8") if line.strip()]
    result = []
    skipped_protocol = 0
    skipped_bounds = 0
    for row in rows:
        row_protocol = row_label_protocol(row)
        if protocol == "v2" and row_protocol != LABEL_PROTOCOL:
            skipped_protocol += 1
            continue
        if protocol == "v1" and row_protocol and row_protocol == LABEL_PROTOCOL:
            skipped_protocol += 1
            continue
        if protocol == "v1" and not row_protocol and not allow_legacy_v1:
            skipped_protocol += 1
            continue
        bounds = row.get("bounds") or {}
        values = [optional_float(bounds.get(key)) for key in ("cache_mb", "one_pass_mb", "multi_pass_mb")]
        if not all(value is not None and value > 0 for value in values[:2]):
            skipped_bounds += 1
            continue
        source_status = str(row.get("multi_pass_status") or "")
        source_semantics = str(row.get("label_semantics") or bounds.get("label_semantics") or "")
        observed_multi = bool(
            values[2] is not None
            and source_status not in {"derived", "missing"}
            and source_semantics == "native_executor_boundaries"
        )
        if observed_multi_pass_only and not observed_multi:
            skipped_bounds += 1
            continue
        if values[2] is None and allow_missing_multi_pass:
            row["_multi_pass_observed"] = False
            row["_multi_pass_status"] = "derived"
            values[2] = max(MIN_MB, values[1] - MULTI_PASS_DELTA_MB)
        elif values[2] is None or values[2] <= 0:
            skipped_bounds += 1
            continue
        else:
            row["_multi_pass_observed"] = observed_multi
            row["_multi_pass_status"] = "observed" if observed_multi else "derived"
        row["bounds"] = dict(bounds)
        row["bounds"]["cache_mb"] = values[0]
        row["bounds"]["one_pass_mb"] = values[1]
        row["bounds"]["multi_pass_mb"] = values[2]
        if not (values[0] >= values[1] >= values[2]):
            skipped_bounds += 1
            continue
        if feature_abi == "mixed":
            row["_features"], row["_feature_missing"] = feature_row_mixed(row)
        elif feature_abi == "mixed_clean":
            row["_features"], row["_feature_missing"] = feature_row_mixed_clean(row)
        elif feature_abi == "mixed_clean_sort":
            row["_features"], row["_feature_missing"] = feature_row_mixed_clean_sort(row)
        elif feature_abi == "mixed_clean_p0":
            row["_features"], row["_feature_missing"] = feature_row_mixed_clean_p0(row)
        elif feature_abi == "mixed_v3":
            row["_features"], row["_feature_missing"] = feature_row_mixed_v3(row)
        elif feature_abi == "v3":
            row["_features"], row["_feature_missing"] = feature_row_v3(row)
        elif protocol == "v2":
            row["_features"], row["_feature_missing"] = feature_row_v2(row)
        else:
            row["_features"] = feature_row_v1(row)
            row["_feature_missing"] = []
        row["_protocol"] = row_protocol or "legacy-v1"
        row["_template"] = hashlib.sha1(template_group(row).encode()).hexdigest()[:16]
        row["_scale"] = str(row.get("dbname", "unknown"))
        row["_workload"] = record_workload(row)
        row["_scale_factor"] = record_scale_factor(row)
        result.append(row)
    if not result:
        raise ValueError(
            f"no valid {protocol} rows in {path} (protocol_skipped={skipped_protocol}, "
            f"bounds_skipped={skipped_bounds}); use --protocol v1 --allow-legacy-v1 for old data"
        )
    return result


def fit_imputation(rows: list[dict[str, Any]], feature_names: tuple[str, ...], indices: np.ndarray | None = None) -> dict[str, float]:
    selected = rows if indices is None else [rows[int(i)] for i in indices]
    imputation: dict[str, float] = {}
    for name in feature_names:
        if name.endswith("_missing"):
            imputation[name] = 0.0
            continue
        values = [float(row["_features"].get(name, 0.0)) for row in selected if name not in row.get("_feature_missing", [])]
        imputation[name] = float(np.median(values)) if values else 0.0
    return imputation


def matrix_from_rows(rows: list[dict[str, Any]], feature_names: tuple[str, ...], imputation: dict[str, float]) -> np.ndarray:
    matrix = []
    for row in rows:
        missing = set(row.get("_feature_missing", []))
        matrix.append([
            float(imputation.get(name, 0.0) if name in missing else row["_features"].get(name, imputation.get(name, 0.0)))
            for name in feature_names
        ])
    return np.asarray(matrix, dtype=np.float32)


def feature_ranges(rows: list[dict[str, Any]], feature_names: tuple[str, ...]) -> dict[str, dict[str, float | int]]:
    output: dict[str, dict[str, float | int]] = {}
    for name in feature_names:
        if name.endswith("_missing"):
            continue
        values = [float(row["_features"].get(name, 0.0)) for row in rows if name not in row.get("_feature_missing", [])]
        if values:
            output[name] = {"count": len(values), "min": min(values), "max": max(values)}
        else:
            output[name] = {"count": 0, "min": 0.0, "max": 0.0}
    return output


def ood_warnings(rows: list[dict[str, Any]], ranges: dict[str, dict[str, float | int]], feature_names: tuple[str, ...]) -> list[list[dict[str, Any]]]:
    warnings_by_row: list[list[dict[str, Any]]] = []
    for row in rows:
        current: list[dict[str, Any]] = []
        missing = set(row.get("_feature_missing", []))
        for name in feature_names:
            if name.endswith("_missing"):
                continue
            if name in missing:
                current.append({"code": "MISSING_FEATURE", "feature": name})
                continue
            bounds = ranges.get(name)
            value = float(row["_features"].get(name, 0.0))
            if bounds and (value < float(bounds["min"]) or value > float(bounds["max"])):
                current.append({
                    "code": "OOD_FEATURE",
                    "feature": name,
                    "value": value,
                    "train_min": float(bounds["min"]),
                    "train_max": float(bounds["max"]),
                })
        warnings_by_row.append(current)
    return warnings_by_row


def warning_query_counts(warnings_by_row: list[list[dict[str, Any]]]) -> dict[str, int]:
    """Count OOD and missing-feature warnings independently per query."""
    return {
        "ood_query_count": int(sum(
            any(warning.get("code") == "OOD_FEATURE" for warning in warnings)
            for warnings in warnings_by_row
        )),
        "missing_feature_query_count": int(sum(
            any(warning.get("code") == "MISSING_FEATURE" for warning in warnings)
            for warnings in warnings_by_row
        )),
    }


def choose_gpu() -> int | None:
    try:
        raw = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=index,memory.used,memory.total", "--format=csv,noheader,nounits"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        cards = []
        for line in raw.splitlines():
            idx, used, total = [int(x.strip()) for x in line.split(",")]
            cards.append((used / max(total, 1), used, idx))
        return min(cards)[2] if cards else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def make_params(args: argparse.Namespace, params: dict[str, Any] | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "objective": args.objective,
        "n_estimators": args.n_estimators,
        "max_depth": args.max_depth,
        "learning_rate": args.learning_rate,
        "min_child_weight": args.min_child_weight,
        "subsample": 0.9,
        "colsample_bytree": 0.9,
        "reg_alpha": 0.05,
        "reg_lambda": 2.0,
        "max_bin": args.max_bin,
        "n_jobs": args.n_jobs,
        "random_state": args.seed,
    }
    if args.objective == "reg:quantileerror":
        result["quantile_alpha"] = args.quantile_alpha
    if params:
        result.update(params)
    return result


def make_model(xgb: Any, args: argparse.Namespace, params: dict[str, Any] | None = None):
    kwargs = make_params(args, params)
    if args.device == "cuda":
        # XGBoost >=2 uses device; older releases use gpu_hist/gpu_id.
        kwargs.update(device="cuda", tree_method="hist")
    else:
        kwargs.update(tree_method="hist", device="cpu")
    try:
        return xgb.XGBRegressor(**kwargs)
    except TypeError:
        kwargs.pop("device", None)
        if args.device == "cuda":
            # CUDA_VISIBLE_DEVICES remaps the selected physical card to 0.
            kwargs.update(tree_method="gpu_hist", gpu_id=0)
        return xgb.XGBRegressor(**kwargs)


def predict_pair(models: dict[str, Any], x: np.ndarray, calibration: tuple[float, float] = (1.0, 1.0)) -> np.ndarray:
    # Apply calibration before the final projection.  A factor below one can
    # otherwise push a prediction under MIN_MB and break cache >= one-pass >=
    # multi-pass ordering.
    cache = np.maximum(
        np.expm1(np.asarray(models["cache_mb"].predict(x), dtype=float)) * float(calibration[0]),
        MIN_MB,
    )
    one = np.maximum(
        np.expm1(np.asarray(models["one_pass_mb"].predict(x), dtype=float)) * float(calibration[1]),
        MIN_MB,
    )
    one = np.minimum(one, cache)
    multi = np.maximum(MIN_MB, one - MULTI_PASS_DELTA_MB)
    multi = np.minimum(multi, one)
    return np.column_stack((cache, one, multi))


def metric_rows(gold: np.ndarray, pred: np.ndarray) -> dict[str, Any]:
    qerror = np.maximum(pred / gold, gold / np.maximum(pred, MIN_MB))
    abs_error = np.abs(pred - gold)
    return {
        "count": int(len(gold)),
        "qerror_mean": float(np.mean(qerror)),
        "qerror_median": float(np.median(qerror)),
        "qerror_p90": float(np.quantile(qerror, 0.9, method="linear")),
        "qerror_le_2": float(np.mean(qerror <= 2.0)),
        "qerror_le_5": float(np.mean(qerror <= 5.0)),
        "qerror_le_10": float(np.mean(qerror <= 10.0)),
        "mae_mb": float(np.mean(abs_error)),
        "rmse_mb": float(np.sqrt(np.mean((pred - gold) ** 2))),
        "log_rmse": float(np.sqrt(np.mean((np.log1p(pred) - np.log1p(gold)) ** 2))),
        "low_estimate_rate": float(np.mean(pred < gold)),
    }


def order_accuracy(gold: np.ndarray, pred: np.ndarray) -> float:
    return float(np.mean((pred[:, 0] >= pred[:, 1]) & (pred[:, 1] >= pred[:, 2])))


def grouped_metrics(rows: list[dict[str, Any]], gold: np.ndarray, pred: np.ndarray, key: str) -> dict[str, Any]:
    groups: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        groups.setdefault(str(row.get(key, "unknown")), []).append(i)
    output = {}
    for name, indices in sorted(groups.items()):
        ix = np.asarray(indices, dtype=int)
        output[name] = metric_rows(gold[ix], pred[ix])
        output[name]["order_accuracy"] = order_accuracy(gold[ix], pred[ix])
    return output


def gsbench_shape_metrics(rows: list[dict[str, Any]], gold: np.ndarray, pred: np.ndarray) -> dict[str, Any]:
    """Metrics for static plan regimes used by the GSBench coverage plan."""
    groups: dict[str, list[int]] = {"all_gsbench": [], "wide_sort": [], "large_sort_input": [], "deep_plan": []}
    for index, row in enumerate(rows):
        if record_workload(row) != "gsbench":
            continue
        static = (row.get("features") or {}).get("static_plan_features") or {}
        width = optional_float(static.get("sort_tuple_width")) or 0.0
        bytes_value = optional_float(static.get("sort_input_bytes")) or 0.0
        nodes = optional_float(static.get("plan_node_count")) or 0.0
        groups["all_gsbench"].append(index)
        if width >= 512:
            groups["wide_sort"].append(index)
        if bytes_value >= 64 * 1024 * 1024:
            groups["large_sort_input"].append(index)
        if nodes >= 8:
            groups["deep_plan"].append(index)
    output = {}
    for name, indices in groups.items():
        if not indices:
            output[name] = {"count": 0}
            continue
        ix = np.asarray(indices, dtype=int)
        output[name] = metric_rows(gold[ix], pred[ix])
        output[name]["order_accuracy"] = order_accuracy(gold[ix], pred[ix])
        output[name]["low_estimate_rate"] = {
            "cache_mb": float(np.mean(pred[ix, 0] < gold[ix, 0])),
            "one_pass_mb": float(np.mean(pred[ix, 1] < gold[ix, 1])),
        }
    return output


def fit_models(xgb: Any, x: np.ndarray, y: np.ndarray, args: argparse.Namespace, params: dict[str, Any] | None = None):
    sample_weight = None
    if params and "sample_weight" in params:
        sample_weight = params["sample_weight"]
        params = {key: value for key, value in params.items() if key != "sample_weight"}
    models = {}
    for index, target in enumerate(TARGETS):
        model = make_model(xgb, args, params)
        model.fit(x, np.log1p(y[:, index]), sample_weight=sample_weight, verbose=False)
        models[target] = model
    return models


def save_model_artifact(model: Any, path: Path) -> None:
    """Save the portable Booster, avoiding sklearn-version metadata coupling."""
    model.get_booster().save_model(path)


def split_template(rows: list[dict[str, Any]], seed: int, test_fraction: float = 0.2) -> tuple[np.ndarray, np.ndarray]:
    groups = sorted({row["_template"] for row in rows})
    rng = np.random.default_rng(seed)
    rng.shuffle(groups)
    n_test = max(1, int(round(len(groups) * test_fraction)))
    test_groups = set(groups[:n_test])
    test = np.asarray([i for i, row in enumerate(rows) if row["_template"] in test_groups], dtype=int)
    train = np.asarray([i for i, row in enumerate(rows) if row["_template"] not in test_groups], dtype=int)
    return train, test


def scale_splits(rows: list[dict[str, Any]]) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    result = {}
    for scale in sorted({row["_scale"] for row in rows}):
        test = np.asarray([i for i, row in enumerate(rows) if row["_scale"] == scale], dtype=int)
        train = np.asarray([i for i, row in enumerate(rows) if row["_scale"] != scale], dtype=int)
        result[scale] = (train, test)
    return result


def workload_splits(rows: list[dict[str, Any]]) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    result = {}
    for workload in sorted({row.get("_workload", "unknown") for row in rows}):
        test = np.asarray([i for i, row in enumerate(rows) if row.get("_workload", "unknown") == workload], dtype=int)
        train = np.asarray([i for i, row in enumerate(rows) if row.get("_workload", "unknown") != workload], dtype=int)
        if len(train) and len(test):
            result[workload] = (train, test)
    return result


def sample_weights(rows: list[dict[str, Any]], balanced: bool) -> np.ndarray:
    if not balanced:
        return np.ones(len(rows), dtype=np.float32)
    counts = {}
    for row in rows:
        workload = row.get("_workload", "unknown")
        counts[workload] = counts.get(workload, 0) + 1
    n_workloads = max(len(counts), 1)
    weights = [len(rows) / (n_workloads * counts[row.get("_workload", "unknown")]) for row in rows]
    return np.asarray(weights, dtype=np.float32)


def gsbench_shape_weights(rows: list[dict[str, Any]], enabled: bool) -> np.ndarray:
    """Upweight underrepresented large-sort regions without using labels."""
    if not enabled:
        return np.ones(len(rows), dtype=np.float32)
    buckets: dict[tuple[str, str, str], int] = {}
    keys = []
    for row in rows:
        static = (row.get("features") or {}).get("static_plan_features") or {}
        width = optional_float(static.get("sort_tuple_width")) or 0.0
        bytes_value = optional_float(static.get("sort_input_bytes")) or 0.0
        nodes = optional_float(static.get("plan_node_count")) or 0.0
        width_bin = "w512+" if width >= 512 else "w256-512" if width >= 256 else "w128-256" if width >= 128 else "w0-128"
        byte_mb = bytes_value / (1024.0 * 1024.0)
        byte_bin = "b1024+" if byte_mb >= 1024 else "b256-1024" if byte_mb >= 256 else "b64-256" if byte_mb >= 64 else "b0-64"
        node_bin = "n8+" if nodes >= 8 else "n4-7" if nodes >= 4 else "n0-3"
        key = (record_workload(row), width_bin, byte_bin if record_workload(row) == "gsbench" else node_bin)
        keys.append(key)
        buckets[key] = buckets.get(key, 0) + 1
    target = max(1.0, float(np.median(list(buckets.values()))))
    weights = [min(4.0, max(0.5, target / buckets[key])) for key in keys]
    return np.asarray(weights, dtype=np.float32)


def ap_shape_weights(rows: list[dict[str, Any]], factor: float) -> np.ndarray:
    """Give deliberately collected three-key AP-shape measurements explicit weight."""
    return np.asarray([
        factor if str(row.get("target", "")) == "gsbench_ap_shape_coverage" else 1.0
        for row in rows
    ], dtype=np.float32)


def combined_sample_weights(rows: list[dict[str, Any]], args: argparse.Namespace) -> np.ndarray:
    base = sample_weights(rows, args.balance_workload)
    shape = gsbench_shape_weights(rows, args.gsbench_shape_weight)
    return base * shape * ap_shape_weights(rows, args.ap_shape_weight)


def select_calibration(gold: np.ndarray, pred: np.ndarray, grid: Iterable[float]) -> tuple[float, float]:
    """Select multiplicative per-bound factors on a validation fold.

    The primary score is mean q-error; low estimates are reported but are not
    treated as a hard constraint. The pair is searched jointly and then the
    normal cache >= one-pass projection is applied by predict_pair.
    """
    best = (1.0, 1.0)
    best_score = float("inf")
    values = tuple(float(value) for value in grid)
    for cache_factor in values:
        for one_factor in values:
            candidate = np.column_stack((pred[:, 0] * cache_factor, pred[:, 1] * one_factor))
            candidate[:, 1] = np.minimum(candidate[:, 1], candidate[:, 0])
            q = np.maximum(candidate / gold[:, :2], gold[:, :2] / np.maximum(candidate, MIN_MB))
            score = float(np.mean(q))
            if score < best_score - 1e-12:
                best_score = score
                best = (cache_factor, one_factor)
    return best


def fit_calibration(
    xgb: Any,
    rows: list[dict[str, Any]],
    y: np.ndarray,
    args: argparse.Namespace,
    feature_names: tuple[str, ...],
    params: dict[str, Any],
    seed: int,
) -> tuple[float, float]:
    """Fit a template-disjoint calibration model and select boundary factors.

    Calibration is intentionally learned from a separate template partition so
    the factor does not simply compensate for predictions on rows used to fit
    the calibration regressors.  The returned factors can be applied to a
    model refit on all rows.
    """
    if not args.calibrate or len(rows) < 2:
        return (1.0, 1.0)
    calibration_train, calibration_test = split_template(rows, seed)
    if not len(calibration_train) or not len(calibration_test):
        return (1.0, 1.0)
    fit_rows = [rows[int(i)] for i in calibration_train]
    eval_rows = [rows[int(i)] for i in calibration_test]
    calibration_imputation = fit_imputation(rows, feature_names, calibration_train)
    fit_x = matrix_from_rows(fit_rows, feature_names, calibration_imputation)
    eval_x = matrix_from_rows(eval_rows, feature_names, calibration_imputation)
    calibration_models = fit_models(
        xgb,
        fit_x,
        y[calibration_train],
        args,
        {**dict(params), "sample_weight": combined_sample_weights(fit_rows, args)},
    )
    calibration_pred = predict_pair(calibration_models, eval_x)
    return select_calibration(y[calibration_test], calibration_pred, args.calibration_grid)


def weighted_ood_summary(workload_holdout: dict[str, Any]) -> dict[str, float | int]:
    """Average workload holdout metrics equally across workload families."""
    reports = [value for value in workload_holdout.values() if value.get("count", 0)]
    if not reports:
        return {"workload_count": 0}
    return {
        "workload_count": len(reports),
        "qerror_mean_equal_workload": float(np.mean([value["qerror_mean"] for value in reports])),
        "qerror_p90_equal_workload": float(np.mean([value["qerror_p90"] for value in reports])),
        "low_estimate_rate_equal_workload": float(np.mean([
            value.get("low_estimate_rate", {}).get("one_pass_mb", 0.0) for value in reports
        ])),
    }


def run_eval(
    xgb: Any,
    rows: list[dict[str, Any]],
    y: np.ndarray,
    args: argparse.Namespace,
    train: np.ndarray,
    test: np.ndarray,
    params: dict[str, Any],
    name: str,
    output_dir: Path,
    feature_names: tuple[str, ...],
) -> dict[str, Any]:
    train_rows = [rows[int(i)] for i in train]
    test_rows = [rows[int(i)] for i in test]
    # Keep holdout preprocessing leakage-safe: medians and OOD ranges are fit
    # only from the corresponding training fold.
    imputation = fit_imputation(rows, feature_names, train)
    ranges = feature_ranges(train_rows, feature_names)
    train_x = matrix_from_rows(train_rows, feature_names, imputation)
    test_x = matrix_from_rows(test_rows, feature_names, imputation)
    train_weights = combined_sample_weights(train_rows, args)
    fit_params = dict(params)
    fit_params["sample_weight"] = train_weights
    models = fit_models(xgb, train_x, y[train], args, fit_params)
    calibration = fit_calibration(
        xgb, train_rows, y[train], args, feature_names, params, args.seed + len(name)
    )
    pred = predict_pair(models, test_x, calibration)
    gold = y[test]
    report = metric_rows(gold, pred)
    report["order_accuracy"] = order_accuracy(gold, pred)
    report["by_bound"] = {bound: metric_rows(gold[:, i], pred[:, i]) for i, bound in enumerate(("cache_mb", "one_pass_mb", "multi_pass_mb"))}
    report["by_scale"] = grouped_metrics([rows[i] for i in test], gold, pred, "_scale")
    report["by_template"] = grouped_metrics([rows[i] for i in test], gold, pred, "_template")
    report["gsbench_shape"] = gsbench_shape_metrics(test_rows, gold, pred)
    row_warnings = ood_warnings(test_rows, ranges, feature_names)
    report.update(warning_query_counts(row_warnings))
    report["multi_pass_observed_count"] = int(sum(bool(rows[int(i)].get("_multi_pass_observed", True)) for i in test))
    report["train_count"] = int(len(train)); report["test_count"] = int(len(test))
    report["low_estimate_rate"] = {
        "cache_mb": float(np.mean(pred[:, 0] < gold[:, 0])),
        "one_pass_mb": float(np.mean(pred[:, 1] < gold[:, 1])),
    }
    report["calibration"] = {"cache_mb": calibration[0], "one_pass_mb": calibration[1]}
    (output_dir / f"{name}_predictions.jsonl").write_text(
        "".join(json.dumps({"id": rows[int(i)].get("id"), "scale": rows[int(i)]["_scale"], "gold": gold[j].tolist(), "pred": pred[j].tolist(), "warnings": row_warnings[j]}, ensure_ascii=False) + "\n" for j, i in enumerate(test)),
        encoding="utf-8",
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=Path("output-memory-bounds-v2/sft/all.jsonl"))
    parser.add_argument("--output-dir", type=Path, default=Path("output-memory-xgb-v2"))
    parser.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
    parser.add_argument("--gpu-id", type=int, default=None)
    parser.add_argument("--n-jobs", type=int, default=2)
    parser.add_argument("--n-estimators", type=int, default=240)
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument("--learning-rate", type=float, default=0.04)
    parser.add_argument("--min-child-weight", type=float, default=2.0)
    parser.add_argument("--max-bin", type=int, default=64)
    parser.add_argument("--objective", choices=("reg:squarederror", "reg:quantileerror"), default="reg:squarederror")
    parser.add_argument("--quantile-alpha", type=float, default=0.55,
                        help="Quantile alpha for reg:quantileerror; values above 0.5 mildly penalize underestimates")
    parser.add_argument("--calibrate", action=argparse.BooleanOptionalAction, default=True,
                        help="Select multiplicative boundary calibration on a template-disjoint training fold")
    parser.add_argument("--calibration-grid", type=float, nargs="+",
                        default=(0.90, 0.95, 1.0, 1.05, 1.10, 1.20),
                        help="Candidate multiplicative factors for validation calibration")
    parser.add_argument("--seed", type=int, default=20260904)
    parser.add_argument("--protocol", choices=("v1", "v2"), default="v2")
    parser.add_argument("--feature-abi", choices=("auto", "v1", "v2", "v3", "mixed", "mixed_clean", "mixed_clean_sort", "mixed_v3", "mixed_clean_p0"), default="auto")
    parser.add_argument("--allow-missing-multi-pass", action="store_true",
                        help="Keep rows with valid cache/one-pass labels but no observed multi-pass boundary")
    parser.add_argument("--observed-multi-pass-only", action="store_true",
                        help="Train only on rows with an observed native-v2 multi-pass boundary")
    parser.add_argument("--balance-workload", action="store_true",
                        help="Use inverse-frequency sample weights per workload")
    parser.add_argument("--gsbench-shape-weight", action="store_true",
                        help="Upweight underrepresented static GSBench Sort shape buckets")
    parser.add_argument("--ap-shape-weight", type=float, default=1.0,
                        help="Multiplier for native gsbench_ap_shape_coverage rows (default: 1.0)")
    parser.add_argument("--allow-legacy-v1", action="store_true", help="Allow unversioned records with --protocol v1")
    args = parser.parse_args()
    if args.ap_shape_weight <= 0:
        parser.error("--ap-shape-weight must be positive")
    if not 0.0 < args.quantile_alpha < 1.0:
        parser.error("--quantile-alpha must be between 0 and 1")
    if not args.calibration_grid or any(not math.isfinite(value) or value <= 0 for value in args.calibration_grid):
        parser.error("--calibration-grid values must be finite and positive")

    if args.device == "auto":
        args.device = "cuda" if choose_gpu() is not None else "cpu"
    if args.device == "cuda":
        args.gpu_id = choose_gpu() if args.gpu_id is None else args.gpu_id
        if args.gpu_id is None:
            print("CUDA requested but no GPU was found; falling back to CPU", file=sys.stderr)
            args.device = "cpu"
    if args.device == "cuda":
        os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu_id)
        # Avoid XGBoost allocating a large global pool on a shared GPU.
        os.environ.setdefault("XGBOOST_BUILD_CACHE", "0")

    try:
        import xgboost as xgb
    except ImportError as exc:
        raise SystemExit("xgboost is required; install with `python -m pip install xgboost`") from exc

    feature_abi = args.feature_abi
    if feature_abi == "auto":
        feature_abi = "v2" if args.protocol == "v2" else "v1"
    feature_names = {
        "v1": FEATURE_NAMES_V1,
        "v2": FEATURE_NAMES_V2,
        "mixed": FEATURE_NAMES_MIXED,
        "mixed_clean": FEATURE_NAMES_MIXED_CLEAN,
        "mixed_clean_sort": FEATURE_NAMES_MIXED_CLEAN_SORT,
        "v3": FEATURE_NAMES_V3,
        "mixed_v3": FEATURE_NAMES_MIXED_V3,
        "mixed_clean_p0": FEATURE_NAMES_MIXED_CLEAN_P0,
    }[feature_abi]
    rows = load_rows(
        args.data,
        protocol=args.protocol,
        allow_legacy_v1=args.allow_legacy_v1,
        allow_missing_multi_pass=args.allow_missing_multi_pass,
        observed_multi_pass_only=args.observed_multi_pass_only,
        feature_abi=feature_abi,
    )
    imputation = fit_imputation(rows, feature_names)
    x = matrix_from_rows(rows, feature_names, imputation)
    y = np.asarray([[finite(row["bounds"]["cache_mb"]), finite(row["bounds"]["one_pass_mb"]),
                     finite(row["bounds"]["multi_pass_mb"])] for row in rows], dtype=np.float32)
    ranges = feature_ranges(rows, feature_names)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "dataset_manifest.json").write_text(json.dumps({
        "schema_version": SCHEMA_VERSION if args.protocol == "v2" else "legacy-v1",
        "label_protocol": LABEL_PROTOCOL if args.protocol == "v2" else "legacy-v1",
        "data": str(args.data), "rows": len(rows), "feature_names": list(feature_names),
        "target_names": [*TARGETS, "multi_pass_mb"],
        "target_transform": "log1p for cache_mb and one_pass_mb",
        "multi_pass_definition": "one_pass_boundary_kb_minus_1" if args.protocol == "v2" else "legacy: max(0.0625, one_pass_mb - 1/1024)",
        "feature_imputation": imputation,
        "feature_ranges": ranges,
        "sha256": hashlib.sha256(args.data.read_bytes()).hexdigest(),
        "device": args.device, "gpu_id": args.gpu_id,
        "feature_abi": feature_abi,
        "balance_workload": args.balance_workload,
        "gsbench_shape_weight": args.gsbench_shape_weight,
        "ap_shape_weight": args.ap_shape_weight,
        "multi_pass_observed_count": int(sum(bool(row.get("_multi_pass_observed", True)) for row in rows)),
    }, indent=2) + "\n", encoding="utf-8")

    params = make_params(args)
    template_train, template_test = split_template(rows, args.seed)
    report: dict[str, Any] = {
        "config": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "protocol": LABEL_PROTOCOL if args.protocol == "v2" else "legacy-v1",
        "template_holdout": run_eval(xgb, rows, y, args, template_train, template_test, params, "template_holdout", args.output_dir, feature_names),
    }
    report["scale_holdout"] = {}
    for scale, (train, test) in scale_splits(rows).items():
        report["scale_holdout"][scale] = run_eval(xgb, rows, y, args, train, test, params, f"scale_{scale}", args.output_dir, feature_names)
    report["workload_holdout"] = {}
    for workload, (train, test) in workload_splits(rows).items():
        report["workload_holdout"][workload] = run_eval(
            xgb, rows, y, args, train, test, params, f"workload_{workload}", args.output_dir, feature_names
        )
    report["ood_summary"] = weighted_ood_summary(report["workload_holdout"])

    final_calibration = fit_calibration(
        xgb, rows, y, args, feature_names, params, args.seed + 100003
    )
    final_params = dict(params)
    final_params["sample_weight"] = combined_sample_weights(rows, args)
    final_models = fit_models(xgb, x, y, args, final_params)
    final_pred = predict_pair(final_models, x, final_calibration)
    report["in_sample"] = metric_rows(y, final_pred)
    report["in_sample"]["order_accuracy"] = order_accuracy(y, final_pred)
    report["final_calibration"] = {
        "cache_mb": final_calibration[0],
        "one_pass_mb": final_calibration[1],
        "selection_metric": "mean_qerror",
        "source": "template_disjoint_validation",
    }
    report["feature_names"] = list(feature_names)
    report["notes"] = "Holdouts are leakage-safe; in_sample is descriptive only. AP/TPC-H should remain OOD evaluation."
    (args.output_dir / "metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    for target, model in final_models.items():
        save_model_artifact(model, args.output_dir / f"{target}.json")
    (args.output_dir / "calibration.json").write_text(json.dumps({
        "cache_mb": final_calibration[0],
        "one_pass_mb": final_calibration[1],
        "selection_metric": "mean_qerror",
        "source": "template_disjoint_validation",
        "grid": list(args.calibration_grid),
    }, indent=2) + "\n", encoding="utf-8")
    (args.output_dir / "model_config.json").write_text(json.dumps({
        "model_abi_version": MODEL_ABI_VERSION if args.protocol == "v2" else "xgb-memory-v1",
        "schema_version": SCHEMA_VERSION if args.protocol == "v2" else "legacy-v1",
        "label_protocol": LABEL_PROTOCOL if args.protocol == "v2" else "legacy-v1",
        "xgboost_version": getattr(xgb, "__version__", "unknown"),
        "feature_names": list(feature_names), "targets": list(TARGETS),
        "target_transform": "log1p", "device": args.device, "gpu_id": args.gpu_id,
        "feature_abi": feature_abi, "balance_workload": args.balance_workload,
        "gsbench_shape_weight": args.gsbench_shape_weight,
        "ap_shape_weight": args.ap_shape_weight,
        "objective": args.objective,
        "quantile_alpha": args.quantile_alpha,
        "calibrate": args.calibrate,
        "calibration_grid": list(args.calibration_grid),
        "calibration_file": "calibration.json",
        "calibration": {
            "cache_mb": final_calibration[0],
            "one_pass_mb": final_calibration[1],
            "selection_metric": "mean_qerror",
            "source": "template_disjoint_validation",
        },
        "observed_multi_pass_only": args.observed_multi_pass_only,
        "feature_imputation": imputation,
        "feature_ranges": ranges,
        "params": params,
    }, indent=2) + "\n", encoding="utf-8")
    np.save(args.output_dir / "feature_matrix.npy", x)
    print(json.dumps({"device": args.device, "gpu_id": args.gpu_id, "rows": len(rows), "template_holdout": report["template_holdout"], "metrics": str(args.output_dir / "metrics.json")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
