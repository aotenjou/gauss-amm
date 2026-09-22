#!/usr/bin/env python3
"""Evaluate a trained XGBoost artifact on scalar AP JSONL files."""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np

from train_xgb_memory import (
    FEATURE_NAMES_V1,
    FEATURE_NAMES_V2,
    FEATURE_NAMES_MIXED,
    FEATURE_NAMES_MIXED_CLEAN,
    FEATURE_NAMES_MIXED_CLEAN_SORT,
    FEATURE_NAMES_MIXED_V3,
    FEATURE_NAMES_MIXED_CLEAN_P0,
    FEATURE_NAMES_V3,
    LABEL_PROTOCOL,
    MIN_MB,
    MULTI_PASS_DELTA_MB,
    feature_row_v1,
    feature_row_v2,
    feature_row_mixed,
    feature_row_mixed_clean,
    feature_row_mixed_clean_sort,
    feature_row_mixed_v3,
    feature_row_mixed_clean_p0,
    feature_row_v3,
    matrix_from_rows,
    ood_warnings,
    row_label_protocol,
    warning_query_counts,
)


BOUNDS = ("cache", "one_pass", "multi_pass")


def load(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.open(encoding="utf-8") if line.strip()]


def scalar_metrics(gold: np.ndarray, pred: np.ndarray) -> dict[str, Any]:
    qerror = np.maximum(pred / gold, gold / np.maximum(pred, MIN_MB))
    delta = pred - gold
    return {
        "count": int(gold.size),
        "qerror_mean": float(np.mean(qerror)),
        "qerror_median": float(np.median(qerror)),
        "qerror_p90": float(np.quantile(qerror, 0.9, method="linear")),
        "qerror_le_2": float(np.mean(qerror <= 2.0)),
        "qerror_le_5": float(np.mean(qerror <= 5.0)),
        "qerror_le_10": float(np.mean(qerror <= 10.0)),
        "mae_mb": float(np.mean(np.abs(delta))),
        "rmse_mb": float(np.sqrt(np.mean(delta ** 2))),
        "log_rmse": float(np.sqrt(np.mean((np.log1p(pred) - np.log1p(gold)) ** 2))),
    }


def bound_order_accuracy(pred: np.ndarray, bounds: tuple[str, ...]) -> float:
    """Check the ordering constraints represented by the evaluated bounds."""
    if len(bounds) < 2:
        return 1.0
    indices = {name: index for index, name in enumerate(bounds)}
    checks = []
    if "cache" in indices and "one_pass" in indices:
        checks.append(pred[:, indices["cache"]] >= pred[:, indices["one_pass"]])
    if "one_pass" in indices and "multi_pass" in indices:
        checks.append(pred[:, indices["one_pass"]] >= pred[:, indices["multi_pass"]])
    return float(np.mean(np.logical_and.reduce(checks))) if checks else 1.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--prefix", default="ap_", help="Input filename prefix before cache/one_pass/multi_pass")
    parser.add_argument("--bounds", nargs="+", choices=BOUNDS, default=list(BOUNDS),
                        help="Bounds with native gold labels; Sort-only APs commonly use cache one_pass")
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    parser.add_argument("--gpu-id", type=int, default=0)
    parser.add_argument("--allow-semantic-mismatch", action="store_true", help="Evaluate mismatched label protocols for diagnostics only")
    args = parser.parse_args()

    if args.device == "cuda":
        os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu_id)
    import xgboost as xgb

    config_path = args.models_dir / "model_config.json"
    model_config = json.loads(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
    feature_names = tuple(model_config.get("feature_names", FEATURE_NAMES_V1))
    expected_protocol = str(model_config.get("label_protocol", "legacy-v1"))
    if feature_names not in (FEATURE_NAMES_V1, FEATURE_NAMES_V2, FEATURE_NAMES_MIXED,
                             FEATURE_NAMES_MIXED_CLEAN, FEATURE_NAMES_MIXED_CLEAN_SORT, FEATURE_NAMES_V3,
                             FEATURE_NAMES_MIXED_V3, FEATURE_NAMES_MIXED_CLEAN_P0):
        raise SystemExit(f"unsupported model feature ABI in {config_path}: {len(feature_names)} features")

    bounds = tuple(args.bounds)
    rows_by_bound = {bound: load(args.data_dir / f"{args.prefix}{bound}.jsonl") for bound in bounds}
    by_id: dict[str, dict[str, Any]] = {}
    for bound, rows in rows_by_bound.items():
        for row in rows:
            query_id = str(row.get("query_id", row.get("id")))
            by_id.setdefault(query_id, {"query_id": query_id})
            by_id[query_id][bound] = row
    rows = [row for row in by_id.values() if all(bound in row for bound in bounds)]
    if not rows:
        raise SystemExit(f"no complete query rows in {args.data_dir}")

    # ``None`` is a valid diagnostic value for an unversioned legacy row, but
    # Python 3 does not order ``None`` and ``str`` values directly.
    protocols = sorted(
        {row_label_protocol(row[bounds[0]]) for row in rows},
        key=lambda value: (value is not None, str(value) if value is not None else ""),
    )
    semantic_mismatches = [protocol for protocol in protocols if protocol is not None and protocol != expected_protocol]
    if expected_protocol == LABEL_PROTOCOL and any(protocol is None for protocol in protocols):
        semantic_mismatches.append("missing-protocol")
    if semantic_mismatches and not args.allow_semantic_mismatch:
        raise SystemExit(
            f"label protocol mismatch: model={expected_protocol}, data={protocols}; "
            "use --allow-semantic-mismatch only for diagnostic metrics"
        )
    if semantic_mismatches:
        print(f"warning: evaluating semantic mismatches: {semantic_mismatches}", file=sys.stderr)
    prepared_rows = []
    for row in rows:
        prepared = dict(row[bounds[0]])
        if feature_names == FEATURE_NAMES_MIXED:
            prepared["_features"], prepared["_feature_missing"] = feature_row_mixed(prepared)
        elif feature_names == FEATURE_NAMES_MIXED_CLEAN:
            prepared["_features"], prepared["_feature_missing"] = feature_row_mixed_clean(prepared)
        elif feature_names == FEATURE_NAMES_MIXED_CLEAN_SORT:
            prepared["_features"], prepared["_feature_missing"] = feature_row_mixed_clean_sort(prepared)
        elif feature_names == FEATURE_NAMES_MIXED_V3:
            prepared["_features"], prepared["_feature_missing"] = feature_row_mixed_v3(prepared)
        elif feature_names == FEATURE_NAMES_MIXED_CLEAN_P0:
            prepared["_features"], prepared["_feature_missing"] = feature_row_mixed_clean_p0(prepared)
        elif feature_names == FEATURE_NAMES_V3:
            prepared["_features"], prepared["_feature_missing"] = feature_row_v3(prepared)
        elif feature_names == FEATURE_NAMES_V2:
            prepared["_features"], prepared["_feature_missing"] = feature_row_v2(prepared)
        else:
            prepared["_features"] = feature_row_v1(prepared)
            prepared["_feature_missing"] = []
        prepared_rows.append(prepared)
    imputation = {str(k): float(v) for k, v in (model_config.get("feature_imputation") or {}).items()}
    matrix = matrix_from_rows(prepared_rows, feature_names, imputation)
    dmatrix = xgb.DMatrix(matrix)
    models = {}
    for target in ("cache_mb", "one_pass_mb"):
        model = xgb.Booster()
        model.load_model(args.models_dir / f"{target}.json")
        if args.device == "cuda":
            model.set_param({"device": "cuda:0"})
        models[target] = model
    cache = np.maximum(np.expm1(models["cache_mb"].predict(dmatrix)), MIN_MB)
    one = np.maximum(np.expm1(models["one_pass_mb"].predict(dmatrix)), MIN_MB)
    one = np.minimum(one, cache)
    all_pred = np.column_stack((cache, one, np.maximum(MIN_MB, one - MULTI_PASS_DELTA_MB)))
    selected = np.asarray([BOUNDS.index(bound) for bound in bounds], dtype=int)
    pred = all_pred[:, selected]
    gold = np.asarray([[float(row[bound]["gold_mb"]) for bound in bounds] for row in rows], dtype=float)
    ranges = model_config.get("feature_ranges") or {}
    row_warnings = ood_warnings(prepared_rows, ranges, feature_names) if ranges else [[] for _ in rows]

    all_gold, flattened_pred = gold.reshape(-1), pred.reshape(-1)
    report: dict[str, Any] = {
        "models_dir": str(args.models_dir), "data_dir": str(args.data_dir),
        "query_count": len(rows), "point_count": int(all_gold.size),
        "evaluated_bounds": list(bounds),
        "model_label_protocol": expected_protocol,
        "data_label_protocols": protocols,
        "semantic_mismatch_count": len(semantic_mismatches),
        **warning_query_counts(row_warnings),
        "overall": scalar_metrics(all_gold, flattened_pred),
        "by_bound": {bound: scalar_metrics(gold[:, i], pred[:, i]) for i, bound in enumerate(bounds)},
        "by_target": {},
        "order_accuracy": bound_order_accuracy(pred, bounds),
    }
    targets: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        target = str(row["cache"].get("target", row["cache"].get("dbname", "unknown")))
        targets.setdefault(target, []).append(i)
    for target, indices in sorted(targets.items()):
        ix = np.asarray(indices, dtype=int)
        report["by_target"][target] = scalar_metrics(gold[ix].reshape(-1), pred[ix].reshape(-1))
        report["by_target"][target]["query_count"] = int(len(ix))
        report["by_target"][target]["order_accuracy"] = bound_order_accuracy(pred[ix], bounds)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "metrics.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    with (args.output_dir / "predictions.jsonl").open("w", encoding="utf-8") as handle:
        for index, (row, gold_row, pred_row) in enumerate(zip(rows, gold, pred)):
            handle.write(json.dumps({"query_id": row["query_id"], "target": row[bounds[0]].get("target"),
                                     "bounds": list(bounds), "gold": gold_row.tolist(), "pred": pred_row.tolist(),
                                     "warnings": row_warnings[index]}, ensure_ascii=False) + "\n")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
