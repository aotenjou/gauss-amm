#!/usr/bin/env python3
"""Evaluate an XGBoost artifact on labeled memory-bound JSONL slices.

Unlike ``evaluate_xgb_memory.py``, this entry point consumes ordinary
collector/SFT rows with a ``bounds`` object.  It is intended for reproducible
GSBench source, coverage, and shape-slice comparisons.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import numpy as np

from train_xgb_memory import (
    FEATURE_NAMES_MIXED,
    FEATURE_NAMES_MIXED_CLEAN,
    FEATURE_NAMES_MIXED_CLEAN_P0,
    FEATURE_NAMES_MIXED_CLEAN_SORT,
    FEATURE_NAMES_MIXED_V3,
    FEATURE_NAMES_V1,
    FEATURE_NAMES_V2,
    FEATURE_NAMES_V3,
    MIN_MB,
    feature_row_mixed,
    feature_row_mixed_clean,
    feature_row_mixed_clean_p0,
    feature_row_mixed_clean_sort,
    feature_row_mixed_v3,
    feature_row_v1,
    feature_row_v2,
    feature_row_v3,
    matrix_from_rows,
    ood_warnings,
    warning_query_counts,
)


BOUNDS = ("cache_mb", "one_pass_mb")


def load(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.open(encoding="utf-8") if line.strip()]


def metric(gold: np.ndarray, pred: np.ndarray) -> dict[str, float | int]:
    qerror = np.maximum(pred / gold, gold / np.maximum(pred, MIN_MB))
    return {
        "count": int(gold.size),
        "qerror_mean": float(np.mean(qerror)),
        "qerror_median": float(np.median(qerror)),
        "qerror_p90": float(np.quantile(qerror, 0.9, method="linear")),
        "qerror_le_2": float(np.mean(qerror <= 2.0)),
        "low_estimate_rate": float(np.mean(pred < gold)),
        "mae_mb": float(np.mean(np.abs(pred - gold))),
        "rmse_mb": float(np.sqrt(np.mean((pred - gold) ** 2))),
    }


def prepare(rows: list[dict[str, Any]], feature_names: tuple[str, ...]) -> list[dict[str, Any]]:
    output = []
    for source in rows:
        row = dict(source)
        if feature_names == FEATURE_NAMES_MIXED:
            row["_features"], row["_feature_missing"] = feature_row_mixed(row)
        elif feature_names == FEATURE_NAMES_MIXED_CLEAN:
            row["_features"], row["_feature_missing"] = feature_row_mixed_clean(row)
        elif feature_names == FEATURE_NAMES_MIXED_CLEAN_SORT:
            row["_features"], row["_feature_missing"] = feature_row_mixed_clean_sort(row)
        elif feature_names == FEATURE_NAMES_MIXED_V3:
            row["_features"], row["_feature_missing"] = feature_row_mixed_v3(row)
        elif feature_names == FEATURE_NAMES_MIXED_CLEAN_P0:
            row["_features"], row["_feature_missing"] = feature_row_mixed_clean_p0(row)
        elif feature_names == FEATURE_NAMES_V3:
            row["_features"], row["_feature_missing"] = feature_row_v3(row)
        elif feature_names == FEATURE_NAMES_V2:
            row["_features"], row["_feature_missing"] = feature_row_v2(row)
        elif feature_names == FEATURE_NAMES_V1:
            row["_features"] = feature_row_v1(row)
            row["_feature_missing"] = []
        else:
            raise ValueError(f"unsupported model ABI with {len(feature_names)} features")
        output.append(row)
    return output


def group_metrics(rows: list[dict[str, Any]], gold: np.ndarray, pred: np.ndarray, key: str) -> dict[str, Any]:
    groups: dict[str, list[int]] = {}
    for index, row in enumerate(rows):
        groups.setdefault(str(row.get(key, "unknown")), []).append(index)
    result = {}
    for name, indices in sorted(groups.items()):
        ix = np.asarray(indices, dtype=int)
        result[name] = {
            "overall": metric(gold[ix].reshape(-1), pred[ix].reshape(-1)),
            "cache_mb": metric(gold[ix, 0], pred[ix, 0]),
            "one_pass_mb": metric(gold[ix, 1], pred[ix, 1]),
        }
    return result


def size_group(row: dict[str, Any]) -> str:
    value = row.get("size_gb")
    if isinstance(value, (int, float)) and float(value) > 0:
        return str(int(value) if float(value).is_integer() else value)
    match = re.search(r"_s(\d+(?:\.\d+)?)gb(?:$|_)", str(row.get("dbname", "")).lower())
    return match.group(1) if match else "unknown"


def parse_named_path(value: str) -> tuple[str, Path]:
    try:
        name, raw_path = value.split("=", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--data must be NAME=PATH") from exc
    if not name:
        raise argparse.ArgumentTypeError("--data name must not be empty")
    return name, Path(raw_path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--data", type=parse_named_path, action="append", required=True,
                        metavar="NAME=PATH", help="Labeled collector/SFT JSONL; repeatable")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    import xgboost as xgb

    config = json.loads((args.models_dir / "model_config.json").read_text(encoding="utf-8"))
    feature_names = tuple(config["feature_names"])
    imputation = {str(key): float(value) for key, value in (config.get("feature_imputation") or {}).items()}
    models = {}
    for target in BOUNDS:
        model = xgb.Booster()
        model.load_model(args.models_dir / f"{target}.json")
        models[target] = model

    report: dict[str, Any] = {"models_dir": str(args.models_dir), "slices": {}}
    for name, path in args.data:
        rows = load(path)
        if not rows:
            raise SystemExit(f"empty input slice: {path}")
        invalid = [row.get("id", row.get("query_id", "unknown")) for row in rows
                   if not all(isinstance((row.get("bounds") or {}).get(bound), (int, float))
                              and float(row["bounds"][bound]) > 0 for bound in BOUNDS)]
        if invalid:
            raise SystemExit(f"{path}: {len(invalid)} rows have no valid cache/one-pass label")
        prepared = prepare(rows, feature_names)
        matrix = matrix_from_rows(prepared, feature_names, imputation)
        dmatrix = xgb.DMatrix(matrix)
        pred = np.column_stack([
            np.maximum(np.expm1(models[bound].predict(dmatrix)), MIN_MB) for bound in BOUNDS
        ])
        pred[:, 1] = np.minimum(pred[:, 1], pred[:, 0])
        gold = np.asarray([[float(row["bounds"][bound]) for bound in BOUNDS] for row in rows], dtype=float)
        warnings = ood_warnings(prepared, config.get("feature_ranges") or {}, feature_names)
        report["slices"][name] = {
            "path": str(path),
            **warning_query_counts(warnings),
            "overall": metric(gold.reshape(-1), pred.reshape(-1)),
            "cache_mb": metric(gold[:, 0], pred[:, 0]),
            "one_pass_mb": metric(gold[:, 1], pred[:, 1]),
            "by_size_gb": group_metrics(
                [{**row, "_size_group": size_group(row)} for row in rows], gold, pred, "_size_group"
            ),
        }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
