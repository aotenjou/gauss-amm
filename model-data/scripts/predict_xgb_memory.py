#!/usr/bin/env python3
"""Predict three work_mem boundaries with a trained XGBoost artifact."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np

from train_xgb_memory import (
    FEATURE_NAMES_V1,
    FEATURE_NAMES_V2,
    FEATURE_NAMES_MIXED,
    FEATURE_NAMES_MIXED_CLEAN,
    FEATURE_NAMES_MIXED_CLEAN_SORT,
    FEATURE_NAMES_V3,
    FEATURE_NAMES_MIXED_V3,
    FEATURE_NAMES_MIXED_CLEAN_P0,
    LABEL_PROTOCOL,
    MIN_MB,
    MULTI_PASS_DELTA_MB,
    feature_row_v1,
    feature_row_v2,
    feature_row_mixed,
    feature_row_mixed_clean,
    feature_row_mixed_clean_sort,
    feature_row_v3,
    feature_row_mixed_v3,
    feature_row_mixed_clean_p0,
    matrix_from_rows,
    ood_warnings,
    row_label_protocol,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    parser.add_argument("--gpu-id", type=int, default=0)
    parser.add_argument("--strict-ood", action="store_true", help="Fail instead of returning predictions for OOD or protocol warnings")
    args = parser.parse_args()

    if args.device == "cuda":
        os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu_id)
    import xgboost as xgb

    config_path = args.models_dir / "model_config.json"
    model_config = json.loads(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
    calibration = model_config.get("calibration") or {}
    calibration_file = model_config.get("calibration_file")
    if calibration_file:
        candidate = args.models_dir / str(calibration_file)
        if candidate.exists():
            try:
                file_calibration = json.loads(candidate.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise SystemExit(f"invalid calibration file {candidate}: {exc}") from exc
            if isinstance(file_calibration, dict):
                calibration = file_calibration

    def calibration_factor(name: str) -> float:
        value = calibration.get(name, 1.0) if isinstance(calibration, dict) else 1.0
        try:
            value = float(value)
        except (TypeError, ValueError):
            raise SystemExit(f"invalid calibration factor for {name}: {value!r}")
        if not np.isfinite(value) or value <= 0:
            raise SystemExit(f"invalid calibration factor for {name}: {value!r}")
        return value

    cache_factor = calibration_factor("cache_mb")
    one_factor = calibration_factor("one_pass_mb")
    feature_names = tuple(model_config.get("feature_names", FEATURE_NAMES_V1))
    protocol = str(model_config.get("label_protocol", "legacy-v1"))
    if feature_names not in (FEATURE_NAMES_V1, FEATURE_NAMES_V2, FEATURE_NAMES_MIXED,
                             FEATURE_NAMES_MIXED_CLEAN, FEATURE_NAMES_MIXED_CLEAN_SORT,
                             FEATURE_NAMES_V3, FEATURE_NAMES_MIXED_V3, FEATURE_NAMES_MIXED_CLEAN_P0):
        raise SystemExit(f"unsupported model feature ABI in {config_path}: {len(feature_names)} features")

    models = {
        "cache_mb": xgb.Booster(),
        "one_pass_mb": xgb.Booster(),
    }
    for target, model in models.items():
        model.load_model(args.models_dir / f"{target}.json")

    source_rows = [json.loads(line) for line in args.input.open(encoding="utf-8") if line.strip()]
    if not source_rows:
        raise SystemExit(f"no rows in {args.input}")
    prepared_rows = []
    protocol_warnings = []
    for row in source_rows:
        prepared = dict(row)
        if feature_names == FEATURE_NAMES_MIXED:
            prepared["_features"], prepared["_feature_missing"] = feature_row_mixed(prepared)
        elif feature_names == FEATURE_NAMES_MIXED_CLEAN:
            prepared["_features"], prepared["_feature_missing"] = feature_row_mixed_clean(prepared)
        elif feature_names == FEATURE_NAMES_MIXED_CLEAN_SORT:
            prepared["_features"], prepared["_feature_missing"] = feature_row_mixed_clean_sort(prepared)
        elif feature_names == FEATURE_NAMES_MIXED_CLEAN_P0:
            prepared["_features"], prepared["_feature_missing"] = feature_row_mixed_clean_p0(prepared)
        elif feature_names == FEATURE_NAMES_MIXED_V3:
            prepared["_features"], prepared["_feature_missing"] = feature_row_mixed_v3(prepared)
        elif feature_names == FEATURE_NAMES_V3:
            prepared["_features"], prepared["_feature_missing"] = feature_row_v3(prepared)
        elif feature_names == FEATURE_NAMES_V2:
            prepared["_features"], prepared["_feature_missing"] = feature_row_v2(prepared)
        else:
            prepared["_features"] = feature_row_v1(prepared)
            prepared["_feature_missing"] = []
        row_protocol = row_label_protocol(row)
        current_warnings = []
        if protocol == LABEL_PROTOCOL:
            if row_protocol is None:
                current_warnings.append({"code": "MISSING_PROTOCOL", "expected": LABEL_PROTOCOL})
            elif row_protocol != LABEL_PROTOCOL:
                current_warnings.append({"code": "SEMANTIC_MISMATCH", "expected": LABEL_PROTOCOL, "actual": row_protocol})
        protocol_warnings.append(current_warnings)
        prepared_rows.append(prepared)
    imputation = {str(k): float(v) for k, v in (model_config.get("feature_imputation") or {}).items()}
    matrix = matrix_from_rows(prepared_rows, feature_names, imputation)
    dmatrix = xgb.DMatrix(matrix)
    if args.device == "cuda":
        # Booster prediction accepts a CPU DMatrix and transparently transfers
        # it to the selected CUDA device; DMatrix.set_info does not support a
        # device field in XGBoost 2.x.
        for model in models.values():
            model.set_param({"device": "cuda:0"})
    # Calibrate before the final floor/projection so factors below one cannot
    # produce one-pass values below MIN_MB.
    cache = np.maximum(np.expm1(models["cache_mb"].predict(dmatrix)) * cache_factor, MIN_MB)
    one = np.maximum(np.expm1(models["one_pass_mb"].predict(dmatrix)) * one_factor, MIN_MB)
    one = np.minimum(one, cache)
    multi = np.maximum(MIN_MB, one - MULTI_PASS_DELTA_MB)
    multi = np.minimum(multi, one)
    ranges = model_config.get("feature_ranges") or {}
    feature_warnings = ood_warnings(prepared_rows, ranges, feature_names) if ranges else [[] for _ in prepared_rows]
    all_warnings = [protocol_warnings[i] + feature_warnings[i] for i in range(len(prepared_rows))]
    if any(all_warnings):
        print(f"warning: {sum(bool(item) for item in all_warnings)}/{len(all_warnings)} predictions have protocol/OOD warnings", file=sys.stderr)
        if args.strict_ood:
            raise SystemExit("strict OOD mode rejected one or more input rows")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for index, (row, c, o, m, row_warnings) in enumerate(zip(source_rows, cache, one, multi, all_warnings)):
            result = {
                "model_abi_version": model_config.get("model_abi_version", "xgb-memory-v1"),
                "label_protocol": protocol,
                "id": row.get("id", index),
                "cache_mb": float(c),
                "one_pass_mb": float(o),
                "multi_pass_mb": float(m),
                "ood": bool(row_warnings),
                "warnings": row_warnings,
            }
            if "bounds" in row:
                result["gold"] = row["bounds"]
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")
    print(json.dumps({"rows": len(source_rows), "output": str(args.output), "models_dir": str(args.models_dir)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
