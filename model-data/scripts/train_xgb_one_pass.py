#!/usr/bin/env python3
"""Refit only the one-pass XGBoost target while freezing a deployed cache tree.

The final artifact copies ``cache_mb.json`` byte-for-byte from ``--base-model``.
Only ``one_pass_mb.json`` is fitted.  The two five-stage AP records are an
external deployment gate and are rejected as training data.
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Callable

import numpy as np

try:
    from scripts.prepare_combined_xgb_data import normalize_row
    from scripts.train_xgb_memory import (
        FEATURE_NAMES_MIXED,
        FEATURE_NAMES_MIXED_CLEAN,
        FEATURE_NAMES_MIXED_CLEAN_P0,
        FEATURE_NAMES_MIXED_CLEAN_SORT,
        FEATURE_NAMES_MIXED_V3,
        FEATURE_NAMES_V1,
        FEATURE_NAMES_V2,
        FEATURE_NAMES_V3,
        LABEL_PROTOCOL,
        MIN_MB,
        ap_shape_weights,
        choose_gpu,
        feature_row_mixed,
        feature_row_mixed_clean,
        feature_row_mixed_clean_p0,
        feature_row_mixed_clean_sort,
        feature_row_mixed_v3,
        feature_row_v1,
        feature_row_v2,
        feature_row_v3,
        matrix_from_rows,
        optional_float,
        sample_weights,
    )
except ModuleNotFoundError:
    from prepare_combined_xgb_data import normalize_row
    from train_xgb_memory import (
        FEATURE_NAMES_MIXED,
        FEATURE_NAMES_MIXED_CLEAN,
        FEATURE_NAMES_MIXED_CLEAN_P0,
        FEATURE_NAMES_MIXED_CLEAN_SORT,
        FEATURE_NAMES_MIXED_V3,
        FEATURE_NAMES_V1,
        FEATURE_NAMES_V2,
        FEATURE_NAMES_V3,
        LABEL_PROTOCOL,
        MIN_MB,
        ap_shape_weights,
        choose_gpu,
        feature_row_mixed,
        feature_row_mixed_clean,
        feature_row_mixed_clean_p0,
        feature_row_mixed_clean_sort,
        feature_row_mixed_v3,
        feature_row_v1,
        feature_row_v2,
        feature_row_v3,
        matrix_from_rows,
        optional_float,
        sample_weights,
    )


EPSILON = 1e-12
TARGET = "one_pass_mb"
EXPECTED_FEATURES = FEATURE_NAMES_MIXED_CLEAN
FROZEN_QERROR_MEAN_TARGET = 1.25
FROZEN_QERROR_PER_QUERY_TARGET = 1.50


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)
    return [json.loads(line) for line in path.open(encoding="utf-8") if line.strip()]


def ensure_bound_row(row: dict[str, Any], source: Path) -> dict[str, Any]:
    result = dict(row)
    bounds = dict(result.get("bounds") or {})
    if not bounds:
        bounds = {
            "cache_mb": result.get("mem_cache_mb"),
            "one_pass_mb": result.get("mem_one_pass_mb"),
            "multi_pass_mb": result.get("mem_multi_pass_mb"),
        }
    cache = optional_float(bounds.get("cache_mb"))
    one = optional_float(bounds.get("one_pass_mb"))
    if cache is None or one is None or cache <= 0 or one <= 0 or cache < one:
        raise ValueError(f"{source}: invalid cache/one-pass bounds for {result.get('query_id', result.get('id'))}")
    result["bounds"] = dict(bounds, cache_mb=cache, one_pass_mb=one)
    result.setdefault("id", result.get("query_id"))
    result.setdefault("query_id", result.get("id"))
    result.setdefault("label_protocol", LABEL_PROTOCOL)
    if str(result["label_protocol"]) != LABEL_PROTOCOL:
        raise ValueError(f"{source}: incompatible label protocol {result['label_protocol']!r}")
    return result


def normalize_ap_shape_rows(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        for raw in read_jsonl(path):
            normalized, error = normalize_row(raw, path, "gsbench")
            if normalized is None:
                raise ValueError(f"{path}: cannot normalize AP-shape row: {error}")
            normalized["target"] = raw.get("target", "gsbench_ap_shape_coverage")
            normalized["coverage_source"] = str(path)
            rows.append(ensure_bound_row(normalized, path))
    return rows


def ap_shape_partition(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split each collected four-row size block by its stable ordinal.

    Ordinals 000/002 are training records and 001/003 are validation records.
    This keeps distinct intervals in each split and avoids random leakage.
    """
    train: list[dict[str, Any]] = []
    validation: list[dict[str, Any]] = []
    for row in rows:
        query_id = str(row.get("query_id", ""))
        try:
            ordinal = int(query_id.rsplit("_", 1)[1])
        except (IndexError, ValueError) as exc:
            raise ValueError(f"AP-shape row has no numeric ordinal: {query_id}") from exc
        if ordinal not in {0, 1, 2, 3}:
            raise ValueError(f"AP-shape row has unsupported ordinal: {query_id}")
        (train if ordinal in {0, 2} else validation).append(row)
    if not train or not validation:
        raise ValueError("AP-shape partition needs both train and validation records")
    return train, validation


def supplement_partition(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    train = [row for row in rows if row.get("xgb_split") == "train"]
    validation = [row for row in rows if row.get("xgb_split") == "validation"]
    if len(train) + len(validation) != len(rows) or not train or not validation:
        raise ValueError("supplement rows require nonempty xgb_split=train and xgb_split=validation partitions")
    return train, validation


def feature_builder(feature_names: tuple[str, ...]) -> Callable[[dict[str, Any]], tuple[dict[str, float], list[str]]]:
    builders = {
        FEATURE_NAMES_V2: feature_row_v2,
        FEATURE_NAMES_V3: feature_row_v3,
        FEATURE_NAMES_MIXED: feature_row_mixed,
        FEATURE_NAMES_MIXED_V3: feature_row_mixed_v3,
        FEATURE_NAMES_MIXED_CLEAN: feature_row_mixed_clean,
        FEATURE_NAMES_MIXED_CLEAN_P0: feature_row_mixed_clean_p0,
        FEATURE_NAMES_MIXED_CLEAN_SORT: feature_row_mixed_clean_sort,
    }
    if feature_names == FEATURE_NAMES_V1:
        return lambda row: (feature_row_v1(row), [])
    try:
        return builders[feature_names]
    except KeyError as exc:
        raise ValueError(f"unsupported feature ABI ({len(feature_names)} features)") from exc


def prepare_rows(rows: list[dict[str, Any]], feature_names: tuple[str, ...]) -> list[dict[str, Any]]:
    builder = feature_builder(feature_names)
    prepared = []
    for source in rows:
        row = dict(source)
        row["_features"], row["_feature_missing"] = builder(row)
        row["_workload"] = str(row.get("workload", row.get("dataset", "unknown"))).lower()
        prepared.append(row)
    return prepared


def label_vector(rows: list[dict[str, Any]]) -> np.ndarray:
    return np.asarray([float(row["bounds"][TARGET]) for row in rows], dtype=np.float32)


def one_pass_metrics(gold: np.ndarray, prediction: np.ndarray) -> dict[str, float | int]:
    qerror = np.maximum(prediction / gold, gold / np.maximum(prediction, MIN_MB))
    return {
        "count": int(gold.size),
        "qerror_mean": float(np.mean(qerror)),
        "qerror_median": float(np.median(qerror)),
        "qerror_p90": float(np.quantile(qerror, 0.9, method="linear")),
        "qerror_le_2": float(np.mean(qerror <= 2.0)),
        "low_estimate_rate": float(np.mean(prediction < gold)),
        "mae_mb": float(np.mean(np.abs(prediction - gold))),
        "rmse_mb": float(np.sqrt(np.mean((prediction - gold) ** 2))),
        "log_rmse": float(np.sqrt(np.mean((np.log1p(prediction) - np.log1p(gold)) ** 2))),
    }


def qerror(gold: float, prediction: float) -> float:
    return max(prediction / gold, gold / max(prediction, MIN_MB))


def candidate_is_feasible(candidate: dict[str, Any], baseline: dict[str, Any]) -> bool:
    return (
        float(candidate["qerror_p90"]) <= float(baseline["qerror_p90"]) + EPSILON
        and float(candidate["low_estimate_rate"]) <= float(baseline["low_estimate_rate"]) + EPSILON
    )


def frozen_gate(rows: list[dict[str, Any]], base_prediction: np.ndarray,
                candidate_prediction: np.ndarray) -> dict[str, Any]:
    gold = label_vector(rows)
    base_metrics = one_pass_metrics(gold, base_prediction)
    candidate_metrics = one_pass_metrics(gold, candidate_prediction)
    queries = []
    for row, expected, base, candidate in zip(rows, gold, base_prediction, candidate_prediction):
        base_q = qerror(float(expected), float(base))
        candidate_q = qerror(float(expected), float(candidate))
        queries.append({
            "query_id": row.get("query_id", row.get("id")),
            "gold_one_pass_mb": float(expected),
            "base_one_pass_mb": float(base),
            "candidate_one_pass_mb": float(candidate),
            "base_qerror": base_q,
            "candidate_qerror": candidate_q,
            "candidate_low_estimate": bool(candidate < expected),
            "qerror_not_worse": bool(candidate_q <= base_q + EPSILON),
        })
    per_query_ok = all(
        item["qerror_not_worse"]
        and not item["candidate_low_estimate"]
        and item["candidate_qerror"] <= FROZEN_QERROR_PER_QUERY_TARGET + EPSILON
        for item in queries
    )
    aggregate_ok = (
        candidate_metrics["qerror_mean"] <= base_metrics["qerror_mean"] + EPSILON
        and candidate_metrics["qerror_p90"] <= base_metrics["qerror_p90"] + EPSILON
        and candidate_metrics["qerror_mean"] <= FROZEN_QERROR_MEAN_TARGET + EPSILON
    )
    return {
        "passed": bool(per_query_ok and aggregate_ok),
        "strict_policy": (
            "each frozen AP must not be underestimated, have higher q-error than v3, "
            "or exceed q-error 1.50; aggregate mean/p90 must not regress and mean must be <= 1.25"
        ),
        "targets": {
            "qerror_mean_max": FROZEN_QERROR_MEAN_TARGET,
            "per_query_qerror_max": FROZEN_QERROR_PER_QUERY_TARGET,
        },
        "base": base_metrics,
        "candidate": candidate_metrics,
        "queries": queries,
    }


def set_device(args: argparse.Namespace) -> None:
    if args.device == "auto":
        args.device = "cuda" if choose_gpu() is not None else "cpu"
    if args.device == "cuda":
        args.gpu_id = choose_gpu() if args.gpu_id is None else args.gpu_id
        if args.gpu_id is None:
            print("CUDA requested but no GPU was available; using CPU", file=sys.stderr)
            args.device = "cpu"
        else:
            os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu_id)
            os.environ.setdefault("XGBOOST_BUILD_CACHE", "0")


def fit_one_pass(xgb: Any, x: np.ndarray, y: np.ndarray, weights: np.ndarray,
                 params: dict[str, Any], device: str) -> Any:
    kwargs = dict(params)
    kwargs["tree_method"] = "hist"
    kwargs["device"] = "cuda" if device == "cuda" else "cpu"
    try:
        model = xgb.XGBRegressor(**kwargs)
    except TypeError:
        kwargs.pop("device", None)
        if device == "cuda":
            kwargs.update(tree_method="gpu_hist", gpu_id=0)
        model = xgb.XGBRegressor(**kwargs)
    model.fit(x, np.log1p(y), sample_weight=weights, verbose=False)
    return model


def predict_booster(xgb: Any, booster: Any, matrix: np.ndarray) -> np.ndarray:
    result = np.asarray(booster.predict(xgb.DMatrix(matrix)), dtype=float)
    return np.maximum(np.expm1(result), MIN_MB)


def predict_deployed_one_pass(xgb: Any, cache_booster: Any, one_booster: Any,
                              matrix: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Apply the same cache ceiling used by Python and native inference."""
    cache = predict_booster(xgb, cache_booster, matrix)
    one = np.minimum(predict_booster(xgb, one_booster, matrix), cache)
    return cache, one


def candidate_params(base: dict[str, Any], depth: int, min_child_weight: float) -> dict[str, Any]:
    params = dict(base)
    params["max_depth"] = depth
    params["min_child_weight"] = min_child_weight
    return params


def verify_frozen_is_external(frozen: list[dict[str, Any]], all_other_rows: list[dict[str, Any]]) -> None:
    frozen_hashes = {str(row.get("sql_sha256") or hashlib.sha256(str(row.get("sql", "")).encode()).hexdigest()) for row in frozen}
    source_hashes = {str(row.get("sql_sha256") or hashlib.sha256(str(row.get("sql", "")).encode()).hexdigest()) for row in all_other_rows}
    overlap = sorted(frozen_hashes & source_hashes)
    if overlap:
        raise ValueError(f"frozen five-stage AP leaked into train/validation/test data: {overlap}")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-model", type=Path, required=True)
    parser.add_argument("--base-data-dir", type=Path, required=True,
                        help="v3 data directory containing train.jsonl, validation.jsonl, and test.jsonl")
    parser.add_argument("--ap-shape", type=Path, action="append", required=True,
                        help="Raw native AP-shape bounds JSONL; repeatable")
    parser.add_argument("--supplement", type=Path, action="append", default=[],
                        help="Optional adjacent non-frozen coverage with xgb_split=train|validation")
    parser.add_argument("--frozen-ap", type=Path, action="append", required=True,
                        help="Immutable five-stage native AP JSONL; repeatable")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
    parser.add_argument("--gpu-id", type=int, default=None)
    parser.add_argument("--n-jobs", type=int, default=2)
    parser.add_argument("--max-depth-grid", type=int, nargs="+", default=(3, 4, 5))
    parser.add_argument("--min-child-weight-grid", type=float, nargs="+", default=(1.0, 3.0, 5.0))
    parser.add_argument("--ap-shape-weight-grid", type=float, nargs="+", default=(1.0, 4.0, 8.0, 16.0, 32.0))
    parser.add_argument("--require-deployment-gate", action="store_true",
                        help="Return nonzero after writing reports when the selected artifact is not deployable")
    args = parser.parse_args()
    if not all(value > 0 for value in [
        *args.max_depth_grid, *args.min_child_weight_grid, *args.ap_shape_weight_grid,
    ]):
        parser.error("all search-grid values must be positive")
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        raise SystemExit(f"refusing to overwrite nonempty output directory: {args.output_dir}")

    cache_source = args.base_model / "cache_mb.json"
    one_source = args.base_model / "one_pass_mb.json"
    config_source = args.base_model / "model_config.json"
    for path in (cache_source, one_source, config_source):
        if not path.exists():
            raise FileNotFoundError(path)
    base_config = json.loads(config_source.read_text(encoding="utf-8"))
    feature_names = tuple(base_config.get("feature_names") or ())
    if feature_names != EXPECTED_FEATURES:
        raise SystemExit("base model must use the deployed 24-feature mixed_clean ABI")
    imputation = {str(key): float(value) for key, value in (base_config.get("feature_imputation") or {}).items()}
    if set(feature_names) != set(imputation):
        raise SystemExit("base model has incomplete feature imputation; cannot freeze cache preprocessing")
    base_params = dict(base_config.get("params") or {})
    required_params = {"objective", "n_estimators", "learning_rate", "subsample", "colsample_bytree", "reg_alpha", "reg_lambda", "max_bin", "random_state"}
    if not required_params.issubset(base_params):
        raise SystemExit("base model has incomplete XGBoost parameters")
    base_params["n_jobs"] = args.n_jobs

    set_device(args)
    try:
        import xgboost as xgb
    except ImportError as exc:
        raise SystemExit("xgboost is required") from exc

    base_splits = {
        name: [ensure_bound_row(row, args.base_data_dir / f"{name}.jsonl")
               for row in read_jsonl(args.base_data_dir / f"{name}.jsonl")]
        for name in ("train", "validation", "test")
    }
    ap_shape_all = normalize_ap_shape_rows(args.ap_shape)
    ap_shape_train, ap_shape_validation = ap_shape_partition(ap_shape_all)
    supplement_all = normalize_ap_shape_rows(args.supplement)
    supplement_train, supplement_validation = supplement_partition(supplement_all) if supplement_all else ([], [])
    frozen_rows = [ensure_bound_row(row, path) for path in args.frozen_ap for row in read_jsonl(path)]
    if len(frozen_rows) != 2:
        raise ValueError(f"expected exactly two frozen five-stage APs, got {len(frozen_rows)}")
    if any(row.get("target") != "five_stage_ap_holdout" or not row.get("frozen") for row in frozen_rows):
        raise ValueError("--frozen-ap must contain only records emitted by collect_five_stage_ap_bounds.py")
    verify_frozen_is_external(
        frozen_rows,
        [*base_splits["train"], *base_splits["validation"], *base_splits["test"], *ap_shape_all, *supplement_all],
    )

    train_rows = prepare_rows([*base_splits["train"], *ap_shape_train, *supplement_train], feature_names)
    validation_rows = prepare_rows([*base_splits["validation"], *ap_shape_validation, *supplement_validation], feature_names)
    final_rows = prepare_rows([
        *base_splits["train"], *base_splits["validation"], *ap_shape_all, *supplement_all,
    ], feature_names)
    test_rows = prepare_rows(base_splits["test"], feature_names)
    frozen_prepared = prepare_rows(frozen_rows, feature_names)
    train_x = matrix_from_rows(train_rows, feature_names, imputation)
    validation_x = matrix_from_rows(validation_rows, feature_names, imputation)
    final_x = matrix_from_rows(final_rows, feature_names, imputation)
    test_x = matrix_from_rows(test_rows, feature_names, imputation)
    frozen_x = matrix_from_rows(frozen_prepared, feature_names, imputation)
    train_y = label_vector(train_rows)
    validation_y = label_vector(validation_rows)
    final_y = label_vector(final_rows)
    test_y = label_vector(test_rows)

    base_one = xgb.Booster()
    base_one.load_model(one_source)
    base_cache = xgb.Booster()
    base_cache.load_model(cache_source)
    _, base_validation = predict_deployed_one_pass(xgb, base_cache, base_one, validation_x)
    validation_baseline = one_pass_metrics(validation_y, base_validation)
    base_frozen_cache, base_frozen_one = predict_deployed_one_pass(xgb, base_cache, base_one, frozen_x)

    candidates = []
    for depth, child_weight, ap_weight in itertools.product(
        args.max_depth_grid, args.min_child_weight_grid, args.ap_shape_weight_grid
    ):
        params = candidate_params(base_params, int(depth), float(child_weight))
        weights = sample_weights(train_rows, balanced=True) * ap_shape_weights(train_rows, float(ap_weight))
        model = fit_one_pass(xgb, train_x, train_y, weights, params, args.device)
        _, prediction = predict_deployed_one_pass(xgb, base_cache, model.get_booster(), validation_x)
        metrics = one_pass_metrics(validation_y, prediction)
        candidates.append({
            "id": f"d{depth}-cw{child_weight:g}-apw{ap_weight:g}",
            "max_depth": int(depth),
            "min_child_weight": float(child_weight),
            "ap_shape_weight": float(ap_weight),
            "params": params,
            "validation": metrics,
            "validation_constraints_passed": candidate_is_feasible(metrics, validation_baseline),
        })
    feasible = [candidate for candidate in candidates if candidate["validation_constraints_passed"]]
    ranked = feasible or candidates
    selected = min(
        ranked,
        key=lambda candidate: (
            float(candidate["validation"]["log_rmse"]),
            float(candidate["validation"]["qerror_mean"]),
            float(candidate["validation"]["qerror_p90"]),
            float(candidate["validation"]["low_estimate_rate"]),
            candidate["id"],
        ),
    )
    final_weights = sample_weights(final_rows, balanced=True) * ap_shape_weights(final_rows, float(selected["ap_shape_weight"]))
    final_model = fit_one_pass(xgb, final_x, final_y, final_weights, selected["params"], args.device)
    _, final_test_prediction = predict_deployed_one_pass(xgb, base_cache, final_model.get_booster(), test_x)
    final_frozen_cache, final_frozen_one = predict_deployed_one_pass(
        xgb, base_cache, final_model.get_booster(), frozen_x
    )
    if not np.array_equal(base_frozen_cache, final_frozen_cache):
        raise RuntimeError("cache predictor changed while constructing a one-pass-only artifact")
    gate = frozen_gate(frozen_rows, base_frozen_one, final_frozen_one)
    gate["validation_constraints_passed"] = bool(selected["validation_constraints_passed"])
    gate["passed"] = bool(gate["passed"] and selected["validation_constraints_passed"])

    args.output_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(cache_source, args.output_dir / "cache_mb.json")
    if sha256_file(cache_source) != sha256_file(args.output_dir / "cache_mb.json"):
        raise RuntimeError("copied cache model hash differs from v3")
    final_model.get_booster().save_model(args.output_dir / "one_pass_mb.json")
    config = json.loads(json.dumps(base_config))
    config["device"] = args.device
    config["gpu_id"] = args.gpu_id
    config["params"] = selected["params"]
    config["one_pass_refit"] = {
        "base_model": str(args.base_model),
        "base_cache_sha256": sha256_file(cache_source),
        "base_one_pass_sha256": sha256_file(one_source),
        "output_cache_sha256": sha256_file(args.output_dir / "cache_mb.json"),
        "cache_frozen": True,
        "selection_validation_constraints_passed": bool(selected["validation_constraints_passed"]),
        "selected_candidate": {key: selected[key] for key in ("id", "max_depth", "min_child_weight", "ap_shape_weight")},
        "deployment_gate": gate,
    }
    write_json(args.output_dir / "model_config.json", config)
    report = {
        "base_model": str(args.base_model),
        "base_data_dir": str(args.base_data_dir),
        "device": args.device,
        "gpu_id": args.gpu_id,
        "cache_frozen": {
            "base_sha256": sha256_file(cache_source),
            "output_sha256": sha256_file(args.output_dir / "cache_mb.json"),
            "frozen_predictions_identical": True,
        },
        "data": {
            "base_split_counts": {name: len(rows) for name, rows in base_splits.items()},
            "ap_shape_train_count": len(ap_shape_train),
            "ap_shape_validation_count": len(ap_shape_validation),
            "supplement_train_count": len(supplement_train),
            "supplement_validation_count": len(supplement_validation),
            "frozen_ap_count": len(frozen_rows),
            "base_data_sha256": {name: sha256_file(args.base_data_dir / f"{name}.jsonl") for name in base_splits},
            "ap_shape_sha256": {str(path): sha256_file(path) for path in args.ap_shape},
            "supplement_sha256": {str(path): sha256_file(path) for path in args.supplement},
            "frozen_ap_sha256": {str(path): sha256_file(path) for path in args.frozen_ap},
        },
        "validation_baseline": validation_baseline,
        "candidate_count": len(candidates),
        "candidate_selection_used_feasible_pool": bool(feasible),
        "selected_candidate": selected,
        "final_test": {
            "base_v3": one_pass_metrics(test_y, predict_deployed_one_pass(xgb, base_cache, base_one, test_x)[1]),
            "candidate": one_pass_metrics(test_y, final_test_prediction),
        },
        "frozen_five_stage_gate": gate,
    }
    write_json(args.output_dir / "metrics.json", report)
    write_json(args.output_dir / "candidate_results.json", {"baseline": validation_baseline, "candidates": candidates})
    with (args.output_dir / "frozen_ap_predictions.jsonl").open("w", encoding="utf-8") as handle:
        for row, cache, base, candidate in zip(frozen_rows, base_frozen_cache, base_frozen_one, final_frozen_one):
            handle.write(json.dumps({
                "query_id": row["query_id"], "gold": row["bounds"], "cache_mb": float(cache),
                "base_one_pass_mb": float(base), "candidate_one_pass_mb": float(candidate),
            }, ensure_ascii=False) + "\n")
    print(json.dumps({
        "output_dir": str(args.output_dir),
        "selected_candidate": selected["id"],
        "validation_constraints_passed": selected["validation_constraints_passed"],
        "frozen_five_stage_gate_passed": gate["passed"],
        "metrics": str(args.output_dir / "metrics.json"),
    }, indent=2))
    if args.require_deployment_gate and not gate["passed"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
