# XGBoost memory-bound model

The model consumes planner, runtime, and relation features from the
three-bound collector JSONL. SQL text is only used for leakage-safe template
groups. Cache and one-pass are trained in `log1p` space; multi-pass is derived
as `max(0.0625, one_pass_mb - 1/1024)` to match the collector's upper-edge
label semantics.

The normalized combined dataset records multi-pass provenance explicitly:
`observed` is a native executor boundary from the `native-v2` collector,
`derived` is the `one_pass - 1KB` proxy retained for compatibility, and
`missing` has no usable third boundary. Use `--observed-multi-pass-only` for
the primary three-bound model; legacy and derived rows are auxiliary data.

## Train

```bash
python scripts/train_xgb_memory.py \
  --data output-memory-bounds-v2/sft/all.jsonl \
  --output-dir output-memory-xgb-v2 \
  --device auto --n-jobs 2 --n-estimators 240 --max-depth 4 --max-bin 64
```

For the primary native-v2 set, add `--observed-multi-pass-only`. Mild
under-estimate aversion can be tested with
`--objective reg:quantileerror --quantile-alpha 0.55`; the default
`reg:squarederror` remains the precision baseline. Calibration searches
per-bound multiplicative factors on a template-disjoint validation fold. The
selected factors are written to `calibration.json` and copied into
`model_config.json`; `predict_xgb_memory.py` applies them automatically.

`--device auto` selects the GPU with the lowest current memory-use ratio and
exposes only that physical GPU to XGBoost. The small histogram configuration
keeps the additional allocation low. Use `--device cpu` on a shared host or
when the installed XGBoost CUDA build is incompatible with the driver.

The output includes `metrics.json`, two portable Booster JSON files,
`model_config.json`, leakage-safe holdout predictions, and a dataset hash.
Metrics include q-error mean/median/P90, q-error threshold rates, MAE, RMSE,
log-space RMSE, and bound-order accuracy for the template holdout and each
leave-one-scale-out split. `metrics.json` also reports leave-one-workload
and leave-one-size results; `ood_summary` averages q-error and low-estimate
rate equally across held-out workloads. Model selection uses mean q-error as
the primary score and reports low-estimate rate without imposing a hard cap.

## Predict

```bash
python scripts/predict_xgb_memory.py \
  --models-dir output-memory-xgb-v2 \
  --input output-memory-bounds-v2/sft/all.jsonl \
  --output /tmp/memory_predictions.jsonl
```

Input records must contain the same `features` object as the collector output;
gold `bounds` are optional and are copied into the prediction output when
present. The predictor loads the saved calibration factors and preserves
`cache >= one_pass >= multi_pass` after calibration.

## Recollecting stable native boundaries

`generate_memory_bounds_batch.py` defaults to three probes per SQL. Each
probe must succeed; the stored boundary is the median and the record includes
`repeat_count`, `repeat_bounds`, and `boundary_stability_mb`. Keep one output
root per collection campaign and use a fresh native-v2 manifest when adding
workload/size/shape coverage (the target budget is about 100 accepted rows per
workload/size/shape).

## Evaluate the previous LLM AP test

```bash
python scripts/evaluate_xgb_memory.py \
  --models-dir output-memory-xgb-v2 \
  --data-dir output-memory-llm-v2/ap \
  --output-dir output-memory-xgb-v2/ap_eval
```

## Collect and evaluate the native Sort AP

```bash
python scripts/collect_ap_native_v2.py \
  --instance runtime/instance.json \
  --size 20 \
  --output-dir output-memory-xgb-v2/ap

python scripts/evaluate_xgb_memory.py \
  --models-dir output-memory-xgb-v2 \
  --data-dir output-memory-xgb-v2/ap \
  --bounds cache one_pass \
  --output-dir output-memory-xgb-v2/ap_eval
```

This uses the report's Sort SQL and re-collects native cache/one-pass bounds.
The Sort is one-pass at minimum work_mem, therefore it has no native multi-pass
gold label. Do not evaluate a native-v2 model against the report's AMM grant
labels.

## Five-stage one-pass-only refit

The AMM five-stage Sort appears at two input intervals.  Collect its native
cache/one-pass labels first; this manifest is a frozen evaluation set and is
never included in fitting or hyperparameter selection:

```bash
python scripts/collect_five_stage_ap_bounds.py \
  --host /tmp/amm-stage5 --port 15436 --user baiyutao --database postgres \
  --output output-memory-five-stage-ap/frozen_native_bounds.jsonl
```

Refit only `one_pass_mb.json` while copying the deployed v3 cache model
byte-for-byte.  The AP-shape collector data is split deterministically by
ordinal (`000/002` train, `001/003` validation).  The two frozen APs are
strict deployment gates: neither may be underestimated or have higher q-error
than v3; each must be at most `1.50` q-error, aggregate mean must be at most
`1.25`, and aggregate mean/P90 q-error may not regress.

```bash
python scripts/train_xgb_one_pass.py \
  --base-model output-memory-xgb-combined-v3/model_clean_gpu2 \
  --base-data-dir output-memory-xgb-combined-v3/data \
  --ap-shape output-memory-ap-shape-coverage/gsbench-1gb/bounds.jsonl \
  --ap-shape output-memory-ap-shape-coverage/gsbench-2gb/bounds.jsonl \
  --ap-shape output-memory-ap-shape-coverage/gsbench-5gb/bounds.jsonl \
  --ap-shape output-memory-ap-shape-coverage/gsbench-10gb/bounds.jsonl \
  --ap-shape output-memory-ap-shape-coverage/gsbench-15gb/bounds.jsonl \
  --ap-shape output-memory-ap-shape-coverage/gsbench-20gb/bounds.jsonl \
  --frozen-ap output-memory-five-stage-ap/frozen_native_bounds.jsonl \
  --output-dir output-memory-xgb-onepass-v1 --device auto --n-jobs 2
```

`metrics.json` records all validation candidates, the held-out v3 test split,
and a per-AP frozen gate report.  The C++ exporter rejects a one-pass refit
whose gate did not pass, so an experimental candidate cannot silently replace
the AMM model.

When the frozen 512MB AP remains above the target q-error, collect only
nearby, non-identical intervals from the same AMM instance and add them as a
separate fixed-split supplement.  The collector rejects the frozen SQL texts:

```bash
python scripts/collect_five_stage_ap_neighborhood.py \
  --host /tmp/amm-stage5 --port 15436 --user baiyutao --database postgres \
  --output output-memory-five-stage-ap/neighborhood_native_bounds.jsonl
```

Append the following option to the preceding `train_xgb_one_pass.py` command:

```bash
--supplement output-memory-five-stage-ap/neighborhood_native_bounds.jsonl
```

## Joint TPCH/TPCDS/DSB/JOB + gsbench training

Build the normalized joint dataset from the prior native benchmark records and
the completed gsbench SFT file:

~~~bash
python scripts/prepare_combined_xgb_data.py \\
  --gsbench output-memory-bounds-v2/sft/all.jsonl \\
  --output-dir output-memory-xgb-combined-v2/data
~~~

The builder emits combined.jsonl, leakage-safe train/validation/test splits,
an audit manifest, and a three-bound-only subset. DSB rows with no observed
multi-pass boundary remain usable for the cache/one-pass regressors and are
marked as synthetic multi-pass values for ordering diagnostics only.

Train the workload-balanced mixed-feature model:

~~~bash
python scripts/train_xgb_memory.py \\
  --data output-memory-xgb-combined-v2/data/combined.jsonl \\
  --output-dir output-memory-xgb-combined-v2/model_balanced_gpu2 \\
  --protocol v2 --feature-abi mixed --allow-missing-multi-pass \\
  --balance-workload --device auto --n-jobs 2 \\
  --n-estimators 240 --max-depth 4 --max-bin 64
~~~

--feature-abi mixed adds workload one-hot features and scale-factor
features. --balance-workload applies inverse-frequency weights so gsbench
does not dominate the older benchmark workloads. The report includes template,
scale, and leave-one-workload-out metrics.

For an environment-independent ablation, use `--feature-abi mixed_clean`.
This removes system/session state, all `workload_*` identity features, all
`*_missing` indicators, and both scale-factor features while retaining planner,
relation, and sort-shape features. The resulting model must be evaluated with
its matching `model_config.json`.
