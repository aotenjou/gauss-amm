# WorkMem Boundary Data and XGBoost Pipeline

This directory is the reproducible data, collection, training, and test part
of the GS AMM project. The parent repository keeps the kernel at its existing
top level and keeps this model pipeline in the separate `model-data/` folder.

The deployed kernel model ABI is separate from the Python training model. The
Python artifacts here are the validation source of truth; C++ model exports
are produced only after the Python model passes the locked OOD checks.

## Contents

```text
model-data/
  data/       native-v2 labels, normalized combined data, fixed splits
  models/     squarederror and quantile candidate artifacts and metrics
  scripts/    normalization, training, prediction, evaluation, export
  tests/      focused collection and data-contract tests
  docs/       detailed pipeline notes
  prompts-memory/  SQL generation prompts
```

The primary dataset is `data/native-v2-gsbench-1200.jsonl`: 1,200 complete
three-bound records across GSBench sizes 1, 2, 5, 10, 15, and 20GB. A row is
`observed` only when its multi-pass value came from a native-v2 executor
boundary. The `one_pass - 1KB` value is retained as a `derived` compatibility
label for old records and is not used as the primary three-bound target.

`data/combined-native-v2-and-auxiliary.jsonl` contains the normalized joint
set with legacy and two-bound auxiliary rows. Use it for source comparison and
cache/one-pass coverage. Use `--observed-multi-pass-only` for the primary
three-bound model. The accompanying `combined-dataset-manifest.json` records
the source counts and fixed template splits.

## Recreate the dataset

The collector needs an openGauss instance described by an instance JSON file.
Do not commit the instance file if it contains credentials or local paths.

```bash
cd model-data
python generate_memory_bounds_batch.py \
  --instance /path/to/instance.json \
  --output-root output-memory-bounds-v2 \
  --target-count 200 \
  --repeat-count 3 \
  --sizes 1 2 5 10 15 20
```

Each accepted SQL is probed three times. Every repetition must succeed; the
stored boundary is the median and the record includes repeat bounds and a
stability range. Existing candidates can be remeasured with
`recollect_memory_bounds_v2.py --repeat-count 3 --resume`.

After collection, merge the six manifests with:

```bash
python scripts/merge_memory_bounds_v2.py \
  --input-root output-memory-bounds-v2 \
  --output output-memory-bounds-v2/sft/all.jsonl
```

## Train candidates

The committed baseline was trained on the native-v2 1,200-row set with
feature ABI `v2`, 240 histogram trees, depth 4, max-bin 64, and a
template-disjoint calibration fold:

```bash
python scripts/train_xgb_memory.py \
  --data data/native-v2-gsbench-1200.jsonl \
  --output-dir output-memory-xgb/squarederror-v2 \
  --protocol v2 --feature-abi v2 \
  --observed-multi-pass-only \
  --objective reg:squarederror \
  --device cpu --n-estimators 240 --max-depth 4 --max-bin 64 \
  --calibration-grid 0.95 1.0 1.05 1.1
```

The high-estimate candidate uses the same features and tree capacity with
`--objective reg:quantileerror --quantile-alpha 0.55`. Mean q-error remains the
selection metric; low-estimate rate is reported and is not a hard constraint.
The saved `calibration.json` is applied automatically by the predictor.

## Predict and test

```bash
python scripts/predict_xgb_memory.py \
  --models-dir models/squarederror-v2 \
  --input data/native-v2-gsbench-1200.jsonl \
  --output /tmp/memory-predictions.jsonl

PYTHONPATH=. pytest -q tests/test_memory_bounds.py
```

The predictor applies the saved cache and one-pass calibration factors and
enforces `cache >= one_pass >= multi_pass`. It emits protocol and feature OOD
warnings for rows outside the training contract.

## Acceptance

Every candidate is evaluated with template holdout, leave-one-size-out, and
leave-one-workload-out splits. Release comparison uses the equal-workload
mean q-error and P90 q-error, together with cache/one-pass low-estimate rates.
The primary objective is minimum mean q-error. Low estimates are visible in
the report and guide model selection without a hard upper bound.

The committed baseline report is in
`models/squarederror-v2/metrics.json`; the quantile comparison is in
`models/quantile055-v2/metrics.json`. On the current 1,200-row set, the
squarederror template holdout mean q-error is about 1.96, while alpha 0.55 is
about 2.76. This comparison is a GSBench in-distribution baseline; final
release requires the new cross-workload and cross-size native-v2 collection.

## Data policy

Raw database data directories, API keys, generated logs, partial recollection
runs, and unrelated hyperparameter sweeps are intentionally excluded. The
committed JSONL files and model artifacts are sufficient to reproduce the
training and prediction checks without exposing local credentials.
