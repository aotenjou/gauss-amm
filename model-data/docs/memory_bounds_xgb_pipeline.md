# 三档内存边界与 XGBoost 流程

本文记录从 SQL 生成、native-v2 三档边界采集、数据规范化、XGBoost 训练、校准、预测到 OOD 验收的完整流程。工作目录为仓库根目录下的 `model-data/`；以下命令从该目录执行。

## 1. 目标与标签

每条 SQL 采集三个 `work_mem` 边界，单位为 MB：

- `cache_mb`：执行不发生磁盘临时读写的边界。
- `one_pass_mb`：执行进入 one-pass 的边界。
- `multi_pass_mb`：multi-pass 上沿，当前 native-v2 定义为 `one_pass_boundary_kb - 1KB`，并满足 `cache >= one_pass >= multi_pass`。

主目标是三档边界平均 q-error 最低。低估率需要报告，但不是硬约束；可以用 quantile objective 或乘性校准改善高估倾向。模型验收使用留一 workload 和留一 size 的结果，按 workload 等权汇总 q-error 与低估率。

## 2. 数据目录

建议将不同采集活动放在独立目录，避免覆盖历史结果：

```text
output-memory-bounds/                    # 旧 SQL 候选和旧标签
output-memory-bounds-v2/                 # 已有 native-v2 主集
output-memory-bounds-v2-repeated-*/      # 三次重复补采
output-memory-xgb-*/                     # 模型、metrics 和预测结果
```

已有 native-v2 GSBench 主集为 `output-memory-bounds-v2/sft/all.jsonl`，当前约 1200 条，覆盖 1/2/5/10/15/20GB。混合数据由 `scripts/prepare_combined_xgb_data.py` 重新规范化后使用。

## 3. 生成和采集 SQL

### 新生成并采集

`generate_memory_bounds_batch.py` 负责生成符合内存形状约束的 SQL，并通过 `EXPLAIN`、`EXPLAIN ANALYZE` 采集标签。每个 size 默认 200 条，采集默认串行，以避免并发改变边界：

```bash
python generate_memory_bounds_batch.py \
  --instance runtime/instance.json \
  --output-root output-memory-bounds-v2-new \
  --target-count 200 \
  --repeat-count 3 \
  --sizes 1 2 5 10 15 20
```

中断后使用 `--resume`。每次重复探针都必须成功，最终边界取三个结果的中位数；记录还包含 `repeat_bounds` 和 `boundary_stability_mb`。

### 旧候选重新测量

`recollect_memory_bounds_v2.py` 只重测标签和计划特征，不复用旧标签。实例重启后若端口变化，可使用 `--port` 覆盖 instance 文件中的端口：

```bash
python recollect_memory_bounds_v2.py \
  --instance runtime/instance.json \
  --port 15441 \
  --source-root output-memory-bounds \
  --output-root output-memory-bounds-v2-repeated \
  --size 1 --target-count 200 --repeat-count 3 --resume
```

六个 size 需要分别执行。开始前检查数据库连接和 schema：

```bash
psql -h /tmp -p 15432 -U baiyutao -d postgres -c '\\l'
python - <<'PY'
import psycopg2
c = psycopg2.connect(host='runtime/socket', port=15441,
                     user='baiyutao', dbname='llm4sqlgen_s1gb')
print('connection ok')
c.close()
PY
```

采集前确认数据库服务、目标数据库、`gsbench` schema、磁盘空间和 `ANALYZE` 状态。不要删除已有输出目录；恢复使用 `--resume`。

## 4. 数据规范化

统一数据：

```bash
python scripts/prepare_combined_xgb_data.py \
  --gsbench output-memory-bounds-v2/sft/all.jsonl \
  --coverage output-memory-sort-coverage-v4/sft/all.jsonl \
  --output-dir output-memory-xgb-combined/data
```

只使用真实 native-v2 三档标签作为主训练集：

```bash
python scripts/prepare_combined_xgb_data.py \
  --gsbench output-memory-bounds-v2-repeated/sft/all.jsonl \
  --output-dir output-memory-xgb-observed/data \
  --observed-only
```

每条规范化记录包含：

- `multi_pass_status=observed`：`native-v2` 且语义为 `native_executor_boundaries`。
- `multi_pass_status=derived`：保留的 `one_pass - 1KB` 代理或旧协议字段。
- `multi_pass_status=missing`：没有第三档数值。

`dataset_manifest.json` 必须检查 `observed_three_bound_rows`、`derived_multi_pass_rows`、`missing_multi_pass_rows`、workload 数量和 source 列表。旧 TPCH/TPCDS/JOB/DSB 数据只能作辅助或对照，不能伪装成 observed native-v2 三档主标签。

## 5. XGBoost 训练

### 精度基线

```bash
python scripts/train_xgb_memory.py \
  --data output-memory-xgb-observed/data/combined.jsonl \
  --output-dir output-memory-xgb-squarederror \
  --protocol v2 --feature-abi mixed_clean \
  --observed-multi-pass-only \
  --device cpu --n-jobs 2 \
  --n-estimators 240 --max-depth 4 --max-bin 64 \
  --calibration-grid 0.95 1.0 1.05 1.10 1.20
```

### 轻度高估候选

```bash
python scripts/train_xgb_memory.py \
  --data output-memory-xgb-observed/data/combined.jsonl \
  --output-dir output-memory-xgb-quantile055 \
  --protocol v2 --feature-abi mixed_clean \
  --observed-multi-pass-only \
  --objective reg:quantileerror --quantile-alpha 0.55 \
  --device cpu --n-jobs 2 \
  --n-estimators 240 --max-depth 4 --max-bin 64 \
  --calibration-grid 0.95 1.0 1.05 1.10 1.20
```

可调参数包括 `n_estimators`、`max_depth`、`learning_rate`、`min_child_weight`、`max_bin` 和 `feature-abi`。建议先固定数据切分，只比较少量候选，避免把测试集用于调参。

支持的主要 feature ABI：`v2`、`mixed`、`mixed_clean`、`mixed_clean_sort`、`v3`、`mixed_v3`、`mixed_clean_p0`。跨 workload 泛化优先使用不含 workload identity 和 scale identity 的 `mixed_clean`，需要 operator P0 特征时再比较 `mixed_clean_p0`。

训练输出包括：

- `cache_mb.json`、`one_pass_mb.json`：portable XGBoost Booster。
- `model_config.json`：ABI、特征、训练参数、imputation、范围和校准引用。
- `calibration.json`：最终 cache/one-pass 乘性系数。
- `metrics.json`：template、size、workload holdout 和 in-sample 报告。
- `*_predictions.jsonl`：每条 holdout 的 gold、pred 和 OOD warning。

## 6. 校准和预测

校准模型在模板隔离验证折上选择 cache/one-pass 两个乘性系数，主指标是平均 q-error。最终模型使用全量数据重训，并保存所选系数。预测脚本会自动读取 `calibration.json`：

```bash
python scripts/predict_xgb_memory.py \
  --models-dir output-memory-xgb-squarederror \
  --input output-memory-bounds-v2/sft/all.jsonl \
  --output /tmp/memory-predictions.jsonl \
  --device cpu
```

预测后始终投影为 `cache >= one_pass >= multi_pass`。输入协议与模型不一致、特征缺失或超出训练范围时，输出会带 `warnings`；需要严格拒绝 OOD 输入时增加 `--strict-ood`。

## 7. 验收和模型选择

`metrics.json` 中重点检查：

- `template_holdout.qerror_mean`、`qerror_p90`、`log_rmse`。
- `scale_holdout`：每个 size 留出时的 q-error 和低估率。
- `workload_holdout`：每个 workload 留出时的 q-error 和低估率。
- `ood_summary.qerror_mean_equal_workload`、`qerror_p90_equal_workload`、`low_estimate_rate_equal_workload`。
- `order_accuracy`，应保持为 1。

候选选择顺序是：先比较留一 workload/size 的加权平均 q-error，再看 q-error P90、log RMSE 和低估率。低估率用于共同判断和风险记录，不设置硬上限。模型只能在验证结果确定后用全量数据重训发布；in-sample 指标只作描述，不能作为验收依据。

## 8. 常见问题

### 端口和 socket 不一致

`instance.json` 可能记录 15440，但服务重启后实际监听 15441。使用 `ps -ef | rg gaussdb`、`ss -ltnp`、socket 目录和 `psql`/`psycopg2` 检查实际端口，然后给重采脚本传 `--port`。不要直接覆盖历史 manifest。

### 补采中断

查看每个 size 的 `bounds.jsonl` 和 `rejected.jsonl`，用相同 `--output-root --size --resume` 继续。已接受的 SQL 由 hash 去重，重复采集信息保留在记录内。

### 没有 observed 三档

检查 `label_protocol`、`label_semantics` 和 `multi_pass_status`。two-bound Sort 数据只能辅助 cache/one-pass，不能用于 observed-only 三档训练。

### XGBoost 预测被拒绝

确认 `model_config.json` 的 `feature_abi` 与预测脚本支持的 ABI 一致；输入必须包含 collector 的 `features`，训练和预测使用同一套 feature ABI 和 imputation。

## 9. 当前验证记录

历史 1200 条 GSBench native-v2 observed-only 数据上，squarederror 基线模板留出 q-error 均值约为 1.96；quantile alpha=0.55 的候选约为 2.76，低估率改善并不保证平均误差改善。因此正式模型应等待跨 workload/size 的统一 native-v2 补采完成后再选型。

### 4444 条数据诊断（2026-09-21）

当前 `output-memory-xgb-combined-v6/data/combined.jsonl` 共 4444 条：2820 条有真实 native-v2 三档标签，1624 条只有 cache/one-pass，multi-pass 由 one-pass 派生。DSB 的 600 条中 575 条 cache 和全部 one-pass 都在最小值；GSBench 包含少量极端 Sort cache 值和特征超出训练范围的记录。这些分布混合是 q-error 过大的主要来源。

在相同模板留出切分上，`reg:squarederror` 的全量模型
`output-memory-xgb-combined-v6/model_squarederror_4444_r2` 得到：q-error 均值 3.13、P90 6.01、三档顺序准确率 1.0。`reg:quantileerror` alpha=0.60/0.65 的均值分别约为 5.20/5.57，因此不应仅为降低低估率切换到 quantile loss。真实三档 observed-only 模型的模板 q-error 约 3.35，但留一 workload 仍受 OOD 影响。

全量模型的留一 workload q-error 为：DSB 73.8、GSBench 49.9、JOB 38.0、TPCDS 6.1、TPCH 3.9；这些折的 OOD/缺失特征告警率很高。部署预测会强制投影 `cache >= one_pass >= multi_pass`，并在输入特征缺失或越界时写入 `warnings`。在补齐各 workload 的统一 plan/runtime 特征前，这些 OOD 预测只能作为带风险标记的估计，不能作为模型已经泛化的证据。

因此当前推荐：使用 squarederror 全量模型作为基线，保留低估率和 OOD warning 记录；优先补齐 workload/size 的真实三档和 operator-shape 特征，再按留一 workload/size 的 q-error 重新验收。增加树数量或提高 quantile alpha 不是当前主要解决方案。
