# openGauss AMM 使用手册

## 1. 目的、范围和版本

本文说明交付内核 `openGauss-hard-tps-guard` 中 AMM（Adaptive Memory Management）的启用方式、运行模式、配置参数、SQL 接口、状态观测、测试负载和验收方法。

AMM 将 normal shared buffer 按固定 granule 划分。当 AP 查询获得 grant 时，控制器在 TP TPS 和 IO 保护允许的前提下，drain 可安全回收的 buffer granule，并将该物理内存交给 AP 的 sort/hash MemoryContext 使用。AP 结束后 granule 被归还为 FREE 或重新加入 shared buffer。

本文覆盖的是当前交付源码实现，默认 granule 为 64 MB，AP 专用 allocator 首期只接入 sort/hash。它不代表所有执行器算子均已经使用 AMM allocator。

## 2. 使用前提

1. 使用本交付源码完成编译、安装和实例初始化。AMM 不是可以加载到未改造 openGauss 的独立 SQL 扩展。
2. 使用具有 superuser 权限的账号进行配置、legacy 调试接口调用和状态复位。
3. 保证 `shared_buffers` 启动时的容量足以覆盖目标的 shared buffer 上限。AMM 只能在该启动时分配的 buffer envelope 内转换 granule，不能在运行中增加物理 shared buffer 总上限。
4. 为 shared buffer 设定明确的最小保留值 `gs_amm_shared_buffers_min_mb`。该值必须高于 TP 活跃工作集的安全下界，否则即使控制器按 granule 缩容，也可能长期提高 physical read 并损伤 TPS。
5. 生产前必须完成 TP-only 和 TP+AP 混合压测。主要验收指标为 TPS jitter、AP 执行时间和 AP spill-to-disk，不应只根据模型离线误差决定是否上线。

## 3. AMM 的运行模式

### 3.1 关闭 AMM：直接基线

```conf
gs_amm_native_auto_mode = off
```

执行器不会进入 AMM 的候选 AP 识别、预测、grant、granule 分配和原生反馈闭环。这是与 raw dtree、calibrated dtree 对比时的 direct 基线。

`gs_amm_status()` 仍可读取状态，但不会为普通 SQL 自动创建 AP grant。

### 3.2 原生 raw dtree：推荐的首轮灰度模式

```conf
gs_amm_native_auto_mode = on
gs_amm_dtree_calibration_enabled = off
gs_amm_dtree_record_only = on
gs_amm_legacy_control_enabled = off
```

内核为符合条件的 SELECT 自动提取特征、调用静态决策树、执行 admission，并创建真实 granule grant。预测值为决策树原始输出，不应用叶子校准。反馈仍会记录，用于观察模型低估、spill、AP 时延和 TP/IO 影响。

这是首次启用 AMM 时应使用的模式。先确认 granule drain、grant release、TPS guard、IO guard 和 backpressure 均符合预期，再开启校准。

### 3.3 校准学习模式

```conf
gs_amm_native_auto_mode = on
gs_amm_dtree_calibration_enabled = on
gs_amm_dtree_record_only = off
gs_amm_legacy_control_enabled = off
```

内核按叶子收集在线样本，在样本数、TPS guard 和 IO guard 条件允许时更新校准倍率。下一个同叶子预测会使用校准后的三档边界。

校准表为共享内存易失数据，实例重启后恢复默认。建议先运行 record-only，再短时间、小流量开启该模式，并持续比较 AP spill、AP P95 和 TP TPS jitter。

### 3.4 校准冻结回放模式

```conf
gs_amm_native_auto_mode = on
gs_amm_dtree_calibration_enabled = on
gs_amm_dtree_record_only = on
gs_amm_legacy_control_enabled = off
```

该模式不更新校准状态，但仍读取现有叶子 scale 并将其应用于预测。因此它可用于回放已经学习出的校准值，或验证同一 scale 的稳定性。若尚未学习到有效 scale，初始 scale 为 1.0，效果与 raw dtree 接近。

### 3.5 allocator-only：验证内存池而不让预测值决定 grant

```conf
gs_amm_native_auto_mode = on
gs_amm_allocator_only_mode = on
gs_amm_allocator_only_grant_mb = 128
gs_amm_legacy_control_enabled = off
```

在此模式下，AP 的实际请求和授予额度使用 `gs_amm_allocator_only_grant_mb`，不使用 cache/one-pass/multi-pass 预测值决定 grant。granule pool、drain、sort/hash allocator、TP/IO guard 和 backpressure 仍然生效。

当前实现仍会采集 19 维特征并调用决策树，以保持原生生命周期和状态记录一致；该开关是“预测值不参与授予”，不是“完全不执行决策树”。当前没有独立的 `gs_amm_dtree_prediction_enabled` 开关可以跳过特征采集和模型推理。

### 3.6 feedback-only：不依赖预测值的渐进式 AP 调度

```conf
gs_amm_native_auto_mode = on
gs_amm_feedback_only_mode = on
gs_amm_allocator_only_mode = off
gs_amm_dtree_calibration_enabled = off
gs_amm_dtree_record_only = on
gs_amm_feedback_bootstrap_grant_mb = 64
gs_amm_feedback_max_grant_mb = 128
gs_amm_feedback_initial_ap_slots = 1
gs_amm_feedback_max_ap_slots = 2
gs_amm_feedback_stable_windows = 2
gs_amm_feedback_spill_threshold_mb = 32
gs_amm_legacy_control_enabled = off
```

该模式保留原生 AP 候选识别、granule pool、sort/hash allocator、TP/IO guard 与 backpressure，但不构造 19 维决策树特征，也不运行决策树预测。控制器从保守 grant 和单槽位开始，稳定完成的 AP 逐步增加一个 granule 和一个槽位；发生 spill、错误、TPS 保护或 IO 保护时回退 grant 和槽位。达到槽位上限的 AP 进入反压队列，不会绕过保护直接执行。

适用于需要先验证内存池和保护机制、或者决策树在目标负载上泛化不足的场景。它与 `allocator_only` 的区别是：allocator-only 使用固定 grant，feedback-only 根据已完成 AP 的真实结果渐进调整 grant 和并发。

### 3.7 legacy SQL 控制模式：仅调试与回归

```conf
gs_amm_native_auto_mode = off
gs_amm_legacy_control_enabled = on
```

legacy 接口允许外部 SQL 手工设置压力、推进 controller、请求 AP grant、注入反馈和调整 shared buffer。它用于调试、源码契约测试和历史实验兼容，**不应作为生产 AMM 的控制面**。原生路径由执行器、pagewriter 和事务回调管理，正常运行必须保持 `gs_amm_legacy_control_enabled = off`。

## 4. 配置方式与生效范围

除 `gs_amm_resize_batch_mb` 外，AMM GUC 均为 SIGHUP 级参数：修改实例配置后 reload 即可生效。`gs_amm_resize_batch_mb` 同时定义 granule 大小和共享内存 granule 表布局，属于 postmaster 启动期参数，修改后必须重启实例。

以下示例使用 `postgresql.conf`。集群环境应通过现有的 openGauss 配置管理流程向所有目标节点下发相同配置，再执行 reload 或重启。

```conf
# 原生 raw dtree 的保守起点
gs_amm_native_auto_mode = on
gs_amm_native_ap_cost_threshold = 10000
gs_amm_shared_buffers_min_mb = 1024
gs_amm_dynamic_target_mb = 512
gs_amm_resize_batch_mb = 64              # 修改本项后需重启
gs_amm_resize_rate_limit_mb = 64
gs_amm_tp_jitter_limit = 0.03
gs_amm_dtree_calibration_enabled = off
gs_amm_dtree_record_only = on
gs_amm_legacy_control_enabled = off
```

reload 后在 SQL 客户端确认：

```sql
SELECT pg_reload_conf();

SELECT name, setting
FROM pg_settings
WHERE name LIKE 'gs_amm_%'
ORDER BY name;

SELECT pg_catalog.gs_amm_status();
```

### 4.1 所有 AMM GUC

| 参数 | 默认值 | 生效 | 用途 |
| --- | ---: | --- | --- |
| `gs_amm_native_auto_mode` | `off` | reload | 是否由执行器自动管理符合条件的 AP 查询，是总开关 |
| `gs_amm_native_ap_cost_threshold` | `10000` | reload | 进入原生 AP 候选路径的最小计划成本 |
| `gs_amm_legacy_control_enabled` | `off` | reload | 是否允许 legacy SQL mutator 修改 AMM 状态 |
| `gs_amm_allocator_only_mode` | `off` | reload | 忽略预测值，使用固定 allocator-only grant |
| `gs_amm_allocator_only_grant_mb` | `128` | reload | allocator-only 的固定 AP grant 目标 |
| `gs_amm_dtree_calibration_enabled` | `off` | reload | 是否读取/更新按叶子的校准状态 |
| `gs_amm_dtree_record_only` | `on` | reload | 是否禁止校准更新；开启时保留已有 scale 但不学习新 scale |
| `gs_amm_feedback_only_mode` | `off` | reload | 是否启用不运行决策树的完成反馈调度 |
| `gs_amm_feedback_bootstrap_grant_mb` | `64` | reload | feedback-only 初始 grant |
| `gs_amm_feedback_max_grant_mb` | `256` | reload | feedback-only grant 上限 |
| `gs_amm_feedback_initial_ap_slots` | `1` | reload | feedback-only 初始 AP 槽位数 |
| `gs_amm_feedback_max_ap_slots` | `4` | reload | feedback-only AP 槽位上限 |
| `gs_amm_feedback_stable_windows` | `2` | reload | 扩容前所需的连续稳定完成次数 |
| `gs_amm_feedback_spill_threshold_mb` | `32` | reload | 触发反馈回退的 spill 阈值 |
| `gs_amm_shared_buffers_min_mb` | `64` | reload | shared buffer 允许缩减到的下限 |
| `gs_amm_resize_batch_mb` | `64` | 重启 | granule 大小和单次 resize 批大小，最小 64 MB |
| `gs_amm_resize_rate_limit_mb` | `64` | reload | 每次 controller 决策可调整的 shared buffer 上限 |
| `gs_amm_resize_observe_window_ms` | `5000` | reload | resize 后观测窗口 |
| `gs_amm_resize_cooldown_ms` | `8000` | reload | resize 后及 guard 触发后的冷却时间 |
| `gs_amm_tp_recovery_cooldown_ms` | `8000` | reload | TP 恢复动作后的冷却时间 |
| `gs_amm_controller_horizon` | `3` | reload | controller 规划 horizon |
| `gs_amm_deadband_mb` | `32` | reload | 控制器 deadband，避免频繁 shrink/expand |
| `gs_amm_dynamic_target_mb` | `512` | reload | 动态 AP pool 的默认总目标 |
| `gs_amm_ap_min_grant_mb` | `4` | reload | 可被 admission 的最小 AP grant |
| `gs_amm_ap_queue_limit` | `16` | reload | AP backpressure 队列上限，0 表示不排队 |
| `gs_amm_ap_queue_timeout_ms` | `5000` | reload | AP 等待 grant 的最长时间 |
| `gs_amm_tp_jitter_limit` | `0.03` | reload | TP TPS 跌幅硬保护阈值，0.03 代表约 3% |
| `gs_amm_tp_pressure_guard` | `80` | reload | TP 压力评分达到该值时进入保护 |
| `gs_amm_io_pressure_guard` | `80` | reload | IO 压力评分达到该值时进入保护 |

`gs_amm_shared_buffers_min_mb`、`gs_amm_dynamic_target_mb`、`gs_amm_resize_rate_limit_mb` 必须按机器总内存、TP 工作集和 AP 并发一起标定。不要通过把 shared buffer 下限压到很低来换取 AP 内存。

## 5. 原生自动模式的行为

原生模式开启后，`standard_ExecutorStart` 会检查查询是否为顶层 SELECT、非 EXPLAIN-only、未持有已有 grant，且计划成本达到阈值。只有包含 sort、hash、aggregate 或 window 等内存密集节点的候选才会进入 AMM。

内核会构造 19 维特征：算子数量、计划行数/行宽/总代价、并行信息、活跃会话、会话私有内存、事务年龄、系统内存、AMM 内存压力、涉及表/索引大小、平均列宽和总代价对数。决策树输出三档内存边界：

- `cache`：优先避免 spill 的目标；
- `one-pass`：允许一轮落盘的目标；
- `multi-pass`：允许多轮处理的下界。

随后 grant 仍必须通过以下限制：可用 FREE granule、动态目标、AP queue、TPS 30 秒窗口、physical read、dirty/writeback、AP spill/multipass、IO queue depth、resize cooldown 和 recovery cooldown。预测值不是绕过 TP/IO 保护的权限。启用 feedback-only 时，该路径直接使用反馈控制器的当前 grant，并跳过特征提取和决策树推理。

在 AP 完成、事务提交、事务回滚、子事务异常或执行器错误时，原生路径会释放 grant 并记录反馈。正常使用时不需要应用程序调用 `gs_amm_begin_ap()` 或 `gs_amm_end_ap()`。

## 6. 状态观测和日常检查

### 6.1 主状态接口

```sql
SELECT pg_catalog.gs_amm_status();
```

该函数返回空格分隔的 `key=value` 文本。压测程序可解析为键值对；人工排查时重点关注：

| 类别 | 关键字段 | 正常关注点 |
| --- | --- | --- |
| 原生路径 | `native_auto_mode`、`native_eligible_count`、`native_admit_count`、`native_reject_count`、`native_release_count` | 候选、放行和释放计数应相互合理；拒绝需结合原因分析 |
| granule | `total_granules`、`buffer_active_granules`、`buffer_draining_granules`、`free_granules`、`ap_reserved_granules`、`ap_active_granules` | AP 使用期间可看到 AP granule；结束后应回收 |
| drain | `drain_pending_dirty`、`drain_pending_pinned`、`drain_pending_io`、`drain_pending_hash`、`drain_success_count`、`drain_rollback_count` | pending 长期不清或 rollback 持续增长需停止激进缩容 |
| AP | `ap_grant_bytes`、`ap_granule_used_bytes`、`grant_shrink_count`、`effective_grant_kb`、`backpressure_count` | grant 是否真实使用、是否存在过度降级或排队 |
| Feedback-only | `feedback_current_grant_mb`、`feedback_ap_slot_limit`、`feedback_completed_count`、`feedback_growth_count`、`feedback_backoff_count`、`feedback_slot_block_count`、`feedback_ewma_*`、`feedback_last_action` | 无预测调度的成长、回退、排队和 AP 实际结果 |
| TP | `tp_baseline_tps`、`tp_recent_tps`、`tp_raw_drop_ratio`、`tp_guard_hot` | 约 3% 阈值附近应停止 shrink/新增 AP 扩容 |
| IO | `physical_read_rate`、`ap_spill_mb_rate`、`ap_multipass_count`、`io_queue_depth`、`io_guard_hot` | IO 变差时应阻断 shrink/放大 grant |
| 决策树 | `dtree_*`、`native_last_event_raw_bounds_kb`、`native_last_event_calibrated_bounds_kb` | 比较原始/校准预测、scale、反馈、冻结和回滚 |

### 6.2 决策树调试接口

```sql
-- 兼容接口：返回三档预测数组
SELECT pg_catalog.gs_workmem_dtree_predict(
  ARRAY[1, 1, 0, 0, 6.0, 64, 20000, 0, 0, 8, 64, 2, 65536, 32768, 0.2, 10240, 2048, 32, 9.9]::float8[]
);

-- detail 接口：返回 raw、calibrated、模型版本、叶子和校准信息
SELECT * FROM pg_catalog.gs_workmem_dtree_predict_detail(
  ARRAY[1, 1, 0, 0, 6.0, 64, 20000, 0, 0, 8, 64, 2, 65536, 32768, 0.2, 10240, 2048, 32, 9.9]::float8[]
);
```

数组必须严格按以下顺序传入：

1. hash join 节点数；2. sort 节点数；3. aggregate 节点数；4. window 节点数；5. 最大计划行数的 `log10`；6. 最大计划行宽；7. 计划总代价；8. 并行 worker 数；9. parallel-aware 节点数；10. 活跃会话数；11. 会话私有内存 MB；12. 事务年龄秒；13. 系统总内存 MB；14. 系统可用内存 MB；15. AMM 内存压力评分；16. 涉及表总大小 MB；17. 涉及索引总大小 MB；18. 平均列宽；19. `log1p(计划总代价)`。

该接口用于验证模型和叶子校准；原生执行路径自行收集特征，不需要客户端构造数组。

## 7. Legacy SQL 接口

以下接口全部注册在 `pg_catalog`。除 `gs_amm_status()` 外，其余 AMM state mutator 需要 superuser，且必须先打开 `gs_amm_legacy_control_enabled`。开始 legacy 实验前应确认没有原生 AP grant：

```sql
SELECT pg_catalog.gs_amm_status();
```

### 7.1 初始化、目标和压力注入

```sql
-- 无活动 AP grant 时复位 AMM 状态
SELECT pg_catalog.gs_amm_reset_state();

-- 设置 AP pool 总目标，单位 MB
SELECT pg_catalog.gs_amm_set_dynamic_target_mb(512);

-- 手工驱动一次控制器：AP 需求 MB、TP 压力 0..100、IO 压力 0..100
SELECT pg_catalog.gs_amm_controller_step(512, 0, 0);

-- 更新 TP 指标：TPS、P95 延迟毫秒、统计窗口毫秒
SELECT pg_catalog.gs_amm_update_tp_metrics(12000.0, 8.5, 30000);

-- 更新 IO 指标：物理读率、IO 队列深度、AP spill MB/s、multipass 数、窗口毫秒
SELECT pg_catalog.gs_amm_update_io_metrics(300.0, 2, 0.0, 0, 30000);
```

`gs_amm_controller_step()` 传入的 TP/IO 压力是外部测试输入，只用于 legacy 实验。原生模式由 pagewriter 从内核提交、buffer read miss、spill 和 dirty 页遥测生成压力评分。

### 7.2 手工调整 shared buffer

```sql
-- 目标单位 MB；目标会对齐到 granule，且不得超过启动期 buffer envelope
SELECT pg_catalog.gs_amm_resize_shared_buffers(2048);
```

函数可能返回 deferred 或 rollback，而非保证立刻缩容。不要循环强制调用以绕过 dirty/pinned/IO 检查、TPS guard 或冷却时间。

### 7.3 手工 AP grant 生命周期

```sql
-- 单预测值接口：预测 MB、最小 MB、最大 MB、排队等待毫秒
SELECT pg_catalog.gs_amm_begin_ap(256, 4, 512, 5000);

-- 三档边界接口：cache KB、one-pass KB、multi-pass KB、排队等待毫秒
SELECT pg_catalog.gs_amm_begin_ap_bounds(262144, 131072, 65536, 5000);

-- 同一 backend 中释放当前 AP grant
SELECT pg_catalog.gs_amm_end_ap();
```

返回文本中的 `admitted`、`queued`、`backpressure`、`granted_mb`、`grant_granules`、`memory_mode` 和 `reason` 是判断结果的首选依据。AP grant 应在同一 backend 生命周期内成对释放；不能把 legacy grant 当作跨会话的配额。

### 7.4 allocator-only 与校准调试

```sql
-- 忽略预测值，以固定 128 MB grant 验证 granule allocator
SELECT pg_catalog.gs_amm_set_allocator_only_mode(true, 128);

-- 关闭 allocator-only，恢复按三档预测选择 grant
SELECT pg_catalog.gs_amm_set_allocator_only_mode(false, 128);

-- 启用校准并允许更新
SELECT pg_catalog.gs_amm_set_dtree_calibration_mode(true, false);

-- 启用校准但冻结更新
SELECT pg_catalog.gs_amm_set_dtree_calibration_mode(true, true);

-- 清空所有易失校准状态，恢复 scale=1.0
SELECT pg_catalog.gs_amm_reset_dtree_calibration();
```

### 7.5 手工注入决策树反馈

```sql
SELECT pg_catalog.gs_amm_record_dtree_feedback(
  ARRAY[262144, 131072, 65536]::float8[],  -- raw cache/one-pass/multi-pass KB
  ARRAY[262144, 131072, 65536]::float8[],  -- calibrated cache/one-pass/multi-pass KB
  1,                                        -- model_version
  42,                                       -- leaf_id
  262144.0,                                 -- observed work_mem KB
  2500.0,                                   -- runtime ms
  64.0,                                     -- spill MB
  256.0,                                    -- grant MB
  0.01,                                     -- TP drop ratio
  20.0,                                     -- IO pressure
  false,                                    -- backpressure
  false                                     -- error
);
```

该接口只用于回归、模拟和故障注入。生产原生路径在 AP 结束时自动写入反馈。backpressure/error 样本不会直接用于正常的 runtime/spill 校准更新。

## 8. 保护机制的预期行为

### 8.1 TPS 防抖

pagewriter 维护约 30 秒的 TP 观测窗口。若最近 TPS 相对基线的跌幅接近或超过 `gs_amm_tp_jitter_limit`，默认 3%，控制器进入 TP 保护：停止继续 drain、禁止新增高风险 AP grant/扩容、暂停 shared buffer shrink，并进入恢复冷却期。

外部验收应使用 sysbench 5 秒 rolling TPS 计算最大跌幅、标准差、CV 和 P95/P99；不要把内核的 30 秒保护窗口与用户侧 5 秒验收窗口混为同一指标。

### 8.2 IO 和落盘保护

控制器持续关注 physical read rate、dirty 页/写回估计、AP spill MB rate、multipass 和 IO queue depth。IO guard 热态时会阻止继续借出 buffer granule，并可缩小后续 grant 或触发 backpressure。此时应优先分析工作集、磁盘延迟和 AP 并发，不应简单增大 AP dynamic target。

### 8.3 granule drain 的安全条件

granule 从 `BUFFER_ACTIVE` 迁移到 `FREE` 前必须清除有效 buffer table 映射、有效页、脏页、pinned 页和正在进行的 IO。`BUFFER_DRAINING`、`FREE`、`AP_RESERVED`、`AP_ACTIVE` granule 不会被 buffer lookup、clock sweep、buffer ring 或 pagewriter candidate 当作正常缓存页使用。

## 9. 故障处理与回退

| 现象 | 首先检查 | 回退动作 |
| --- | --- | --- |
| AP 全部被拒绝或反压 | `tp_guard_hot`、`io_guard_hot`、`last_backpressure_reason`、FREE granule、`gs_amm_ap_queue_limit` | 保持 TP/IO 保护，降低 AP 并发；必要时关闭 native auto mode |
| TPS 下跌或物理读升高 | `tp_raw_drop_ratio`、`physical_read_rate`、`buffer_active_granules`、`drain_rollback_count` | 增大 `shared_buffers_min_mb`，停止 shrink，降低 dynamic target |
| AP spill 很多 | `native_last_event_*bounds*`、`ap_spill_mb_rate`、hash/sort 指标、grant shrink | 先检查模型低估和 AP 并发；不要绕过 IO guard 强行增大 grant |
| granule drain 长期 pending | dirty/pinned/IO/hash pending 字段和 pagewriter 状态 | 降低 resize 速率或停止借用，排查长事务、长 IO 和脏页积压 |
| 校准劣化 TP 或 AP | scale、`dtree_rollback_count`、`dtree_frozen_leaf_count` | `record_only=on` 冻结，或用 `gs_amm_reset_dtree_calibration()` 清空 |
| 需要立即停止自动影响 | `native_auto_mode` | 设为 `off` 并 reload；现有查询按其生命周期结束后释放 grant |

不要在有活动 AP grant 时调用 `gs_amm_reset_state()`。不要通过 legacy API 在 native auto mode 运行期间手工调整 controller 或 shared buffer，以免两个控制面互相干扰。

## 10. 相关源码

- 内核控制器：`src/gausskernel/storage/buffer/gs_amm.cpp`
- 原生执行器生命周期与特征提取：`src/gausskernel/storage/buffer/gs_amm_query.cpp`
- granule MemoryContext：`src/common/backend/utils/mmgr/ammgranule.cpp`
- 决策树模型接口：`src/include/utils/workmem_dtree_model.h`


有关内核改动范围、文件矩阵和已知限制，请同时阅读 `docs/AMM_KERNEL_CHANGES.zh.md`。
