# openGauss AMM 使用手册

## 1. 目的、范围和版本

本文说明交付内核 `openGauss-hard-tps-guard` 中 AMM（Adaptive Memory Management）的启用方式、运行模式、配置参数、SQL 接口、状态观测、测试负载和验收方法。

AMM 将 normal shared buffer 按固定 granule 划分。当 AP 查询获得预测 grant 时，控制器在动态池预算内分配 AP granule。TP 压力达到 miss 阈值时，按 FREE granule、闲置 AP granule、运行中 AP 降级到 multipass 的顺序释放容量；TP 连续低压三个窗口后，每次最多向 shared-buffer 基线归还一个 granule。

本文覆盖的是当前交付源码实现，默认 granule 为 8 MB，AP 专用 allocator 首期只接入 sort/hash。它不代表所有执行器算子均已经使用 AMM allocator。

## 2. 使用前提

1. 使用本交付源码完成编译、安装和实例初始化。AMM 不是可以加载到未改造 openGauss 的独立 SQL 扩展。
2. 使用具有 superuser 权限的账号进行实例级配置和状态观测。
3. 保证 `shared_buffers` 启动时的容量足以覆盖目标的 shared buffer 上限。AMM 只能在该启动时分配的 buffer envelope 内转换 granule，不能在运行中增加物理 shared buffer 总上限。
4. 为 shared buffer 设定明确的最小保留值 `gs_amm_shared_buffers_min_mb`。该值必须高于 TP 活跃工作集的安全下界，否则即使控制器按 granule 缩容，也可能长期提高 physical read 并损伤 TPS。
5. 生产前必须完成 TP-only 和 TP+AP 混合压测。主要验收指标为 TPS jitter、AP 执行时间和 AP spill-to-disk，不应只根据模型离线误差决定是否上线。

## 3. AMM 的运行模式

AMM 只有一个运行开关：

```conf
gs_amm_enabled = on
```

开启后执行器不再按 native 模式或计划成本筛选查询。查询正常进入执行器；实际 AP 内存请求由 AMM grant、granule 和内部反压队列控制。队列满或超时只放弃 AMM grant，查询继续使用普通执行路径。

运行时不依赖手工控制 SQL；预测、准入、TP 压力采样和 granule 迁移都在内核路径中完成。

## 4. 配置方式与生效范围

除 `gs_amm_granule_size_mb` 外，AMM GUC 均为 SIGHUP 级参数；`gs_amm_workload_role` 是 session 级参数。`gs_amm_granule_size_mb` 决定共享内存 granule 表布局，修改后必须重启实例。

以下示例使用 `postgresql.conf`。集群环境应通过现有的 openGauss 配置管理流程向所有目标节点下发相同配置，再执行 reload 或重启。

```conf
# AMM 的保守起点
gs_amm_enabled = on
gs_amm_granule_size_mb = 8               # 修改本项后需重启
gs_amm_shared_buffers_min_mb = 1024
gs_amm_dynamic_target_mb = 512
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

### 4.1 运行时 AMM GUC

| 参数 | 默认值 | 生效 | 用途 |
| --- | ---: | --- | --- |
| `gs_amm_enabled` | `off` | reload | AMM controller、granule ownership 和自动执行器接入总开关 |
| `gs_amm_granule_size_mb` | `8` | 重启 | 固定 granule 大小及共享内存布局 |
| `gs_amm_shared_buffers_min_mb` | `64` | reload | shared buffer 允许缩减到的下限 |
| `gs_amm_dynamic_target_mb` | `512` | reload | 动态 AP 内存池目标 |
| `gs_amm_tp_buffer_miss_threshold_pct` | `5` | reload | TP shared-buffer miss 百分比阈值；达到后优先给 buffer 释放 granule |
| `gs_amm_workload_role` | `tp` | session | 当前 session 角色；AP 查询设置为 `ap` 才使用预测准入 |

`gs_amm_shared_buffers_min_mb` 和 `gs_amm_dynamic_target_mb` 必须按机器总内存、TP 工作集和 AP 并发一起标定。TP miss 由执行器自动累计，控制器每秒最多处理一个 granule；连续三个低压窗口后才缓慢回收到 shared-buffer 基线。

## 5. 执行器接入与默认放行

开启 AMM 后，`standard_ExecutorStart` 只对顶层 SELECT 尝试申请内部 AP grant；不再读取执行器自动准入开关或计划成本门槛。查询本身始终按普通执行路径继续，AMM 只负责可选的内存授予；资源不足时立即返回反压，由应用端决定排队、重试和超时策略。特征无法采集、预测失败或保护触发都会放弃本次 grant，不会阻挡查询。

内核会构造 19 维特征：算子数量、计划行数/行宽/总代价、并行信息、活跃会话、会话私有内存、事务年龄、系统内存、AMM 内存压力、涉及表/索引大小、平均列宽和总代价对数。决策树输出三档内存边界：

- `cache`：优先避免 spill 的目标；
- `one-pass`：允许一轮落盘的目标；
- `multi-pass`：允许多轮处理的下界。

随后 grant 仍由 AMM 内部队列、动态目标和可用 granule 控制。TP 高压路径的顺序是 FREE granule、闲置 AP granule、运行中 AP 降级到 multipass；低压路径以 `gs_amm_shared_buffers_min_mb` 为基线缓慢回收。

在 AP 完成、事务提交、事务回滚、子事务异常或执行器错误时，原生路径会释放 grant。正常使用时不需要应用程序调用 `gs_amm_begin_ap()` 或 `gs_amm_end_ap()`。

## 6. 状态观测和日常检查

### 6.1 主状态接口

```sql
SELECT pg_catalog.gs_amm_status();
```

该函数返回空格分隔的 `key=value` 文本。压测程序可解析为键值对；人工排查时重点关注：

| 类别 | 关键字段 | 正常关注点 |
| --- | --- | --- |
| 原生路径 | `active_ap_count`、`ap_registry_count`、`ap_queue_len` | AP session 是否按生命周期注册和释放 |
| granule | `free_granules`、`ap_active_granules`、`active_mb`、`dynamic_used_mb` | AP 使用期间可看到 AP granule；释放后应回到 FREE 或 buffer active |
| AP | `ap_granted_bytes_total`、`ap_used_bytes_total`、`ap_reclaimable_bytes_total`、`ap_downgrade_pending` | grant 是否真实使用、是否有待安全点执行的降级 |
| TP | `tp_pressure_pct`、`tp_pressure_hot`、`tp_recovery_requested_granules`、`tp_recovered_granules`、`tp_recovery_deferred_count` | 高压时恢复动作应增长；没有可安全回收容量时 deferred 增长 |
| 恢复 | `last_action`、`tp_low_pressure_windows` | 高压动作依次为 FREE、idle AP、downgrade；低压窗口达到 3 后执行 baseline drain |
| 决策树 | `last_prediction_mb` | 观察最近一次预测规模和全局 AP 动态目标 |

### 6.2 决策树调试接口

```sql
-- 兼容接口：返回三档预测数组
SELECT pg_catalog.gs_workmem_dtree_predict(
  ARRAY[1, 1, 0, 0, 6.0, 64, 20000, 0, 0, 8, 64, 2, 65536, 32768, 0.2, 10240, 2048, 32, 9.9]::float8[]
);

-- detail 接口：返回三档预测、模型版本和叶子信息
SELECT * FROM pg_catalog.gs_workmem_dtree_predict_detail(
  ARRAY[1, 1, 0, 0, 6.0, 64, 20000, 0, 0, 8, 64, 2, 65536, 32768, 0.2, 10240, 2048, 32, 9.9]::float8[]
);
```

数组必须严格按以下顺序传入：

1. hash join 节点数；2. sort 节点数；3. aggregate 节点数；4. window 节点数；5. 最大计划行数的 `log10`；6. 最大计划行宽；7. 计划总代价；8. 并行 worker 数；9. parallel-aware 节点数；10. 活跃会话数；11. 会话私有内存 MB；12. 事务年龄秒；13. 系统总内存 MB；14. 系统可用内存 MB；15. AMM 内存压力评分；16. 涉及表总大小 MB；17. 涉及索引总大小 MB；18. 平均列宽；19. `log1p(计划总代价)`。

该接口用于验证模型输出；原生执行路径自行收集特征，不需要客户端构造数组。

## 7. SQL 接口边界

生产控制器没有外部压力注入、手工 resize、reset 或反馈接口。以下接口仅用于观察预测和验证 AP grant 生命周期：

```sql
SELECT pg_catalog.gs_amm_status();
```

### 7.1 手工 AP grant 生命周期

```sql
-- 单预测值接口：预测 MB、最小 MB、最大 MB、兼容参数（忽略；队列无限等待）
SELECT pg_catalog.gs_amm_begin_ap(256, 4, 512, 5000);

-- 三档边界接口：cache KB、one-pass KB、multi-pass KB、兼容参数（忽略；队列无限等待）
SELECT pg_catalog.gs_amm_begin_ap_bounds(262144, 131072, 65536, 5000);

-- 同一 backend 中释放当前 AP grant
SELECT pg_catalog.gs_amm_end_ap();
```

返回文本中的 `admitted`、`queued`、`backpressure`、`granted_mb`、`memory_mode` 和 `reason` 是判断结果的首选依据。生产执行器会自动调用同一内部路径；SQL 生命周期接口只适合回归验证，grant 必须在同一 backend 中释放。

## 8. 保护机制的预期行为

### 8.1 TP miss 压力

执行器在 TP session 结束时累计 shared-buffer hit/read。pagewriter 每秒形成一个窗口，按 `read / (hit + read)` 计算 miss 百分比。达到 `gs_amm_tp_buffer_miss_threshold_pct` 后，控制器先使用 FREE granule，再回收闲置 AP granule，最后请求运行中 AP 在 sort/hash 安全点降级到 multipass。

### 8.2 低压恢复

当 miss 百分比低于阈值一半时累计低压窗口；连续三个窗口后，每次只向 `gs_amm_shared_buffers_min_mb` 回收一个 granule，避免 TP 流量下降后瞬间改变 shared-buffer 容量。

### 8.3 granule drain 的安全条件

granule 从 `BUFFER_ACTIVE` 迁移到 `FREE` 前必须清除有效 buffer table 映射、有效页、脏页、pinned 页和正在进行的 IO。`BUFFER_DRAINING`、`FREE`、`AP_RESERVED`、`AP_ACTIVE` granule 不会被 buffer lookup、clock sweep、buffer ring 或 pagewriter candidate 当作正常缓存页使用。

## 9. 故障处理与回退

| 现象 | 首先检查 | 回退动作 |
| --- | --- | --- |
| AP 全部被拒绝或反压 | `tp_pressure_hot`、`free_granules`、`ap_queue_len` | 降低 AP 并发；查询仍可继续执行 |
| TP miss 升高 | `tp_pressure_pct`、`tp_recovery_deferred_count`、`last_action` | 提高 `gs_amm_shared_buffers_min_mb` 或降低 AP 并发 |
| AP 被降级 | `ap_downgrade_pending`、`ap_reclaimable_bytes_total` | 等待 sort/hash 安全点完成释放 |
| dynamic 池长期偏高 | `dynamic_used_mb`、`tp_low_pressure_windows`、`last_action` | 检查 TP session 是否持续产生低压窗口，并确认 shared-buffer 基线配置 |
| 需要立即停止 AMM 影响 | `gs_amm_enabled` | 设为 `off` 并 reload；现有查询按其生命周期结束后释放 grant |

不要依赖已删除的手工 reset、controller step、resize、TP/IO 注入或校准接口。生产路径由执行器、pagewriter 和 AMM 内部队列协同控制。

## 10. 相关源码

- 内核控制器：`src/gausskernel/storage/buffer/gs_amm.cpp`
- 原生执行器生命周期与特征提取：`src/gausskernel/storage/buffer/gs_amm_query.cpp`
- granule MemoryContext：`src/common/backend/utils/mmgr/ammgranule.cpp`
- 决策树模型接口：`src/include/utils/workmem_dtree_model.h`


有关内核改动范围、文件矩阵和已知限制，请同时阅读 `docs/AMM_KERNEL_CHANGES.zh.md`。
