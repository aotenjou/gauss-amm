# openGauss 内核 AMM 改造说明

## 1. 文档目的与比较基线

本文用于向甲方工程师说明本交付源码中，相对于原始 openGauss 基线所做的全部 AMM（Adaptive Memory Management）相关内核改造，以及当前实现边界、运维接口和验收方式。

**比较基线**为本工作树 Git 基线提交 `ef29ebc311f905471cbac77382d7b0cd5fe9a3d3`（提交说明：`docs: design native AMM decoupling`）。该基线是本项目开始改造时使用的 openGauss 源码快照，**不是经过单独核验的上游正式发行标签**。因此，本文中的“原始 openGauss”均指该基线中的原有行为。

交付内核源码位于本包的 `openGauss-hard-tps-guard` 目录。

本文覆盖以下内容：

- shared buffer 与 AP 动态内存之间的 granule 化在线调度；
- AP 准入、决策树预测、叶子在线校准与反馈；
- TP TPS 和 IO 压力保护；
- sort/hash 对真实 AMM 内存池的接入；
- 内核生命周期接入、状态接口、GUC 与兼容模式；

本文不把实验编排脚本、测试数据或构建产物当作内核能力的一部分。

## 2. 改造前后总体差异

| 维度 | 原始基线 | 本次改造后 |
| --- | --- | --- |
| 内存控制对象 | `shared_buffers` 与执行器内存相互独立，执行器主要通过 `work_mem` 限制 | 使用统一的 AMM granule pool，在共享缓冲区与 AP grant 之间在线转换 |
| 调节粒度 | 无 shared buffer 到 AP 的可复用物理内存粒度 | 默认 64 MB granule，有界 drain、回收与扩容 |
| AP 内存 | 常规内存上下文分配，`work_mem` 主要是额度控制 | sort/hash 可使用 `AmmGranuleContext`，从 AP 拥有的共享缓冲区 granule 直接分配 |
| 预测来源 | 无内核内置 work_mem 决策树与叶子信息 | 内核静态决策树输出三档预测、模型版本和叶子 ID |
| 预测反馈 | 无按模型叶子的内核在线反馈 | 共享内存环形样本队列和叶子校准表，支持 record-only、校准、冻结和回滚 |
| 无预测调度 | 无面向 AP 完成结果的渐进式内存控制 | `feedback_only` 可跳过特征提取和决策树，以真实 AP 完成、spill、TP/IO 结果渐进调整 grant 与并发槽位 |
| AP 调度 | 无 TP/IO 关联的 AP 准入队列 | grant、backpressure 队列、AP 降级、闲置 AP granule 回收 |
| TP 保护 | 缺少以 TPS 窗口为中心的 AMM 硬保护 | 30 秒 TP 窗口、约 3% TPS 跌幅保护、IO/脏页/溢写保护、冷却与恢复期 |
| 控制位置 | 外部实验工具可直接驱动 AMM 兼容接口 | 默认由执行器、pagewriter、事务回调和内核遥测闭环驱动；遗留 SQL 控制接口默认禁用 |
| 可观测性 | 无 AMM granule/预测/准入闭环指标 | `gs_amm_status()` 与决策树调试接口输出 granule、drain、grant、TPS/IO、spill、校准和原生生命周期状态 |

## 3. 总体架构与运行闭环

### 3.1 内核内原生数据流

开启 `gs_amm_native_auto_mode` 后，普通 SQL 的路径不需要实验驱动程序再发送“预测、grant、反馈、状态迁移”等 SQL。内核执行以下闭环：

1. `standard_ExecutorStart` 进入执行器前调用 AMM 生命周期入口。
2. AMM 过滤不适用语句，仅识别成本达到阈值且包含 sort、hash、aggregate 或 window 等内存密集型节点的候选 AP 语句。
3. 常规 raw/calibrated dtree 路径遍历计划树、关联对象和运行时状态，构造 19 维决策树特征；`feedback_only` 路径在此处直接跳过。
4. 常规路径通过内部 C 接口执行决策树推理，得到 cache、one-pass、multi-pass 三档原始预测、模型版本和叶子 ID，并可叠加叶子校准倍率；`feedback_only` 直接使用反馈控制器当前的保守 grant。
5. 控制器根据可用 granule、TP/IO 保护态和排队状态决定准入、grant、降级或反压。
6. 获准的 AP 查询在 sort/hash 上使用 grant 对应的 `AmmGranuleContext`；实际分配来自已从 shared buffer drain 出来的 AP granule。
7. `standard_ExecutorEnd`、事务提交、事务回滚、子事务中止和会话错误清理路径释放 grant；同时记录运行时、spill、TP 跌幅和 IO 压力反馈。
8. pagewriter 主循环低频调用 AMM controller，处理 drain、shared buffer 恢复、闲置 AP 回收、TPS/IO guard 和决策树校准。

### 3.2 关键设计原则

- **TP 优先**：TPS 或 IO 保护触发后，停止继续从 shared buffer 借内存，不再扩大 AP grant，并进入恢复/冷却逻辑。
- **真实物理复用**：AP 内存不是仅修改 `work_mem` 数值，而是从已失活的 shared buffer 物理页中分配。
- **先安全 drain 后复用**：granule 转为 AP 前必须没有有效页面、脏页、固定页、进行中 IO 或 buffer hash 映射。
- **控制面与数据面分离**：控制器维护 granule 状态、grant 和保护态；MemoryContext 仅在已授权的 granule 内分配。
- **失效优先于误用**：非 `BUFFER_ACTIVE` granule 不参与 buffer victim 选择、buffer ring、pagewriter candidate 或 buffer table 命中。
- **默认保守**：原生自动模式、遗留控制模式和在线校准默认关闭，校准 record-only 默认开启。

## 4. Shared Buffer Granule Pool 改造

### 4.1 新增 granule 元数据与共享内存状态

新增核心头文件 `src/include/storage/gs_amm.h` 和控制器实现 `src/gausskernel/storage/buffer/gs_amm.cpp`。AMM 在共享内存中维护全局控制状态、锁、grant 信息、队列、遥测窗口、决策树反馈结构和 granule 表。

每个 granule 记录：

- 状态、generation、grant ID；
- 所属 shared buffer 的首 buffer ID 和 buffer 数；
- drain 过程中的 dirty、pinned、IO、hash entry 计数；
- AP reserved granule 数、活动 grant 字节数、已用字节数和分配游标。

granule 状态机为：

| 状态 | 含义 |
| --- | --- |
| `BUFFER_ACTIVE` | 参与正常 shared buffer 缓存、查找、victim 选择和 pagewriter 维护 |
| `BUFFER_DRAINING` | 已停止接受新的 buffer 分配，正在驱逐/刷出有效页面 |
| `FREE` | 已完成 drain，可分配给 AP，不能作为 buffer cache 使用 |
| `AP_RESERVED` | 已为 AP grant 预留，但尚未进入实际分配使用 |
| `AP_ACTIVE` | 正被 AP 专用 MemoryContext 使用 |

`InitBufferPool()` 完成 shared buffer 初始化后，会初始化 AMM 共享内存与 granule 表。正常 shared buffer 覆盖的 granule 初始标记为 `BUFFER_ACTIVE`。默认 granule 大小为 64 MB；尾部不足一个 granule 的 buffer 也作为一个受管理 granule 处理。

`generation` 在 granule 重新分配给不同用途时递增，grant 释放时同时校验 generation。该机制防止旧 grant 或已经失效的引用跨代使用。

### 4.2 Shared Buffer 分配和查找路径的安全隔离

对原始 buffer manager 的以下路径增加了 granule 状态判断：

- `freelist.cpp`：clock sweep 仅从 `BUFFER_ACTIVE` granule 选择 victim；遇到非 active granule 时跳过。
- `freelist.cpp`：buffer ring 命中非 active granule 时使该 ring 槽位失效，避免旧 ring 引用继续使用已移交给 AP 的 buffer。
- `bufmgr.cpp`：新页面插入 buffer table 前、buffer recycle、后台刷写、checkpoint、同步、失效和其他批量扫描路径均跳过不可维护的 granule。
- `buf_table.cpp`：`BufTableLookup()` 命中后校验目标 buffer 所属 granule；若处于 `BUFFER_DRAINING`、`FREE`、`AP_RESERVED` 或 `AP_ACTIVE`，则拒绝返回该映射。
- `pagewriter.cpp`：candidate list 和 candidate map 发现非 active granule 时清除 candidate 标识并跳过，不会继续刷写或重新选中该 buffer。

该隔离是物理内存复用的安全前提：一旦 shared buffer 的页框交给 AP，任何缓存查找或后台维护都不能再将其解释为有效数据库页。

### 4.3 在线 shrink：drain 协议

控制器需要从 shared buffer 借 granule 时，按以下流程执行：

1. 选取可借出的尾部/候选 granule，并将状态从 `BUFFER_ACTIVE` 切换为 `BUFFER_DRAINING`。
2. 从此时起，buffer 分配路径不能再将该 granule 作为 victim 或新缓存页使用。
3. 扫描 granule 中的 buffer descriptor，统计 dirty、pinned、IO in progress 和 hash entry。
4. 对 clean、unpinned、无 IO 的有效 buffer，在 buffer header lock 和对应 buffer mapping partition lock 保护下删除 buffer table 映射，清空 tag 和有效位。
5. 对 dirty buffer，要求 pagewriter 优先刷脏；对 pinned 或 IO 中 buffer 保留 pending 计数，后续 controller tick 重试。
6. 只有当所有 buffer 均不再有效、无脏页、无固定页、无进行中 IO、无 hash entry 时，granule 才转为 `FREE`。
7. 完成 drain 后通过 `madvise(MADV_DONTNEED)` 释放可回收的物理页，随后可供 AP grant 使用。

若 drain 期间触发 TPS guard、IO guard、dirty backlog 或其他 resize guard，控制器停止继续 shrink；无法安全完成的 draining granule 回滚为 `BUFFER_ACTIVE`。状态输出同时记录 drain 成功、失败、回滚、优先刷脏和 pending 原因。

### 4.4 在线 expand 与 AP 回收

shared buffer 需要恢复容量时，控制器优先将 `FREE` granule 切回 `BUFFER_ACTIVE`。若 free granule 不足，则先回收闲置 AP grant，或等待 AP 查询自然结束后的 graceful release。

从 AP 归还到 buffer 的 granule 会清理 AP 分配元数据、递增 generation、复位分配游标，再标记为 `BUFFER_ACTIVE`。buffer descriptor 不预填 page tag，后续由正常 buffer manager 路径重新使用。这个顺序保证扩容后 clock sweep 可立即选择新 granule，同时不会读取 AP 遗留内容。

## 5. AP 动态内存池与执行器接入

### 5.1 新增 `AmmGranuleContext`

新增：

- `src/common/backend/utils/mmgr/ammgranule.cpp`
- `src/include/utils/ammgranule.h`

并扩展 `memnodes.h`、`nodes.h`、`nodes.cpp`、`memutils.h` 和 mmgr `Makefile`，使其成为 openGauss 可识别的 MemoryContext 类型。

`AmmGranuleContext` 支持 alloc、free、realloc、reset、delete、empty、stats 和 memory checking 钩子。小块采用 16 级 freelist 管理；分配、重分配和释放均计入当前 grant 的使用量。调用 reset 或 delete 后，归还该 context 持有的 grant granule。

最重要的行为差异是：当 AP grant 的可用 granule 或 grant 上限耗尽时，该 context 不会退回普通 heap 绕过 AMM 限制。调用方进入现有执行器的内存不足/落盘路径，保持 AMM 对实际 AP 内存的约束力。

### 5.2 真实物理内存来源

`GsAmmGrantAllocMemory()` 根据 grant ID 在 AP-owned granule 中计算分配位置，返回已被 drain 的 shared buffer `BufferBlocks` 中的地址。AP MemoryContext 使用的是这部分已从 shared buffer 逻辑上移除的物理页。

因此本实现并非“使用预测值调整 `work_mem`”的软限制，而是同时具备：

- grant KB 对执行器有效额度的限制；
- grant 对可用 granule 容量的硬限制；
- shared buffer 和 AP allocator 对同一物理内存的互斥所有权；
- generation 和 owner 校验，防止跨 grant 访问。

### 5.3 sort/hash 首批接入范围

已改造的执行器算子为：

- `src/common/backend/utils/sort/tuplesort.cpp`：有 AMM grant 时创建 `AmmGranuleContext` 作为 sort 主上下文和 tuple 上下文；按当前有效 grant 动态收紧可用内存，容量不足时触发既有 sort spill 逻辑。
- `src/gausskernel/runtime/executor/nodeHash.cpp`：有 AMM grant 时创建 hash 及 batch 专用 `AmmGranuleContext`；按有效 grant 控制 hash 的 `spaceAllowed`，不足时进入分批/落盘处理。

运行中的 grant 被降级时，不强制搬迁已分配 chunk；新的内存申请受更低的 effective grant 约束，sort/hash 通过原有 spill 或自然释放回落。

**当前范围限制**：首期只接入 sort/hash。aggregate、vectorized hash/sort、window 及其他执行器算子尚未全部接入 AMM 专用 allocator，不能宣称全执行器算子已池化。

## 6. 决策树预测、准入与在线反馈

### 6.1 内核化决策树

新增：

- `src/common/backend/utils/mmgr/workmem_dtree.cpp`
- `src/common/backend/utils/mmgr/workmem_dtree_model.cpp`
- `src/include/utils/workmem_dtree_model.h`
- `contrib/workmem_dtree/`

模型为静态生成的 C/C++ 推理代码，特征 schema 为 `workmem-3bound-features-v1`，共 19 维。预测结果包含三档内存边界：

- `cache`：尽量避免 spill 的预测；
- `one-pass`：允许一轮落盘的预测；
- `multi-pass`：允许多轮处理的预测。

除三档原始预测外，内部预测接口还返回 `model_version` 和 `leaf_id`。SQL 调试接口保留兼容的预测数组输出，并新增 detail 输出，用于核验原始值、校准值、模型版本、叶子 ID、校准版本和校准倍率。

### 6.2 内核特征提取与候选 AP 识别

新增 `src/gausskernel/storage/buffer/gs_amm_query.cpp`。该模块在执行器启动时遍历计划树、关联关系和会话运行时信息，构造如下类别的 19 维特征：

- hash join、sort、aggregate、window 节点数量；
- 最大计划行数对数、最大行宽、总代价、并行 worker、parallel-aware 节点；
- 活跃会话数、当前会话私有内存、当前事务年龄；
- 系统总内存、可用内存、AMM 内存压力评分；
- 涉及表和索引的总大小、平均列宽、总代价 `log1p`。

仅当原生模式开启、查询为可处理的顶层路径、非 EXPLAIN-only、未持有既有 grant、计划成本达到 `gs_amm_native_ap_cost_threshold`，且存在内存密集型节点时才进入 AMM 准入。无法收集特征或 admission API 异常时，原生路径采用 fail-closed，避免无控制地放行预期应受 AMM 管理的 AP 查询。

### 6.3 AP 准入、grant 与 backpressure

预测经叶子校准后传入 admission。控制器将其映射到 cache/one-pass/multi-pass memory mode，并综合：

- FREE granule 数和可授予容量；
- 最小 AP grant；
- 当前 TP TPS 保护态；
- IO、dirty backlog、pending writeback、physical read、temp spill/multipass 等压力；
- drain 是否完成、resize 冷却与恢复冷却；
- AP 队列上限和队列等待超时。

资源充足时，AP 获得由若干 AP granule 组成的 grant。TP 或 IO 有压力时，控制器缩小后续 grant，计入 downgrade/effective grant；当继续放行可能影响 TP 或不存在可安全借用的 granule 时，新 AP 进入 backpressure 队列。队列满或超时不会无限制占用资源，状态中记录原因、次数、等待时间和最后一次反压原因。

### 6.4 无预测 feedback-only 调度

`gs_amm_feedback_only_mode` 是与 raw/calibrated dtree 并列的原生调度模式。它保留候选 AP 识别、granule pool、真实 sort/hash allocator、TP/IO guard 和 backpressure，但不会构造 19 维决策树特征，也不会调用 `GsWorkmemDtreePredictDetail()`。

控制器以 bootstrap grant 和初始 AP 槽位开始。每次 AP 正常完成后，记录 runtime、spill、峰值使用、TPS 和 IO 状态；连续稳定完成达到阈值后，grant 仅增加一个 granule，AP 槽位仅增加一个。出现错误、spill 超阈值、TPS guard 或 IO guard 时，控制器将 grant 和槽位有界回退，并把新 AP 留在 backpressure 队列中。这样不会把模型泛化误差带入 admission，而是用已经到达的 AP 结果逐步提高并发能力。

### 6.5 内核原生生命周期和异常清理

`execMain.cpp` 在执行器初始化前调用 `GsAmmExecutorStart()`，在释放执行器资源后调用 `GsAmmExecutorEnd()`。`gs_amm_query.cpp` 还注册事务和子事务回调；因此正常结束、提交、回滚、子事务异常和错误清理均可释放相应 grant，避免 grant 被遗留在共享状态。

每次 AP 结束时，内核写入以下反馈：模型版本、叶子 ID、原始与校准边界、实际 grant、观察到的 work_mem、执行时长、会话 spill、TP 跌幅、IO 压力以及错误状态。`xact.cpp` 补充事务提交/回滚采样，`pgstat.cpp` 和 `buffile.cpp` 补充 AP spill 字节数/次数，`bufmgr.cpp` 记录 shared buffer read miss。

### 6.6 叶子在线校准

AMM 共享内存维护：

- 固定 4096 条的反馈环形队列；满时覆盖旧样本并计数；重启后不持久化；
- 固定 1024 项的叶子校准表；
- 每叶子的样本数、校准版本、倍率、q-error EWMA、低估率、spill、运行时、异常更新数、回滚数和冻结状态。

校准不改变树结构或分裂条件，只将下次同一叶子的三档原始预测乘以 leaf scale。默认倍率为 1.0，硬边界为 0.5x 到 4.0x；最少积累 8 个有效样本才允许更新。低估、spill 和 multipass 对上调更敏感；长期无 spill 且 grant 明显高于实际使用时，才缓慢下调。

保护措施包括：

- `gs_amm_dtree_calibration_enabled` 全局开关；
- `gs_amm_dtree_record_only` shadow 模式；
- backpressure、error、timeout 样本不直接作为正常 runtime/spill 校准目标；
- TPS guard 或 IO guard 热态时冻结更新；
- 校准导致 TPS 风险时冻结对应叶子并回滚；
- reset 可恢复所有叶子到 scale 1.0；
- 状态输出可解释当前版本、倍率、更新、回滚和冻结。

## 7. TPS 防抖与 IO 保护策略

本实现不以原始 1 秒 `pg_stat_database` 计数直接驱动内存变更。控制器维护 TP 30 秒观测窗口，并将 TPS 基线、最近 TPS、原始跌幅、P95 延迟、样本数和基线重置次数写入 AMM 状态。

### 7.1 TPS 硬保护

`gs_amm_tp_jitter_limit` 默认值为 `0.03`，即约 3% 的 TPS 跌幅阈值。观测窗口成熟且达到保护条件后：

- 停止新的 shared buffer drain；
- 禁止扩大 AP grant；
- 阻断新的高风险 AP 准入或将其置入反压队列；
- 暂停 shared buffer shrink；
- 必要时回收 AP granule，优先恢复 shared buffer；
- 进入恢复冷却期，避免指标刚回升即反复 resize。

### 7.2 IO 与落盘保护

控制器将 physical read rate、AP spill MB rate、multipass 次数、IO queue depth、dirty/flush 状态等纳入 IO 压力评分。IO guard 热态下，不继续从 shared buffer 借 granule，也不降低对 TP 的保护要求；可缩小 AP grant、增加 backpressure 或回收 AP 闲置 granule。

### 7.3 颗粒度、冷却和滞回

所有 shared buffer 到 AP 的迁移按 granule 执行。默认 resize rate limit、resize batch 均为 64 MB，并受 observe window、resize cooldown 和 TP recovery cooldown 约束。控制器使用 deadband，避免因短时测量噪声在 shrink/expand 之间来回切换。

这一组合用于控制三类 TPS 抖动：

| 抖动来源 | 对应措施 |
| --- | --- |
| 单次缩容过大导致瞬时缓存失效、刷脏和页表压力 | 固定 granule 粒度、有界 batch、drain 后再复用 |
| 连续缩容导致 dirty/IO 积压叠加 | pagewriter 优先刷脏、pending 统计、IO guard、resize 冷却 |
| AP 过度扩张挤压 TP 工作集 | 30 秒 TPS guard、AP grant 收缩、backpressure、TP recovery |
| 长期 shared buffer 不足导致 physical read 上升 | physical read rate 观测、停止 shrink、恢复 buffer granule |

## 8. 配置、SQL 接口与兼容性

### 8.1 主要 GUC

GUC 定义位于 `src/common/backend/utils/misc/guc/guc_storage.cpp`，配置样例写入 `src/bin/gs_guc/cluster_guc.conf`。主要默认值如下：

| GUC | 默认值 | 作用 |
| --- | ---: | --- |
| `gs_amm_native_auto_mode` | `false` | 启用执行器内核原生 AMM 路径 |
| `gs_amm_native_ap_cost_threshold` | `10000` | 原生 AP 候选的计划成本阈值 |
| `gs_amm_legacy_control_enabled` | `false` | 允许旧 SQL 直接驱动 AMM 的兼容/调试开关 |
| `gs_amm_dtree_calibration_enabled` | `false` | 允许校准改变预测值 |
| `gs_amm_dtree_record_only` | `true` | 只记录反馈、不改变预测值 |
| `gs_amm_feedback_only_mode` | `false` | 启用无预测的完成反馈调度，并跳过决策树推理 |
| `gs_amm_feedback_bootstrap_grant_mb` | `64` | feedback-only 的初始 AP grant |
| `gs_amm_feedback_max_grant_mb` | `256` | feedback-only 的 grant 上限 |
| `gs_amm_feedback_initial_ap_slots` | `1` | feedback-only 的初始并发 AP 槽位 |
| `gs_amm_feedback_max_ap_slots` | `4` | feedback-only 的 AP 槽位上限 |
| `gs_amm_feedback_stable_windows` | `2` | 连续稳定完成多少次后允许渐进扩容 |
| `gs_amm_feedback_spill_threshold_mb` | `32` | 触发 feedback-only 回退的 spill 阈值 |
| `gs_amm_shared_buffers_min_mb` | `64` | shared buffer 最小保留量 |
| `gs_amm_resize_rate_limit_mb` | `64` | 单轮 resize 的速率上限 |
| `gs_amm_resize_batch_mb` | `64` | resize 批大小，启动期参数 |
| `gs_amm_dynamic_target_mb` | `512` | 动态 AP 内存目标 |
| `gs_amm_ap_min_grant_mb` | `4` | 单 AP 最小 grant |
| `gs_amm_ap_queue_limit` | `16` | AP 反压队列长度 |
| `gs_amm_ap_queue_timeout_ms` | `5000` | 队列等待上限 |
| `gs_amm_tp_jitter_limit` | `0.03` | TP TPS 跌幅保护阈值 |
| `gs_amm_resize_cooldown_ms` | `8000` | resize 冷却时间 |
| `gs_amm_tp_recovery_cooldown_ms` | `8000` | TP 恢复冷却时间 |
| `gs_amm_allocator_only_mode` | `false` | 仅验证 allocator/grant 的受控模式 |
| `gs_amm_allocator_only_grant_mb` | `128` | allocator-only 模式的 grant 大小 |

生产启用建议先保持 `record_only=true`，在 TP+AP 混合压测验证通过后，再显式开启校准。若模型泛化不足，可先使用 feedback-only 验证 granule pool 和保护策略，再比较 raw/calibrated dtree。所有阈值应基于目标机器的内存、IO、TP 基线和 AP 负载重新标定，不建议直接沿用默认值。

共享 AMM runtime config 只由 postmaster 在配置加载时更新。普通 backend 初始化同一组 GUC 时不会覆盖已经学习到的 feedback grant、槽位和计数，避免每个新连接重置控制器。为了获得公平的 A/B 结果，应重启专用测试实例，或在允许 legacy 调试接口时显式调用 `gs_amm_reset_state()`；该 reset 会清空 feedback 控制器状态，但保留决策树叶子校准状态。

### 8.2 SQL 函数和状态接口

函数注册位于 `src/common/backend/catalog/builtin_funcs.ini`，声明位于 `src/include/utils/builtins.h`。

| 接口类别 | 接口 | 说明 |
| --- | --- | --- |
| 状态 | `gs_amm_status()` | 输出控制器、TPS/IO、granule、drain、grant、队列、spill、决策树校准和原生生命周期状态 |
| 预测调试 | `gs_workmem_dtree_predict(float8[])` | 保持兼容的三档预测输出 |
| 预测调试 | `gs_workmem_dtree_predict_detail(float8[])` | 输出 raw/calibrated 三档值、model version、leaf ID、calibration version、scale |
| 遗留反馈 | `gs_amm_record_dtree_feedback(...)` | 兼容/调试用的外部反馈注入 |
| 遗留校准控制 | `gs_amm_set_dtree_calibration_mode(...)`、`gs_amm_reset_dtree_calibration()` | 兼容/调试用校准控制和 reset |

遗留的 SQL mutator 并未删除，但调用前会经过 `gs_amm_require_legacy_control()` 检查。默认 `gs_amm_legacy_control_enabled=off`，因此正常原生运行不会允许外部脚本直接驱动控制状态。`gs_amm_status()` 作为只读观测接口可在不打开 legacy control 的情况下使用。

## 9. 可观测性与验收指标

`gs_amm_status()` 至少提供以下类别的信息：

| 类别 | 代表性指标 |
| --- | --- |
| TP | 基线 TPS、最近 TPS、原始跌幅、P95 延迟、窗口样本数、TPS guard 状态 |
| IO | physical read rate、spill MB rate、multipass 次数、IO queue depth、IO guard 状态 |
| Granule | granule MB/blocks、总数、buffer active/draining/free、AP reserved/active 数 |
| Drain | pending dirty/pinned/IO/hash、drain success/fail/rollback、priority flush、reclaimed MB |
| AP | grant bytes、granule 已用字节、分配成功/失败原因、grant shrink、grant debt、effective grant |
| 调度 | queue wait、backpressure count、最后反压原因、新 AP guard block、恢复动作 |
| 决策树 | model/leaf/calibration version、raw/calibrated bounds、scale、反馈样本、丢弃样本、更新、回滚、冻结叶子 |
| Feedback-only | 当前 grant、AP 槽位、完成/增长/回退/槽位阻塞计数、runtime/spill/peak EWMA、最后动作 |
| 原生路径 | native mode、候选数、准入数、释放数、失败数、当前 grant、最近事件原因 |

甲方验收应以业务负载压测结果为准，而不是只检查接口返回。建议固定数据集、并发和机器配置，至少比较以下三组：

1. 原始 direct 预测或固定 `work_mem` 路径；
2. raw dtree 路径，即校准关闭或 record-only；
3. calibrated dtree 路径，即确认安全后开启在线校准。
4. feedback-only 路径，即关闭决策树预测，仅使用已完成 AP 的反馈调度。

每组应运行足够长的稳定期和扰动期，并至少报告：

- **TPS jitter**：用户侧以 sysbench 5 秒 rolling TPS 计算标准差、CV、P95/P99 下跌以及相对基线最大跌幅；内核侧以 30 秒窗口验证 guard 是否按预期触发。
- **AP 执行时间**：AP 的平均值、P95、P99、成功率和被反压/超时数量。
- **AP spill-to-disk**：spill bytes、spill files、temp read/write、hash batch/multipass、sort disk 使用量。
- **内存池动作**：buffer active/free/AP active granule、drain 成功率、drain rollback、grant 拒绝/降级、backpressure、AP 回收、shared buffer 恢复。
- **TP/IO 安全性**：TP 最大 5 秒 rolling TPS 跌幅不应突破约 3% 的目标；发生保护后不应继续 shrink 或扩大 AP grant；physical read 和 dirty/IO backlog 恶化时应停止借用或回退。

## 10. 文件级改动矩阵

### 10.1 新增的 AMM 核心源文件

| 文件 | 改动说明 |
| --- | --- |
| `src/include/storage/gs_amm.h` | AMM 共享状态、granule 状态机、grant、预测、反馈、控制器公开接口 |
| `src/gausskernel/storage/buffer/gs_amm.cpp` | 共享内存控制器、granule drain/expand、grant、队列、guard、状态输出、校准实现 |
| `src/gausskernel/storage/buffer/gs_amm_query.cpp` | 执行器生命周期、内核特征提取、原生准入、事务/子事务清理、执行结束反馈 |
| `src/include/utils/ammgranule.h` | AP grant 专用 MemoryContext 接口和统计结构 |
| `src/common/backend/utils/mmgr/ammgranule.cpp` | `AmmGranuleContext` 分配器实现 |
| `src/include/utils/workmem_dtree_model.h` | 19 维特征 schema、预测结果和内部推理接口 |
| `src/common/backend/utils/mmgr/workmem_dtree.cpp` | SQL 调试 UDF 和内部预测 detail 包装 |
| `src/common/backend/utils/mmgr/workmem_dtree_model.cpp` | 自动生成的决策树静态推理代码 |
| `contrib/workmem_dtree/` | 决策树扩展的 SQL/构建支撑文件 |

### 10.2 修改的 buffer、pagewriter 与存储路径

| 文件 | 改动说明 |
| --- | --- |
| `src/gausskernel/storage/buffer/Makefile` | 编译 `gs_amm.cpp` 和 `gs_amm_query.cpp` |
| `src/gausskernel/storage/buffer/buf_init.cpp` | buffer pool 初始化后初始化 AMM shared memory/granule 表 |
| `src/include/storage/buf/buf_internals.h` | 暴露 active buffer count 的读取/设置接口，供 AMM 协调 buffer 策略活动范围 |
| `src/gausskernel/storage/buffer/freelist.cpp` | clock sweep 和 buffer ring 避开非 active granule |
| `src/gausskernel/storage/buffer/buf_table.cpp` | buffer table 命中校验 granule active 状态 |
| `src/gausskernel/storage/buffer/bufmgr.cpp` | buffer 分配、回收、刷写、同步、失效和读 miss 统计接入 granule 安全检查/遥测 |
| `src/gausskernel/process/postmaster/pagewriter.cpp` | pagewriter 主循环驱动 AMM controller；候选页路径排除非 active granule |
| `src/gausskernel/storage/file/buffile.cpp` | temp file/spill 指标接入 AMM 遥测 |
| `src/gausskernel/process/postmaster/pgstat.cpp` | AP spill 次数和字节数统计 |
| `src/gausskernel/storage/access/transam/xact.cpp` | 事务提交与回滚的 AMM 采样 |
| `src/include/pgstat.h` | spill 遥测接口声明 |

### 10.3 修改的执行器和内存管理路径

| 文件 | 改动说明 |
| --- | --- |
| `src/gausskernel/runtime/executor/execMain.cpp` | ExecutorStart/ExecutorEnd 接入 AMM 原生生命周期 |
| `src/common/backend/utils/sort/tuplesort.cpp` | sort 在有 grant 时使用 `AmmGranuleContext` 并按 effective grant 控制内存/落盘 |
| `src/gausskernel/runtime/executor/nodeHash.cpp` | hash 在有 grant 时使用 `AmmGranuleContext` 并按 effective grant 控制 batch/spill |
| `src/common/backend/utils/mmgr/Makefile` | 编译 AMM allocator 和决策树模块 |
| `src/include/nodes/memnodes.h` | 新增 AMM MemoryContext 结构/方法定义 |
| `src/include/nodes/nodes.h` | 声明新的 MemoryContext node 类型 |
| `src/common/backend/nodes/nodes.cpp` | 注册/识别 AMM MemoryContext node 类型 |
| `src/include/utils/memutils.h` | 暴露 AMM allocator 相关接口 |

### 10.4 修改的配置、函数注册和构建路径

| 文件 | 改动说明 |
| --- | --- |
| `src/common/backend/utils/misc/guc/guc_storage.cpp` | 注册 AMM、guard、granule、队列、校准和 native mode GUC |
| `src/bin/gs_guc/cluster_guc.conf` | 增加 AMM 配置项样例 |
| `src/common/backend/catalog/builtin_funcs.ini` | 注册 AMM 状态、预测、校准和兼容控制函数 |
| `src/common/backend/catalog/Makefile` | 增加 `builtin_funcs.ini` 到内置函数对象的构建依赖，确保新增函数注册可触发重建 |
| `src/include/utils/builtins.h` | AMM/决策树 SQL 函数声明 |
| `contrib/Makefile` | 构建决策树 contrib 组件 |

## 11. 当前实现限制与上线风险

### 11.1 功能边界

1. AP 专用 allocator 首期只覆盖 sort/hash，不代表所有执行器算子都已经从 granule pool 分配。
2. 决策树模型版本当前为内置静态生成模型；模型热更新、在线重训练、跨重启保留校准状态不在本期范围。
3. 校准表和反馈环形队列均为共享内存易失数据，实例重启后恢复默认状态。
4. shared buffer granule 管理覆盖 normal shared buffer 相关路径；不同存储介质、NVM、DMS/分布式环境及全部边缘 buffer 路径需要在目标部署形态下继续专项验证。
5. native AP 候选由计划成本与内存密集算子识别，未接入完整 SQL 指纹级别的业务优先级、租户配额或复杂队列调度策略。
6. `gs_amm_status()` 当前为文本状态输出，适合联调和压测；生产监控平台若需要结构化时序数据，建议后续增加 system view 或 metrics exporter。

### 11.2 性能与稳定性风险

1. shared buffer shrink 直接影响缓存工作集。即使采用 granule、drain 和 guard，也必须按真实 TP 工作集标定 `shared_buffers_min_mb`，不能把最终保留容量压低到活跃工作集以下。
2. 过小 granule 会提升状态维护和 drain 次数，过大 granule 会增大一次迁移影响；默认 64 MB 是初始折中，需在目标机上测量。
3. 物理读率、dirty backlog、pagewriter 节奏、存储延迟和 sysbench 观察窗口会影响 guard 的触发准确性，部署前必须建立 TP-only 基线。
4. fail-closed 的原生准入策略可保护资源，但在特征收集或 admission 故障时会拒绝候选 AP，需配合告警和灰度开关。
5. 在线校准应先以 record-only 运行足够样本；直接启用会改变 grant，可能影响 AP 延迟、spill 和 TP 并发关系。

## 12. 推荐上线与验收流程

1. 从本交付源码构建独立实例，确认 `gs_amm_status()` 可用，且默认 native/legacy/calibration 开关均处于预期状态。
2. 先运行 TP-only 基线，确认没有 AMM 自动调节时的 TPS CV、P95/P99、physical read 和 pagewriter 基线。
3. 开启 `gs_amm_native_auto_mode`，保持 `gs_amm_dtree_calibration_enabled=off`、`gs_amm_dtree_record_only=on`，运行混合 TP+AP。验证原生 admission、grant release、spill 反馈、granule drain/expand、TPS/IO guard 和 backpressure。
4. 对 raw dtree、calibrated dtree 和原始 direct 路径执行同一大规模数据集、同一并发和同一持续时间的 A/B 对比。验收主指标依次为 TPS jitter、AP 执行时间、AP spill-to-disk，其次为内存池动作和保护触发解释性。
5. 仅当 TP 5 秒 rolling TPS 跌幅、TP CV、AP spill、AP P95 和 guard 回滚行为符合目标时，再小流量开启校准；发生异常时关闭 native auto mode 或保持 calibration 为 record-only，即可停止预测值对准入/授予的影响。

## 13. 交付核验信息

以下核心文件已在交付目录与内核工作树之间完成内容核验：

- `src/gausskernel/storage/buffer/gs_amm.cpp`
- `src/gausskernel/storage/buffer/gs_amm_query.cpp`
- `src/common/backend/utils/mmgr/ammgranule.cpp`
- `src/include/storage/gs_amm.h`

交付包保留的是可审阅源码而非构建结果。甲方应在目标 openGauss 编译环境完成全量构建、回归测试和上述混合压测后，再将该 AMM 机制纳入生产变更窗口。
