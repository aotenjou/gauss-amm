/* -------------------------------------------------------------------------
 *
 * gs_amm.h
 *      OpenGauss AMM shared-buffer and AP grant controller interfaces.
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_AMM_H
#define GS_AMM_H

#include "postgres.h"
#include "storage/buf/buf.h"
#include "storage/gs_amm_types.h"

struct QueryDesc;
typedef struct MemTuneWorkMemFeatures MemTuneWorkMemFeatures;
typedef struct MemTuneWorkMemFeaturesV3 MemTuneWorkMemFeaturesV3;
typedef struct MemTuneWorkMemFeaturesV5 MemTuneWorkMemFeaturesV5;

typedef enum GsAmmGranuleState {
    GS_AMM_GRANULE_BUFFER_ACTIVE = 0,
    GS_AMM_GRANULE_BUFFER_DRAINING,
    GS_AMM_GRANULE_RECLAIMING,
    GS_AMM_GRANULE_FREE,
    GS_AMM_GRANULE_AP_RESERVED,
    GS_AMM_GRANULE_AP_ACTIVE
} GsAmmGranuleState;

typedef struct GsAmmGranuleToken {
    GsAmmGranuleState state;
    uint32 owner_epoch;
} GsAmmGranuleToken;

typedef struct GsAmmGranuleMeta {
    pg_atomic_uint64 packed_state_epoch;
    GsAmmGranuleState state;
    uint32 generation;
    uint32 owner_epoch;
    uint64 drain_attempt;
    uint64 scan_lease_attempt;
    bool scan_inflight;
    uint64 reclaim_attempt;
    bool reclaim_inflight;
    TimestampTz reclaim_retry_after;
    uint64 grant_id;
    uint64 grant_generation;
    int first_buffer_id;
    int buffer_count;
    int dirty_count;
    int pinned_count;
    int io_count;
    int hash_count;
    int reserved_granules;
    uint64 active_grant_bytes;
    uint64 used_bytes;
    uint64 alloc_cursor_bytes;
    uint64 access_score;
    TimestampTz last_access_tick;
} GsAmmGranuleMeta;

#define GS_AMM_DTREE_BOUND_COUNT 3
#define GS_AMM_ADMISSION_REASON_LENGTH 32
#define GS_AMM_AP_LIFECYCLE_GENERATION_STEPS 4
#define GS_AMM_AP_LIFECYCLE_OWNER_EPOCH_STEPS 2

typedef struct GsAmmDtreeDetail {
    double bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    /* Initial controller demand; native bounds retain executor semantics. */
    int admission_target_kb;
    int64 model_version;
    int64 leaf_id;
    bool test_label_override;
} GsAmmDtreeDetail;

typedef enum GsAmmMemoryMode {
    GS_AMM_MEMORY_MODE_NONE = 0,
    GS_AMM_MEMORY_MODE_BACKPRESSURE,
    GS_AMM_MEMORY_MODE_CACHE,
    GS_AMM_MEMORY_MODE_ONEPASS,
    GS_AMM_MEMORY_MODE_MULTIPASS
} GsAmmMemoryMode;

typedef enum GsAmmWorkloadRole {
    GS_AMM_WORKLOAD_TP = 0,
    GS_AMM_WORKLOAD_AP
} GsAmmWorkloadRole;

typedef enum GsAmmOperatorType {
    GS_AMM_OP_NONE = 0,
    GS_AMM_OP_SEQSCAN,
    GS_AMM_OP_SORT,
    GS_AMM_OP_HASH_JOIN,
    GS_AMM_OP_HASH_AGG,
    GS_AMM_OP_MATERIALIZE,
    GS_AMM_OP_WINDOW_AGG,
    GS_AMM_OP_COUNT
} GsAmmOperatorType;

typedef struct GsAmmOperatorHandle {
    GsAmmOperatorType type;
    uint64 grant_id;
    MemoryContext memory_context;
    uint64 poll_count;
    bool registered;
} GsAmmOperatorHandle;

typedef struct GsAmmAdmissionResult {
    bool admitted;
    bool queued;
    bool backpressure;
    uint64 grant_id;
    uint64 grant_generation;
    int granted_kb;
    int granted_mb;
    int queue_wait_ms;
    uint64 queue_ticket;
    int grant_granules;
    int prediction_mb;
    int cache_bound_kb;
    int one_pass_bound_kb;
    int multi_pass_bound_kb;
    int admission_request_kb;
    GsAmmMemoryMode memory_mode;
    char reason[GS_AMM_ADMISSION_REASON_LENGTH];
} GsAmmAdmissionResult;

#define GS_AMM_MIN_DYNAMIC_GRANULES 16

extern int gs_amm_shared_buffers_min_mb;
extern int gs_amm_tp_reserve_mb;
extern int gs_amm_shared_buffers_reserved_mb;
extern int gs_amm_dynamic_target_mb;
extern int gs_amm_memory_target_mb;
extern int gs_amm_granule_size_mb;
extern int gs_amm_ap_scan_ring_pages;
extern bool gs_amm_enabled;
extern THR_LOCAL int gs_amm_workload_role;
extern THR_LOCAL int gs_amm_test_ap_cache_label_kb;
extern THR_LOCAL int gs_amm_test_ap_one_pass_label_kb;
extern THR_LOCAL int gs_amm_test_ap_multi_pass_label_kb;
/* Compatibility alias; a positive value applies to all three test bounds. */
extern THR_LOCAL int gs_amm_test_ap_label_kb;
/* Test-only guard around the label GUCs above.  Production uses the model. */
extern THR_LOCAL bool gs_amm_test_ap_use_labels;
extern int gs_amm_tp_buffer_miss_threshold_pct;
extern int gs_amm_ap_borrow_buffer_hit_guard_pct;
extern int gs_amm_tp_tps_decline_guard_pct;
extern int gs_amm_tp_cpu_pressure_threshold_pct;
extern int gs_amm_resize_granule_per_tick;
extern bool gs_amm_borrow_cold_granules;
extern int gs_amm_restore_delay_ticks;
extern int gs_amm_restore_rate_mb_per_s;
extern int gs_amm_ap_admit_rate_mb_per_s;
extern bool gs_amm_controller_idle_sleep;
extern int gs_amm_tp_jitter_threshold_pct;
/* Acceptance-test override.  Production TP recovery is CPU-pressure driven. */
extern THR_LOCAL bool gs_amm_tp_test_mode;
extern THR_LOCAL bool gs_amm_tp_test_worker_surge;

extern Size GsAmmShmemSize(void);
extern void GsAmmShmemInit(void);
extern void GsAmmOnEnabledGucChange(bool enabled);
extern bool GsAmmOperationBegin(void);
extern void GsAmmOperationEnd(void);
extern bool GsAmmApExecutorStart(QueryDesc *query_desc, int eflags);
extern void GsAmmApExecutorEnd(QueryDesc *query_desc, bool success);
extern bool GsAmmTpExecutorStart(QueryDesc *query_desc, int eflags);
extern void GsAmmTpExecutorEnd(QueryDesc *query_desc, bool success);
/* Compatibility dispatcher for callers outside the standard executor. */
extern bool GsAmmExecutorStart(QueryDesc *query_desc, int eflags);
/*
 * Runs after executor contexts are freed; use backend-local AMM state, not
 * query_desc->estate or query_desc->planstate.
 */
extern void GsAmmExecutorEnd(QueryDesc *query_desc, bool success);
extern bool GsAmmApProcessPendingReclaim(void);
extern void GsAmmRecordTpBufferUsage(uint64 shared_blks_hit, uint64 shared_blks_read,
    uint64 completed_queries);
extern void GsAmmRecordTpCpuUsage(uint64 cpu_us);
extern void GsAmmRecordApReclaimPoll(uint64 released_bytes);
extern void GsAmmControllerTick(void);
/* queue_timeout_ms is retained for the legacy SQL function ABI and ignored. */
extern bool GsAmmAdmitBounds(int cache_bound_kb, int one_pass_bound_kb, int multi_pass_bound_kb,
    int queue_timeout_ms, int prediction_mb, GsAmmAdmissionResult *result);
extern bool GsAmmAdmitBoundsWithTarget(int cache_bound_kb, int one_pass_bound_kb,
    int multi_pass_bound_kb, int admission_target_kb, int queue_timeout_ms, int prediction_mb,
    GsAmmAdmissionResult *result);
extern bool GsAmmEvaluateAdmission(int prediction_mb);
extern bool GsAmmReleaseGrant(uint64 expected_generation);
extern bool GsAmmReleaseGrantToken(GsAmmGrantToken expected_token);
extern uint64 GsAmmCurrentBackendGrantGeneration(void);
extern uint64 GsAmmCurrentQueryLifecycleGeneration(void);
extern void GsAmmRecordNativeEligible(void);
extern void GsAmmRecordNativeFailure(const char *reason);
extern void GsAmmRecordNativeAdmission(const GsAmmDtreeDetail *detail, const GsAmmAdmissionResult *result);
extern bool GsAmmBuildWorkMemFeaturesV5(QueryDesc *query_desc, MemTuneWorkMemFeaturesV5 *features);
extern int GsAmmCurrentBackendGrantKB(void);
extern uint64 GsAmmCurrentBackendGrantId(void);
extern uint64 GsAmmCurrentBackendGrantBytes(void);
extern uint64 GsAmmCurrentBackendGrantPoolBytes(void);
extern uint64 GsAmmGrantEffectiveMemoryLimit(uint64 grant_id, uint64 requested_max_bytes);
extern bool GsAmmGrantTokenIsValid(GsAmmGrantToken token);
extern bool GsAmmGrantCanAllocateMemory(GsAmmGrantToken token, Size size);
extern void *GsAmmGrantAllocMemory(GsAmmGrantToken token, Size size);
extern bool GsAmmGrantReturnMemory(GsAmmGrantToken token, void *pointer, Size size);
extern void GsAmmGrantAccountUsedMemory(GsAmmGrantToken token, void *pointer, Size size);
extern void GsAmmGrantAccountFreedMemory(GsAmmGrantToken token, void *pointer, Size size);
extern bool GsAmmGrantDynamicMemoryAvailable(GsAmmGrantToken token, Size size);
extern void *GsAmmGrantAllocDynamicMemory(GsAmmGrantToken token, Size size);
extern bool GsAmmGrantReturnDynamicMemory(GsAmmGrantToken token, void *pointer, Size size);
extern void GsAmmGrantAccountUsedDynamicMemory(GsAmmGrantToken token, Size size);
extern void GsAmmGrantAccountFreedDynamicMemory(GsAmmGrantToken token, Size size);
extern bool GsAmmGrantReclaimPending(void);
extern bool GsAmmGrantProcessPendingReclaim(void);
/* Session-pool teardown runs without terminating the backend thread. */
extern void GsAmmSessionCleanup(int code, Datum arg);
extern void gs_amm_dtree_detail(
    const double raw_bounds_mb[GS_AMM_DTREE_BOUND_COUNT],
    int64 model_version,
    int64 leaf_id,
    GsAmmDtreeDetail *detail);
extern bool GsAmmBufferIdIsActive(int buf_id);
extern bool GsAmmBufferIdGetActiveToken(int buf_id, GsAmmGranuleToken *token);
extern bool GsAmmBufferTokenIsActive(int buf_id, GsAmmGranuleToken token);
extern bool GsAmmBufferIdIsDraining(int buf_id);
extern bool GsAmmBufferNumberIsActive(Buffer buffer);
extern bool GsAmmBufferIdIsInBufferGranule(int buf_id);
extern bool GsAmmGranuleCountersCanAdvance(uint32 generation, uint32 owner_epoch, bool owner_changes);
extern bool GsAmmGranuleCanCompleteApLifecycle(uint32 generation, uint32 owner_epoch);
extern void GsAmmOperatorRegister(GsAmmOperatorHandle *handle, GsAmmOperatorType type,
    uint64 grant_id, MemoryContext memory_context);
extern bool GsAmmOperatorPollReclaim(GsAmmOperatorHandle *handle);
extern Size GsAmmOperatorReleaseFreeMemory(GsAmmOperatorHandle *handle);
extern void GsAmmOperatorUnregister(GsAmmOperatorHandle *handle);

#endif /* GS_AMM_H */
