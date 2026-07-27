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
} GsAmmGranuleMeta;

#define GS_AMM_DTREE_BOUND_COUNT 3
#define GS_AMM_DTREE_FEEDBACK_RING_SIZE 4096
#define GS_AMM_DTREE_CALIBRATION_TABLE_SIZE 1024
#define GS_AMM_DTREE_MIN_CALIBRATION_SAMPLES 8
#define GS_AMM_DTREE_SCALE_MIN 0.5
#define GS_AMM_DTREE_SCALE_MAX 4.0
#define GS_AMM_ADMISSION_REASON_LENGTH 32
#define GS_AMM_AP_LIFECYCLE_GENERATION_STEPS 4
#define GS_AMM_AP_LIFECYCLE_OWNER_EPOCH_STEPS 2

typedef struct GsAmmDtreeDetail {
    double raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double calibrated_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    int64 model_version;
    int64 leaf_id;
    int64 calibration_version;
    double calibration_scale;
} GsAmmDtreeDetail;

typedef struct GsAmmDtreeFeedbackSample {
    uint64 sample_id;
    uint64 session_id;
    int64 model_version;
    int64 leaf_id;
    int64 calibration_version;
    double calibration_scale;
    double raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double calibrated_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double observed_work_mem_kb;
    double runtime_ms;
    double spill_mb;
    uint64 spill_bytes;
    uint32 spill_files;
    uint32 spill_events;
    int hash_nbatch;
    int hash_multipass_count;
    double grant_mb;
    double tp_drop_ratio;
    double io_pressure;
    bool backpressure;
    bool error;
    bool measurement_valid;
    int64 sample_time;
} GsAmmDtreeFeedbackSample;

typedef struct GsAmmDtreeCalibrationLeafState {
    bool initialized;
    int64 model_version;
    int64 leaf_id;
    int64 calibration_version;
    double calibration_scale;
    uint64 feedback_sample_count;
    uint64 feedback_sample_dropped;
    double ewma_qerror;
    double ewma_underpredict_rate;
    double ewma_spill_mb;
    double ewma_runtime_ms;
    double last_observed_work_mem_kb;
    double last_raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double last_calibrated_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    int bad_update_count;
    int rollback_count;
    bool frozen;
    int64 last_update_time;
} GsAmmDtreeCalibrationLeafState;

typedef struct GsAmmDtreeFeedbackRing {
    uint64 next_sample_id;
    uint64 next_calibration_sample_id;
    uint64 feedback_sample_count;
    uint64 feedback_sample_dropped;
    uint64 calibration_sample_dropped;
    int64 last_calibration_tick;
    GsAmmDtreeFeedbackSample samples[GS_AMM_DTREE_FEEDBACK_RING_SIZE];
} GsAmmDtreeFeedbackRing;

typedef enum GsAmmMemoryMode {
    GS_AMM_MEMORY_MODE_NONE = 0,
    GS_AMM_MEMORY_MODE_BACKPRESSURE,
    GS_AMM_MEMORY_MODE_ALLOCATOR_ONLY,
    GS_AMM_MEMORY_MODE_FEEDBACK_ONLY,
    GS_AMM_MEMORY_MODE_CACHE,
    GS_AMM_MEMORY_MODE_ONEPASS,
    GS_AMM_MEMORY_MODE_MULTIPASS
} GsAmmMemoryMode;

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
    bool allocator_only_mode;
    int allocator_only_grant_mb;
    bool feedback_only;
    GsAmmMemoryMode memory_mode;
    char reason[GS_AMM_ADMISSION_REASON_LENGTH];
} GsAmmAdmissionResult;

typedef struct GsAmmFeedbackRecord {
    uint64 session_id;
    int64 model_version;
    int64 leaf_id;
    double raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double calibrated_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double observed_work_mem_kb;
    double runtime_ms;
    double spill_mb;
    uint64 spill_bytes;
    uint32 spill_files;
    uint32 spill_events;
    int hash_nbatch;
    int hash_multipass_count;
    double grant_mb;
    double tp_drop_ratio;
    double io_pressure;
    bool backpressure;
    bool error;
    bool measurement_valid;
    bool feedback_only;
} GsAmmFeedbackRecord;

typedef struct GsAmmDtreeCalibrationTable {
    int64 calibration_version;
    double calibration_scale;
    uint64 feedback_sample_count;
    uint64 feedback_sample_dropped;
    uint64 calibration_update_count;
    int rollback_count;
    int frozen_leaf_count;
    int leaf_count;
    GsAmmDtreeCalibrationLeafState leaves[GS_AMM_DTREE_CALIBRATION_TABLE_SIZE];
} GsAmmDtreeCalibrationTable;

extern int gs_amm_shared_buffers_min_mb;
extern int gs_amm_controller_horizon;
extern int gs_amm_resize_rate_limit_mb;
extern int gs_amm_tp_pressure_guard;
extern int gs_amm_io_pressure_guard;
extern int gs_amm_deadband_mb;
extern int gs_amm_dynamic_target_mb;
extern int gs_amm_ap_min_grant_mb;
extern int gs_amm_ap_queue_limit;
extern int gs_amm_ap_queue_timeout_ms;
extern double gs_amm_tp_jitter_limit;
extern int gs_amm_resize_observe_window_ms;
extern int gs_amm_resize_cooldown_ms;
extern int gs_amm_resize_batch_mb;
extern int gs_amm_granule_size_mb;
extern int gs_amm_tp_recovery_cooldown_ms;
extern bool gs_amm_enabled;
extern bool gs_amm_allocator_only_mode;
extern int gs_amm_allocator_only_grant_mb;
extern bool gs_amm_feedback_only_mode;
extern int gs_amm_feedback_bootstrap_grant_mb;
extern int gs_amm_feedback_max_grant_mb;
extern int gs_amm_feedback_initial_ap_slots;
extern int gs_amm_feedback_max_ap_slots;
extern int gs_amm_feedback_stable_windows;
extern int gs_amm_feedback_spill_threshold_mb;
extern bool gs_amm_dtree_calibration_enabled;
extern bool gs_amm_dtree_record_only;
extern bool gs_amm_native_auto_mode;
extern double gs_amm_native_ap_cost_threshold;

#define GS_AMM_ADMISSION_FALLBACK 0
#define GS_AMM_ADMISSION_ERROR 1

extern int gs_amm_admission_failure_policy;
extern int gs_amm_fallback_work_mem_kb;

extern Size GsAmmShmemSize(void);
extern void GsAmmShmemInit(void);
extern void GsAmmOnEnabledGucChange(bool enabled);
extern void GsAmmOnRuntimeConfigGucReload(bool allocator_only_mode, int allocator_only_grant_mb,
    bool calibration_enabled, bool record_only);
extern void GsAmmOnFeedbackConfigGucReload(bool feedback_only_mode, int bootstrap_grant_mb,
    int max_grant_mb, int initial_ap_slots, int max_ap_slots, int stable_windows, int spill_threshold_mb);
extern bool GsAmmOperationBegin(void);
extern void GsAmmOperationEnd(void);
extern bool GsAmmExecutorStart(QueryDesc *query_desc, int eflags);
/*
 * Runs after executor contexts are freed; use backend-local AMM state, not
 * query_desc->estate or query_desc->planstate.
 */
extern void GsAmmExecutorEnd(QueryDesc *query_desc, bool success);
extern bool GsAmmAdmitBounds(int cache_bound_kb, int one_pass_bound_kb, int multi_pass_bound_kb,
    int queue_timeout_ms, int prediction_mb, GsAmmAdmissionResult *result);
extern bool GsAmmAdmitFeedbackOnly(GsAmmAdmissionResult *result);
extern bool GsAmmEvaluateAdmission(int prediction_mb);
extern bool GsAmmReleaseGrant(uint64 expected_generation, bool restore_work_mem);
extern bool GsAmmReleaseGrantToken(GsAmmGrantToken expected_token, bool restore_work_mem);
extern uint64 GsAmmCurrentBackendGrantGeneration(void);
extern uint64 GsAmmCurrentQueryLifecycleGeneration(void);
extern void GsAmmReportOperatorPeak(uint64 lifecycle_generation, uint64 bytes);
extern void GsAmmReportHashBatches(uint64 lifecycle_generation, int nbatch, int multipass_count);
extern void GsAmmReportQuerySpill(uint64 lifecycle_generation, uint64 bytes, uint32 events);
extern void GsAmmReportTempFileIO(uint64 lifecycle_generation, uint64 bytes, uint32 files);
extern void GsAmmRecordFeedback(const GsAmmFeedbackRecord *feedback);
extern void GsAmmGetFeedbackTelemetry(double *tp_drop_ratio, double *io_pressure);
extern void GsAmmDtreeCalibrationTick(void);
extern void GsAmmRecordNativeEligible(void);
extern void GsAmmRecordNativeFailure(const char *reason);
extern void GsAmmRecordNativeAdmission(const GsAmmDtreeDetail *detail, const GsAmmAdmissionResult *result);
extern void GsAmmRecordNativeErrorCleanup(void);
extern void GsAmmRecordTransactionCommit(void);
extern void GsAmmRecordTransactionAbort(void);
extern void GsAmmRecordSharedBufferReadMiss(void);
extern void GsAmmRecordSharedBufferPhysicalRead(void);
extern void GsAmmRecordApSpill(uint64 spill_bytes, uint32 spill_count);
extern void GsAmmRecordApExecutionSignals(uint64 spill_bytes, uint32 spill_count, int hash_multipass_count);
extern void GsAmmRecordPendingWritebackEnqueue(uint32 pages);
extern void GsAmmRecordPendingWritebackRetire(uint32 pages);
extern void GsAmmRecordWritebackFlushComplete(uint32 pages);
extern void GsAmmRecordDirtyPageEnqueue(void);
extern void GsAmmRecordDirtyPageDequeue(void);
extern void GsAmmPagewriterControllerTick(void);
extern bool GsAmmBuildWorkMemFeatures(QueryDesc *query_desc, MemTuneWorkMemFeatures *features);
extern double GsAmmCurrentMemoryPressureScore(void);
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

#endif /* GS_AMM_H */
