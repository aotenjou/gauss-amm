/* -------------------------------------------------------------------------
 *
 * gs_amm.cpp
 *      Minimal OpenGauss AMM controller for shared-buffer envelope and AP grants.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/mman.h>
#include <unistd.h>

#include "fmgr.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "port.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
#include "storage/gs_amm.h"
#include "storage/gs_amm_io.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "postmaster/pagewriter.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "ddes/dms/ss_dms_bufmgr.h"

extern bool superuser(void);

int gs_amm_shared_buffers_min_mb = 64;
int gs_amm_controller_horizon = 3;
int gs_amm_resize_rate_limit_mb = 64;
int gs_amm_tp_pressure_guard = 80;
int gs_amm_io_pressure_guard = 80;
int gs_amm_deadband_mb = 32;
int gs_amm_dynamic_target_mb = 512;
int gs_amm_ap_min_grant_mb = 4;
int gs_amm_ap_queue_limit = 16;
int gs_amm_ap_queue_timeout_ms = 5000;
double gs_amm_tp_jitter_limit = 0.03;
int gs_amm_resize_observe_window_ms = 5000;
int gs_amm_resize_cooldown_ms = 8000;
int gs_amm_resize_batch_mb = 64;
int gs_amm_granule_size_mb = 64;
int gs_amm_tp_recovery_cooldown_ms = 8000;
bool gs_amm_enabled = false;
bool gs_amm_allocator_only_mode = false;
int gs_amm_allocator_only_grant_mb = 128;
bool gs_amm_feedback_only_mode = false;
int gs_amm_feedback_bootstrap_grant_mb = 64;
int gs_amm_feedback_max_grant_mb = 256;
int gs_amm_feedback_initial_ap_slots = 1;
int gs_amm_feedback_max_ap_slots = 4;
int gs_amm_feedback_stable_windows = 2;
int gs_amm_feedback_spill_threshold_mb = 32;
bool gs_amm_dtree_calibration_enabled = false;
bool gs_amm_dtree_record_only = true;
bool gs_amm_native_auto_mode = false;
double gs_amm_native_ap_cost_threshold = 10000.0;
int gs_amm_admission_failure_policy = GS_AMM_ADMISSION_FALLBACK;
int gs_amm_fallback_work_mem_kb = 65536;

typedef enum GsAmmAction {
    GS_AMM_OBSERVE = 0,
    GS_AMM_AP_EXPAND,
    GS_AMM_BORROW_FROM_BUFFER,
    GS_AMM_AP_SHRINK,
    GS_AMM_BACKPRESSURE,
    GS_AMM_TP_RECOVERY,
    GS_AMM_FAIL_CLOSED,
    GS_AMM_ACTION_COUNT
} GsAmmAction;

#define GS_AMM_MAX_HORIZON 8
#define GS_AMM_MAX_BEAM 8
#define GS_AMM_SELECTABLE_ACTIONS 6
#define GS_AMM_QUEUE_RING_SIZE 128
#define GS_AMM_QUEUE_WAITING 1
#define GS_AMM_QUEUE_CANCELLED 2
#define GS_AMM_TP_WINDOW_SAMPLE_COUNT 128
#define GS_AMM_MIN_GRANULE_MB 1
#define GS_AMM_NATIVE_TP_WINDOW_MS 30000
#define GS_AMM_TP_BASELINE_REBASE_IDLE_MS 30000
#define GS_AMM_TP_BASELINE_REBASE_ALPHA 0.10
/* A 30s window below this rate cannot distinguish a 3% drop from commit noise. */
#define GS_AMM_TP_GUARD_MIN_BASELINE_TPS 10.0
#define GS_AMM_DTREE_SPILL_MEMORY_EQUIVALENT 0.50
#define GS_AMM_DTREE_CALIBRATION_TICK_MS 1000
#define GS_AMM_DTREE_CALIBRATION_MAX_SAMPLES 32
#define GS_AMM_RECLAIM_RETRY_INTERVAL_MS 1000

static void gs_amm_require_legacy_control(void)
{
    if (!u_sess->attr.attr_storage.gs_amm_legacy_control_enabled)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("external GS AMM control is disabled"),
                errhint("Set gs_amm_legacy_control_enabled and use gs_guc reload only for legacy or debug experiments.")));
}

static void gs_amm_require_admin_legacy_control(void)
{
    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be superuser to control GS AMM")));
    gs_amm_require_legacy_control();
}

typedef struct GsAmmConfig {
    int shared_buffers_min_mb;
    int controller_horizon;
    int resize_rate_limit_mb;
    int tp_pressure_guard;
    int io_pressure_guard;
    int deadband_mb;
    int hysteresis_enter_delta;
    double w_ap_benefit;
    double w_tp_recovery_benefit;
    double w_io_risk_penalty;
    double w_resize_cost;
    double w_grant_debt_risk;
    int beam_width;
} GsAmmConfig;

typedef struct GsAmmSimState {
    int active_mb;
    int max_mb;
    int min_mb;
    int grant_debt_mb;
    bool tail_reclaimable;
    int ap_demand_mb;
    int tp_pressure;
    int io_pressure;
} GsAmmSimState;

typedef struct GsAmmObservation {
    int ap_demand_mb;
    int tp_pressure;
    int io_pressure;
    bool tail_reclaimable;
    bool telemetry_ok;
} GsAmmObservation;

typedef struct GsAmmPlanResult {
    GsAmmAction chosen_action;
    GsAmmAction best_sequence[GS_AMM_MAX_HORIZON];
    int sequence_len;
    double score;
    int candidate_count;
    int rejected_count;
    const char *binding_constraint;
} GsAmmPlanResult;

typedef struct GsAmmResizeOutcome {
    const char *decision;
    const char *reason;
    int active_blocks;
    int target_blocks;
    int pending_retire_blocks;
    bool rolled_back;
} GsAmmResizeOutcome;

typedef struct GsAmmDrainStats {
    int dirty;
    int pinned;
    int io;
    int hash;
    int flush;
} GsAmmDrainStats;

typedef struct GsAmmGranuleSummary {
    int total_granules;
    int buffer_active_granules;
    int buffer_draining_granules;
    int reclaiming_granules;
    int free_granules;
    int ap_reserved_granules;
    int ap_active_granules;
    int buffer_active_blocks;
    uint64 ap_grant_bytes;
    uint64 ap_granule_used_bytes;
    uint64 ap_granule_alloc_cursor_bytes;
} GsAmmGranuleSummary;

typedef struct GsAmmRuntimeConfig {
    uint64 config_version;
    bool amm_enabled;
    bool runtime_override_active;
    bool allocator_only_mode;
    int allocator_only_grant_mb;
    bool feedback_only;
    int feedback_bootstrap_grant_mb;
    int feedback_max_grant_mb;
    int feedback_initial_ap_slots;
    int feedback_max_ap_slots;
    int feedback_stable_windows;
    int feedback_spill_threshold_mb;
    bool dtree_calibration_enabled;
    bool dtree_record_only;
} GsAmmRuntimeConfig;

typedef struct GsAmmSharedState {
    slock_t mutex;
    GsAmmRuntimeConfig runtime_config;
    bool maintenance_resetting;
    uint64 operation_inflight;
    bool reclaim_syscall_inflight;
    uint64 reclaim_syscall_attempt;
    pg_atomic_uint64 tp_commit_count;
    pg_atomic_uint64 shared_buffer_read_miss_count;
    pg_atomic_uint64 shared_buffer_physical_read_count;
    pg_atomic_uint64 dirty_page_count;
    pg_atomic_uint64 pending_writeback_page_count;
    pg_atomic_uint64 writeback_flush_completed_pages;
    pg_atomic_uint64 pending_writeback_retire_underflow_count;
    pg_atomic_uint64 ap_temp_spill_bytes;
    pg_atomic_uint64 ap_temp_spill_events;
    pg_atomic_uint64 ap_hash_multipass_count;
    TimestampTz native_telemetry_last_tick;
    uint64 native_telemetry_last_commit_count;
    uint64 native_telemetry_last_physical_read_count;
    uint64 native_telemetry_last_temp_spill_bytes;
    uint64 native_telemetry_last_hash_multipass_count;
    uint64 native_telemetry_tick_count;
    int dynamic_target_mb;
    int dynamic_used_mb;
    int active_ap_count;
    int feedback_current_grant_mb;
    int feedback_ap_slot_limit;
    int feedback_stable_windows;
    uint64 feedback_completed_count;
    uint64 feedback_admit_count;
    uint64 feedback_growth_count;
    uint64 feedback_backoff_count;
    uint64 feedback_slot_block_count;
    double feedback_ewma_runtime_ms;
    double feedback_ewma_spill_mb;
    double feedback_ewma_peak_mb;
    char feedback_last_action[32];
    int ap_queue_len;
    uint64 ap_queue_head;
    uint64 ap_queue_tail;
    int ap_queue_admit_count;
    int ap_queue_timeout_count;
    int last_queue_wait_ms;
    unsigned char ap_queue_slots[GS_AMM_QUEUE_RING_SIZE];
    int backpressure_count;
    int new_ap_guard_block_count;
    int grant_shrink_count;
    int grant_debt_mb;
    int effective_grant_kb;
    int effective_downgrade_count;
    double tp_baseline_tps;
    double tp_recent_tps;
    double tp_p95_latency_ms;
    double tp_raw_drop_ratio;
    int tp_window_ms;
    int tp_sample_count;
    int tp_baseline_rebase_count;
    double physical_read_rate;
    uint64 pending_writeback_pages;
    double dirty_page_ratio;
    int device_io_in_flight;
    double device_io_ms_rate;
    bool device_io_available;
    double temp_spill_mb_rate;
    double hash_multipass_rate;
    bool io_guard_latched;
    int io_recovery_stable_windows;
    TimestampTz io_recovery_last_window;
    int io_pressure_observed;
    int io_window_ms;
    int tp_sample_next;
    uint64 tp_sample_generation;
    bool tp_generation_exhausted;
    TimestampTz tp_sample_time[GS_AMM_TP_WINDOW_SAMPLE_COUNT];
    double tp_sample_tps[GS_AMM_TP_WINDOW_SAMPLE_COUNT];
    double tp_sample_p95_latency_ms[GS_AMM_TP_WINDOW_SAMPLE_COUNT];
    TimestampTz last_resize_time;
    TimestampTz resize_observe_until;
    TimestampTz cooldown_until;
    TimestampTz recovery_cooldown_until;
    int guard_block_count;
    int rollback_count;
    int recovery_action_count;
    int recovery_cooldown_block_count;
    int last_rollback_target_mb;
    int last_prediction_mb;
    int last_tp_pressure;
    int last_io_pressure;
    int last_grant_mb;
    int last_effective_grant_kb;
    char last_backpressure_reason[32];
    GsAmmAction last_action;
    uint64 next_pool_event_id;
    uint64 last_pool_event_id;
    TimestampTz last_pool_event_time;
    GsAmmAction last_pool_event_action;
    int last_pool_event_duration_ms;
    GsAmmGranuleSummary last_pool_event_before;
    GsAmmGranuleSummary last_pool_event_after;
    char last_pool_event_reason[64];
    int granule_mb;
    int granule_blocks;
    int total_granules;
    int buffer_active_granules;
    int buffer_draining_granules;
    int reclaiming_granules;
    int free_granules;
    int ap_reserved_granules;
    int ap_active_granules;
    uint64 ap_grant_bytes;
    uint64 ap_granule_used_bytes;
    uint64 ap_granule_alloc_cursor_bytes;
    uint64 granule_alloc_success_count;
    uint64 granule_alloc_no_mapping_count;
    uint64 granule_alloc_no_owner_count;
    uint64 granule_alloc_capacity_exhausted_count;
    uint64 grant_reused_bytes;
    uint64 illegal_transition_count;
    uint64 epoch_mismatch_reject_count;
    uint64 stale_grant_token_count;
    int drain_pending_dirty;
    int drain_pending_pinned;
    int drain_pending_io;
    int drain_pending_hash;
    int drain_success_count;
    int drain_fail_count;
    int drain_rollback_count;
    int drain_priority_flush_count;
    uint64 reclaimed_mb;
    int reclaim_fail_count;
    int ap_idle_reclaim_count;
    int ap_idle_reclaim_mb;
    int auto_controller_step_count;
    uint64 native_eligible_count;
    uint64 native_admit_count;
    uint64 native_reject_count;
    uint64 native_release_count;
    uint64 native_error_cleanup_count;
    int native_active_grant_count;
    bool native_current_active;
    uint64 native_current_grant_id;
    uint64 native_current_grant_generation;
    int native_current_granted_kb;
    GsAmmMemoryMode native_current_memory_mode;
    int64 native_current_model_version;
    int64 native_current_leaf_id;
    int64 native_current_calibration_version;
    double native_current_calibration_scale;
    double native_current_raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double native_current_calibrated_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    char native_last_reason[GS_AMM_ADMISSION_REASON_LENGTH];
    GsAmmDtreeFeedbackRing dtree_feedback_ring;
    GsAmmDtreeCalibrationTable dtree_calibration_table;
    uint64 next_drain_attempt;
    uint64 next_reclaim_attempt;
    uint64 next_grant_id;
    uint64 next_grant_generation;
    GsAmmGranuleMeta granules[1];
} GsAmmSharedState;

typedef struct GsAmmBeamEntry {
    GsAmmSimState chain[GS_AMM_MAX_HORIZON + 1];
    GsAmmAction seq[GS_AMM_MAX_HORIZON];
    int len;
    double score;
} GsAmmBeamEntry;

static const char *const GS_AMM_ACTION_NAMES[GS_AMM_ACTION_COUNT] = {
    "OBSERVE",
    "AP_EXPAND",
    "BORROW_FROM_BUFFER",
    "AP_SHRINK",
    "BACKPRESSURE",
    "TP_RECOVERY",
    "FAIL_CLOSED"
};

static const GsAmmAction GS_AMM_SELECTABLE[GS_AMM_SELECTABLE_ACTIONS] = {
    GS_AMM_OBSERVE,
    GS_AMM_AP_EXPAND,
    GS_AMM_BORROW_FROM_BUFFER,
    GS_AMM_AP_SHRINK,
    GS_AMM_BACKPRESSURE,
    GS_AMM_TP_RECOVERY
};

static const char *gs_amm_memory_mode_name(GsAmmMemoryMode mode)
{
    switch (mode) {
        case GS_AMM_MEMORY_MODE_ALLOCATOR_ONLY:
            return "allocator_only";
        case GS_AMM_MEMORY_MODE_FEEDBACK_ONLY:
            return "feedback_only";
        case GS_AMM_MEMORY_MODE_CACHE:
            return "cache";
        case GS_AMM_MEMORY_MODE_ONEPASS:
            return "onepass";
        case GS_AMM_MEMORY_MODE_MULTIPASS:
            return "multipass";
        case GS_AMM_MEMORY_MODE_BACKPRESSURE:
            return "backpressure";
        case GS_AMM_MEMORY_MODE_NONE:
        default:
            return "none";
    }
}

static void gs_amm_copy_admission_reason(char destination[GS_AMM_ADMISSION_REASON_LENGTH], const char *reason)
{
    int rc;

    if (reason == NULL || reason[0] == '\0')
        reason = "none";
    rc = snprintf_s(destination, GS_AMM_ADMISSION_REASON_LENGTH, GS_AMM_ADMISSION_REASON_LENGTH - 1,
        "%s", reason);
    securec_check_ss(rc, "\0", "\0");
}

static GsAmmSharedState *GsAmmState = NULL;
static THR_LOCAL int MyGsAmmGrantMb = 0;
static THR_LOCAL int MyGsAmmGrantKb = 0;
static THR_LOCAL uint64 MyGsAmmGrantId = 0;
static THR_LOCAL uint64 MyGsAmmGrantGeneration = 0;
static THR_LOCAL int MyGsAmmGrantGranules = 0;
static THR_LOCAL bool MyGsAmmGrantNative = false;
static THR_LOCAL int MyGsAmmSavedWorkMemKb = 0;
static THR_LOCAL bool MyGsAmmCleanupRegistered = false;
static THR_LOCAL bool MyGsAmmTransactionHasNativeAP = false;

typedef struct GsAmmGrantFreeExtent {
    struct GsAmmGrantFreeExtent *next;
    uint64 size;
    int granule_id;
} GsAmmGrantFreeExtent;

typedef struct GsAmmBackendGrantArena {
    GsAmmGrantToken token;
    GsAmmGrantFreeExtent *free_extents;
} GsAmmBackendGrantArena;

static THR_LOCAL GsAmmBackendGrantArena MyGsAmmGrantArena = {{0, 0}, NULL};

static void gs_amm_backend_cleanup(int code, Datum arg);
static void gs_amm_register_backend_cleanup(void);
static void gs_amm_release_backend_grant(bool restore_work_mem);
static void gs_amm_set_backpressure_reason_locked(GsAmmSharedState *state, const char *reason);
static void gs_amm_controller_step_internal(
    int ap_demand_mb, int tp_pressure, int io_pressure, char *status, Size status_size);
static bool gs_amm_tp_drop_guard_hot_locked(GsAmmSharedState *state);
static bool gs_amm_io_guard_hot_locked(GsAmmSharedState *state);
static bool gs_amm_recovery_cooldown_hot_locked(GsAmmSharedState *state, TimestampTz now);
static int gs_amm_current_active_buffer_blocks(GsAmmSharedState *state);
static bool gs_amm_pointer_in_granule(
    const GsAmmGranuleMeta *granule, const char *buffer_blocks, const char *pointer, uint64 size);

static int gs_amm_configured_granule_mb(void)
{
    return Max(gs_amm_granule_size_mb, GS_AMM_MIN_GRANULE_MB);
}

static int gs_amm_configured_granule_blocks(void)
{
    int64 blocks = ((int64)gs_amm_configured_granule_mb() * 1024 * 1024) / BLCKSZ;

    if (blocks <= 0)
        blocks = 1;
    if (blocks > INT_MAX)
        blocks = INT_MAX;
    return (int)blocks;
}

static int gs_amm_total_granules_for_blocks(int buffer_count, int granule_blocks)
{
    if (buffer_count <= 0)
        return 1;
    granule_blocks = Max(granule_blocks, 1);
    return (buffer_count + granule_blocks - 1) / granule_blocks;
}

static Size gs_amm_state_shmem_size(int total_granules)
{
    total_granules = Max(total_granules, 1);
    return add_size(offsetof(GsAmmSharedState, granules),
        mul_size((Size)total_granules, sizeof(GsAmmGranuleMeta)));
}

typedef enum GsAmmGranuleOwnerDomain {
    GS_AMM_OWNER_NONE = 0,
    GS_AMM_OWNER_BUFFER,
    GS_AMM_OWNER_AP
} GsAmmGranuleOwnerDomain;

static GsAmmGranuleOwnerDomain gs_amm_granule_owner_domain(GsAmmGranuleState state)
{
    if (state == GS_AMM_GRANULE_BUFFER_ACTIVE || state == GS_AMM_GRANULE_BUFFER_DRAINING)
        return GS_AMM_OWNER_BUFFER;
    if (state == GS_AMM_GRANULE_AP_RESERVED || state == GS_AMM_GRANULE_AP_ACTIVE)
        return GS_AMM_OWNER_AP;
    return GS_AMM_OWNER_NONE;
}

bool GsAmmGranuleCountersCanAdvance(uint32 generation, uint32 owner_epoch, bool owner_changes)
{
    if (generation == PG_UINT32_MAX)
        return false;
    if (owner_changes && owner_epoch == PG_UINT32_MAX)
        return false;
    return true;
}

bool GsAmmGranuleCanCompleteApLifecycle(uint32 generation, uint32 owner_epoch)
{
    if (generation > PG_UINT32_MAX - GS_AMM_AP_LIFECYCLE_GENERATION_STEPS)
        return false;
    if (owner_epoch > PG_UINT32_MAX - GS_AMM_AP_LIFECYCLE_OWNER_EPOCH_STEPS)
        return false;
    return true;
}

static uint64 gs_amm_pack_granule_token(GsAmmGranuleState state, uint32 owner_epoch)
{
    return ((uint64)owner_epoch << 32) | (uint32)state;
}

static GsAmmGranuleToken gs_amm_read_granule_token(GsAmmGranuleMeta *granule)
{
    uint64 packed = pg_atomic_read_u64(&granule->packed_state_epoch);
    GsAmmGranuleToken token;

    token.state = (GsAmmGranuleState)(uint32)packed;
    token.owner_epoch = (uint32)(packed >> 32);
    return token;
}

static bool gs_amm_granule_transition_allowed(GsAmmGranuleState old_state, GsAmmGranuleState new_state)
{
    switch (old_state) {
        case GS_AMM_GRANULE_BUFFER_ACTIVE:
            return new_state == GS_AMM_GRANULE_BUFFER_DRAINING;
        case GS_AMM_GRANULE_BUFFER_DRAINING:
            return new_state == GS_AMM_GRANULE_BUFFER_ACTIVE || new_state == GS_AMM_GRANULE_RECLAIMING;
        case GS_AMM_GRANULE_RECLAIMING:
            return new_state == GS_AMM_GRANULE_FREE;
        case GS_AMM_GRANULE_FREE:
            return new_state == GS_AMM_GRANULE_BUFFER_ACTIVE || new_state == GS_AMM_GRANULE_AP_RESERVED;
        case GS_AMM_GRANULE_AP_RESERVED:
            return new_state == GS_AMM_GRANULE_AP_ACTIVE || new_state == GS_AMM_GRANULE_RECLAIMING;
        case GS_AMM_GRANULE_AP_ACTIVE:
            return new_state == GS_AMM_GRANULE_RECLAIMING;
        default:
            return false;
    }
}

static bool gs_amm_publish_granule_state_locked(
    GsAmmSharedState *state, GsAmmGranuleMeta *granule, GsAmmGranuleState new_state)
{
    GsAmmGranuleOwnerDomain old_owner;
    GsAmmGranuleOwnerDomain new_owner;

    if (state == NULL || granule == NULL ||
        ((granule->scan_inflight || granule->reclaim_inflight) && granule->state != new_state) ||
        !gs_amm_granule_transition_allowed(granule->state, new_state)) {
        if (state != NULL)
            state->illegal_transition_count++;
        return false;
    }

    old_owner = gs_amm_granule_owner_domain(granule->state);
    new_owner = gs_amm_granule_owner_domain(new_state);
    if (!GsAmmGranuleCountersCanAdvance(
            granule->generation, granule->owner_epoch, old_owner != new_owner)) {
        state->illegal_transition_count++;
        return false;
    }
    granule->state = new_state;
    granule->generation++;
    if (old_owner != new_owner)
        granule->owner_epoch++;
    pg_atomic_write_u64(&granule->packed_state_epoch,
        gs_amm_pack_granule_token(granule->state, granule->owner_epoch));
    return true;
}

Size GsAmmShmemSize(void)
{
    int granule_blocks = gs_amm_configured_granule_blocks();
    int total_granules = gs_amm_total_granules_for_blocks(NORMAL_SHARED_BUFFER_NUM, granule_blocks);

    return MAXALIGN(gs_amm_state_shmem_size(total_granules));
}

static void gs_amm_refresh_granule_counts_locked(GsAmmSharedState *state)
{
    int active_blocks = 0;
    uint64 ap_grant_bytes = 0;

    state->buffer_active_granules = 0;
    state->buffer_draining_granules = 0;
    state->reclaiming_granules = 0;
    state->free_granules = 0;
    state->ap_reserved_granules = 0;
    state->ap_active_granules = 0;

    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];

        switch (granule->state) {
            case GS_AMM_GRANULE_BUFFER_ACTIVE:
                state->buffer_active_granules++;
                active_blocks += granule->buffer_count;
                break;
            case GS_AMM_GRANULE_BUFFER_DRAINING:
                state->buffer_draining_granules++;
                break;
            case GS_AMM_GRANULE_RECLAIMING:
                state->reclaiming_granules++;
                break;
            case GS_AMM_GRANULE_FREE:
                state->free_granules++;
                break;
            case GS_AMM_GRANULE_AP_RESERVED:
                state->ap_reserved_granules++;
                ap_grant_bytes += granule->active_grant_bytes;
                break;
            case GS_AMM_GRANULE_AP_ACTIVE:
                state->ap_active_granules++;
                ap_grant_bytes += granule->active_grant_bytes;
                break;
            default:
                break;
        }
    }
    state->ap_grant_bytes = ap_grant_bytes;
    StrategySetActiveBufferCount(active_blocks > 0 ? active_blocks : NORMAL_SHARED_BUFFER_NUM);
}

static void gs_amm_init_granule_table(GsAmmSharedState *state)
{
    int granule_mb = gs_amm_configured_granule_mb();
    int granule_blocks = gs_amm_configured_granule_blocks();
    int total_granules = gs_amm_total_granules_for_blocks(NORMAL_SHARED_BUFFER_NUM, granule_blocks);

    state->granule_mb = granule_mb;
    state->granule_blocks = granule_blocks;
    state->total_granules = total_granules;

    for (int i = 0; i < total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];
        int first_buffer_id = i * granule_blocks;
        int buffer_count = Min(granule_blocks, NORMAL_SHARED_BUFFER_NUM - first_buffer_id);

        granule->state = GS_AMM_GRANULE_BUFFER_ACTIVE;
        granule->generation = 1;
        granule->owner_epoch = 1;
        granule->drain_attempt = 0;
        granule->scan_lease_attempt = 0;
        granule->scan_inflight = false;
        granule->reclaim_attempt = 0;
        granule->reclaim_inflight = false;
        granule->reclaim_retry_after = 0;
        pg_atomic_init_u64(&granule->packed_state_epoch,
            gs_amm_pack_granule_token(granule->state, granule->owner_epoch));
        granule->grant_id = 0;
        granule->grant_generation = 0;
        granule->first_buffer_id = first_buffer_id;
        granule->buffer_count = Max(buffer_count, 0);
        granule->dirty_count = 0;
        granule->pinned_count = 0;
        granule->io_count = 0;
        granule->hash_count = 0;
        granule->reserved_granules = 0;
        granule->active_grant_bytes = 0;
        granule->used_bytes = 0;
        granule->alloc_cursor_bytes = 0;
    }
    gs_amm_refresh_granule_counts_locked(state);
}

static void gs_amm_reinitialize_granule_table_locked(GsAmmSharedState *state)
{
    int granule_blocks = state->granule_blocks;

    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];
        int first_buffer_id = i * granule_blocks;
        int buffer_count = Min(granule_blocks, NORMAL_SHARED_BUFFER_NUM - first_buffer_id);

        Assert(GsAmmGranuleCountersCanAdvance(granule->generation, granule->owner_epoch, true));
        granule->state = GS_AMM_GRANULE_BUFFER_ACTIVE;
        granule->generation++;
        granule->owner_epoch++;
        granule->drain_attempt = 0;
        granule->scan_lease_attempt = 0;
        granule->scan_inflight = false;
        granule->reclaim_attempt = 0;
        granule->reclaim_inflight = false;
        granule->reclaim_retry_after = 0;
        granule->grant_id = 0;
        granule->grant_generation = 0;
        granule->first_buffer_id = first_buffer_id;
        granule->buffer_count = Max(buffer_count, 0);
        granule->dirty_count = 0;
        granule->pinned_count = 0;
        granule->io_count = 0;
        granule->hash_count = 0;
        granule->reserved_granules = 0;
        granule->active_grant_bytes = 0;
        granule->used_bytes = 0;
        granule->alloc_cursor_bytes = 0;
        pg_atomic_write_u64(&granule->packed_state_epoch,
            gs_amm_pack_granule_token(granule->state, granule->owner_epoch));
    }
    gs_amm_refresh_granule_counts_locked(state);
}

static double gs_amm_dtree_min_bound_kb(void)
{
    return 64.0;
}

static double gs_amm_dtree_clamp_scale(double scale)
{
    if (!(scale > 0.0))
        return 1.0;
    if (scale < GS_AMM_DTREE_SCALE_MIN)
        return GS_AMM_DTREE_SCALE_MIN;
    if (scale > GS_AMM_DTREE_SCALE_MAX)
        return GS_AMM_DTREE_SCALE_MAX;
    return scale;
}

static double gs_amm_dtree_bound_kb(double value_mb)
{
    double value_kb;

    if (!(value_mb > 0.0))
        return gs_amm_dtree_min_bound_kb();
    value_kb = value_mb * 1024.0;
    return Max(value_kb, gs_amm_dtree_min_bound_kb());
}

static int gs_amm_dtree_leaf_slot_index(int64 model_version, int64 leaf_id)
{
    uint64 hash = (uint64)model_version * 1315423911ULL ^ (uint64)leaf_id;

    return (int)(hash % GS_AMM_DTREE_CALIBRATION_TABLE_SIZE);
}

static GsAmmDtreeCalibrationLeafState *gs_amm_dtree_leaf_state_locked(
    GsAmmSharedState *state, int64 model_version, int64 leaf_id, bool allow_replace)
{
    GsAmmDtreeCalibrationTable *table = &state->dtree_calibration_table;
    GsAmmDtreeCalibrationLeafState *leaf = &table->leaves[gs_amm_dtree_leaf_slot_index(model_version, leaf_id)];
    bool occupied = leaf->initialized;

    if (!allow_replace && (!occupied || leaf->model_version != model_version || leaf->leaf_id != leaf_id))
        return NULL;

    if (occupied && (leaf->model_version != model_version || leaf->leaf_id != leaf_id)) {
        if (!allow_replace)
            return NULL;
        table->feedback_sample_dropped++;
        leaf->feedback_sample_dropped++;
    }

    if (!occupied || leaf->model_version != model_version || leaf->leaf_id != leaf_id) {
        if (!occupied)
            table->leaf_count++;
        leaf->initialized = true;
        leaf->model_version = model_version;
        leaf->leaf_id = leaf_id;
        leaf->calibration_version = table->calibration_version;
        leaf->calibration_scale = table->calibration_scale > 0.0 ? table->calibration_scale : 1.0;
        leaf->feedback_sample_count = 0;
        leaf->feedback_sample_dropped = 0;
        leaf->ewma_qerror = 0.0;
        leaf->ewma_underpredict_rate = 0.0;
        leaf->ewma_spill_mb = 0.0;
        leaf->ewma_runtime_ms = 0.0;
        leaf->last_observed_work_mem_kb = 0.0;
        leaf->bad_update_count = 0;
        leaf->rollback_count = 0;
        leaf->frozen = false;
        leaf->last_update_time = 0;
        for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
            leaf->last_raw_bounds_kb[i] = 0.0;
            leaf->last_calibrated_bounds_kb[i] = 0.0;
        }
    }

    return leaf;
}

static void gs_amm_init_dtree_state(GsAmmSharedState *state)
{
    errno_t rc;

    rc = memset_s(&state->dtree_feedback_ring, sizeof(state->dtree_feedback_ring), 0,
        sizeof(state->dtree_feedback_ring));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&state->dtree_calibration_table, sizeof(state->dtree_calibration_table), 0,
        sizeof(state->dtree_calibration_table));
    securec_check(rc, "\0", "\0");
    state->dtree_calibration_table.calibration_version = 1;
    state->dtree_calibration_table.calibration_scale = 1.0;
    state->dtree_calibration_table.calibration_update_count = 0;
    state->dtree_calibration_table.rollback_count = 0;
    state->dtree_calibration_table.frozen_leaf_count = 0;
    state->dtree_calibration_table.leaf_count = 0;
}

static void gs_amm_init_runtime_config(GsAmmSharedState *state)
{
    state->runtime_config.config_version = 1;
    state->runtime_config.amm_enabled = gs_amm_enabled;
    state->runtime_config.runtime_override_active = false;
    state->runtime_config.allocator_only_mode = gs_amm_allocator_only_mode;
    state->runtime_config.allocator_only_grant_mb = gs_amm_allocator_only_grant_mb;
    state->runtime_config.feedback_only = gs_amm_feedback_only_mode;
    state->runtime_config.feedback_bootstrap_grant_mb = gs_amm_feedback_bootstrap_grant_mb;
    state->runtime_config.feedback_max_grant_mb = gs_amm_feedback_max_grant_mb;
    state->runtime_config.feedback_initial_ap_slots = gs_amm_feedback_initial_ap_slots;
    state->runtime_config.feedback_max_ap_slots = gs_amm_feedback_max_ap_slots;
    state->runtime_config.feedback_stable_windows = gs_amm_feedback_stable_windows;
    state->runtime_config.feedback_spill_threshold_mb = gs_amm_feedback_spill_threshold_mb;
    state->runtime_config.dtree_calibration_enabled = gs_amm_dtree_calibration_enabled;
    state->runtime_config.dtree_record_only = gs_amm_dtree_record_only;
}

static void gs_amm_runtime_config_snapshot_locked(GsAmmSharedState *state, GsAmmRuntimeConfig *config)
{
    *config = state->runtime_config;
    if (!config->runtime_override_active) {
        config->allocator_only_mode = gs_amm_allocator_only_mode;
        config->allocator_only_grant_mb = gs_amm_allocator_only_grant_mb;
        config->feedback_only = gs_amm_feedback_only_mode;
        config->feedback_bootstrap_grant_mb = gs_amm_feedback_bootstrap_grant_mb;
        config->feedback_max_grant_mb = gs_amm_feedback_max_grant_mb;
        config->feedback_initial_ap_slots = gs_amm_feedback_initial_ap_slots;
        config->feedback_max_ap_slots = gs_amm_feedback_max_ap_slots;
        config->feedback_stable_windows = gs_amm_feedback_stable_windows;
        config->feedback_spill_threshold_mb = gs_amm_feedback_spill_threshold_mb;
        config->dtree_calibration_enabled = gs_amm_dtree_calibration_enabled;
        config->dtree_record_only = gs_amm_dtree_record_only;
    }
}

static void gs_amm_runtime_config_snapshot(GsAmmSharedState *state, GsAmmRuntimeConfig *config)
{
    SpinLockAcquire(&state->mutex);
    gs_amm_runtime_config_snapshot_locked(state, config);
    SpinLockRelease(&state->mutex);
}

static bool gs_amm_update_runtime_override_locked(GsAmmSharedState *state, bool allocator_only_mode,
    int allocator_only_grant_mb, bool calibration_enabled, bool record_only)
{
    if (state->runtime_config.config_version == PG_UINT64_MAX)
        return false;

    state->runtime_config.runtime_override_active = true;
    state->runtime_config.allocator_only_mode = allocator_only_mode;
    state->runtime_config.allocator_only_grant_mb = allocator_only_grant_mb;
    state->runtime_config.dtree_calibration_enabled = calibration_enabled;
    state->runtime_config.dtree_record_only = record_only;
    state->runtime_config.config_version++;
    return true;
}

static void gs_amm_copy_feedback_action_locked(GsAmmSharedState *state, const char *action)
{
    int rc = snprintf_s(state->feedback_last_action, sizeof(state->feedback_last_action),
        sizeof(state->feedback_last_action) - 1, "%s", action == NULL ? "none" : action);

    securec_check_ss(rc, "\0", "\0");
}

static void gs_amm_reset_feedback_controller_locked(GsAmmSharedState *state, const GsAmmRuntimeConfig *config)
{
    state->feedback_current_grant_mb = Max(config->feedback_bootstrap_grant_mb, 1);
    state->feedback_ap_slot_limit = Max(config->feedback_initial_ap_slots, 1);
    state->feedback_stable_windows = 0;
    gs_amm_copy_feedback_action_locked(state, "config_reset");
}

void GsAmmShmemInit(void)
{
    bool found = false;

    GsAmmState = (GsAmmSharedState *)ShmemInitStruct("GS AMM Shared State", GsAmmShmemSize(), &found);
    if (!found) {
        errno_t rc = memset_s(GsAmmState, GsAmmShmemSize(), 0, GsAmmShmemSize());
        securec_check(rc, "\0", "\0");
        SpinLockInit(&GsAmmState->mutex);
        pg_atomic_init_u64(&GsAmmState->tp_commit_count, 0);
        pg_atomic_init_u64(&GsAmmState->shared_buffer_read_miss_count, 0);
        pg_atomic_init_u64(&GsAmmState->shared_buffer_physical_read_count, 0);
        pg_atomic_init_u64(&GsAmmState->dirty_page_count, 0);
        pg_atomic_init_u64(&GsAmmState->pending_writeback_page_count, 0);
        pg_atomic_init_u64(&GsAmmState->writeback_flush_completed_pages, 0);
        pg_atomic_init_u64(&GsAmmState->pending_writeback_retire_underflow_count, 0);
        pg_atomic_init_u64(&GsAmmState->ap_temp_spill_bytes, 0);
        pg_atomic_init_u64(&GsAmmState->ap_temp_spill_events, 0);
        pg_atomic_init_u64(&GsAmmState->ap_hash_multipass_count, 0);
        GsAmmState->dynamic_target_mb = gs_amm_dynamic_target_mb;
        GsAmmState->last_action = GS_AMM_OBSERVE;
        gs_amm_init_runtime_config(GsAmmState);
        GsAmmState->feedback_current_grant_mb = Max(gs_amm_feedback_bootstrap_grant_mb, 1);
        GsAmmState->feedback_ap_slot_limit = Max(gs_amm_feedback_initial_ap_slots, 1);
        GsAmmState->feedback_last_action[0] = '\0';
        gs_amm_init_dtree_state(GsAmmState);
        gs_amm_init_granule_table(GsAmmState);
    }
}

static GsAmmSharedState *gs_amm_get_state(void)
{
    if (GsAmmState == NULL)
        GsAmmShmemInit();
    return GsAmmState;
}

void GsAmmOnRuntimeConfigGucReload(bool allocator_only_mode, int allocator_only_grant_mb,
    bool calibration_enabled, bool record_only)
{
    GsAmmSharedState *state = GsAmmState;

    if (state == NULL)
        return;

    SpinLockAcquire(&state->mutex);
    state->runtime_config.runtime_override_active = false;
    state->runtime_config.allocator_only_mode = allocator_only_mode;
    state->runtime_config.allocator_only_grant_mb = allocator_only_grant_mb;
    state->runtime_config.dtree_calibration_enabled = calibration_enabled;
    state->runtime_config.dtree_record_only = record_only;
    if (state->runtime_config.config_version != PG_UINT64_MAX)
        state->runtime_config.config_version++;
    else
        state->illegal_transition_count++;
    SpinLockRelease(&state->mutex);
}

void GsAmmOnFeedbackConfigGucReload(bool feedback_only_mode, int bootstrap_grant_mb,
    int max_grant_mb, int initial_ap_slots, int max_ap_slots, int stable_windows, int spill_threshold_mb)
{
    GsAmmSharedState *state = GsAmmState;
    bool reset_controller;

    if (state == NULL)
        return;

    SpinLockAcquire(&state->mutex);
    reset_controller = state->runtime_config.feedback_only != feedback_only_mode ||
        state->runtime_config.feedback_bootstrap_grant_mb != bootstrap_grant_mb ||
        state->runtime_config.feedback_initial_ap_slots != initial_ap_slots;
    state->runtime_config.runtime_override_active = false;
    state->runtime_config.feedback_only = feedback_only_mode;
    state->runtime_config.feedback_bootstrap_grant_mb = bootstrap_grant_mb;
    state->runtime_config.feedback_max_grant_mb = Max(max_grant_mb, bootstrap_grant_mb);
    state->runtime_config.feedback_initial_ap_slots = initial_ap_slots;
    state->runtime_config.feedback_max_ap_slots = Max(max_ap_slots, initial_ap_slots);
    state->runtime_config.feedback_stable_windows = stable_windows;
    state->runtime_config.feedback_spill_threshold_mb = spill_threshold_mb;
    if (reset_controller)
        gs_amm_reset_feedback_controller_locked(state, &state->runtime_config);
    else {
        state->feedback_current_grant_mb = Min(state->runtime_config.feedback_max_grant_mb,
            Max(state->runtime_config.feedback_bootstrap_grant_mb, state->feedback_current_grant_mb));
        state->feedback_ap_slot_limit = Min(state->runtime_config.feedback_max_ap_slots,
            Max(1, state->feedback_ap_slot_limit));
    }
    if (state->runtime_config.config_version != PG_UINT64_MAX)
        state->runtime_config.config_version++;
    else
        state->illegal_transition_count++;
    SpinLockRelease(&state->mutex);
}

bool GsAmmOperationBegin(void)
{
    GsAmmSharedState *state = gs_amm_get_state();
    bool acquired = false;

    SpinLockAcquire(&state->mutex);
    if (!state->maintenance_resetting && state->operation_inflight != PG_UINT64_MAX) {
        state->operation_inflight++;
        acquired = true;
    }
    SpinLockRelease(&state->mutex);
    return acquired;
}

void GsAmmOperationEnd(void)
{
    GsAmmSharedState *state = gs_amm_get_state();

    SpinLockAcquire(&state->mutex);
    if (state->operation_inflight > 0)
        state->operation_inflight--;
    else
        state->illegal_transition_count++;
    SpinLockRelease(&state->mutex);
}

void gs_amm_dtree_detail(
    const double raw_bounds_mb[GS_AMM_DTREE_BOUND_COUNT],
    int64 model_version,
    int64 leaf_id,
    GsAmmDtreeDetail *detail)
{
    GsAmmSharedState *state = gs_amm_get_state();
    double raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double calibrated_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double scale = 1.0;
    int64 calibration_version = 0;
    bool calibration_enabled;
    bool record_only;
    bool apply_calibration;
    GsAmmRuntimeConfig runtime_config;
    GsAmmDtreeCalibrationLeafState *leaf = NULL;

    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++)
        raw_bounds_kb[i] = gs_amm_dtree_bound_kb(raw_bounds_mb[i]);

    SpinLockAcquire(&state->mutex);
    gs_amm_runtime_config_snapshot_locked(state, &runtime_config);
    calibration_enabled = runtime_config.dtree_calibration_enabled;
    record_only = runtime_config.dtree_record_only;
    apply_calibration = calibration_enabled && !record_only;
    if (apply_calibration) {
        scale = gs_amm_dtree_clamp_scale(state->dtree_calibration_table.calibration_scale);
        calibration_version = state->dtree_calibration_table.calibration_version;
        leaf = gs_amm_dtree_leaf_state_locked(state, model_version, leaf_id, false);
        if (leaf != NULL && leaf->calibration_scale > 0.0) {
            scale = gs_amm_dtree_clamp_scale(leaf->calibration_scale);
            calibration_version = leaf->calibration_version;
        }
    }
    SpinLockRelease(&state->mutex);

    if (!apply_calibration) {
        scale = 1.0;
        calibration_version = 0;
    }

    detail->model_version = model_version;
    detail->leaf_id = leaf_id;
    detail->calibration_version = calibration_version;
    detail->calibration_scale = scale;

    if (!apply_calibration) {
        for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++)
            calibrated_bounds_kb[i] = raw_bounds_kb[i];
    } else {
        calibrated_bounds_kb[2] = Max(raw_bounds_kb[2] * scale, gs_amm_dtree_min_bound_kb());
        calibrated_bounds_kb[1] = Max(raw_bounds_kb[1] * scale, calibrated_bounds_kb[2]);
        calibrated_bounds_kb[0] = Max(raw_bounds_kb[0] * scale, calibrated_bounds_kb[1]);
    }

    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
        detail->raw_bounds_kb[i] = raw_bounds_kb[i];
        detail->calibrated_bounds_kb[i] = calibrated_bounds_kb[i];
    }
}

bool GsAmmBufferIdIsInBufferGranule(int buf_id)
{
    return buf_id >= 0 && buf_id < NORMAL_SHARED_BUFFER_NUM;
}

bool GsAmmBufferIdIsActive(int buf_id)
{
    GsAmmSharedState *state = GsAmmState;
    int granule_id;
    GsAmmGranuleToken token;

    if (buf_id < 0)
        return false;
    if (buf_id >= NORMAL_SHARED_BUFFER_NUM && buf_id < TOTAL_BUFFER_NUM)
        return true;
    if (buf_id >= TOTAL_BUFFER_NUM)
        return false;
    if (state == NULL || state->total_granules <= 0 || state->granule_blocks <= 0)
        return buf_id < StrategyActiveBufferCount();

    granule_id = Min(buf_id / state->granule_blocks, state->total_granules - 1);
    token = gs_amm_read_granule_token(&state->granules[granule_id]);
    return token.state == GS_AMM_GRANULE_BUFFER_ACTIVE;
}

bool GsAmmBufferIdGetActiveToken(int buf_id, GsAmmGranuleToken *token)
{
    GsAmmSharedState *state = GsAmmState;
    int granule_id;

    if (token == NULL || buf_id < 0 || buf_id >= TOTAL_BUFFER_NUM)
        return false;
    token->state = GS_AMM_GRANULE_BUFFER_ACTIVE;
    token->owner_epoch = 0;
    if (buf_id >= NORMAL_SHARED_BUFFER_NUM)
        return true;
    if (state == NULL || state->total_granules <= 0 || state->granule_blocks <= 0)
        return buf_id < StrategyActiveBufferCount();

    granule_id = Min(buf_id / state->granule_blocks, state->total_granules - 1);
    *token = gs_amm_read_granule_token(&state->granules[granule_id]);
    return token->state == GS_AMM_GRANULE_BUFFER_ACTIVE;
}

bool GsAmmBufferTokenIsActive(int buf_id, GsAmmGranuleToken token)
{
    GsAmmSharedState *state = GsAmmState;
    int granule_id;
    bool matches;

    if (buf_id < 0 || buf_id >= TOTAL_BUFFER_NUM)
        return false;
    if (buf_id >= NORMAL_SHARED_BUFFER_NUM)
        return true;
    if (state == NULL || state->total_granules <= 0 || state->granule_blocks <= 0)
        return false;

    granule_id = Min(buf_id / state->granule_blocks, state->total_granules - 1);
    GsAmmGranuleToken current = gs_amm_read_granule_token(&state->granules[granule_id]);
    matches = current.state == GS_AMM_GRANULE_BUFFER_ACTIVE &&
        token.state == GS_AMM_GRANULE_BUFFER_ACTIVE &&
        current.owner_epoch == token.owner_epoch;
    if (!matches) {
        SpinLockAcquire(&state->mutex);
        state->epoch_mismatch_reject_count++;
        SpinLockRelease(&state->mutex);
    }
    return matches;
}

bool GsAmmBufferIdIsDraining(int buf_id)
{
    GsAmmSharedState *state = GsAmmState;
    int granule_id;
    GsAmmGranuleToken token;

    if (!GsAmmBufferIdIsInBufferGranule(buf_id))
        return false;
    if (state == NULL || state->total_granules <= 0 || state->granule_blocks <= 0)
        return false;

    granule_id = Min(buf_id / state->granule_blocks, state->total_granules - 1);
    token = gs_amm_read_granule_token(&state->granules[granule_id]);
    return token.state == GS_AMM_GRANULE_BUFFER_DRAINING;
}

bool GsAmmBufferNumberIsActive(Buffer buffer)
{
    return buffer != InvalidBuffer && GsAmmBufferIdIsActive((int)buffer - 1);
}

static void gs_amm_enqueue_dtree_feedback_locked(
    GsAmmSharedState *state,
    const double raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT],
    const double calibrated_bounds_kb[GS_AMM_DTREE_BOUND_COUNT],
    uint64 session_id,
    int64 model_version,
    int64 leaf_id,
    double observed_work_mem_kb,
    double runtime_ms,
    double spill_mb,
    uint64 spill_bytes,
    uint32 spill_files,
    uint32 spill_events,
    int hash_nbatch,
    int hash_multipass_count,
    double grant_mb,
    double tp_drop_ratio,
    double io_pressure,
    bool backpressure,
    bool error,
    bool measurement_valid,
    int64 sample_time)
{
    GsAmmDtreeFeedbackRing *ring = &state->dtree_feedback_ring;
    GsAmmDtreeCalibrationTable *table = &state->dtree_calibration_table;
    GsAmmDtreeCalibrationLeafState *leaf;
    GsAmmDtreeFeedbackSample *sample;
    int slot;

    slot = (int)(ring->next_sample_id % GS_AMM_DTREE_FEEDBACK_RING_SIZE);
    if (ring->feedback_sample_count >= GS_AMM_DTREE_FEEDBACK_RING_SIZE)
        ring->feedback_sample_dropped++;

    sample = &ring->samples[slot];
    sample->sample_id = ring->next_sample_id++;
    sample->session_id = session_id;
    sample->model_version = model_version;
    sample->leaf_id = leaf_id;
    leaf = gs_amm_dtree_leaf_state_locked(state, model_version, leaf_id, false);
    sample->calibration_version = leaf != NULL ? leaf->calibration_version : table->calibration_version;
    sample->calibration_scale = leaf != NULL ? leaf->calibration_scale : table->calibration_scale;
    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
        sample->raw_bounds_kb[i] = raw_bounds_kb[i];
        sample->calibrated_bounds_kb[i] = calibrated_bounds_kb[i];
    }
    sample->observed_work_mem_kb = observed_work_mem_kb;
    sample->runtime_ms = runtime_ms;
    sample->spill_mb = spill_mb;
    sample->spill_bytes = spill_bytes;
    sample->spill_files = spill_files;
    sample->spill_events = spill_events;
    sample->hash_nbatch = hash_nbatch;
    sample->hash_multipass_count = hash_multipass_count;
    sample->grant_mb = grant_mb;
    sample->tp_drop_ratio = tp_drop_ratio;
    sample->io_pressure = io_pressure;
    sample->backpressure = backpressure;
    sample->error = error;
    sample->measurement_valid = measurement_valid;
    sample->sample_time = sample_time;
    ring->feedback_sample_count++;
    table->feedback_sample_count++;
}

static void gs_amm_apply_dtree_feedback_locked(
    GsAmmSharedState *state, const GsAmmDtreeFeedbackSample *sample)
{
    GsAmmDtreeCalibrationTable *table = &state->dtree_calibration_table;
    GsAmmDtreeCalibrationLeafState *leaf;
    double target_scale;
    double current_scale;
    double next_scale;
    double spill_target_scale;
    double qerror;
    double underpredict_rate;
    bool tp_guard_hot;
    bool io_guard_hot;
    GsAmmRuntimeConfig runtime_config;

    leaf = gs_amm_dtree_leaf_state_locked(state, sample->model_version, sample->leaf_id, true);
    if (leaf == NULL)
        return;

    if (sample->error || sample->backpressure || !sample->measurement_valid) {
        leaf->bad_update_count++;
        if (sample->error)
            leaf->rollback_count++;
        return;
    }

    leaf->feedback_sample_count++;
    leaf->last_observed_work_mem_kb = sample->observed_work_mem_kb;
    leaf->ewma_runtime_ms = leaf->ewma_runtime_ms > 0.0 ?
        leaf->ewma_runtime_ms * 0.8 + sample->runtime_ms * 0.2 : sample->runtime_ms;
    leaf->ewma_spill_mb = leaf->ewma_spill_mb > 0.0 ?
        leaf->ewma_spill_mb * 0.8 + sample->spill_mb * 0.2 : sample->spill_mb;
    qerror = 1.0;
    if (sample->observed_work_mem_kb > 0.0 && sample->raw_bounds_kb[0] > 0.0) {
        double under = sample->observed_work_mem_kb / sample->raw_bounds_kb[0];
        double over = sample->raw_bounds_kb[0] / Max(sample->observed_work_mem_kb, 1.0);

        qerror = Max(under, over);
    }
    leaf->ewma_qerror = leaf->ewma_qerror > 0.0 ? leaf->ewma_qerror * 0.8 + qerror * 0.2 : qerror;
    underpredict_rate = (sample->spill_mb > 0.0 ||
        sample->observed_work_mem_kb > sample->calibrated_bounds_kb[0]) ? 1.0 : 0.0;
    leaf->ewma_underpredict_rate = leaf->ewma_underpredict_rate > 0.0 ?
        leaf->ewma_underpredict_rate * 0.8 + underpredict_rate * 0.2 : underpredict_rate;
    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
        leaf->last_raw_bounds_kb[i] = sample->raw_bounds_kb[i];
        leaf->last_calibrated_bounds_kb[i] = sample->calibrated_bounds_kb[i];
    }
    leaf->last_update_time = sample->sample_time;

    gs_amm_runtime_config_snapshot_locked(state, &runtime_config);
    if (!runtime_config.dtree_calibration_enabled || runtime_config.dtree_record_only)
        return;
    tp_guard_hot = gs_amm_tp_drop_guard_hot_locked(state);
    io_guard_hot = gs_amm_io_guard_hot_locked(state);
    if (sample->tp_drop_ratio >= gs_amm_tp_jitter_limit || tp_guard_hot) {
        table->rollback_count++;
        if (!leaf->frozen) {
            leaf->frozen = true;
            table->frozen_leaf_count++;
        }
        leaf->rollback_count++;
        return;
    }
    if (!(sample->observed_work_mem_kb > 0.0) || !(sample->raw_bounds_kb[0] > 0.0))
        return;
    if (leaf->frozen || leaf->feedback_sample_count < GS_AMM_DTREE_MIN_CALIBRATION_SAMPLES)
        return;

    target_scale = gs_amm_dtree_clamp_scale(sample->observed_work_mem_kb / sample->raw_bounds_kb[0]);
    spill_target_scale = gs_amm_dtree_clamp_scale(
        (sample->observed_work_mem_kb + sample->spill_mb * 1024.0 * GS_AMM_DTREE_SPILL_MEMORY_EQUIVALENT) /
            sample->raw_bounds_kb[0]);
    if (sample->spill_mb > 0.0)
        target_scale = Max(target_scale, spill_target_scale);
    current_scale = gs_amm_dtree_clamp_scale(leaf->calibration_scale);
    if (sample->spill_mb > 0.0 || sample->observed_work_mem_kb > sample->calibrated_bounds_kb[0])
        target_scale = Max(current_scale, target_scale);
    else
        target_scale = Min(current_scale, target_scale);
    if (sample->io_pressure >= (double)gs_amm_io_pressure_guard || io_guard_hot)
        target_scale = Max(target_scale, current_scale);
    if (sample->grant_mb > 0.0 && sample->observed_work_mem_kb < sample->grant_mb * 1024.0 * 0.5)
        target_scale = Min(target_scale, current_scale);

    next_scale = gs_amm_dtree_clamp_scale(current_scale * 0.8 + target_scale * 0.2);
    if (next_scale != current_scale) {
        table->calibration_version++;
        table->calibration_update_count++;
        leaf->calibration_scale = next_scale;
        leaf->calibration_version = table->calibration_version;
    }
}

void GsAmmDtreeCalibrationTick(void)
{
    if (!gs_amm_enabled)
        return;

    GsAmmSharedState *state = gs_amm_get_state();
    GsAmmDtreeFeedbackRing *ring;
    TimestampTz now = GetCurrentTimestamp();
    uint64 oldest_sample_id;
    int processed = 0;

    SpinLockAcquire(&state->mutex);
    ring = &state->dtree_feedback_ring;
    if (ring->last_calibration_tick > 0 &&
        now - ring->last_calibration_tick < (TimestampTz)GS_AMM_DTREE_CALIBRATION_TICK_MS * 1000) {
        SpinLockRelease(&state->mutex);
        return;
    }
    ring->last_calibration_tick = now;
    oldest_sample_id = ring->next_sample_id > GS_AMM_DTREE_FEEDBACK_RING_SIZE ?
        ring->next_sample_id - GS_AMM_DTREE_FEEDBACK_RING_SIZE : 0;
    if (ring->next_calibration_sample_id < oldest_sample_id) {
        ring->calibration_sample_dropped += oldest_sample_id - ring->next_calibration_sample_id;
        ring->next_calibration_sample_id = oldest_sample_id;
    }
    while (ring->next_calibration_sample_id < ring->next_sample_id &&
        processed < GS_AMM_DTREE_CALIBRATION_MAX_SAMPLES) {
        GsAmmDtreeFeedbackSample *sample =
            &ring->samples[ring->next_calibration_sample_id % GS_AMM_DTREE_FEEDBACK_RING_SIZE];

        if (sample->sample_id != ring->next_calibration_sample_id) {
            ring->calibration_sample_dropped++;
            ring->next_calibration_sample_id++;
            continue;
        }
        ring->next_calibration_sample_id++;
        gs_amm_apply_dtree_feedback_locked(state, sample);
        processed++;
    }
    SpinLockRelease(&state->mutex);
}

static void gs_amm_apply_feedback_only_completion_locked(GsAmmSharedState *state,
    const GsAmmFeedbackRecord *feedback)
{
    GsAmmRuntimeConfig runtime_config;
    bool hard_pressure;
    int granule_mb;

    gs_amm_runtime_config_snapshot_locked(state, &runtime_config);
    if (!runtime_config.feedback_only || !feedback->feedback_only)
        return;

    state->feedback_completed_count++;
    state->feedback_ewma_runtime_ms = state->feedback_ewma_runtime_ms > 0.0 ?
        state->feedback_ewma_runtime_ms * 0.8 + feedback->runtime_ms * 0.2 : feedback->runtime_ms;
    state->feedback_ewma_spill_mb = state->feedback_ewma_spill_mb > 0.0 ?
        state->feedback_ewma_spill_mb * 0.8 + feedback->spill_mb * 0.2 : feedback->spill_mb;
    state->feedback_ewma_peak_mb = state->feedback_ewma_peak_mb > 0.0 ?
        state->feedback_ewma_peak_mb * 0.8 + feedback->observed_work_mem_kb / 1024.0 * 0.2 :
        feedback->observed_work_mem_kb / 1024.0;
    if (!feedback->measurement_valid || feedback->backpressure) {
        gs_amm_copy_feedback_action_locked(state, "ignored");
        return;
    }

    hard_pressure = feedback->error || feedback->spill_mb >= runtime_config.feedback_spill_threshold_mb ||
        feedback->tp_drop_ratio >= gs_amm_tp_jitter_limit || gs_amm_tp_drop_guard_hot_locked(state) ||
        gs_amm_io_guard_hot_locked(state);
    granule_mb = Max(state->granule_mb, 1);
    if (hard_pressure) {
        state->feedback_ap_slot_limit = Max(1, state->feedback_ap_slot_limit / 2);
        state->feedback_current_grant_mb = Max(runtime_config.feedback_bootstrap_grant_mb,
            ((Max(state->feedback_current_grant_mb / 2, 1) + granule_mb - 1) / granule_mb) * granule_mb);
        state->feedback_stable_windows = 0;
        state->feedback_backoff_count++;
        gs_amm_copy_feedback_action_locked(state, "backoff");
        return;
    }

    state->feedback_stable_windows++;
    if (state->feedback_stable_windows < Max(runtime_config.feedback_stable_windows, 1)) {
        gs_amm_copy_feedback_action_locked(state, "stable");
        return;
    }

    state->feedback_stable_windows = 0;
    state->feedback_ap_slot_limit = Min(runtime_config.feedback_max_ap_slots,
        state->feedback_ap_slot_limit + 1);
    state->feedback_current_grant_mb = Min(runtime_config.feedback_max_grant_mb,
        state->feedback_current_grant_mb + granule_mb);
    state->feedback_growth_count++;
    gs_amm_copy_feedback_action_locked(state, "grow");
}

void GsAmmRecordFeedback(const GsAmmFeedbackRecord *feedback)
{
    GsAmmFeedbackRecord normalized;
    GsAmmSharedState *state;

    if (!gs_amm_enabled || feedback == NULL)
        return;

    normalized = *feedback;
    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
        if (!(normalized.raw_bounds_kb[i] > 0.0))
            normalized.raw_bounds_kb[i] = 0.0;
        if (!(normalized.calibrated_bounds_kb[i] > 0.0))
            normalized.calibrated_bounds_kb[i] = 0.0;
    }
    if (!(normalized.observed_work_mem_kb > 0.0))
        normalized.observed_work_mem_kb = 0.0;
    if (!(normalized.runtime_ms > 0.0))
        normalized.runtime_ms = 0.0;
    if (!(normalized.spill_mb > 0.0))
        normalized.spill_mb = 0.0;
    if (!(normalized.grant_mb > 0.0))
        normalized.grant_mb = 0.0;
    if (!(normalized.tp_drop_ratio > 0.0))
        normalized.tp_drop_ratio = 0.0;
    normalized.tp_drop_ratio = Min(normalized.tp_drop_ratio, 1.0);
    if (!(normalized.io_pressure > 0.0))
        normalized.io_pressure = 0.0;
    normalized.io_pressure = Min(normalized.io_pressure, 100.0);

    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    gs_amm_apply_feedback_only_completion_locked(state, &normalized);
    if (!normalized.feedback_only) {
        gs_amm_enqueue_dtree_feedback_locked(state, normalized.raw_bounds_kb, normalized.calibrated_bounds_kb,
            normalized.session_id, normalized.model_version, normalized.leaf_id, normalized.observed_work_mem_kb,
            normalized.runtime_ms,
            normalized.spill_mb, normalized.spill_bytes, normalized.spill_files, normalized.spill_events,
            normalized.hash_nbatch,
            normalized.hash_multipass_count, normalized.grant_mb, normalized.tp_drop_ratio, normalized.io_pressure,
            normalized.backpressure, normalized.error, normalized.measurement_valid, (int64)GetCurrentTimestamp());
    }
    SpinLockRelease(&state->mutex);
}

void GsAmmRecordNativeEligible(void)
{
    GsAmmSharedState *state = gs_amm_get_state();

    SpinLockAcquire(&state->mutex);
    state->native_eligible_count++;
    SpinLockRelease(&state->mutex);
}

void GsAmmRecordNativeFailure(const char *reason)
{
    GsAmmSharedState *state = gs_amm_get_state();

    SpinLockAcquire(&state->mutex);
    state->native_reject_count++;
    state->native_current_active = false;
    state->native_current_grant_id = 0;
    state->native_current_grant_generation = 0;
    state->native_current_granted_kb = 0;
    state->native_current_memory_mode = GS_AMM_MEMORY_MODE_BACKPRESSURE;
    state->last_action = GS_AMM_FAIL_CLOSED;
    gs_amm_copy_admission_reason(state->native_last_reason, reason);
    SpinLockRelease(&state->mutex);
}

void GsAmmRecordNativeAdmission(const GsAmmDtreeDetail *detail, const GsAmmAdmissionResult *result)
{
    GsAmmSharedState *state;

    if (detail == NULL || result == NULL)
        return;

    if (result->admitted && MyGsAmmGrantId == result->grant_id &&
        MyGsAmmGrantGeneration == result->grant_generation) {
        MyGsAmmGrantNative = true;
        MyGsAmmTransactionHasNativeAP = true;
    }

    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    if (result->admitted) {
        state->native_admit_count++;
        state->native_active_grant_count++;
    } else {
        state->native_reject_count++;
    }
    state->native_current_active = result->admitted;
    state->native_current_grant_id = result->admitted ? result->grant_id : 0;
    state->native_current_grant_generation = result->admitted ? result->grant_generation : 0;
    state->native_current_granted_kb = result->admitted ? result->granted_kb : 0;
    state->native_current_memory_mode = result->memory_mode;
    state->native_current_model_version = detail->model_version;
    state->native_current_leaf_id = detail->leaf_id;
    state->native_current_calibration_version = detail->calibration_version;
    state->native_current_calibration_scale = detail->calibration_scale;
    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
        state->native_current_raw_bounds_kb[i] = detail->raw_bounds_kb[i];
        state->native_current_calibrated_bounds_kb[i] = detail->calibrated_bounds_kb[i];
    }
    gs_amm_copy_admission_reason(state->native_last_reason, result->reason);
    SpinLockRelease(&state->mutex);
}

void GsAmmRecordNativeErrorCleanup(void)
{
    GsAmmSharedState *state = gs_amm_get_state();

    SpinLockAcquire(&state->mutex);
    state->native_error_cleanup_count++;
    SpinLockRelease(&state->mutex);
}

void GsAmmRecordTransactionCommit(void)
{
    if (!gs_amm_enabled) {
        MyGsAmmTransactionHasNativeAP = false;
        return;
    }

    GsAmmSharedState *state;

    if (MyGsAmmTransactionHasNativeAP) {
        MyGsAmmTransactionHasNativeAP = false;
        return;
    }

    state = gs_amm_get_state();
    pg_atomic_fetch_add_u64(&state->tp_commit_count, 1);
}

void GsAmmRecordTransactionAbort(void)
{
    MyGsAmmTransactionHasNativeAP = false;
}

void GsAmmRecordSharedBufferReadMiss(void)
{
    if (!gs_amm_enabled)
        return;

    GsAmmSharedState *state = gs_amm_get_state();

    pg_atomic_fetch_add_u64(&state->shared_buffer_read_miss_count, 1);
}

void GsAmmRecordSharedBufferPhysicalRead(void)
{
    if (!gs_amm_enabled)
        return;

    GsAmmSharedState *state = gs_amm_get_state();

    pg_atomic_fetch_add_u64(&state->shared_buffer_physical_read_count, 1);
}

static void gs_amm_saturating_subtract_u64(
    pg_atomic_uint64 *counter, uint64 amount, pg_atomic_uint64 *underflow_counter)
{
    uint64 previous = pg_atomic_read_u64(counter);

    while (previous > 0) {
        uint64 next = previous > amount ? previous - amount : 0;

        if (pg_atomic_compare_exchange_u64(counter, &previous, next)) {
            if (previous < amount)
                pg_atomic_fetch_add_u64(underflow_counter, 1);
            return;
        }
    }
    if (amount > 0)
        pg_atomic_fetch_add_u64(underflow_counter, 1);
}

void GsAmmRecordApSpill(uint64 spill_bytes, uint32 spill_count)
{
    if (!gs_amm_enabled)
        return;

    GsAmmSharedState *state = gs_amm_get_state();

    if (spill_bytes > 0)
        pg_atomic_fetch_add_u64(&state->ap_temp_spill_bytes, spill_bytes);
    if (spill_count > 0)
        pg_atomic_fetch_add_u64(&state->ap_temp_spill_events, spill_count);
}

void GsAmmRecordApExecutionSignals(uint64 spill_bytes, uint32 spill_count, int hash_multipass_count)
{
    if (!gs_amm_enabled)
        return;

    GsAmmRecordApSpill(spill_bytes, spill_count);
    if (hash_multipass_count > 0) {
        GsAmmSharedState *state = gs_amm_get_state();

        pg_atomic_fetch_add_u64(&state->ap_hash_multipass_count, (uint64)hash_multipass_count);
    }
}

void GsAmmRecordPendingWritebackEnqueue(uint32 pages)
{
    if (!gs_amm_enabled || pages == 0)
        return;

    pg_atomic_fetch_add_u64(&gs_amm_get_state()->pending_writeback_page_count, pages);
}

void GsAmmRecordPendingWritebackRetire(uint32 pages)
{
    if (!gs_amm_enabled || pages == 0)
        return;

    GsAmmSharedState *state = gs_amm_get_state();

    gs_amm_saturating_subtract_u64(
        &state->pending_writeback_page_count, pages, &state->pending_writeback_retire_underflow_count);
}

void GsAmmRecordWritebackFlushComplete(uint32 pages)
{
    if (!gs_amm_enabled || pages == 0)
        return;

    pg_atomic_fetch_add_u64(&gs_amm_get_state()->writeback_flush_completed_pages, pages);
}

void GsAmmRecordDirtyPageEnqueue(void)
{
    if (!gs_amm_enabled)
        return;

    pg_atomic_fetch_add_u64(&gs_amm_get_state()->dirty_page_count, 1);
}

void GsAmmRecordDirtyPageDequeue(void)
{
    if (!gs_amm_enabled)
        return;

    GsAmmSharedState *state = gs_amm_get_state();

    gs_amm_saturating_subtract_u64(&state->dirty_page_count, 1, &state->pending_writeback_retire_underflow_count);
}

static void gs_amm_summarize_granules_locked(GsAmmSharedState *state, GsAmmGranuleSummary *summary)
{
    errno_t rc = memset_s(summary, sizeof(*summary), 0, sizeof(*summary));

    securec_check(rc, "\0", "\0");
    summary->total_granules = state->total_granules;
    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];

        switch (granule->state) {
            case GS_AMM_GRANULE_BUFFER_ACTIVE:
                summary->buffer_active_granules++;
                summary->buffer_active_blocks += granule->buffer_count;
                break;
            case GS_AMM_GRANULE_BUFFER_DRAINING:
                summary->buffer_draining_granules++;
                break;
            case GS_AMM_GRANULE_RECLAIMING:
                summary->reclaiming_granules++;
                break;
            case GS_AMM_GRANULE_FREE:
                summary->free_granules++;
                break;
            case GS_AMM_GRANULE_AP_RESERVED:
                summary->ap_reserved_granules++;
                summary->ap_grant_bytes += granule->active_grant_bytes;
                summary->ap_granule_used_bytes += granule->used_bytes;
                summary->ap_granule_alloc_cursor_bytes += granule->alloc_cursor_bytes;
                break;
            case GS_AMM_GRANULE_AP_ACTIVE:
                summary->ap_active_granules++;
                summary->ap_grant_bytes += granule->active_grant_bytes;
                summary->ap_granule_used_bytes += granule->used_bytes;
                summary->ap_granule_alloc_cursor_bytes += granule->alloc_cursor_bytes;
                break;
            default:
                break;
        }
    }
}

static uint64 gs_amm_check_granule_invariants_locked(GsAmmSharedState *state)
{
    uint64 violations = 0;

    for (int i = 0; i < state->total_granules; i++) {
        int first_buffer_id;
        int buffer_count;
        uint32 owner_epoch;
        GsAmmGranuleState granule_state;
        bool verify;
        uint64 granule_violations = 0;

        SpinLockAcquire(&state->mutex);
        const GsAmmGranuleMeta *granule = &state->granules[i];
        granule_state = granule->state;
        owner_epoch = granule->owner_epoch;
        first_buffer_id = granule->first_buffer_id;
        buffer_count = granule->buffer_count;
        verify = granule_state == GS_AMM_GRANULE_FREE ||
            granule_state == GS_AMM_GRANULE_AP_RESERVED ||
            granule_state == GS_AMM_GRANULE_AP_ACTIVE;
        SpinLockRelease(&state->mutex);
        if (!verify)
            continue;

        for (int offset = 0; offset < buffer_count; offset++) {
            BufferDesc *buf = GetBufferDescriptor(first_buffer_id + offset);
            uint64 buf_state = pg_atomic_read_u64(&buf->state);

            if (buf_state & (BM_VALID | BM_TAG_VALID))
                granule_violations++;
        }

        SpinLockAcquire(&state->mutex);
        granule = &state->granules[i];
        if (granule->state == granule_state && granule->owner_epoch == owner_epoch)
            violations += granule_violations;
        SpinLockRelease(&state->mutex);
    }
    return violations;
}

static void gs_amm_record_pool_event_locked(GsAmmSharedState *state, GsAmmAction action,
    const char *reason, const GsAmmGranuleSummary *before, TimestampTz started)
{
    GsAmmGranuleSummary after;
    TimestampTz now;
    int64 elapsed_us;
    int reason_rc;

    if (state == NULL || before == NULL)
        return;
    if (state->next_pool_event_id == PG_UINT64_MAX) {
        state->illegal_transition_count++;
        return;
    }

    now = GetCurrentTimestamp();
    gs_amm_summarize_granules_locked(state, &after);
    state->next_pool_event_id++;
    state->last_pool_event_id = state->next_pool_event_id;
    state->last_pool_event_time = now;
    state->last_pool_event_action = action;
    elapsed_us = now > started ? now - started : 0;
    state->last_pool_event_duration_ms =
        elapsed_us / 1000 > INT_MAX ? INT_MAX : (int)(elapsed_us / 1000);
    state->last_pool_event_before = *before;
    state->last_pool_event_after = after;
    reason_rc = snprintf_s(state->last_pool_event_reason, sizeof(state->last_pool_event_reason),
        sizeof(state->last_pool_event_reason) - 1, "%s", reason == NULL ? "none" : reason);
    securec_check_ss(reason_rc, "\0", "\0");
}

static bool gs_amm_state_can_reset_locked(const GsAmmSharedState *state)
{
    if (state->active_ap_count > 0 || state->dynamic_used_mb > 0 || state->ap_queue_len > 0 ||
        state->native_active_grant_count > 0 || state->native_current_active ||
        state->native_current_grant_id != 0 || state->native_current_grant_generation != 0 ||
        state->reclaim_syscall_inflight)
        return false;

    for (int i = 0; i < state->total_granules; i++) {
        const GsAmmGranuleMeta *granule = &state->granules[i];

        if (granule->state == GS_AMM_GRANULE_BUFFER_DRAINING ||
            granule->state == GS_AMM_GRANULE_RECLAIMING ||
            granule->state == GS_AMM_GRANULE_AP_ACTIVE ||
            granule->state == GS_AMM_GRANULE_AP_RESERVED ||
            granule->scan_inflight || granule->reclaim_inflight ||
            granule->grant_id != 0 || granule->grant_generation != 0)
            return false;
        if (!GsAmmGranuleCountersCanAdvance(granule->generation, granule->owner_epoch, true))
            return false;
    }
    return true;
}

static TimestampTz gs_amm_timestamp_after_ms(TimestampTz start, int ms)
{
    if (ms <= 0)
        return start;
    return start + (TimestampTz)ms * 1000;
}

static int64 gs_amm_blocks_to_mb(int blocks)
{
    return ((int64)blocks * BLCKSZ) / (1024 * 1024);
}

static int gs_amm_mb_to_blocks(int target_mb)
{
    int64 blocks = ((int64)target_mb * 1024 * 1024) / BLCKSZ;

    if (blocks <= 0 || blocks > INT_MAX)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("shared buffers target is outside the supported range")));

    return (int)blocks;
}

static int gs_amm_resize_granule_blocks(void)
{
    if (GsAmmState != NULL && GsAmmState->granule_blocks > 0)
        return GsAmmState->granule_blocks;
    return gs_amm_configured_granule_blocks();
}

static int gs_amm_align_resize_target_blocks(int target_blocks, int max_blocks)
{
    int granule_blocks = gs_amm_resize_granule_blocks();
    int64 aligned_blocks;

    if (granule_blocks <= 1)
        return Max(1, Min(target_blocks, max_blocks));

    aligned_blocks = ((int64)target_blocks + granule_blocks - 1) / granule_blocks * granule_blocks;
    if (aligned_blocks < granule_blocks)
        aligned_blocks = granule_blocks;
    if (aligned_blocks > max_blocks)
        aligned_blocks = max_blocks;
    return (int)aligned_blocks;
}

static int gs_amm_dynamic_target_default_mb(void)
{
    return Max(gs_amm_dynamic_target_mb, gs_amm_ap_min_grant_mb);
}

static int gs_amm_allocator_only_grant_target_mb(const GsAmmRuntimeConfig *runtime_config)
{
    return Max(runtime_config->allocator_only_grant_mb, gs_amm_ap_min_grant_mb);
}

static int gs_amm_effective_controller_demand_mb(int prediction_mb, const GsAmmRuntimeConfig *runtime_config)
{
    prediction_mb = Max(prediction_mb, 0);
    if (!runtime_config->allocator_only_mode)
        return prediction_mb;
    return gs_amm_allocator_only_grant_target_mb(runtime_config);
}

static int gs_amm_effective_ap_request_mb(
    int prediction_mb, int min_mb, int max_mb, const GsAmmRuntimeConfig *runtime_config)
{
    int request_mb = Max(prediction_mb, 0);

    if (runtime_config->allocator_only_mode)
        request_mb = Max(gs_amm_allocator_only_grant_target_mb(runtime_config), min_mb);
    return Min(request_mb, max_mb);
}

static int gs_amm_effective_ap_request_kb(const GsAmmRuntimeConfig *runtime_config)
{
    int target_mb = gs_amm_allocator_only_grant_target_mb(runtime_config);

    if (target_mb > INT_MAX / 1024)
        target_mb = INT_MAX / 1024;
    return target_mb * 1024;
}

static int gs_amm_queue_limit(void)
{
    return Min(Max(gs_amm_ap_queue_limit, 0), GS_AMM_QUEUE_RING_SIZE - 1);
}

static int gs_amm_queue_slot(uint64 ticket)
{
    return (int)(ticket % GS_AMM_QUEUE_RING_SIZE);
}

static void gs_amm_queue_advance_head_locked(GsAmmSharedState *state)
{
    while (state->ap_queue_head < state->ap_queue_tail) {
        uint64 next_ticket = state->ap_queue_head + 1;
        int slot = gs_amm_queue_slot(next_ticket);

        if (state->ap_queue_slots[slot] == GS_AMM_QUEUE_WAITING)
            break;
        state->ap_queue_slots[slot] = 0;
        state->ap_queue_head = next_ticket;
    }
}

static bool gs_amm_queue_register_locked(GsAmmSharedState *state, uint64 *ticket)
{
    int slot;

    gs_amm_queue_advance_head_locked(state);
    if (state->ap_queue_len >= gs_amm_queue_limit())
        return false;
    if (state->ap_queue_tail - state->ap_queue_head >= GS_AMM_QUEUE_RING_SIZE - 1)
        return false;

    state->ap_queue_tail++;
    slot = gs_amm_queue_slot(state->ap_queue_tail);
    state->ap_queue_slots[slot] = GS_AMM_QUEUE_WAITING;
    state->ap_queue_len++;
    *ticket = state->ap_queue_tail;
    return true;
}

static bool gs_amm_queue_is_head_locked(GsAmmSharedState *state, uint64 ticket)
{
    gs_amm_queue_advance_head_locked(state);
    if (ticket != state->ap_queue_head + 1)
        return false;
    return state->ap_queue_slots[gs_amm_queue_slot(ticket)] == GS_AMM_QUEUE_WAITING;
}

static void gs_amm_queue_finish_locked(GsAmmSharedState *state, uint64 ticket, bool admitted, int wait_ms)
{
    int slot;

    if (ticket == 0)
        return;

    slot = gs_amm_queue_slot(ticket);
    if (state->ap_queue_slots[slot] == GS_AMM_QUEUE_WAITING && state->ap_queue_len > 0)
        state->ap_queue_len--;
    if (admitted) {
        state->ap_queue_slots[slot] = 0;
        if (ticket >= state->ap_queue_head)
            state->ap_queue_head = ticket;
        state->ap_queue_admit_count++;
    } else {
        state->ap_queue_slots[slot] = GS_AMM_QUEUE_CANCELLED;
        state->ap_queue_timeout_count++;
    }
    state->last_queue_wait_ms = wait_ms;
    gs_amm_queue_advance_head_locked(state);
}

typedef struct GsAmmTpWindowSnapshot {
    uint64 generation;
    int sample_next;
    int sample_count;
    TimestampTz sample_time[GS_AMM_TP_WINDOW_SAMPLE_COUNT];
    double sample_tps[GS_AMM_TP_WINDOW_SAMPLE_COUNT];
    double sample_p95_latency_ms[GS_AMM_TP_WINDOW_SAMPLE_COUNT];
    double baseline_tps;
    int active_ap_count;
    int dynamic_used_mb;
    int ap_queue_len;
    TimestampTz recovery_cooldown_until;
    TimestampTz last_resize_time;
} GsAmmTpWindowSnapshot;

static void gs_amm_snapshot_tp_window_locked(GsAmmSharedState *state, GsAmmTpWindowSnapshot *snapshot)
{
    snapshot->generation = state->tp_sample_generation;
    snapshot->sample_next = state->tp_sample_next;
    snapshot->sample_count = state->tp_sample_count;
    snapshot->baseline_tps = state->tp_baseline_tps;
    snapshot->active_ap_count = state->active_ap_count;
    snapshot->dynamic_used_mb = state->dynamic_used_mb;
    snapshot->ap_queue_len = state->ap_queue_len;
    snapshot->recovery_cooldown_until = state->recovery_cooldown_until;
    snapshot->last_resize_time = state->last_resize_time;
    for (int index = 0; index < GS_AMM_TP_WINDOW_SAMPLE_COUNT; index++) {
        snapshot->sample_time[index] = state->tp_sample_time[index];
        snapshot->sample_tps[index] = state->tp_sample_tps[index];
        snapshot->sample_p95_latency_ms[index] = state->tp_sample_p95_latency_ms[index];
    }
}

static void gs_amm_append_tp_window_snapshot(
    GsAmmTpWindowSnapshot *snapshot, TimestampTz now, double tps, double p95_latency_ms)
{
    int slot = snapshot->sample_next;

    snapshot->sample_time[slot] = now;
    snapshot->sample_tps[slot] = tps;
    snapshot->sample_p95_latency_ms[slot] = p95_latency_ms;
    snapshot->sample_next = (snapshot->sample_next + 1) % GS_AMM_TP_WINDOW_SAMPLE_COUNT;
    if (snapshot->sample_count < GS_AMM_TP_WINDOW_SAMPLE_COUNT)
        snapshot->sample_count++;
}

static void gs_amm_compute_tp_window_snapshot(const GsAmmTpWindowSnapshot *snapshot, TimestampTz now, int window_ms,
    double *window_tps, double *window_p95_latency_ms)
{
    TimestampTz cutoff = window_ms > 0 ? now - (TimestampTz)window_ms * 1000 : now;
    double tps_sum = 0.0;
    double p95_max = 0.0;
    int count = 0;

    for (int index = 0; index < snapshot->sample_count; index++) {
        TimestampTz sample_time = snapshot->sample_time[index];

        if (sample_time <= 0 || (window_ms > 0 && sample_time < cutoff))
            continue;
        tps_sum += snapshot->sample_tps[index];
        p95_max = Max(p95_max, snapshot->sample_p95_latency_ms[index]);
        count++;
    }
    if (count <= 0) {
        int latest_slot = (snapshot->sample_next + GS_AMM_TP_WINDOW_SAMPLE_COUNT - 1) %
            GS_AMM_TP_WINDOW_SAMPLE_COUNT;

        *window_tps = snapshot->sample_tps[latest_slot];
        *window_p95_latency_ms = snapshot->sample_p95_latency_ms[latest_slot];
        return;
    }
    *window_tps = tps_sum / (double)count;
    *window_p95_latency_ms = p95_max;
}

static bool gs_amm_tp_window_snapshot_mature(const GsAmmTpWindowSnapshot *snapshot, TimestampTz now, int window_ms)
{
    TimestampTz cutoff;
    TimestampTz oldest = 0;
    TimestampTz newest = 0;
    TimestampTz required_span;
    int count = 0;

    if (window_ms <= 0)
        return true;
    cutoff = now - (TimestampTz)window_ms * 1000;
    for (int index = 0; index < snapshot->sample_count; index++) {
        TimestampTz sample_time = snapshot->sample_time[index];

        if (sample_time <= 0 || sample_time < cutoff)
            continue;
        if (oldest == 0 || sample_time < oldest)
            oldest = sample_time;
        if (newest == 0 || sample_time > newest)
            newest = sample_time;
        count++;
    }
    if (count <= 1 || oldest == 0 || newest == 0)
        return false;

    required_span = (TimestampTz)Max(window_ms - 1000, window_ms / 2) * 1000;
    return newest - oldest >= required_span;
}

static double gs_amm_tp_baseline_candidate(const GsAmmTpWindowSnapshot *snapshot, TimestampTz now,
    bool tp_window_mature, double tp_recent_tps, bool *rebased)
{
    double baseline_tps = snapshot->baseline_tps;

    *rebased = false;
    if (tp_window_mature && (baseline_tps <= 0.0 || tp_recent_tps > baseline_tps))
        baseline_tps = tp_recent_tps;
    if (!tp_window_mature || !(tp_recent_tps > 0.0) || !(baseline_tps > tp_recent_tps))
        return baseline_tps;
    if (snapshot->active_ap_count != 0 || snapshot->dynamic_used_mb != 0 || snapshot->ap_queue_len != 0 ||
        snapshot->recovery_cooldown_until > now)
        return baseline_tps;
    if (snapshot->last_resize_time > 0 &&
        now < gs_amm_timestamp_after_ms(snapshot->last_resize_time, GS_AMM_TP_BASELINE_REBASE_IDLE_MS))
        return baseline_tps;

    *rebased = true;
    return Max(tp_recent_tps, baseline_tps * (1.0 - GS_AMM_TP_BASELINE_REBASE_ALPHA) +
        tp_recent_tps * GS_AMM_TP_BASELINE_REBASE_ALPHA);
}

static bool gs_amm_tp_drop_guard_hot_locked(GsAmmSharedState *state)
{
    if (gs_amm_tp_jitter_limit <= 0.0)
        return false;
    if (state->tp_baseline_tps < GS_AMM_TP_GUARD_MIN_BASELINE_TPS)
        return false;
    return state->tp_raw_drop_ratio >= gs_amm_tp_jitter_limit;
}

static bool gs_amm_io_guard_hot_locked(GsAmmSharedState *state)
{
    return state->io_pressure_observed >= gs_amm_io_pressure_guard || state->io_guard_latched;
}

static bool gs_amm_advance_tp_sample_generation_locked(GsAmmSharedState *state)
{
    if (state->tp_sample_generation == PG_UINT64_MAX) {
        state->tp_generation_exhausted = true;
        return false;
    }
    state->tp_sample_generation++;
    return true;
}

static void gs_amm_update_io_recovery_locked(
    GsAmmSharedState *state, TimestampTz now, bool recovery_sample_complete)
{
    if (state->io_pressure_observed >= gs_amm_io_pressure_guard) {
        bool was_latched = state->io_guard_latched;
        GsAmmGranuleSummary event_before;

        if (!was_latched)
            gs_amm_summarize_granules_locked(state, &event_before);
        state->io_guard_latched = true;
        state->io_recovery_stable_windows = 0;
        state->io_recovery_last_window = now;
        if (!was_latched)
            gs_amm_record_pool_event_locked(state, GS_AMM_FAIL_CLOSED, "io_guard_activated", &event_before, now);
        return;
    }
    if (!state->io_guard_latched)
        return;
    if (!recovery_sample_complete) {
        state->io_recovery_stable_windows = 0;
        state->io_recovery_last_window = now;
        return;
    }
    if (state->io_recovery_last_window > 0 &&
        now - state->io_recovery_last_window < GS_AMM_NATIVE_TP_WINDOW_MS * 1000)
        return;

    state->io_recovery_last_window = now;
    state->io_recovery_stable_windows++;
    if (state->io_recovery_stable_windows >= 2) {
        GsAmmGranuleSummary event_before;

        gs_amm_summarize_granules_locked(state, &event_before);
        state->io_guard_latched = false;
        state->recovery_cooldown_until =
            gs_amm_timestamp_after_ms(now, Max(gs_amm_tp_recovery_cooldown_ms, gs_amm_resize_cooldown_ms));
        gs_amm_record_pool_event_locked(state, GS_AMM_OBSERVE, "io_guard_released", &event_before, now);
    }
}

void GsAmmGetFeedbackTelemetry(double *tp_drop_ratio, double *io_pressure)
{
    GsAmmSharedState *state = gs_amm_get_state();

    if (tp_drop_ratio == NULL || io_pressure == NULL)
        return;

    SpinLockAcquire(&state->mutex);
    *tp_drop_ratio = Max(0.0, Min(state->tp_raw_drop_ratio, 1.0));
    *io_pressure = Max(0.0, Min((double)state->io_pressure_observed, 100.0));
    SpinLockRelease(&state->mutex);
}

double GsAmmCurrentMemoryPressureScore(void)
{
    GsAmmSharedState *state = gs_amm_get_state();
    double dynamic_pressure = 0.0;
    double tp_pressure = 0.0;
    double io_pressure = 0.0;
    double score;

    SpinLockAcquire(&state->mutex);
    if (state->tp_generation_exhausted) {
        SpinLockRelease(&state->mutex);
        return 1.0;
    }
    if (state->dynamic_target_mb > 0)
        dynamic_pressure = (double)Max(state->dynamic_used_mb, 0) / (double)state->dynamic_target_mb;
    else if (state->dynamic_used_mb > 0)
        dynamic_pressure = 1.0;

    tp_pressure = (double)Max(state->last_tp_pressure, 0) / 100.0;
    if (gs_amm_tp_drop_guard_hot_locked(state))
        tp_pressure = 1.0;

    io_pressure = (double)Max(Max(state->last_io_pressure, state->io_pressure_observed), 0) / 100.0;
    if (gs_amm_io_guard_hot_locked(state))
        io_pressure = 1.0;
    score = Max(dynamic_pressure, Max(tp_pressure, io_pressure));
    SpinLockRelease(&state->mutex);

    return Max(0.0, Min(score, 1.0));
}

static bool gs_amm_recent_tps_drop_blocks_resize_locked(GsAmmSharedState *state, TimestampTz now)
{
    if (state->tp_generation_exhausted)
        return true;
    if (state->cooldown_until > now)
        return true;
    if (gs_amm_recovery_cooldown_hot_locked(state, now))
        return true;
    return gs_amm_tp_drop_guard_hot_locked(state) || gs_amm_io_guard_hot_locked(state);
}

static bool gs_amm_recovery_cooldown_hot_locked(GsAmmSharedState *state, TimestampTz now)
{
    return gs_amm_tp_recovery_cooldown_ms > 0 && state->recovery_cooldown_until > now;
}

static const char *gs_amm_new_ap_block_reason_locked(GsAmmSharedState *state, TimestampTz now)
{
    if (state->tp_generation_exhausted)
        return "telemetry_generation_exhausted";
    if (state->cooldown_until > now)
        return "resize_cooldown";
    if (gs_amm_tp_drop_guard_hot_locked(state))
        return "tps_guard";
    if (gs_amm_io_guard_hot_locked(state))
        return "io_pressure";
    if (gs_amm_recovery_cooldown_hot_locked(state, now))
        return "recovery_cooldown";
    if (state->last_tp_pressure >= gs_amm_tp_pressure_guard)
        return "tp_pressure";
    if (state->last_io_pressure >= gs_amm_io_pressure_guard)
        return "io_pressure";
    return "";
}

bool GsAmmEvaluateAdmission(int prediction_mb)
{
    if (!gs_amm_enabled)
        return false;

    GsAmmSharedState *state = gs_amm_get_state();
    const char *block_reason;
    bool allowed;

    SpinLockAcquire(&state->mutex);
    state->last_prediction_mb = Max(prediction_mb, 0);
    block_reason = gs_amm_new_ap_block_reason_locked(state, GetCurrentTimestamp());
    allowed = block_reason[0] == '\0';
    if (!allowed) {
        state->last_action = GS_AMM_BACKPRESSURE;
        gs_amm_set_backpressure_reason_locked(state, block_reason);
    }
    SpinLockRelease(&state->mutex);
    return allowed;
}

static const char *gs_amm_resize_guard_state(GsAmmSharedState *state, TimestampTz now)
{
    if (state->tp_generation_exhausted)
        return "telemetry_generation_exhausted";
    if (state->cooldown_until > now)
        return "cooldown";
    if (gs_amm_recovery_cooldown_hot_locked(state, now))
        return "recovery_cooldown";
    if (gs_amm_tp_drop_guard_hot_locked(state))
        return "hot";
    if (gs_amm_io_guard_hot_locked(state))
        return "io_hot";
    if (state->resize_observe_until >= now)
        return "observing";
    return "ready";
}

static int gs_amm_compute_io_pressure(double physical_read_rate, uint64 pending_writeback_pages, double dirty_page_ratio,
    int device_io_in_flight, double device_io_ms_rate, double temp_spill_mb_rate, double hash_multipass_rate)
{
    double read_score;
    double spill_score;
    double writeback_score;
    double dirty_score;
    double device_queue_score;
    double device_busy_score;
    double operator_score;
    double score;

    physical_read_rate = Max(physical_read_rate, 0.0);
    pending_writeback_pages = Max(pending_writeback_pages, (uint64)0);
    dirty_page_ratio = Max(dirty_page_ratio, 0.0);
    device_io_in_flight = Max(device_io_in_flight, 0);
    device_io_ms_rate = Max(device_io_ms_rate, 0.0);
    temp_spill_mb_rate = Max(temp_spill_mb_rate, 0.0);
    hash_multipass_rate = Max(hash_multipass_rate, 0.0);

    read_score = Min(25.0, physical_read_rate / 100.0 * 25.0);
    writeback_score = Min(20.0, (double)pending_writeback_pages / 1024.0 * 20.0);
    dirty_score = Min(15.0, dirty_page_ratio / 0.20 * 15.0);
    device_queue_score = Min(15.0, (double)device_io_in_flight / 4.0 * 15.0);
    device_busy_score = Min(15.0, device_io_ms_rate / 1000.0 * 15.0);
    spill_score = Min(15.0, temp_spill_mb_rate / 20.0 * 15.0);
    operator_score = Min(15.0, hash_multipass_rate * 15.0);
    score = read_score + writeback_score + dirty_score + device_queue_score + device_busy_score + spill_score + operator_score;
    return Max(0, Min(100, (int)(score + 0.5)));
}

static void gs_amm_set_backpressure_reason_locked(GsAmmSharedState *state, const char *reason)
{
    int rc;

    if (reason == NULL || reason[0] == '\0')
        reason = "capacity";
    rc = snprintf_s(state->last_backpressure_reason, sizeof(state->last_backpressure_reason),
        sizeof(state->last_backpressure_reason) - 1, "%s", reason);
    securec_check_ss(rc, "\0", "\0");
}

static int gs_amm_clamp_grant_mb(int requested_mb, int free_mb)
{
    int grant_mb;

    requested_mb = Max(requested_mb, gs_amm_ap_min_grant_mb);
    grant_mb = Min(requested_mb, Max(free_mb, 0));
    if (grant_mb < gs_amm_ap_min_grant_mb)
        grant_mb = 0;
    return grant_mb;
}

static void gs_amm_apply_backend_work_mem(int grant_kb)
{
    char value[32];
    int rc;

    if (grant_kb <= 0)
        return;

    gs_amm_register_backend_cleanup();
    if (MyGsAmmSavedWorkMemKb <= 0)
        MyGsAmmSavedWorkMemKb = u_sess->attr.attr_memory.work_mem;
    rc = snprintf_s(value, sizeof(value), sizeof(value) - 1, "%dkB", grant_kb);
    securec_check_ss(rc, "\0", "\0");
    (void)set_config_option("work_mem", value, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
}

static void gs_amm_apply_backend_grant(int grant_mb)
{
    MyGsAmmGrantMb = Max(grant_mb, 0);
    MyGsAmmGrantKb = MyGsAmmGrantMb * 1024;
    gs_amm_apply_backend_work_mem(MyGsAmmGrantKb);
}

static void gs_amm_apply_backend_grant_kb(int grant_kb)
{
    MyGsAmmGrantKb = Max(grant_kb, 0);
    MyGsAmmGrantMb = MyGsAmmGrantKb > 0 ? (MyGsAmmGrantKb + 1023) / 1024 : 0;
    gs_amm_apply_backend_work_mem(MyGsAmmGrantKb);
}

static void gs_amm_restore_backend_work_mem(int saved_work_mem_kb)
{
    char value[32];
    int rc;

    if (saved_work_mem_kb <= 0)
        return;

    rc = snprintf_s(value, sizeof(value), sizeof(value) - 1, "%dkB", saved_work_mem_kb);
    securec_check_ss(rc, "\0", "\0");
    (void)set_config_option("work_mem", value, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SET, true, ERROR);
}

static void gs_amm_clear_backend_grant_state(void)
{
    MyGsAmmGrantMb = 0;
    MyGsAmmGrantKb = 0;
    MyGsAmmGrantId = 0;
    MyGsAmmGrantGeneration = 0;
    MyGsAmmGrantGranules = 0;
    MyGsAmmGrantNative = false;
    MyGsAmmSavedWorkMemKb = 0;
}

int GsAmmCurrentBackendGrantKB(void)
{
    GsAmmSharedState *state;
    int effective_grant_kb;

    if (MyGsAmmGrantKb <= 0)
        return 0;

    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    effective_grant_kb = state->effective_grant_kb;
    SpinLockRelease(&state->mutex);

    if (effective_grant_kb > 0)
        return Min(MyGsAmmGrantKb, effective_grant_kb);
    return MyGsAmmGrantKb;
}

uint64 GsAmmCurrentBackendGrantId(void)
{
    return MyGsAmmGrantId;
}

uint64 GsAmmCurrentBackendGrantGeneration(void)
{
    return MyGsAmmGrantGeneration;
}

uint64 GsAmmCurrentBackendGrantBytes(void)
{
    int grant_kb = GsAmmCurrentBackendGrantKB();

    return grant_kb > 0 ? (uint64)grant_kb * 1024 : 0;
}

uint64 GsAmmCurrentBackendGrantPoolBytes(void)
{
    if (MyGsAmmGrantId == 0 || MyGsAmmGrantMb <= 0)
        return 0;

    return (uint64)MyGsAmmGrantMb * 1024 * 1024;
}

uint64 GsAmmGrantEffectiveMemoryLimit(uint64 grant_id, uint64 requested_max_bytes)
{
    GsAmmSharedState *state;
    int effective_grant_kb;
    uint64 effective_bytes;

    if (grant_id == 0 || requested_max_bytes == 0)
        return 0;

    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    effective_grant_kb = state->effective_grant_kb;
    SpinLockRelease(&state->mutex);

    if (effective_grant_kb <= 0)
        return requested_max_bytes;
    if (MyGsAmmGrantId != 0 && grant_id != MyGsAmmGrantId)
        return requested_max_bytes;

    effective_bytes = (uint64)effective_grant_kb * 1024;
    return Min(requested_max_bytes, effective_bytes);
}

static bool gs_amm_granule_owned_by_grant(const GsAmmGranuleMeta *granule, GsAmmGrantToken token)
{
    if (granule == NULL || token.grant_id == 0 || token.grant_generation == 0)
        return false;
    if (granule->grant_id != token.grant_id || granule->grant_generation != token.grant_generation)
        return false;
    return granule->state == GS_AMM_GRANULE_AP_ACTIVE;
}

static bool gs_amm_grant_token_equal(GsAmmGrantToken left, GsAmmGrantToken right)
{
    return left.grant_id == right.grant_id && left.grant_generation == right.grant_generation;
}

static bool gs_amm_grant_token_is_current(GsAmmGrantToken token)
{
    GsAmmGrantToken backend_token = {MyGsAmmGrantId, MyGsAmmGrantGeneration};

    return token.grant_id != 0 && token.grant_generation != 0 &&
        gs_amm_grant_token_equal(token, backend_token);
}

bool GsAmmGrantTokenIsValid(GsAmmGrantToken token)
{
    GsAmmSharedState *state;
    GsAmmGrantToken backend_token = {MyGsAmmGrantId, MyGsAmmGrantGeneration};

    if (token.grant_id != 0 && token.grant_generation != 0 &&
        gs_amm_grant_token_equal(token, backend_token))
        return true;
    state = GsAmmState;
    if (state != NULL) {
        SpinLockAcquire(&state->mutex);
        state->stale_grant_token_count++;
        SpinLockRelease(&state->mutex);
    }
    return false;
}

static bool gs_amm_grant_arena_prepare(GsAmmGrantToken token)
{
    if (!gs_amm_grant_token_is_current(token))
        return false;
    if (!gs_amm_grant_token_equal(MyGsAmmGrantArena.token, token)) {
        MyGsAmmGrantArena.token = token;
        MyGsAmmGrantArena.free_extents = NULL;
    }
    return true;
}

static void gs_amm_grant_arena_reset(GsAmmGrantToken token)
{
    if (!gs_amm_grant_token_equal(MyGsAmmGrantArena.token, token))
        return;
    MyGsAmmGrantArena.token.grant_id = 0;
    MyGsAmmGrantArena.token.grant_generation = 0;
    MyGsAmmGrantArena.free_extents = NULL;
}

static bool gs_amm_grant_arena_has_extent(GsAmmGrantToken token, uint64 request)
{
    if (!gs_amm_grant_arena_prepare(token))
        return false;
    for (GsAmmGrantFreeExtent *extent = MyGsAmmGrantArena.free_extents; extent != NULL; extent = extent->next) {
        uint64 remainder = extent->size - Min(extent->size, request);

        if (extent->size >= request && (remainder == 0 || remainder >= MAXALIGN(sizeof(GsAmmGrantFreeExtent))))
            return true;
    }
    return false;
}

static void *gs_amm_grant_arena_take_best_fit(GsAmmGrantToken token, uint64 request, int *granule_id)
{
    GsAmmGrantFreeExtent *best = NULL;
    GsAmmGrantFreeExtent *best_previous = NULL;
    GsAmmGrantFreeExtent *previous = NULL;

    if (granule_id == NULL || !gs_amm_grant_arena_prepare(token))
        return NULL;
    *granule_id = -1;
    for (GsAmmGrantFreeExtent *extent = MyGsAmmGrantArena.free_extents; extent != NULL; extent = extent->next) {
        uint64 remainder = extent->size - Min(extent->size, request);

        if (extent->size < request || (remainder != 0 && remainder < MAXALIGN(sizeof(GsAmmGrantFreeExtent)))) {
            previous = extent;
            continue;
        }
        if (best == NULL || extent->size < best->size) {
            best = extent;
            best_previous = previous;
            if (extent->size == request)
                break;
        }
        previous = extent;
    }
    if (best == NULL)
        return NULL;

    if (best->size == request) {
        if (best_previous == NULL)
            MyGsAmmGrantArena.free_extents = best->next;
        else
            best_previous->next = best->next;
    } else {
        GsAmmGrantFreeExtent *remainder = (GsAmmGrantFreeExtent *)((char *)best + request);

        remainder->size = best->size - request;
        remainder->granule_id = best->granule_id;
        remainder->next = best->next;
        if (best_previous == NULL)
            MyGsAmmGrantArena.free_extents = remainder;
        else
            best_previous->next = remainder;
    }
    *granule_id = best->granule_id;
    return best;
}

static bool gs_amm_grant_arena_insert_and_coalesce(
    GsAmmGrantToken token, void *pointer, uint64 size, int granule_id)
{
    GsAmmGrantFreeExtent *previous = NULL;
    GsAmmGrantFreeExtent *next;
    GsAmmGrantFreeExtent *extent;
    char *start = (char *)pointer;
    char *end = start + size;

    if (!gs_amm_grant_arena_prepare(token) || size < MAXALIGN(sizeof(GsAmmGrantFreeExtent)) || granule_id < 0)
        return false;

    next = MyGsAmmGrantArena.free_extents;
    while (next != NULL && (char *)next < start) {
        previous = next;
        next = next->next;
    }
    if ((previous != NULL && (char *)previous + previous->size > start) ||
        (next != NULL && end > (char *)next))
        return false;

    extent = (GsAmmGrantFreeExtent *)pointer;
    extent->size = size;
    extent->granule_id = granule_id;
    extent->next = next;
    if (previous == NULL)
        MyGsAmmGrantArena.free_extents = extent;
    else
        previous->next = extent;

    if (next != NULL && next->granule_id == granule_id && end == (char *)next) {
        extent->size += next->size;
        extent->next = next->next;
    }
    if (previous != NULL && previous->granule_id == granule_id &&
        (char *)previous + previous->size == (char *)extent) {
        previous->size += extent->size;
        previous->next = extent->next;
    }
    return true;
}

static uint64 gs_amm_granule_active_capacity_bytes(const GsAmmGranuleMeta *granule)
{
    uint64 physical_capacity = (uint64)granule->buffer_count * BLCKSZ;

    if (granule->active_grant_bytes == 0)
        return physical_capacity;
    return Min(granule->active_grant_bytes, physical_capacity);
}

static char *gs_amm_resolve_buffer_blocks(void)
{
    Size buffer_size;
    char *buffer_blocks;
    bool found = false;

    if (t_thrd.storage_cxt.BufferBlocks != NULL)
        return t_thrd.storage_cxt.BufferBlocks;

#ifdef __aarch64__
    buffer_size = (TOTAL_BUFFER_NUM - NVM_BUFFER_NUM) * (Size)BLCKSZ + PG_CACHE_LINE_SIZE;
    buffer_blocks = (char *)CACHELINEALIGN(ShmemInitStruct("Buffer Blocks", buffer_size, &found));
#else
    if (ENABLE_DSS) {
        buffer_size = (TOTAL_BUFFER_NUM - NVM_BUFFER_NUM) * (Size)BLCKSZ + ALIGNOF_BUFFER;
        buffer_blocks = (char *)BUFFERALIGN(ShmemInitStruct("Buffer Blocks", buffer_size, &found));
    } else {
        buffer_size = (TOTAL_BUFFER_NUM - NVM_BUFFER_NUM) * (Size)BLCKSZ;
        buffer_blocks = (char *)ShmemInitStruct("Buffer Blocks", buffer_size, &found);
    }
#endif

    if (!found)
        return NULL;

    t_thrd.storage_cxt.BufferBlocks = buffer_blocks;
    return buffer_blocks;
}

bool GsAmmGrantCanAllocateMemory(GsAmmGrantToken token, Size size)
{
    GsAmmSharedState *state;
    uint64 request;
    bool can_allocate = false;
    char *buffer_blocks = gs_amm_resolve_buffer_blocks();

    if (!GsAmmGrantTokenIsValid(token) || size == 0 || buffer_blocks == NULL)
        return false;

    request = (uint64)MAXALIGN(size);
    if (gs_amm_grant_arena_has_extent(token, request))
        return true;
    state = gs_amm_get_state();

    SpinLockAcquire(&state->mutex);
    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];
        uint64 cursor;
        uint64 capacity;

        if (!gs_amm_granule_owned_by_grant(granule, token))
            continue;

        cursor = (uint64)MAXALIGN(granule->alloc_cursor_bytes);
        capacity = gs_amm_granule_active_capacity_bytes(granule);
        if (cursor + request <= capacity) {
            can_allocate = true;
            break;
        }
    }
    SpinLockRelease(&state->mutex);

    return can_allocate;
}

void *GsAmmGrantAllocMemory(GsAmmGrantToken token, Size size)
{
    GsAmmSharedState *state;
    void *ptr = NULL;
    uint64 request;
    char *buffer_blocks = gs_amm_resolve_buffer_blocks();
    bool owns_granule = false;
    int reused_granule_id = -1;

    if (!GsAmmGrantTokenIsValid(token) || size == 0)
        return NULL;

    state = gs_amm_get_state();
    if (buffer_blocks == NULL) {
        SpinLockAcquire(&state->mutex);
        state->granule_alloc_no_mapping_count++;
        SpinLockRelease(&state->mutex);
        return NULL;
    }

    request = (uint64)MAXALIGN(size);

    ptr = gs_amm_grant_arena_take_best_fit(token, request, &reused_granule_id);
    if (ptr != NULL) {
        SpinLockAcquire(&state->mutex);
        if (reused_granule_id >= 0 && reused_granule_id < state->total_granules) {
            GsAmmGranuleMeta *granule = &state->granules[reused_granule_id];

            if (gs_amm_granule_owned_by_grant(granule, token) &&
                gs_amm_pointer_in_granule(granule, buffer_blocks, (const char *)ptr, request)) {
                granule->used_bytes += request;
                state->grant_reused_bytes += request;
                state->granule_alloc_success_count++;
                SpinLockRelease(&state->mutex);
                return ptr;
            }
        }
        SpinLockRelease(&state->mutex);
        (void)gs_amm_grant_arena_insert_and_coalesce(token, ptr, request, reused_granule_id);
        return NULL;
    }

    SpinLockAcquire(&state->mutex);
    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];
        uint64 cursor;
        uint64 capacity;

        if (!gs_amm_granule_owned_by_grant(granule, token))
            continue;
        owns_granule = true;

        cursor = (uint64)MAXALIGN(granule->alloc_cursor_bytes);
        capacity = gs_amm_granule_active_capacity_bytes(granule);
        if (cursor + request > capacity)
            continue;

        ptr = (void *)(buffer_blocks + ((Size)granule->first_buffer_id * BLCKSZ) + cursor);
        granule->alloc_cursor_bytes = cursor + request;
        granule->used_bytes += request;
        break;
    }
    if (ptr != NULL)
        state->granule_alloc_success_count++;
    else if (!owns_granule)
        state->granule_alloc_no_owner_count++;
    else
        state->granule_alloc_capacity_exhausted_count++;
    SpinLockRelease(&state->mutex);

    return ptr;
}

bool GsAmmGrantReturnMemory(GsAmmGrantToken token, void *pointer, Size size)
{
    GsAmmSharedState *state;
    uint64 request;
    bool owned = false;
    int granule_id = -1;
    char *buffer_blocks;

    if (!GsAmmGrantTokenIsValid(token) || pointer == NULL || size == 0)
        return false;

    request = (uint64)MAXALIGN(size);
    state = gs_amm_get_state();
    buffer_blocks = gs_amm_resolve_buffer_blocks();
    if (buffer_blocks == NULL) {
        SpinLockAcquire(&state->mutex);
        state->stale_grant_token_count++;
        SpinLockRelease(&state->mutex);
        return false;
    }
    SpinLockAcquire(&state->mutex);
    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];

        if (!gs_amm_granule_owned_by_grant(granule, token))
            continue;
        if (!gs_amm_pointer_in_granule(granule, buffer_blocks, (const char *)pointer, request))
            continue;
        owned = true;
        granule_id = i;
        break;
    }
    if (!owned)
        state->stale_grant_token_count++;
    SpinLockRelease(&state->mutex);
    if (!owned)
        return false;
    if (!gs_amm_grant_arena_insert_and_coalesce(token, pointer, request, granule_id)) {
        SpinLockAcquire(&state->mutex);
        state->stale_grant_token_count++;
        SpinLockRelease(&state->mutex);
        return false;
    }
    return true;
}

static bool gs_amm_pointer_in_granule(
    const GsAmmGranuleMeta *granule, const char *buffer_blocks, const char *pointer, uint64 size)
{
    char *start;
    char *end;
    uint64 capacity;

    if (granule == NULL || buffer_blocks == NULL || pointer == NULL)
        return false;

    start = (char *)buffer_blocks + ((Size)granule->first_buffer_id * BLCKSZ);
    capacity = (uint64)granule->buffer_count * BLCKSZ;
    end = start + capacity;

    return pointer >= start && pointer < end && size <= (uint64)(end - pointer);
}

static void gs_amm_grant_account_pointer_memory(GsAmmGrantToken token, void *pointer, Size size, bool account_used)
{
    GsAmmSharedState *state;
    uint64 request;
    char *buffer_blocks;

    if (!GsAmmGrantTokenIsValid(token) || pointer == NULL || size == 0)
        return;

    request = (uint64)MAXALIGN(size);
    buffer_blocks = gs_amm_resolve_buffer_blocks();
    if (buffer_blocks == NULL)
        return;
    state = gs_amm_get_state();

    SpinLockAcquire(&state->mutex);
    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];
        uint64 capacity;

        if (!gs_amm_granule_owned_by_grant(granule, token))
            continue;
        if (!gs_amm_pointer_in_granule(granule, buffer_blocks, (const char *)pointer, request))
            continue;

        capacity = gs_amm_granule_active_capacity_bytes(granule);
        if (account_used)
            granule->used_bytes = Min(granule->used_bytes + request, capacity);
        else
            granule->used_bytes = granule->used_bytes > request ? granule->used_bytes - request : 0;
        break;
    }
    SpinLockRelease(&state->mutex);
}

void GsAmmGrantAccountUsedMemory(GsAmmGrantToken token, void *pointer, Size size)
{
    gs_amm_grant_account_pointer_memory(token, pointer, size, true);
}

void GsAmmGrantAccountFreedMemory(GsAmmGrantToken token, void *pointer, Size size)
{
    gs_amm_grant_account_pointer_memory(token, pointer, size, false);
}

static const char *gs_amm_action_name(GsAmmAction action)
{
    if (action < 0 || action >= GS_AMM_ACTION_COUNT)
        return "UNKNOWN";
    return GS_AMM_ACTION_NAMES[action];
}

static void gs_amm_default_config(GsAmmConfig *cfg, int max_mb)
{
    cfg->shared_buffers_min_mb = gs_amm_shared_buffers_min_mb;
    cfg->controller_horizon = gs_amm_controller_horizon;
    cfg->resize_rate_limit_mb = gs_amm_resize_rate_limit_mb;
    cfg->tp_pressure_guard = gs_amm_tp_pressure_guard;
    cfg->io_pressure_guard = gs_amm_io_pressure_guard;
    cfg->deadband_mb = gs_amm_deadband_mb;
    cfg->hysteresis_enter_delta = 10;
    cfg->w_ap_benefit = 1.0;
    cfg->w_tp_recovery_benefit = 1.2;
    cfg->w_io_risk_penalty = 1.0;
    cfg->w_resize_cost = 0.25;
    cfg->w_grant_debt_risk = 0.5;
    cfg->beam_width = 4;

    cfg->shared_buffers_min_mb = Max(cfg->shared_buffers_min_mb, 1);
    cfg->shared_buffers_min_mb = Min(cfg->shared_buffers_min_mb, max_mb);
    cfg->controller_horizon = Max(cfg->controller_horizon, 1);
    cfg->controller_horizon = Min(cfg->controller_horizon, GS_AMM_MAX_HORIZON);
    cfg->resize_rate_limit_mb = Max(cfg->resize_rate_limit_mb, 1);
    if (gs_amm_resize_batch_mb > 0)
        cfg->resize_rate_limit_mb = Min(cfg->resize_rate_limit_mb, gs_amm_resize_batch_mb);
    cfg->beam_width = Max(cfg->beam_width, 1);
    cfg->beam_width = Min(cfg->beam_width, GS_AMM_MAX_BEAM);
}

static int gs_amm_resize_step_mb(const GsAmmSimState *state, GsAmmAction action, const GsAmmConfig *cfg)
{
    int granule_mb = gs_amm_configured_granule_mb();

    if (action == GS_AMM_BORROW_FROM_BUFFER) {
        int available = Max(state->active_mb - state->min_mb, 0);
        int target_change = Min(Max(state->ap_demand_mb, granule_mb), available);

        target_change = Min(target_change, Max(cfg->resize_rate_limit_mb, granule_mb));
        return Max(target_change, 0);
    }
    if (action == GS_AMM_TP_RECOVERY) {
        int available = Max(state->max_mb - state->active_mb, 0);
        return Max(Min(available, Max(cfg->resize_rate_limit_mb, granule_mb)), 0);
    }
    return 0;
}

static const char *gs_amm_single_step_reason(const GsAmmSimState *state, GsAmmAction action, const GsAmmConfig *cfg)
{
    if (action == GS_AMM_OBSERVE || action == GS_AMM_AP_EXPAND || action == GS_AMM_AP_SHRINK ||
        action == GS_AMM_BACKPRESSURE)
        return "";
    if (action == GS_AMM_TP_RECOVERY)
        return state->active_mb < state->max_mb ? "" : "at_max";
    if (action == GS_AMM_BORROW_FROM_BUFFER) {
        if (state->tp_pressure >= cfg->tp_pressure_guard)
            return "tp_pressure_guard";
        if (state->io_pressure >= cfg->io_pressure_guard)
            return "io_pressure_guard";
        if (!state->tail_reclaimable)
            return "tail_not_reclaimable";
        if (state->ap_demand_mb <= cfg->deadband_mb)
            return "no_demand";
        if (state->active_mb - state->min_mb <= 0)
            return "shared_buffers_min";
        return "";
    }
    return "not_selectable";
}

static bool gs_amm_legal_single_step(const GsAmmSimState *state, GsAmmAction action, const GsAmmConfig *cfg)
{
    return gs_amm_single_step_reason(state, action, cfg)[0] == '\0';
}

static void gs_amm_apply_action(
    const GsAmmSimState *state, GsAmmAction action, const GsAmmConfig *cfg, GsAmmSimState *out)
{
    *out = *state;
    if (action == GS_AMM_BORROW_FROM_BUFFER)
        out->active_mb = state->active_mb - gs_amm_resize_step_mb(state, action, cfg);
    else if (action == GS_AMM_TP_RECOVERY)
        out->active_mb = state->active_mb + gs_amm_resize_step_mb(state, action, cfg);
    out->active_mb = Max(out->min_mb, Min(out->active_mb, out->max_mb));
}

static const char *gs_amm_violates_hard_constraints(
    const GsAmmSimState *chain, const GsAmmAction *seq, int seq_len, const GsAmmConfig *cfg)
{
    for (int i = 0; i <= seq_len; i++) {
        if (chain[i].active_mb < chain[i].min_mb)
            return "below_min";
        if (chain[i].active_mb > chain[i].max_mb)
            return "above_max";
    }
    for (int i = 0; i < seq_len; i++) {
        const GsAmmSimState *before = &chain[i];

        if (seq[i] == GS_AMM_BORROW_FROM_BUFFER) {
            if (!before->tail_reclaimable)
                return "tail_not_reclaimable";
            if (before->tp_pressure >= cfg->tp_pressure_guard)
                return "tp_pressure_guard";
            if (before->io_pressure >= cfg->io_pressure_guard)
                return "io_pressure_guard";
        } else if (seq[i] == GS_AMM_TP_RECOVERY && chain[i + 1].active_mb > before->max_mb)
            return "above_max";
    }
    return NULL;
}

static double gs_amm_score_sequence(
    const GsAmmSimState *chain, const GsAmmAction *seq, int seq_len, const GsAmmObservation *obs, const GsAmmConfig *cfg)
{
    int borrowed = 0;
    int recovered = 0;
    int nonzero_delta = 0;

    for (int i = 0; i < seq_len; i++) {
        int before = chain[i].active_mb;
        int after = chain[i + 1].active_mb;

        if (after != before)
            nonzero_delta++;
        if (seq[i] == GS_AMM_BORROW_FROM_BUFFER)
            borrowed += Max(before - after, 0);
        else if (seq[i] == GS_AMM_TP_RECOVERY)
            recovered += Max(after - before, 0);
    }

    double ap_benefit = 0.0;
    if (borrowed > 0 && obs->ap_demand_mb > 0) {
        int useful = Min(borrowed, obs->ap_demand_mb);
        ap_benefit = (double)useful * (double)useful / (double)Max(obs->ap_demand_mb, 1);
    }
    double tp_recovery_benefit = (double)recovered * obs->tp_pressure / 100.0;
    int io_excess = Max(obs->io_pressure - (cfg->io_pressure_guard - cfg->hysteresis_enter_delta), 0);
    double io_risk_penalty = (double)borrowed * io_excess / 100.0;
    double resize_cost = (double)nonzero_delta;
    int final_active = chain[seq_len].active_mb;
    double grant_debt_risk = (double)chain[0].grant_debt_mb / (double)Max(final_active, 1);

    return cfg->w_ap_benefit * ap_benefit + cfg->w_tp_recovery_benefit * tp_recovery_benefit -
        cfg->w_io_risk_penalty * io_risk_penalty - cfg->w_resize_cost * resize_cost -
        cfg->w_grant_debt_risk * grant_debt_risk;
}

static void gs_amm_sort_beam(GsAmmBeamEntry *beam, int count)
{
    for (int i = 1; i < count; i++) {
        GsAmmBeamEntry key = beam[i];
        int j = i - 1;

        while (j >= 0 && beam[j].score < key.score) {
            beam[j + 1] = beam[j];
            j--;
        }
        beam[j + 1] = key;
    }
}

static void gs_amm_plan_actions(
    const GsAmmSimState *state0, const GsAmmObservation *obs, const GsAmmConfig *cfg, GsAmmPlanResult *result)
{
    GsAmmBeamEntry beam[GS_AMM_MAX_BEAM];
    GsAmmBeamEntry next[GS_AMM_MAX_BEAM * GS_AMM_SELECTABLE_ACTIONS];
    int beam_count = 1;
    int legal0 = 0;
    int rejected0 = 0;

    errno_t rc = memset_s(result, sizeof(*result), 0, sizeof(*result));
    securec_check(rc, "\0", "\0");
    beam[0].chain[0] = *state0;
    beam[0].len = 0;
    beam[0].score = 0.0;

    for (int step = 0; step < cfg->controller_horizon; step++) {
        int next_count = 0;

        for (int b = 0; b < beam_count; b++) {
            const GsAmmSimState *last = &beam[b].chain[beam[b].len];

            for (int a = 0; a < GS_AMM_SELECTABLE_ACTIONS; a++) {
                GsAmmAction action = GS_AMM_SELECTABLE[a];
                GsAmmBeamEntry *cand = NULL;

                if (!gs_amm_legal_single_step(last, action, cfg))
                    continue;

                cand = &next[next_count];
                *cand = beam[b];
                gs_amm_apply_action(last, action, cfg, &cand->chain[cand->len + 1]);
                cand->seq[cand->len] = action;
                cand->len++;
                if (gs_amm_violates_hard_constraints(cand->chain, cand->seq, cand->len, cfg) != NULL)
                    continue;
                cand->score = gs_amm_score_sequence(cand->chain, cand->seq, cand->len, obs, cfg);
                next_count++;
            }
        }
        if (next_count == 0) {
            beam_count = 0;
            break;
        }
        gs_amm_sort_beam(next, next_count);
        next_count = Min(next_count, cfg->beam_width);
        for (int b = 0; b < next_count; b++)
            beam[b] = next[b];
        beam_count = next_count;
    }

    for (int a = 0; a < GS_AMM_SELECTABLE_ACTIONS; a++) {
        if (gs_amm_legal_single_step(state0, GS_AMM_SELECTABLE[a], cfg))
            legal0++;
        else
            rejected0++;
    }
    result->candidate_count = legal0;
    result->rejected_count = rejected0;
    result->binding_constraint = gs_amm_single_step_reason(state0, GS_AMM_BORROW_FROM_BUFFER, cfg);

    if (beam_count == 0) {
        if (gs_amm_legal_single_step(state0, GS_AMM_TP_RECOVERY, cfg) && state0->tp_pressure >= cfg->tp_pressure_guard) {
            result->chosen_action = GS_AMM_TP_RECOVERY;
            result->best_sequence[0] = GS_AMM_TP_RECOVERY;
            result->sequence_len = 1;
            return;
        }
        result->chosen_action = GS_AMM_FAIL_CLOSED;
        result->best_sequence[0] = GS_AMM_FAIL_CLOSED;
        result->sequence_len = 1;
        if (result->binding_constraint[0] == '\0')
            result->binding_constraint = "no_safe_action";
        return;
    }

    gs_amm_sort_beam(beam, beam_count);
    result->chosen_action = beam[0].seq[0];
    result->sequence_len = beam[0].len;
    result->score = beam[0].score;
    for (int a = 0; a < beam[0].len && a < GS_AMM_MAX_HORIZON; a++)
        result->best_sequence[a] = beam[0].seq[a];
}

static int gs_amm_drain_pending_total(const GsAmmDrainStats *stats)
{
    return stats->dirty + stats->pinned + stats->io + stats->hash;
}

static void gs_amm_add_drain_stat(GsAmmDrainStats *stats, uint64 buf_state)
{
    bool dirty = (buf_state & BM_DIRTY) != 0;
    bool pinned = BUF_STATE_GET_REFCOUNT(buf_state) != 0 || (buf_state & BM_IS_META);
    bool io = (buf_state & BM_IO_IN_PROGRESS) != 0;

    if (dirty)
        stats->dirty++;
    if (pinned)
        stats->pinned++;
    if (io)
        stats->io++;
    if ((buf_state & BM_TAG_VALID) && (dirty || pinned || io))
        stats->hash++;
}

static void gs_amm_store_granule_drain_stats_locked(
    GsAmmSharedState *state, GsAmmGranuleMeta *granule, const GsAmmDrainStats *stats)
{
    granule->dirty_count = stats->dirty;
    granule->pinned_count = stats->pinned;
    granule->io_count = stats->io;
    granule->hash_count = stats->hash;
    state->drain_pending_dirty = stats->dirty;
    state->drain_pending_pinned = stats->pinned;
    state->drain_pending_io = stats->io;
    state->drain_pending_hash = stats->hash;
    state->drain_priority_flush_count += stats->flush;
}

static uint64 gs_amm_next_drain_attempt_locked(GsAmmSharedState *state)
{
    if (state->next_drain_attempt == PG_UINT64_MAX)
        return 0;
    state->next_drain_attempt++;
    return state->next_drain_attempt;
}

static uint64 gs_amm_next_reclaim_attempt_locked(GsAmmSharedState *state)
{
    if (state->next_reclaim_attempt == PG_UINT64_MAX)
        return 0;
    state->next_reclaim_attempt++;
    return state->next_reclaim_attempt;
}

static bool gs_amm_granule_matches_drain_attempt_locked(
    const GsAmmGranuleMeta *granule, uint64 drain_attempt)
{
    return granule != NULL && drain_attempt != 0 &&
        granule->state == GS_AMM_GRANULE_BUFFER_DRAINING && granule->drain_attempt == drain_attempt;
}

static bool gs_amm_acquire_drain_scan_lease_locked(
    GsAmmGranuleMeta *granule, uint64 drain_attempt)
{
    if (granule == NULL || granule->state != GS_AMM_GRANULE_BUFFER_DRAINING ||
        drain_attempt == 0 || granule->drain_attempt != drain_attempt || granule->scan_inflight)
        return false;

    granule->scan_inflight = true;
    granule->scan_lease_attempt = drain_attempt;
    return true;
}

static void gs_amm_release_drain_scan_lease(
    GsAmmSharedState *state, GsAmmGranuleMeta *granule, uint64 drain_attempt)
{
    SpinLockAcquire(&state->mutex);
    if (granule->scan_inflight && granule->scan_lease_attempt == drain_attempt) {
        granule->scan_inflight = false;
        granule->scan_lease_attempt = 0;
    }
    SpinLockRelease(&state->mutex);
}

static bool gs_amm_acquire_reclaim_syscall_lease_locked(GsAmmSharedState *state,
    GsAmmGranuleMeta *granule, uint64 drain_attempt, uint32 owner_epoch, uint64 reclaim_attempt)
{
    if (state == NULL || granule == NULL || state->reclaim_syscall_inflight ||
        granule->reclaim_inflight || granule->state != GS_AMM_GRANULE_RECLAIMING ||
        (drain_attempt != 0 && granule->drain_attempt != drain_attempt) ||
        granule->owner_epoch != owner_epoch || granule->reclaim_attempt != reclaim_attempt)
        return false;

    granule->reclaim_inflight = true;
    state->reclaim_syscall_inflight = true;
    state->reclaim_syscall_attempt = reclaim_attempt;
    return true;
}

static bool gs_amm_begin_reclaim_locked(
    GsAmmSharedState *state, GsAmmGranuleMeta *granule, uint64 drain_attempt, uint64 *reclaim_attempt)
{
    uint64 next_reclaim_attempt;

    if (state == NULL || granule == NULL || reclaim_attempt == NULL)
        return false;
    if (granule->state == GS_AMM_GRANULE_BUFFER_DRAINING) {
        if (!gs_amm_granule_matches_drain_attempt_locked(granule, drain_attempt))
            return false;
    } else if (granule->state != GS_AMM_GRANULE_AP_ACTIVE &&
        granule->state != GS_AMM_GRANULE_AP_RESERVED) {
        return false;
    }
    next_reclaim_attempt = gs_amm_next_reclaim_attempt_locked(state);
    if (next_reclaim_attempt == 0)
        return false;
    if (!gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_RECLAIMING))
        return false;

    granule->reclaim_attempt = next_reclaim_attempt;
    *reclaim_attempt = granule->reclaim_attempt;
    return true;
}

static bool gs_amm_complete_reclaim_locked(GsAmmSharedState *state, GsAmmGranuleMeta *granule,
    uint64 drain_attempt, uint32 owner_epoch, uint64 reclaim_attempt, bool reclaimed, uint64 reclaimed_mb)
{
    bool granule_lease_matches;
    bool shared_lease_matches;

    if (state == NULL || granule == NULL)
        return false;

    granule_lease_matches = granule->reclaim_inflight && granule->reclaim_attempt == reclaim_attempt;
    shared_lease_matches = state->reclaim_syscall_inflight &&
        state->reclaim_syscall_attempt == reclaim_attempt;
    if (granule_lease_matches)
        granule->reclaim_inflight = false;
    if (shared_lease_matches) {
        state->reclaim_syscall_inflight = false;
        state->reclaim_syscall_attempt = 0;
    }

    if (!granule_lease_matches || !shared_lease_matches ||
        granule->state != GS_AMM_GRANULE_RECLAIMING ||
        (drain_attempt != 0 && granule->drain_attempt != drain_attempt) ||
        granule->owner_epoch != owner_epoch || granule->reclaim_attempt != reclaim_attempt) {
        state->epoch_mismatch_reject_count++;
        return false;
    }
    if (!reclaimed) {
        state->reclaim_fail_count++;
        if (state->runtime_config.amm_enabled) {
            granule->reclaim_retry_after = gs_amm_timestamp_after_ms(
                GetCurrentTimestamp(), GS_AMM_RECLAIM_RETRY_INTERVAL_MS);
            return false;
        }
        /* Disabled AMM cannot retain a retry-only granule outside shared buffers. */
    }

    granule->grant_id = 0;
    granule->grant_generation = 0;
    granule->reserved_granules = 0;
    granule->active_grant_bytes = 0;
    granule->used_bytes = 0;
    granule->alloc_cursor_bytes = 0;
    granule->reclaim_retry_after = 0;
    if (!gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_FREE))
        return false;
    state->reclaimed_mb += reclaimed_mb;
    return true;
}

static bool gs_amm_reclaim_granule_memory(const GsAmmGranuleMeta *granule, uint64 *reclaimed_mb)
{
    char *start;
    char *buffer_blocks;
    Size length;
    uintptr_t range_start;
    uintptr_t range_end;
    uintptr_t aligned_start;
    uintptr_t aligned_end;
    Size aligned_length;
    long page_size;
    errno_t rc;

    if (reclaimed_mb != NULL)
        *reclaimed_mb = 0;
    if (granule == NULL || granule->buffer_count <= 0)
        return false;

    buffer_blocks = gs_amm_resolve_buffer_blocks();
    if (buffer_blocks == NULL)
        return false;

    start = buffer_blocks + ((Size)granule->first_buffer_id * BLCKSZ);
    length = (Size)granule->buffer_count * BLCKSZ;
    page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0)
        page_size = 4096;

    range_start = (uintptr_t)start;
    range_end = range_start + (uintptr_t)length;
    aligned_start = ((range_start + (uintptr_t)page_size - 1) / (uintptr_t)page_size) * (uintptr_t)page_size;
    aligned_end = (range_end / (uintptr_t)page_size) * (uintptr_t)page_size;
    if (aligned_end > aligned_start) {
        aligned_length = (Size)(aligned_end - aligned_start);
        if (madvise((void *)aligned_start, aligned_length, MADV_DONTNEED) == 0) {
            if (reclaimed_mb != NULL)
                *reclaimed_mb = ((uint64)length + 1024 * 1024 - 1) / (1024 * 1024);
            return true;
        }
    }

    rc = memset_s(start, length, 0, length);
    if (rc != EOK)
        return false;
    if (reclaimed_mb != NULL)
        *reclaimed_mb = ((uint64)length + 1024 * 1024 - 1) / (1024 * 1024);
    return true;
}

static bool gs_amm_buffer_safe_to_invalidate(uint64 buf_state)
{
    if (BUF_STATE_GET_REFCOUNT(buf_state) != 0)
        return false;
    if (buf_state & (BM_DIRTY | BM_IO_IN_PROGRESS | BM_IS_META))
        return false;
    return true;
}

static bool gs_amm_request_pagewriter_flush_for_drain(BufferDesc *buf, uint64 *buf_state)
{
    if (buf == NULL || buf_state == NULL)
        return false;
    if (!ENABLE_INCRE_CKPT)
        return false;
    if ((*buf_state & (BM_VALID | BM_DIRTY)) != (BM_VALID | BM_DIRTY))
        return false;
    if (*buf_state & (BM_IO_IN_PROGRESS | BM_IS_META))
        return false;
    if (BUF_STATE_GET_REFCOUNT(*buf_state) != 0)
        return false;

    *buf_state |= BM_CHECKPOINT_NEEDED;
    if (!XLogRecPtrIsInvalid(pg_atomic_read_u64(&buf->extra->rec_lsn))) {
        wakeup_pagewriter_thread();
        return true;
    }
    if (is_dirty_page_queue_full(buf))
        return false;
    if (!push_pending_flush_queue(BufferDescriptorGetBuffer(buf)))
        return false;

    wakeup_pagewriter_thread();
    return true;
}

static bool gs_amm_try_invalidate_buffer_for_granule(BufferDesc *buf, GsAmmDrainStats *stats)
{
    BufferTag old_tag;
    uint32 old_hash = 0;
    LWLock *old_partition_lock = NULL;
    uint64 old_flags;
    uint64 buf_state;

    buf_state = LockBufHdr(buf);
    if (!gs_amm_buffer_safe_to_invalidate(buf_state)) {
        gs_amm_add_drain_stat(stats, buf_state);
        UnlockBufHdr(buf, buf_state);
        return false;
    }

    old_flags = buf_state & BUF_FLAG_MASK;
    if (!(old_flags & (BM_TAG_VALID | BM_VALID))) {
        CLEAR_BUFFERTAG(buf->tag);
        buf->extra->amm_owner_epoch = 0;
        buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
        UnlockBufHdr(buf, buf_state);
        return true;
    }

    if (!(old_flags & BM_TAG_VALID)) {
        CLEAR_BUFFERTAG(buf->tag);
        buf->extra->amm_owner_epoch = 0;
        buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
        UnlockBufHdr(buf, buf_state);
        return true;
    }

    old_tag = buf->tag;
    old_hash = BufTableHashCode(&old_tag);
    old_partition_lock = BufMappingPartitionLock(old_hash);
    UnlockBufHdr(buf, buf_state);

    (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);
    buf_state = LockBufHdr(buf);

    if (!(buf_state & BM_TAG_VALID) || !BUFFERTAGS_EQUAL(buf->tag, old_tag)) {
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(old_partition_lock);
        return true;
    }

    if (!gs_amm_buffer_safe_to_invalidate(buf_state)) {
        gs_amm_add_drain_stat(stats, buf_state);
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(old_partition_lock);
        return false;
    }

    if (ENABLE_DMS) {
        UnlockBufHdr(buf, buf_state);
        LWLockRelease(old_partition_lock);

        if (!DmsReleaseOwner(old_tag, buf->buf_id)) {
            stats->hash++;
            return false;
        }
        ClearReadHint(buf->buf_id, true);

        (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);
        buf_state = LockBufHdr(buf);
        if (!(buf_state & BM_TAG_VALID) || !BUFFERTAGS_EQUAL(buf->tag, old_tag)) {
            UnlockBufHdr(buf, buf_state);
            LWLockRelease(old_partition_lock);
            return true;
        }
        if (!gs_amm_buffer_safe_to_invalidate(buf_state)) {
            gs_amm_add_drain_stat(stats, buf_state);
            UnlockBufHdr(buf, buf_state);
            LWLockRelease(old_partition_lock);
            return false;
        }
    }

    old_flags = buf_state & BUF_FLAG_MASK;
    CLEAR_BUFFERTAG(buf->tag);
    buf->extra->amm_owner_epoch = 0;
    buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
    UnlockBufHdr(buf, buf_state);

    if (old_flags & BM_TAG_VALID)
        BufTableDelete(&old_tag, old_hash);
    LWLockRelease(old_partition_lock);
    return true;
}

static bool gs_amm_scan_granule(GsAmmSharedState *state, GsAmmGranuleMeta *granule,
    bool invalidate, uint64 drain_attempt, GsAmmDrainStats *stats)
{
    errno_t rc = memset_s(stats, sizeof(*stats), 0, sizeof(*stats));
    volatile bool scan_complete = true;

    securec_check(rc, "\0", "\0");
    if (!invalidate) {
        for (int offset = 0; offset < granule->buffer_count; offset++) {
            int buf_id = granule->first_buffer_id + offset;
            BufferDesc *buf = GetBufferDescriptor(buf_id);
            uint64 buf_state = pg_atomic_read_u64(&buf->state);

            if (!gs_amm_buffer_safe_to_invalidate(buf_state))
                gs_amm_add_drain_stat(stats, buf_state);
        }
        return true;
    }

    SpinLockAcquire(&state->mutex);
    scan_complete = gs_amm_acquire_drain_scan_lease_locked(granule, drain_attempt);
    SpinLockRelease(&state->mutex);
    if (!scan_complete)
        return false;

    PG_TRY();
    {
        for (int offset = 0; offset < granule->buffer_count; offset++) {
            int buf_id = granule->first_buffer_id + offset;
            BufferDesc *buf = GetBufferDescriptor(buf_id);
            bool attempt_is_current;

            SpinLockAcquire(&state->mutex);
            attempt_is_current = gs_amm_granule_matches_drain_attempt_locked(granule, drain_attempt) &&
                granule->scan_inflight && granule->scan_lease_attempt == drain_attempt;
            SpinLockRelease(&state->mutex);
            if (!attempt_is_current) {
                scan_complete = false;
                break;
            }
            (void)gs_amm_try_invalidate_buffer_for_granule(buf, stats);
        }
        SpinLockAcquire(&state->mutex);
        scan_complete = scan_complete &&
            gs_amm_granule_matches_drain_attempt_locked(granule, drain_attempt) &&
            granule->scan_inflight && granule->scan_lease_attempt == drain_attempt;
        SpinLockRelease(&state->mutex);
    }
    PG_CATCH();
    {
        gs_amm_release_drain_scan_lease(state, granule, drain_attempt);
        PG_RE_THROW();
    }
    PG_END_TRY();

    gs_amm_release_drain_scan_lease(state, granule, drain_attempt);
    return scan_complete;
}

static void gs_amm_restore_buffer_granules_if_disabled(GsAmmSharedState *state);

static bool gs_amm_drain_granule(GsAmmSharedState *state, int granule_id, GsAmmDrainStats *stats)
{
    GsAmmGranuleMeta *granule;
    GsAmmGranuleMeta reclaim_snapshot;
    uint64 drain_attempt = 0;
    uint64 reclaim_attempt = 0;
    uint32 reclaim_owner_epoch = 0;
    uint64 reclaimed_mb = 0;
    bool scan_complete;
    volatile bool reclaimed = false;

    if (granule_id < 0 || granule_id >= state->total_granules)
        return false;

    granule = &state->granules[granule_id];
    (void)gs_amm_scan_granule(state, granule, false, 0, stats);
    if (gs_amm_drain_pending_total(stats) > 0) {
        if (stats->dirty > 0) {
            for (int offset = 0; offset < granule->buffer_count; offset++) {
                int buf_id = granule->first_buffer_id + offset;
                BufferDesc *buf = GetBufferDescriptor(buf_id);
                uint64 buf_state = LockBufHdr(buf);

                if (gs_amm_request_pagewriter_flush_for_drain(buf, &buf_state))
                    stats->flush++;
                UnlockBufHdr(buf, buf_state);
            }
        }
        SpinLockAcquire(&state->mutex);
        gs_amm_store_granule_drain_stats_locked(state, granule, stats);
        state->drain_fail_count++;
        SpinLockRelease(&state->mutex);
        return false;
    }

    SpinLockAcquire(&state->mutex);
    if (granule->state != GS_AMM_GRANULE_BUFFER_ACTIVE) {
        state->drain_fail_count++;
        SpinLockRelease(&state->mutex);
        return false;
    }
    drain_attempt = gs_amm_next_drain_attempt_locked(state);
    if (drain_attempt == 0) {
        state->drain_fail_count++;
        SpinLockRelease(&state->mutex);
        return false;
    }
    if (!gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_BUFFER_DRAINING)) {
        state->drain_fail_count++;
        SpinLockRelease(&state->mutex);
        return false;
    }
    granule->drain_attempt = drain_attempt;
    gs_amm_refresh_granule_counts_locked(state);
    SpinLockRelease(&state->mutex);

    scan_complete = gs_amm_scan_granule(state, granule, true, drain_attempt, stats);

    SpinLockAcquire(&state->mutex);
    if (!scan_complete || !gs_amm_granule_matches_drain_attempt_locked(granule, drain_attempt)) {
        state->epoch_mismatch_reject_count++;
        state->drain_fail_count++;
        SpinLockRelease(&state->mutex);
        return false;
    }
    gs_amm_store_granule_drain_stats_locked(state, granule, stats);
    if (gs_amm_drain_pending_total(stats) == 0) {
        if (!gs_amm_begin_reclaim_locked(state, granule, drain_attempt, &reclaim_attempt)) {
            state->drain_fail_count++;
            SpinLockRelease(&state->mutex);
            return false;
        }
        if (granule->state != GS_AMM_GRANULE_RECLAIMING) {
            state->drain_fail_count++;
            SpinLockRelease(&state->mutex);
            return false;
        }
        reclaim_owner_epoch = granule->owner_epoch;
        if (!gs_amm_acquire_reclaim_syscall_lease_locked(state, granule, drain_attempt,
            reclaim_owner_epoch, reclaim_attempt)) {
            state->drain_fail_count++;
            gs_amm_refresh_granule_counts_locked(state);
            SpinLockRelease(&state->mutex);
            return false;
        }
        reclaim_snapshot = *granule;
        gs_amm_refresh_granule_counts_locked(state);
        SpinLockRelease(&state->mutex);

        PG_TRY();
        {
            reclaimed = gs_amm_reclaim_granule_memory(&reclaim_snapshot, &reclaimed_mb);
        }
        PG_CATCH();
        {
            SpinLockAcquire(&state->mutex);
            (void)gs_amm_complete_reclaim_locked(state, granule, drain_attempt, reclaim_owner_epoch,
                reclaim_attempt, false, 0);
            gs_amm_refresh_granule_counts_locked(state);
            SpinLockRelease(&state->mutex);
            gs_amm_restore_buffer_granules_if_disabled(state);
            PG_RE_THROW();
        }
        PG_END_TRY();

        SpinLockAcquire(&state->mutex);
        if (!gs_amm_complete_reclaim_locked(state, granule, drain_attempt, reclaim_owner_epoch,
            reclaim_attempt, reclaimed, reclaimed_mb) || granule->state != GS_AMM_GRANULE_FREE) {
            state->drain_fail_count++;
            SpinLockRelease(&state->mutex);
            return false;
        }
        state->drain_success_count++;
        gs_amm_refresh_granule_counts_locked(state);
        SpinLockRelease(&state->mutex);
        gs_amm_restore_buffer_granules_if_disabled(state);
        return true;
    }

    if (!gs_amm_granule_matches_drain_attempt_locked(granule, drain_attempt)) {
        state->epoch_mismatch_reject_count++;
        state->drain_fail_count++;
        SpinLockRelease(&state->mutex);
        return false;
    }
    if (granule->scan_inflight ||
        !gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_BUFFER_ACTIVE)) {
        state->drain_fail_count++;
        SpinLockRelease(&state->mutex);
        return false;
    }
    state->drain_fail_count++;
    state->drain_rollback_count++;
    gs_amm_refresh_granule_counts_locked(state);
    SpinLockRelease(&state->mutex);
    return false;
}

static bool gs_amm_drain_granule_with_operation(
    GsAmmSharedState *state, int granule_id, GsAmmDrainStats *stats)
{
    volatile bool drained = false;
    bool scan_inflight = false;

    if (state == NULL || granule_id < 0 || granule_id >= state->total_granules)
        return false;

    SpinLockAcquire(&state->mutex);
    scan_inflight = state->granules[granule_id].scan_inflight;
    SpinLockRelease(&state->mutex);
    if (scan_inflight || !GsAmmOperationBegin())
        return false;

    PG_TRY();
    {
        drained = gs_amm_drain_granule(state, granule_id, stats);
    }
    PG_CATCH();
    {
        GsAmmOperationEnd();
        PG_RE_THROW();
    }
    PG_END_TRY();

    GsAmmOperationEnd();
    return drained;
}

static int gs_amm_expand_granules(GsAmmSharedState *state, int target_blocks, bool only_when_disabled)
{
    GsAmmGranuleSummary summary;
    int active_blocks;

    target_blocks = Max(1, Min(target_blocks, NORMAL_SHARED_BUFFER_NUM));

    SpinLockAcquire(&state->mutex);
    gs_amm_summarize_granules_locked(state, &summary);
    active_blocks = summary.buffer_active_blocks;
    if (only_when_disabled && state->runtime_config.amm_enabled) {
        SpinLockRelease(&state->mutex);
        return active_blocks;
    }

    for (int pass = 0; pass < 2 && active_blocks < target_blocks; pass++) {
        for (int i = 0; i < state->total_granules && active_blocks < target_blocks; i++) {
            GsAmmGranuleMeta *granule = &state->granules[i];
            bool can_activate;

            if (granule->first_buffer_id >= target_blocks)
                continue;
            can_activate = pass == 0 ? (granule->state == GS_AMM_GRANULE_FREE) :
                                       (granule->state == GS_AMM_GRANULE_BUFFER_DRAINING &&
                                           !granule->scan_inflight);
            if (!can_activate)
                continue;

            if (!gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_BUFFER_ACTIVE))
                continue;
            granule->dirty_count = 0;
            granule->pinned_count = 0;
            granule->io_count = 0;
            granule->hash_count = 0;
            granule->grant_id = 0;
            granule->grant_generation = 0;
            granule->reserved_granules = 0;
            granule->active_grant_bytes = 0;
            granule->used_bytes = 0;
            granule->alloc_cursor_bytes = 0;
            active_blocks += granule->buffer_count;
        }
    }

    gs_amm_refresh_granule_counts_locked(state);
    gs_amm_summarize_granules_locked(state, &summary);
    active_blocks = summary.buffer_active_blocks;
    SpinLockRelease(&state->mutex);
    return active_blocks;
}

static void gs_amm_finalize_disabled_reclaims_locked(GsAmmSharedState *state)
{
    if (state == NULL || state->runtime_config.amm_enabled)
        return;

    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];
        bool ap_reclaim;
        uint64 granule_bytes;
        int granule_mb;

        if (granule->state != GS_AMM_GRANULE_RECLAIMING || granule->reclaim_inflight ||
            granule->scan_inflight)
            continue;

        ap_reclaim = granule->grant_id != 0 && granule->grant_generation != 0;
        granule_bytes = granule->active_grant_bytes;
        if (granule_bytes == 0)
            granule_bytes = (uint64)granule->buffer_count * BLCKSZ;
        granule_mb = (int)((granule_bytes + 1024 * 1024 - 1) / (1024 * 1024));
        if (!gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_FREE))
            continue;

        granule->grant_id = 0;
        granule->grant_generation = 0;
        granule->reserved_granules = 0;
        granule->active_grant_bytes = 0;
        granule->used_bytes = 0;
        granule->alloc_cursor_bytes = 0;
        granule->reclaim_retry_after = 0;
        if (ap_reclaim)
            state->dynamic_used_mb = Max(state->dynamic_used_mb - granule_mb, 0);
    }
    gs_amm_refresh_granule_counts_locked(state);
}

static void gs_amm_restore_buffer_granules_if_disabled(GsAmmSharedState *state)
{
    if (state == NULL)
        return;

    SpinLockAcquire(&state->mutex);
    gs_amm_finalize_disabled_reclaims_locked(state);
    SpinLockRelease(&state->mutex);
    (void)gs_amm_expand_granules(state, NORMAL_SHARED_BUFFER_NUM, true);
}

void GsAmmOnEnabledGucChange(bool enabled)
{
    GsAmmSharedState *state = GsAmmState;

    if (state == NULL)
        return;

    SpinLockAcquire(&state->mutex);
    state->runtime_config.amm_enabled = enabled;
    SpinLockRelease(&state->mutex);
    /* GUC defaults are assigned before the postmaster attaches buffer strategy shared memory. */
    if (!enabled && t_thrd.storage_cxt.StrategyControl != NULL)
        gs_amm_restore_buffer_granules_if_disabled(state);
}

static int gs_amm_granule_capacity_mb(const GsAmmGranuleMeta *granule)
{
    int64 bytes = (int64)granule->buffer_count * BLCKSZ;
    int64 mb = (bytes + 1024 * 1024 - 1) / (1024 * 1024);

    return (int)Max(mb, 1);
}

static int gs_amm_free_granule_mb_locked(GsAmmSharedState *state)
{
    int free_mb = 0;

    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];

        if (granule->state == GS_AMM_GRANULE_FREE &&
            GsAmmGranuleCanCompleteApLifecycle(granule->generation, granule->owner_epoch))
            free_mb += gs_amm_granule_capacity_mb(granule);
    }
    return free_mb;
}

static int gs_amm_lifecycle_exhausted_granules_locked(GsAmmSharedState *state)
{
    int exhausted = 0;

    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];

        if (granule->state == GS_AMM_GRANULE_FREE &&
            !GsAmmGranuleCanCompleteApLifecycle(granule->generation, granule->owner_epoch))
            exhausted++;
    }
    return exhausted;
}

static int gs_amm_granule_grant_mb(const GsAmmGranuleMeta *granule)
{
    if (granule->active_grant_bytes == 0)
        return gs_amm_granule_capacity_mb(granule);
    return (int)((granule->active_grant_bytes + 1024 * 1024 - 1) / (1024 * 1024));
}

static bool gs_amm_grant_retains_minimum_capacity_locked(
    const GsAmmSharedState *state, const GsAmmGranuleMeta *reclaiming_granule)
{
    uint64 retained_bytes = 0;
    uint64 minimum_bytes;

    if (state == NULL || reclaiming_granule == NULL || reclaiming_granule->grant_id == 0 ||
        reclaiming_granule->grant_generation == 0)
        return false;

    minimum_bytes = (uint64)Max(gs_amm_ap_min_grant_mb, 1) * 1024 * 1024;
    for (int i = 0; i < state->total_granules; i++) {
        const GsAmmGranuleMeta *candidate = &state->granules[i];
        uint64 capacity;
        uint64 cursor;

        if (candidate == reclaiming_granule || candidate->state != GS_AMM_GRANULE_AP_ACTIVE ||
            candidate->scan_inflight || candidate->reclaim_inflight ||
            candidate->grant_id != reclaiming_granule->grant_id ||
            candidate->grant_generation != reclaiming_granule->grant_generation)
            continue;

        capacity = gs_amm_granule_active_capacity_bytes(candidate);
        cursor = (uint64)MAXALIGN(candidate->alloc_cursor_bytes);
        if (capacity > cursor)
            retained_bytes += capacity - cursor;
        if (retained_bytes >= minimum_bytes)
            return true;
    }

    return false;
}

static GsAmmGrantToken gs_amm_next_grant_token_locked(GsAmmSharedState *state)
{
    GsAmmGrantToken token = {0, 0};

    if (state->next_grant_id == PG_UINT64_MAX || state->next_grant_generation == PG_UINT64_MAX)
        return token;
    state->next_grant_id++;
    state->next_grant_generation++;
    token.grant_id = state->next_grant_id;
    token.grant_generation = state->next_grant_generation;
    return token;
}

static bool gs_amm_forward_unwind_ap_granules_locked(GsAmmSharedState *state, GsAmmGrantToken token)
{
    int matching_granules = 0;

    if (state == NULL || token.grant_id == 0 || token.grant_generation == 0)
        return false;

    /* Preflight the complete forward unwind before changing any granule. */
    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];

        if (granule->grant_id != token.grant_id || granule->grant_generation != token.grant_generation)
            continue;
        if ((granule->state != GS_AMM_GRANULE_AP_RESERVED &&
                granule->state != GS_AMM_GRANULE_AP_ACTIVE) ||
            granule->scan_inflight || granule->reclaim_inflight ||
            granule->generation > PG_UINT32_MAX - 2 || granule->owner_epoch == PG_UINT32_MAX) {
            state->illegal_transition_count++;
            return false;
        }
        matching_granules++;
    }
    if (matching_granules == 0)
        return false;

    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];
        bool reclaiming_published;
        bool free_published;

        if (granule->grant_id != token.grant_id || granule->grant_generation != token.grant_generation)
            continue;

        reclaiming_published =
            gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_RECLAIMING);
        Assert(reclaiming_published);
        if (!reclaiming_published)
            return false;
        free_published = gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_FREE);
        Assert(free_published);
        if (!free_published)
            return false;
        granule->grant_id = 0;
        granule->grant_generation = 0;
        granule->reserved_granules = 0;
        granule->active_grant_bytes = 0;
        granule->used_bytes = 0;
        granule->alloc_cursor_bytes = 0;
    }
    return true;
}

static bool gs_amm_activate_ap_granules_locked(GsAmmSharedState *state, GsAmmGrantToken token)
{
    int matching_granules = 0;

    if (state == NULL || token.grant_id == 0 || token.grant_generation == 0)
        return false;

    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];

        if (granule->grant_id != token.grant_id || granule->grant_generation != token.grant_generation)
            continue;
        if (granule->state != GS_AMM_GRANULE_AP_RESERVED ||
            granule->scan_inflight || granule->reclaim_inflight ||
            !GsAmmGranuleCountersCanAdvance(granule->generation, granule->owner_epoch, false)) {
            (void)gs_amm_forward_unwind_ap_granules_locked(state, token);
            gs_amm_refresh_granule_counts_locked(state);
            return false;
        }
        matching_granules++;
    }
    if (matching_granules == 0)
        return false;

    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *granule = &state->granules[i];

        if (granule->grant_id == token.grant_id && granule->grant_generation == token.grant_generation &&
            granule->state == GS_AMM_GRANULE_AP_RESERVED) {
            bool activation_published =
                gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_AP_ACTIVE);

            Assert(activation_published);
            if (!activation_published) {
                (void)gs_amm_forward_unwind_ap_granules_locked(state, token);
                gs_amm_refresh_granule_counts_locked(state);
                return false;
            }
        }
    }
    gs_amm_refresh_granule_counts_locked(state);
    return true;
}

static int gs_amm_release_ap_granules_locked(GsAmmSharedState *state, GsAmmGrantToken token)
{
    int released_mb = 0;

    if (state == NULL || token.grant_id == 0 || token.grant_generation == 0)
        return 0;

    for (;;) {
        GsAmmGranuleMeta reclaim_snapshot;
        GsAmmGranuleMeta *granule = NULL;
        uint64 reclaim_attempt = 0;
        uint32 reclaim_owner_epoch = 0;
        uint64 reclaimed_mb = 0;
        int granule_mb = 0;
        volatile bool reclaimed = false;

        SpinLockAcquire(&state->mutex);
        for (int i = 0; i < state->total_granules; i++) {
            GsAmmGranuleMeta *candidate = &state->granules[i];

            if ((candidate->state == GS_AMM_GRANULE_AP_ACTIVE ||
                    candidate->state == GS_AMM_GRANULE_AP_RESERVED) &&
                candidate->grant_id == token.grant_id &&
                candidate->grant_generation == token.grant_generation) {
                granule = candidate;
                break;
            }
        }
        if (granule == NULL) {
            gs_amm_refresh_granule_counts_locked(state);
            SpinLockRelease(&state->mutex);
            break;
        }

        granule_mb = gs_amm_granule_grant_mb(granule);
        if (!gs_amm_begin_reclaim_locked(state, granule, 0, &reclaim_attempt)) {
            SpinLockRelease(&state->mutex);
            break;
        }
        if (granule->state != GS_AMM_GRANULE_RECLAIMING) {
            SpinLockRelease(&state->mutex);
            break;
        }
        reclaim_owner_epoch = granule->owner_epoch;
        if (!gs_amm_acquire_reclaim_syscall_lease_locked(state, granule, 0,
            reclaim_owner_epoch, reclaim_attempt)) {
            gs_amm_refresh_granule_counts_locked(state);
            SpinLockRelease(&state->mutex);
            break;
        }
        reclaim_snapshot = *granule;
        gs_amm_refresh_granule_counts_locked(state);
        SpinLockRelease(&state->mutex);

        PG_TRY();
        {
            reclaimed = gs_amm_reclaim_granule_memory(&reclaim_snapshot, &reclaimed_mb);
        }
        PG_CATCH();
        {
            SpinLockAcquire(&state->mutex);
            (void)gs_amm_complete_reclaim_locked(state, granule, 0, reclaim_owner_epoch,
                reclaim_attempt, false, 0);
            gs_amm_refresh_granule_counts_locked(state);
            SpinLockRelease(&state->mutex);
            gs_amm_restore_buffer_granules_if_disabled(state);
            PG_RE_THROW();
        }
        PG_END_TRY();

        SpinLockAcquire(&state->mutex);
        if (gs_amm_complete_reclaim_locked(state, granule, 0, reclaim_owner_epoch,
            reclaim_attempt, reclaimed, reclaimed_mb) && granule->state == GS_AMM_GRANULE_FREE) {
            released_mb += granule_mb;
        }
        gs_amm_refresh_granule_counts_locked(state);
        SpinLockRelease(&state->mutex);
    }
    return released_mb;
}

static bool gs_amm_retry_failed_reclaim(GsAmmSharedState *state)
{
    GsAmmGranuleMeta reclaim_snapshot;
    GsAmmGranuleMeta *granule = NULL;
    TimestampTz now = GetCurrentTimestamp();
    uint64 drain_attempt = 0;
    uint32 owner_epoch = 0;
    uint64 reclaim_attempt = 0;
    uint64 reclaimed_mb = 0;
    int granule_mb = 0;
    bool ap_reclaim = false;
    volatile bool reclaimed = false;
    bool completed;

    SpinLockAcquire(&state->mutex);
    for (int i = 0; i < state->total_granules; i++) {
        GsAmmGranuleMeta *candidate = &state->granules[i];

        if (candidate->state != GS_AMM_GRANULE_RECLAIMING || candidate->reclaim_attempt == 0 ||
            candidate->reclaim_inflight ||
            candidate->reclaim_retry_after > now)
            continue;
        granule = candidate;
        owner_epoch = granule->owner_epoch;
        reclaim_attempt = granule->reclaim_attempt;
        granule_mb = gs_amm_granule_grant_mb(granule);
        ap_reclaim = granule->grant_id != 0 && granule->grant_generation != 0;
        drain_attempt = ap_reclaim ? 0 : granule->drain_attempt;
        if (!gs_amm_acquire_reclaim_syscall_lease_locked(state, granule, drain_attempt,
            owner_epoch, reclaim_attempt)) {
            granule = NULL;
            break;
        }
        reclaim_snapshot = *granule;
        break;
    }
    SpinLockRelease(&state->mutex);
    if (granule == NULL)
        return false;

    PG_TRY();
    {
        reclaimed = gs_amm_reclaim_granule_memory(&reclaim_snapshot, &reclaimed_mb);
    }
    PG_CATCH();
    {
        SpinLockAcquire(&state->mutex);
        (void)gs_amm_complete_reclaim_locked(
            state, granule, drain_attempt, owner_epoch, reclaim_attempt, false, 0);
        gs_amm_refresh_granule_counts_locked(state);
        SpinLockRelease(&state->mutex);
        gs_amm_restore_buffer_granules_if_disabled(state);
        PG_RE_THROW();
    }
    PG_END_TRY();

    SpinLockAcquire(&state->mutex);
    completed = gs_amm_complete_reclaim_locked(
        state, granule, drain_attempt, owner_epoch, reclaim_attempt, reclaimed, reclaimed_mb);
    if (completed && granule->state == GS_AMM_GRANULE_FREE && ap_reclaim)
        state->dynamic_used_mb = Max(state->dynamic_used_mb - granule_mb, 0);
    gs_amm_refresh_granule_counts_locked(state);
    SpinLockRelease(&state->mutex);
    if (completed)
        gs_amm_restore_buffer_granules_if_disabled(state);
    return completed;
}

static int gs_amm_reclaim_unused_ap_granules_locked(GsAmmSharedState *state, int requested_mb)
{
    int released_mb = 0;

    if (state == NULL || requested_mb <= 0)
        return 0;

    /*
     * The controller enters with state->mutex held.  Preserve that contract,
     * but never retain the spinlock across madvise or the fallback memset.
     */
    while (released_mb < requested_mb) {
        GsAmmGranuleMeta reclaim_snapshot;
        GsAmmGranuleMeta *granule = NULL;
        uint64 reclaim_attempt = 0;
        uint32 reclaim_owner_epoch = 0;
        uint64 reclaimed_mb = 0;
        int granule_mb = 0;
        volatile bool reclaimed = false;
        bool completed;

        for (int i = state->total_granules - 1; i >= 0; i--) {
            GsAmmGranuleMeta *candidate = &state->granules[i];

            if ((candidate->state != GS_AMM_GRANULE_AP_ACTIVE &&
                    candidate->state != GS_AMM_GRANULE_AP_RESERVED) ||
                candidate->used_bytes != 0 || candidate->alloc_cursor_bytes != 0 ||
                candidate->scan_inflight || candidate->reclaim_inflight ||
                !gs_amm_grant_retains_minimum_capacity_locked(state, candidate))
                continue;
            granule = candidate;
            break;
        }
        if (granule == NULL)
            break;

        granule_mb = gs_amm_granule_grant_mb(granule);
        if (!gs_amm_begin_reclaim_locked(state, granule, 0, &reclaim_attempt))
            break;
        if (granule->state != GS_AMM_GRANULE_RECLAIMING)
            break;
        reclaim_owner_epoch = granule->owner_epoch;
        if (!gs_amm_acquire_reclaim_syscall_lease_locked(state, granule, 0,
            reclaim_owner_epoch, reclaim_attempt))
            break;

        reclaim_snapshot = *granule;
        gs_amm_refresh_granule_counts_locked(state);
        SpinLockRelease(&state->mutex);

        PG_TRY();
        {
            reclaimed = gs_amm_reclaim_granule_memory(&reclaim_snapshot, &reclaimed_mb);
        }
        PG_CATCH();
        {
            SpinLockAcquire(&state->mutex);
            (void)gs_amm_complete_reclaim_locked(state, granule, 0, reclaim_owner_epoch,
                reclaim_attempt, false, 0);
            gs_amm_refresh_granule_counts_locked(state);
            SpinLockRelease(&state->mutex);
            PG_RE_THROW();
        }
        PG_END_TRY();

        SpinLockAcquire(&state->mutex);
        completed = gs_amm_complete_reclaim_locked(state, granule, 0, reclaim_owner_epoch,
            reclaim_attempt, reclaimed, reclaimed_mb);
        if (completed && granule->state == GS_AMM_GRANULE_FREE) {
            released_mb += granule_mb;
            state->ap_idle_reclaim_count++;
            state->ap_idle_reclaim_mb += granule_mb;
        }
        gs_amm_refresh_granule_counts_locked(state);
        if (!completed)
            break;
    }

    return released_mb;
}

static void gs_amm_backend_cleanup(int code, Datum arg)
{
    (void)code;
    (void)arg;

    gs_amm_release_backend_grant(false);
    MyGsAmmCleanupRegistered = false;
}

static void gs_amm_register_backend_cleanup(void)
{
    if (MyGsAmmCleanupRegistered)
        return;

    on_proc_exit(gs_amm_backend_cleanup, 0);
    MyGsAmmCleanupRegistered = true;
}

bool GsAmmReleaseGrantToken(GsAmmGrantToken expected_token, bool restore_work_mem)
{
    GsAmmSharedState *state = GsAmmState;
    GsAmmGrantToken backend_token = {MyGsAmmGrantId, MyGsAmmGrantGeneration};
    uint64 grant_id = expected_token.grant_id;
    int released_mb = 0;
    int saved_work_mem_kb = MyGsAmmSavedWorkMemKb;
    bool had_grant = MyGsAmmGrantMb > 0;
    bool native_grant = MyGsAmmGrantNative;

    if (expected_token.grant_id == 0 || expected_token.grant_generation == 0 ||
        backend_token.grant_id == 0 || backend_token.grant_generation == 0)
        return false;
    if (!gs_amm_grant_token_equal(expected_token, backend_token))
        return false;
    if (!GsAmmOperationBegin())
        return false;

    PG_TRY();
    {
    gs_amm_grant_arena_reset(expected_token);
    MyGsAmmGrantGeneration = 0;

    if (state != NULL && grant_id != 0) {
        released_mb = gs_amm_release_ap_granules_locked(state, expected_token);
        SpinLockAcquire(&state->mutex);
        if (released_mb > 0)
            state->dynamic_used_mb = Max(state->dynamic_used_mb - released_mb, 0);
        if (had_grant && state->active_ap_count > 0)
            state->active_ap_count--;
        if (state->active_ap_count == 0) {
            state->effective_grant_kb = 0;
            state->last_effective_grant_kb = 0;
        }
        if (native_grant) {
            state->native_release_count++;
            if (state->native_active_grant_count > 0)
                state->native_active_grant_count--;
            if (state->native_current_active && state->native_current_grant_id == grant_id &&
                state->native_current_grant_generation == expected_token.grant_generation) {
                state->native_current_active = false;
                state->native_current_grant_id = 0;
                state->native_current_grant_generation = 0;
                state->native_current_granted_kb = 0;
                state->native_current_memory_mode = GS_AMM_MEMORY_MODE_NONE;
            }
        }
        SpinLockRelease(&state->mutex);
        gs_amm_restore_buffer_granules_if_disabled(state);
    }

    /* GUC restoration can report errors; never leave a released grant owned by this backend. */
    gs_amm_clear_backend_grant_state();
    if (restore_work_mem)
        gs_amm_restore_backend_work_mem(saved_work_mem_kb);
    }
    PG_CATCH();
    {
        GsAmmOperationEnd();
        PG_RE_THROW();
    }
    PG_END_TRY();

    GsAmmOperationEnd();
    return true;
}

bool GsAmmReleaseGrant(uint64 expected_generation, bool restore_work_mem)
{
    GsAmmGrantToken expected_token = {MyGsAmmGrantId, expected_generation};

    return GsAmmReleaseGrantToken(expected_token, restore_work_mem);
}

static void gs_amm_release_backend_grant(bool restore_work_mem)
{
    GsAmmGrantToken token = {MyGsAmmGrantId, MyGsAmmGrantGeneration};

    (void)GsAmmReleaseGrantToken(token, restore_work_mem);
}

static int gs_amm_reserve_ap_granules_locked(
    GsAmmSharedState *state, GsAmmGrantToken token, int requested_mb, int *grant_granules)
{
    int granted_mb = 0;
    int granules = 0;
    int64 available_mb = 0;

    *grant_granules = 0;
    if (state->maintenance_resetting)
        return 0;
    if (token.grant_id == 0 || token.grant_generation == 0 || requested_mb <= 0)
        return 0;

    for (int i = state->total_granules - 1; i >= 0; i--) {
        GsAmmGranuleMeta *granule = &state->granules[i];

        if (granule->state == GS_AMM_GRANULE_FREE && !granule->scan_inflight &&
            !granule->reclaim_inflight &&
            GsAmmGranuleCanCompleteApLifecycle(granule->generation, granule->owner_epoch))
            available_mb += gs_amm_granule_capacity_mb(granule);
    }
    if (available_mb < requested_mb)
        return 0;

    for (int i = state->total_granules - 1; i >= 0 && granted_mb < requested_mb; i--) {
        GsAmmGranuleMeta *granule = &state->granules[i];
        int capacity_mb;
        int assign_mb;
        bool reservation_published;

        if (granule->state != GS_AMM_GRANULE_FREE || granule->scan_inflight ||
            granule->reclaim_inflight ||
            !GsAmmGranuleCanCompleteApLifecycle(granule->generation, granule->owner_epoch))
            continue;

        capacity_mb = gs_amm_granule_capacity_mb(granule);
        assign_mb = Min(capacity_mb, requested_mb - granted_mb);
        reservation_published =
            gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_AP_RESERVED);
        Assert(reservation_published);
        if (!reservation_published) {
            (void)gs_amm_forward_unwind_ap_granules_locked(state, token);
            gs_amm_refresh_granule_counts_locked(state);
            return 0;
        }
        granule->grant_id = token.grant_id;
        granule->grant_generation = token.grant_generation;
        granule->reserved_granules = 1;
        granule->active_grant_bytes = (uint64)assign_mb * 1024 * 1024;
        granule->used_bytes = 0;
        granule->alloc_cursor_bytes = 0;
        granted_mb += assign_mb;
        granules++;
    }

    if (granted_mb < requested_mb) {
        (void)gs_amm_forward_unwind_ap_granules_locked(state, token);
        gs_amm_refresh_granule_counts_locked(state);
        return 0;
    }

    *grant_granules = granules;
    gs_amm_refresh_granule_counts_locked(state);
    return granted_mb;
}

static int gs_amm_current_active_buffer_blocks(GsAmmSharedState *state)
{
    GsAmmGranuleSummary summary;
    int active_blocks;

    SpinLockAcquire(&state->mutex);
    gs_amm_summarize_granules_locked(state, &summary);
    active_blocks = summary.buffer_active_blocks;
    SpinLockRelease(&state->mutex);

    return active_blocks > 0 ? active_blocks : StrategyActiveBufferCount();
}

static void gs_amm_resize_core(int target_blocks, GsAmmResizeOutcome *out)
{
    GsAmmSharedState *state = gs_amm_get_state();
    int active_buffers = gs_amm_current_active_buffer_blocks(state);
    TimestampTz now = GetCurrentTimestamp();
    bool guard_blocked = false;
    const char *guard_state = "ready";
    int final_active_buffers = active_buffers;
    int pending_retire_blocks = 0;
    bool drain_deferred = false;

    out->decision = "noop";
    out->reason = "none";
    out->target_blocks = target_blocks;
    out->active_blocks = active_buffers;
    out->pending_retire_blocks = 0;
    out->rolled_back = false;

    if (target_blocks != active_buffers) {
        GsAmmGranuleSummary guard_event_before;

        SpinLockAcquire(&state->mutex);
        guard_state = gs_amm_resize_guard_state(state, now);
        gs_amm_summarize_granules_locked(state, &guard_event_before);
        if (gs_amm_recent_tps_drop_blocks_resize_locked(state, now)) {
            state->guard_block_count++;
            if (target_blocks < active_buffers) {
                state->rollback_count++;
                state->last_rollback_target_mb = (int)gs_amm_blocks_to_mb(active_buffers);
                state->cooldown_until = gs_amm_timestamp_after_ms(now, gs_amm_resize_cooldown_ms);
            }
            if (gs_amm_recovery_cooldown_hot_locked(state, now))
                state->recovery_cooldown_block_count++;
            guard_state = gs_amm_resize_guard_state(state, now);
            gs_amm_record_pool_event_locked(state, GS_AMM_FAIL_CLOSED, guard_state,
                &guard_event_before, now);
            guard_blocked = true;
        } else {
            state->last_resize_time = now;
            state->resize_observe_until = gs_amm_timestamp_after_ms(now, gs_amm_resize_observe_window_ms);
        }
        SpinLockRelease(&state->mutex);

        if (guard_blocked) {
            out->decision = "guard_blocked";
            out->reason = guard_state;
            out->rolled_back = true;
            return;
        }
    }

    if (target_blocks < active_buffers) {
        for (int granule_id = state->total_granules - 1; granule_id >= 0 && final_active_buffers > target_blocks;
             granule_id--) {
            GsAmmDrainStats stats;
            int first_buffer_id;
            int buffer_count;
            bool can_drain = false;

            SpinLockAcquire(&state->mutex);
            if (state->granules[granule_id].state == GS_AMM_GRANULE_BUFFER_ACTIVE &&
                state->granules[granule_id].first_buffer_id >= target_blocks) {
                can_drain = true;
                first_buffer_id = state->granules[granule_id].first_buffer_id;
                buffer_count = state->granules[granule_id].buffer_count;
            } else {
                first_buffer_id = 0;
                buffer_count = 0;
            }
            SpinLockRelease(&state->mutex);

            if (!can_drain)
                continue;

            now = GetCurrentTimestamp();
            GsAmmGranuleSummary guard_event_before;

            SpinLockAcquire(&state->mutex);
            gs_amm_summarize_granules_locked(state, &guard_event_before);
            if (gs_amm_recent_tps_drop_blocks_resize_locked(state, now)) {
                state->guard_block_count++;
                state->rollback_count++;
                state->last_rollback_target_mb = (int)gs_amm_blocks_to_mb(final_active_buffers);
                state->cooldown_until = gs_amm_timestamp_after_ms(now, gs_amm_resize_cooldown_ms);
                guard_state = gs_amm_resize_guard_state(state, now);
                gs_amm_record_pool_event_locked(state, GS_AMM_FAIL_CLOSED, guard_state,
                    &guard_event_before, now);
                guard_blocked = true;
            }
            SpinLockRelease(&state->mutex);
            if (guard_blocked) {
                out->decision = "guard_blocked";
                out->reason = guard_state;
                out->rolled_back = true;
                break;
            }

            if (gs_amm_drain_granule_with_operation(state, granule_id, &stats)) {
                final_active_buffers = gs_amm_current_active_buffer_blocks(state);
            } else {
                drain_deferred = true;
                pending_retire_blocks += buffer_count;
                (void)first_buffer_id;
                break;
            }
        }
    } else if (target_blocks > active_buffers) {
        final_active_buffers = gs_amm_expand_granules(state, target_blocks, false);
    }

    if (!guard_blocked) {
        if (target_blocks < active_buffers) {
            if (final_active_buffers <= target_blocks)
                out->decision = "shrunk";
            else if (drain_deferred)
                out->decision = "deferred";
            else
                out->decision = "noop";
            out->reason = drain_deferred ? "drain_pending" : "none";
        } else if (target_blocks > active_buffers) {
            out->decision = final_active_buffers >= target_blocks ? "expanded" : "deferred";
            out->reason = final_active_buffers >= target_blocks ? "none" : "free_granules_unavailable";
        } else {
            out->decision = "noop";
            out->reason = "none";
        }
    }

    out->active_blocks = gs_amm_current_active_buffer_blocks(state);
    out->pending_retire_blocks = Max(pending_retire_blocks, Max(out->active_blocks - target_blocks, 0));
}

Datum gs_amm_status(PG_FUNCTION_ARGS)
{
    char status[10240];
    GsAmmSharedState *state = gs_amm_get_state();
    TimestampTz now = GetCurrentTimestamp();
    GsAmmGranuleSummary granule_summary;
    int active_buffers;
    int dynamic_target_mb;
    int dynamic_used_mb;
    int active_ap_count;
    int ap_queue_len;
    uint64 ap_queue_head;
    uint64 ap_queue_tail;
    int ap_queue_admit_count;
    int ap_queue_timeout_count;
    int last_queue_wait_ms;
    int backpressure_count;
    int new_ap_guard_block_count;
    int grant_shrink_count;
    int grant_debt_mb;
    int effective_grant_kb;
    int effective_downgrade_count;
    double tp_baseline_tps;
    double tp_recent_tps;
    double tp_p95_latency_ms;
    double tp_raw_drop_ratio;
    int tp_window_ms;
    int tp_sample_count;
    int tp_baseline_rebase_count;
    bool tp_guard_hot;
    bool tp_generation_exhausted;
    double physical_read_rate;
    uint64 pending_writeback_pages;
    double dirty_page_ratio;
    int device_io_in_flight;
    double device_io_ms_rate;
    bool device_io_available;
    double temp_spill_mb_rate;
    double hash_multipass_rate;
    int io_recovery_stable_windows;
    int io_pressure_observed;
    int io_window_ms;
    bool io_guard_hot;
    long resize_cooldown_ms;
    long recovery_cooldown_ms;
    int guard_block_count;
    int rollback_count;
    int recovery_action_count;
    int recovery_cooldown_block_count;
    int last_rollback_target_mb;
    const char *resize_guard_state;
    char last_backpressure_reason[32];
    int last_prediction_mb;
    int last_tp_pressure;
    int last_io_pressure;
    int last_grant_mb;
    int last_effective_grant_kb;
    GsAmmAction last_action;
    int granule_mb;
    int granule_blocks;
    int total_granules;
    int buffer_active_granules;
    int buffer_draining_granules;
    int reclaiming_granules;
    int free_granules;
    int lifecycle_exhausted_granules;
    int ap_reserved_granules;
    int ap_active_granules;
    uint64 ap_grant_bytes;
    uint64 ap_granule_used_bytes;
    uint64 ap_granule_alloc_cursor_bytes;
    uint64 granule_alloc_success_count;
    uint64 granule_alloc_no_mapping_count;
    uint64 granule_alloc_no_owner_count;
    uint64 granule_alloc_capacity_exhausted_count;
    uint64 grant_reused_bytes;
    uint64 illegal_transition_count;
    uint64 epoch_mismatch_reject_count;
    uint64 stale_grant_token_count;
    int drain_pending_dirty;
    int drain_pending_pinned;
    int drain_pending_io;
    int drain_pending_hash;
    int drain_success_count;
    int drain_fail_count;
    int drain_rollback_count;
    int drain_priority_flush_count;
    uint64 reclaimed_mb;
    int reclaim_fail_count;
    int ap_idle_reclaim_count;
    int ap_idle_reclaim_mb;
    int auto_controller_step_count;
    uint64 native_eligible_count;
    uint64 native_admit_count;
    uint64 native_reject_count;
    uint64 native_release_count;
    uint64 native_error_cleanup_count;
    int native_active_grant_count;
    bool native_current_active;
    uint64 native_current_grant_id;
    uint64 native_current_grant_generation;
    int native_current_granted_kb;
    GsAmmMemoryMode native_current_memory_mode;
    int64 native_current_model_version;
    int64 native_current_leaf_id;
    int64 native_current_calibration_version;
    double native_current_calibration_scale;
    double native_current_raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double native_current_calibrated_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    char native_last_reason[GS_AMM_ADMISSION_REASON_LENGTH];
    uint64 dtree_feedback_sample_count;
    uint64 dtree_feedback_sample_dropped;
    uint64 dtree_feedback_pending;
    uint64 dtree_calibration_sample_dropped;
    bool dtree_last_feedback_available;
    bool dtree_last_feedback_measurement_valid;
    double dtree_last_feedback_observed_work_mem_kb;
    double dtree_last_feedback_grant_mb;
    uint64 dtree_last_feedback_spill_bytes;
    uint32 dtree_last_feedback_spill_files;
    uint32 dtree_last_feedback_spill_events;
    uint64 dtree_last_feedback_session_id;
    int64 dtree_model_version;
    int64 dtree_leaf_id;
    int64 dtree_calibration_version;
    double dtree_calibration_scale;
    uint64 dtree_calibration_update_count;
    int dtree_calibrated_leaf_count;
    int dtree_rollback_count;
    int dtree_frozen_leaf_count;
    double dtree_scale_min;
    double dtree_scale_max;
    double dtree_scale_avg;
    double dtree_ewma_underpredict_rate;
    double dtree_ewma_spill_mb;
    double dtree_ewma_runtime_ms;
    bool allocator_only_mode;
    int allocator_only_grant_mb;
    bool feedback_only_mode;
    int feedback_bootstrap_grant_mb;
    int feedback_max_grant_mb;
    int feedback_current_grant_mb;
    int feedback_ap_slot_limit;
    int feedback_stable_windows;
    uint64 feedback_completed_count;
    uint64 feedback_admit_count;
    uint64 feedback_growth_count;
    uint64 feedback_backoff_count;
    uint64 feedback_slot_block_count;
    double feedback_ewma_runtime_ms;
    double feedback_ewma_spill_mb;
    double feedback_ewma_peak_mb;
    char feedback_last_action[32];
    bool dtree_calibration_enabled;
    bool dtree_record_only;
    GsAmmRuntimeConfig runtime_config;
    uint64 config_version;
    int visible_auto_controller_step_count;
    uint64 pool_event_id;
    TimestampTz pool_event_time;
    GsAmmAction pool_event_action;
    int pool_event_duration_ms;
    GsAmmGranuleSummary pool_event_before;
    GsAmmGranuleSummary pool_event_after;
    char pool_event_reason[64];
    int granule_state_sum;
    bool granule_state_consistent;
    uint64 recorded_ap_grant_bytes;
    bool granule_grant_bytes_consistent;
    bool granule_used_within_grant;
    uint64 inactive_granule_buffer_violations;

    SpinLockAcquire(&state->mutex);
    gs_amm_summarize_granules_locked(state, &granule_summary);
    active_buffers = granule_summary.buffer_active_blocks;
    dynamic_target_mb = state->dynamic_target_mb;
    dynamic_used_mb = state->dynamic_used_mb;
    active_ap_count = state->active_ap_count;
    ap_queue_len = state->ap_queue_len;
    ap_queue_head = state->ap_queue_head;
    ap_queue_tail = state->ap_queue_tail;
    ap_queue_admit_count = state->ap_queue_admit_count;
    ap_queue_timeout_count = state->ap_queue_timeout_count;
    last_queue_wait_ms = state->last_queue_wait_ms;
    backpressure_count = state->backpressure_count;
    new_ap_guard_block_count = state->new_ap_guard_block_count;
    grant_shrink_count = state->grant_shrink_count;
    grant_debt_mb = state->grant_debt_mb;
    effective_grant_kb = state->effective_grant_kb;
    effective_downgrade_count = state->effective_downgrade_count;
    tp_baseline_tps = state->tp_baseline_tps;
    tp_recent_tps = state->tp_recent_tps;
    tp_p95_latency_ms = state->tp_p95_latency_ms;
    tp_raw_drop_ratio = state->tp_raw_drop_ratio;
    tp_window_ms = state->tp_window_ms;
    tp_sample_count = state->tp_sample_count;
    tp_baseline_rebase_count = state->tp_baseline_rebase_count;
    tp_guard_hot = gs_amm_tp_drop_guard_hot_locked(state);
    tp_generation_exhausted = state->tp_generation_exhausted;
    physical_read_rate = state->physical_read_rate;
    pending_writeback_pages = state->pending_writeback_pages;
    dirty_page_ratio = state->dirty_page_ratio;
    device_io_in_flight = state->device_io_in_flight;
    device_io_ms_rate = state->device_io_ms_rate;
    device_io_available = state->device_io_available;
    temp_spill_mb_rate = state->temp_spill_mb_rate;
    hash_multipass_rate = state->hash_multipass_rate;
    io_recovery_stable_windows = state->io_recovery_stable_windows;
    io_pressure_observed = state->io_pressure_observed;
    io_window_ms = state->io_window_ms;
    io_guard_hot = gs_amm_io_guard_hot_locked(state);
    resize_cooldown_ms = state->cooldown_until > now ? (long)((state->cooldown_until - now) / 1000) : 0L;
    recovery_cooldown_ms =
        state->recovery_cooldown_until > now ? (long)((state->recovery_cooldown_until - now) / 1000) : 0L;
    guard_block_count = state->guard_block_count;
    rollback_count = state->rollback_count;
    recovery_action_count = state->recovery_action_count;
    recovery_cooldown_block_count = state->recovery_cooldown_block_count;
    last_rollback_target_mb = state->last_rollback_target_mb;
    resize_guard_state = gs_amm_resize_guard_state(state, now);
    int reason_rc = snprintf_s(last_backpressure_reason, sizeof(last_backpressure_reason),
        sizeof(last_backpressure_reason) - 1, "%s", state->last_backpressure_reason);
    securec_check_ss(reason_rc, "\0", "\0");
    last_prediction_mb = state->last_prediction_mb;
    last_tp_pressure = state->last_tp_pressure;
    last_io_pressure = state->last_io_pressure;
    last_grant_mb = state->last_grant_mb;
    last_effective_grant_kb = state->last_effective_grant_kb;
    last_action = state->last_action;
    pool_event_id = state->last_pool_event_id;
    pool_event_time = state->last_pool_event_time;
    pool_event_action = state->last_pool_event_action;
    pool_event_duration_ms = state->last_pool_event_duration_ms;
    pool_event_before = state->last_pool_event_before;
    pool_event_after = state->last_pool_event_after;
    int event_reason_rc = snprintf_s(pool_event_reason, sizeof(pool_event_reason), sizeof(pool_event_reason) - 1,
        "%s", state->last_pool_event_reason);
    securec_check_ss(event_reason_rc, "\0", "\0");
    granule_mb = state->granule_mb;
    granule_blocks = state->granule_blocks;
    total_granules = granule_summary.total_granules;
    buffer_active_granules = granule_summary.buffer_active_granules;
    buffer_draining_granules = granule_summary.buffer_draining_granules;
    reclaiming_granules = granule_summary.reclaiming_granules;
    free_granules = granule_summary.free_granules;
    lifecycle_exhausted_granules = gs_amm_lifecycle_exhausted_granules_locked(state);
    ap_reserved_granules = granule_summary.ap_reserved_granules;
    ap_active_granules = granule_summary.ap_active_granules;
    ap_grant_bytes = granule_summary.ap_grant_bytes;
    ap_granule_used_bytes = granule_summary.ap_granule_used_bytes;
    ap_granule_alloc_cursor_bytes = granule_summary.ap_granule_alloc_cursor_bytes;
    recorded_ap_grant_bytes = state->ap_grant_bytes;
    granule_state_sum = granule_summary.buffer_active_granules + granule_summary.buffer_draining_granules +
        granule_summary.reclaiming_granules + granule_summary.free_granules +
        granule_summary.ap_reserved_granules + granule_summary.ap_active_granules;
    granule_state_consistent = granule_state_sum == granule_summary.total_granules;
    granule_grant_bytes_consistent = recorded_ap_grant_bytes == granule_summary.ap_grant_bytes;
    granule_used_within_grant = granule_summary.ap_granule_used_bytes <= granule_summary.ap_grant_bytes;
    granule_alloc_success_count = state->granule_alloc_success_count;
    granule_alloc_no_mapping_count = state->granule_alloc_no_mapping_count;
    granule_alloc_no_owner_count = state->granule_alloc_no_owner_count;
    granule_alloc_capacity_exhausted_count = state->granule_alloc_capacity_exhausted_count;
    grant_reused_bytes = state->grant_reused_bytes;
    illegal_transition_count = state->illegal_transition_count;
    epoch_mismatch_reject_count = state->epoch_mismatch_reject_count;
    stale_grant_token_count = state->stale_grant_token_count;
    drain_pending_dirty = state->drain_pending_dirty;
    drain_pending_pinned = state->drain_pending_pinned;
    drain_pending_io = state->drain_pending_io;
    drain_pending_hash = state->drain_pending_hash;
    drain_success_count = state->drain_success_count;
    drain_fail_count = state->drain_fail_count;
    drain_rollback_count = state->drain_rollback_count;
    drain_priority_flush_count = state->drain_priority_flush_count;
    reclaimed_mb = state->reclaimed_mb;
    reclaim_fail_count = state->reclaim_fail_count;
    ap_idle_reclaim_count = state->ap_idle_reclaim_count;
    ap_idle_reclaim_mb = state->ap_idle_reclaim_mb;
    auto_controller_step_count = state->auto_controller_step_count;
    native_eligible_count = state->native_eligible_count;
    native_admit_count = state->native_admit_count;
    native_reject_count = state->native_reject_count;
    native_release_count = state->native_release_count;
    native_error_cleanup_count = state->native_error_cleanup_count;
    native_active_grant_count = state->native_active_grant_count;
    native_current_active = state->native_current_active;
    native_current_grant_id = state->native_current_grant_id;
    native_current_grant_generation = state->native_current_grant_generation;
    native_current_granted_kb = state->native_current_granted_kb;
    native_current_memory_mode = state->native_current_memory_mode;
    native_current_model_version = state->native_current_model_version;
    native_current_leaf_id = state->native_current_leaf_id;
    native_current_calibration_version = state->native_current_calibration_version;
    native_current_calibration_scale = state->native_current_calibration_scale;
    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
        native_current_raw_bounds_kb[i] = state->native_current_raw_bounds_kb[i];
        native_current_calibrated_bounds_kb[i] = state->native_current_calibrated_bounds_kb[i];
    }
    int native_reason_rc = snprintf_s(native_last_reason, sizeof(native_last_reason), sizeof(native_last_reason) - 1,
        "%s", state->native_last_reason);
    securec_check_ss(native_reason_rc, "\0", "\0");
    dtree_feedback_sample_count = state->dtree_feedback_ring.feedback_sample_count;
    dtree_feedback_sample_dropped = state->dtree_feedback_ring.feedback_sample_dropped;
    dtree_feedback_pending = state->dtree_feedback_ring.next_sample_id -
        state->dtree_feedback_ring.next_calibration_sample_id;
    dtree_calibration_sample_dropped = state->dtree_feedback_ring.calibration_sample_dropped;
    dtree_last_feedback_available = false;
    dtree_last_feedback_measurement_valid = false;
    dtree_last_feedback_observed_work_mem_kb = 0.0;
    dtree_last_feedback_grant_mb = 0.0;
    dtree_last_feedback_spill_bytes = 0;
    dtree_last_feedback_spill_files = 0;
    dtree_last_feedback_spill_events = 0;
    dtree_last_feedback_session_id = 0;
    if (state->dtree_feedback_ring.feedback_sample_count > 0 &&
        state->dtree_feedback_ring.next_sample_id > 0) {
        const GsAmmDtreeFeedbackSample *sample =
            &state->dtree_feedback_ring.samples[(state->dtree_feedback_ring.next_sample_id - 1) %
                GS_AMM_DTREE_FEEDBACK_RING_SIZE];

        if (sample->sample_id == state->dtree_feedback_ring.next_sample_id - 1) {
            dtree_last_feedback_available = true;
            dtree_last_feedback_measurement_valid = sample->measurement_valid;
            dtree_last_feedback_observed_work_mem_kb = sample->observed_work_mem_kb;
            dtree_last_feedback_grant_mb = sample->grant_mb;
            dtree_last_feedback_spill_bytes = sample->spill_bytes;
            dtree_last_feedback_spill_files = sample->spill_files;
            dtree_last_feedback_spill_events = sample->spill_events;
            dtree_last_feedback_session_id = sample->session_id;
        }
    }
    dtree_model_version = 0;
    dtree_leaf_id = 0;
    dtree_calibration_version = state->dtree_calibration_table.calibration_version;
    dtree_calibration_scale = state->dtree_calibration_table.calibration_scale;
    dtree_calibration_update_count = state->dtree_calibration_table.calibration_update_count;
    dtree_calibrated_leaf_count = 0;
    dtree_rollback_count = state->dtree_calibration_table.rollback_count;
    dtree_frozen_leaf_count = state->dtree_calibration_table.frozen_leaf_count;
    dtree_scale_min = 0.0;
    dtree_scale_max = 0.0;
    dtree_scale_avg = 0.0;
    dtree_ewma_underpredict_rate = 0.0;
    dtree_ewma_spill_mb = 0.0;
    dtree_ewma_runtime_ms = 0.0;
    gs_amm_runtime_config_snapshot_locked(state, &runtime_config);
    config_version = runtime_config.config_version;
    allocator_only_mode = runtime_config.allocator_only_mode;
    allocator_only_grant_mb = gs_amm_allocator_only_grant_target_mb(&runtime_config);
    feedback_only_mode = runtime_config.feedback_only;
    feedback_bootstrap_grant_mb = runtime_config.feedback_bootstrap_grant_mb;
    feedback_max_grant_mb = runtime_config.feedback_max_grant_mb;
    feedback_current_grant_mb = state->feedback_current_grant_mb;
    feedback_ap_slot_limit = state->feedback_ap_slot_limit;
    feedback_stable_windows = state->feedback_stable_windows;
    feedback_completed_count = state->feedback_completed_count;
    feedback_admit_count = state->feedback_admit_count;
    feedback_growth_count = state->feedback_growth_count;
    feedback_backoff_count = state->feedback_backoff_count;
    feedback_slot_block_count = state->feedback_slot_block_count;
    feedback_ewma_runtime_ms = state->feedback_ewma_runtime_ms;
    feedback_ewma_spill_mb = state->feedback_ewma_spill_mb;
    feedback_ewma_peak_mb = state->feedback_ewma_peak_mb;
    int feedback_action_rc = snprintf_s(feedback_last_action, sizeof(feedback_last_action),
        sizeof(feedback_last_action) - 1, "%s", state->feedback_last_action[0] == '\0' ? "none" :
        state->feedback_last_action);
    securec_check_ss(feedback_action_rc, "\0", "\0");
    for (int i = 0; i < GS_AMM_DTREE_CALIBRATION_TABLE_SIZE; i++) {
        GsAmmDtreeCalibrationLeafState *leaf = &state->dtree_calibration_table.leaves[i];

        if (!leaf->initialized)
            continue;
        if (dtree_calibrated_leaf_count == 0) {
            dtree_model_version = leaf->model_version;
            dtree_leaf_id = leaf->leaf_id;
            dtree_calibration_version = leaf->calibration_version;
            dtree_calibration_scale = leaf->calibration_scale;
            dtree_ewma_underpredict_rate = leaf->ewma_underpredict_rate;
            dtree_ewma_spill_mb = leaf->ewma_spill_mb;
            dtree_ewma_runtime_ms = leaf->ewma_runtime_ms;
        }
        dtree_calibrated_leaf_count++;
        dtree_scale_avg += leaf->calibration_scale;
        if (dtree_scale_min == 0.0 || leaf->calibration_scale < dtree_scale_min)
            dtree_scale_min = leaf->calibration_scale;
        if (leaf->calibration_scale > dtree_scale_max)
            dtree_scale_max = leaf->calibration_scale;
    }
    if (dtree_calibrated_leaf_count > 0)
        dtree_scale_avg /= (double)dtree_calibrated_leaf_count;
    else
        dtree_scale_min = dtree_scale_max = dtree_scale_avg = dtree_calibration_scale;
    dtree_calibration_enabled = runtime_config.dtree_calibration_enabled;
    dtree_record_only = runtime_config.dtree_record_only;
    SpinLockRelease(&state->mutex);
    inactive_granule_buffer_violations = gs_amm_check_granule_invariants_locked(state);
    active_buffers = active_buffers > 0 ? active_buffers : StrategyActiveBufferCount();
    visible_auto_controller_step_count = gs_amm_enabled ? auto_controller_step_count : 0;

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "amm_enabled=%s config_version=%llu background_action_count=%d "
        "active_blocks=%d active_mb=%ld max_blocks=%d max_mb=%ld "
        "shared_buffers_min_mb=%d dynamic_target_mb=%d "
        "dynamic_used_mb=%d dynamic_free_mb=%d active_ap_count=%d "
        "ap_queue_len=%d ap_queue_head=%llu ap_queue_tail=%llu "
        "ap_queue_admit_count=%d ap_queue_timeout_count=%d last_queue_wait_ms=%d "
        "backpressure_count=%d new_ap_guard_block_count=%d last_backpressure_reason=%s grant_shrink_count=%d "
        "grant_debt_mb=%d effective_grant_kb=%d effective_downgrade_count=%d "
        "tp_baseline_tps=%.3f tp_recent_tps=%.3f "
        "tp_p95_latency_ms=%.3f tp_raw_drop_ratio=%.6f "
        "tp_jitter_limit=%.6f tp_guard_hot=%s tp_generation_exhausted=%s "
        "tp_sample_count=%d tp_baseline_rebase_count=%d "
        "physical_read_rate=%.6f pending_writeback_pages=%llu dirty_page_ratio=%.6f "
        "device_io_in_flight=%d device_io_ms_rate=%.6f device_io_available=%s "
        "temp_spill_mb_rate=%.6f hash_multipass_rate=%.6f io_recovery_stable_windows=%d "
        "io_pressure_observed=%d io_guard_hot=%s io_window_ms=%d "
        "tp_window_ms=%d resize_guard_state=%s resize_cooldown_ms=%ld "
        "recovery_cooldown_ms=%ld guard_block_count=%d rollback_count=%d "
        "recovery_action_count=%d recovery_cooldown_block_count=%d last_rollback_target_mb=%d "
        "last_action=%s last_prediction_mb=%d last_tp_pressure=%d "
        "last_io_pressure=%d last_grant_mb=%d last_effective_grant_kb=%d "
        "pool_event_id=%llu pool_event_timestamp=%lld pool_event_action=%s pool_event_reason=%s "
        "pool_event_duration_ms=%d pool_event_before_granules=%d,%d,%d,%d,%d,%d "
        "pool_event_after_granules=%d,%d,%d,%d,%d,%d "
        "resize_mode=granule_drain_expand resize_granule_mb=%d granule_blocks=%d "
        "total_granules=%d buffer_active_granules=%d buffer_draining_granules=%d reclaiming_granules=%d "
        "free_granules=%d lifecycle_exhausted_granules=%d ap_reserved_granules=%d ap_active_granules=%d "
        "ap_grant_bytes=%llu ap_granule_used_bytes=%llu ap_granule_alloc_cursor_bytes=%llu "
        "granule_state_sum=%d granule_state_consistent=%s granule_grant_bytes_consistent=%s "
        "granule_used_within_grant=%s inactive_granule_buffer_violations=%llu "
        "granule_alloc_success_count=%llu granule_alloc_no_mapping_count=%llu "
        "granule_alloc_no_owner_count=%llu granule_alloc_capacity_exhausted_count=%llu "
        "grant_reused_bytes=%llu "
        "illegal_transition_count=%llu epoch_mismatch_reject_count=%llu stale_grant_token_count=%llu "
        "drain_pending_dirty=%d drain_pending_pinned=%d drain_pending_io=%d drain_pending_hash=%d "
        "drain_success_count=%d drain_fail_count=%d drain_rollback_count=%d drain_priority_flush_count=%d "
        "reclaimed_mb=%llu reclaim_fail_count=%d "
        "ap_idle_reclaim_count=%d ap_idle_reclaim_mb=%d "
        "dtree_calibration_enabled=%s dtree_record_only=%s "
        "dtree_feedback_sample_count=%llu dtree_feedback_sample_dropped=%llu "
        "dtree_feedback_pending=%llu dtree_calibration_sample_dropped=%llu "
        "dtree_last_feedback_available=%s dtree_last_feedback_measurement_valid=%s "
        "dtree_last_feedback_observed_work_mem_kb=%.3f dtree_last_feedback_grant_mb=%.3f "
        "dtree_last_feedback_spill_bytes=%llu dtree_last_feedback_spill_files=%u "
        "dtree_last_feedback_spill_events=%u dtree_last_feedback_session_id=%llu "
        "dtree_model_version=%lld dtree_leaf_id=%lld "
        "dtree_calibration_version=%lld dtree_calibration_scale=%.10f "
        "dtree_calibration_update_count=%llu dtree_calibrated_leaf_count=%d "
        "dtree_scale_min=%.10f dtree_scale_max=%.10f dtree_scale_avg=%.10f "
        "dtree_rollback_count=%d dtree_frozen_leaf_count=%d "
        "dtree_ewma_underpredict_rate=%.10f dtree_ewma_spill_mb=%.10f dtree_ewma_runtime_ms=%.10f "
        "allocator_only_mode=%s allocator_only_grant_mb=%d "
        "feedback_only_mode=%s feedback_bootstrap_grant_mb=%d feedback_max_grant_mb=%d "
        "feedback_current_grant_mb=%d feedback_ap_slot_limit=%d feedback_stable_windows=%d "
        "feedback_completed_count=%llu feedback_admit_count=%llu feedback_growth_count=%llu "
        "feedback_backoff_count=%llu feedback_slot_block_count=%llu "
        "feedback_ewma_runtime_ms=%.3f feedback_ewma_spill_mb=%.3f feedback_ewma_peak_mb=%.3f "
        "feedback_last_action=%s "
        "native_auto_mode=%s native_ap_cost_threshold=%.3f native_eligible_count=%llu native_admit_count=%llu "
        "native_reject_count=%llu native_release_count=%llu native_error_cleanup_count=%llu "
        "native_active_grant_count=%d "
        "native_last_event_active=%s native_last_event_grant_id=%llu native_last_event_grant_generation=%llu "
        "native_last_event_granted_kb=%d native_last_event_model_version=%lld native_last_event_leaf_id=%lld "
        "native_last_event_calibration_version=%lld native_last_event_calibration_scale=%.10f "
        "native_last_event_raw_bounds_kb=%.3f,%.3f,%.3f "
        "native_last_event_calibrated_bounds_kb=%.3f,%.3f,%.3f native_last_event_memory_mode=%s "
        "native_last_reason=%s "
        "controller_autorun=metrics auto_controller_step_count=%d "
        "controller_decisions_observable=true "
        "unsafe_dirty_or_pinned_invalidations=0",
        gs_amm_enabled ? "true" : "false", (unsigned long long)config_version,
        visible_auto_controller_step_count,
        active_buffers, (long)gs_amm_blocks_to_mb(active_buffers), NORMAL_SHARED_BUFFER_NUM,
        (long)gs_amm_blocks_to_mb(NORMAL_SHARED_BUFFER_NUM), gs_amm_shared_buffers_min_mb, dynamic_target_mb,
        dynamic_used_mb, Max(dynamic_target_mb - dynamic_used_mb, 0), active_ap_count, ap_queue_len,
        (unsigned long long)ap_queue_head, (unsigned long long)ap_queue_tail, ap_queue_admit_count,
        ap_queue_timeout_count, last_queue_wait_ms, backpressure_count, new_ap_guard_block_count,
        last_backpressure_reason, grant_shrink_count, grant_debt_mb,
        effective_grant_kb, effective_downgrade_count,
        tp_baseline_tps, tp_recent_tps, tp_p95_latency_ms, tp_raw_drop_ratio, gs_amm_tp_jitter_limit,
        tp_guard_hot ? "true" : "false", tp_generation_exhausted ? "true" : "false", tp_sample_count,
        tp_baseline_rebase_count, physical_read_rate,
        (unsigned long long)pending_writeback_pages, dirty_page_ratio, device_io_in_flight, device_io_ms_rate,
        device_io_available ? "true" : "false", temp_spill_mb_rate, hash_multipass_rate, io_recovery_stable_windows,
        io_pressure_observed, io_guard_hot ? "true" : "false", io_window_ms,
        tp_window_ms, resize_guard_state,
        resize_cooldown_ms, recovery_cooldown_ms, guard_block_count, rollback_count,
        recovery_action_count, recovery_cooldown_block_count, last_rollback_target_mb,
        gs_amm_action_name(last_action), last_prediction_mb, last_tp_pressure,
        last_io_pressure, last_grant_mb, last_effective_grant_kb,
        (unsigned long long)pool_event_id, (long long)pool_event_time, gs_amm_action_name(pool_event_action),
        pool_event_reason, pool_event_duration_ms,
        pool_event_before.buffer_active_granules, pool_event_before.buffer_draining_granules,
        pool_event_before.reclaiming_granules, pool_event_before.free_granules,
        pool_event_before.ap_reserved_granules, pool_event_before.ap_active_granules,
        pool_event_after.buffer_active_granules, pool_event_after.buffer_draining_granules,
        pool_event_after.reclaiming_granules, pool_event_after.free_granules,
        pool_event_after.ap_reserved_granules, pool_event_after.ap_active_granules,
        granule_mb, granule_blocks,
        total_granules, buffer_active_granules, buffer_draining_granules, reclaiming_granules, free_granules,
        lifecycle_exhausted_granules, ap_reserved_granules, ap_active_granules, (unsigned long long)ap_grant_bytes,
        (unsigned long long)ap_granule_used_bytes, (unsigned long long)ap_granule_alloc_cursor_bytes,
        granule_state_sum, granule_state_consistent ? "true" : "false",
        granule_grant_bytes_consistent ? "true" : "false", granule_used_within_grant ? "true" : "false",
        (unsigned long long)inactive_granule_buffer_violations,
        (unsigned long long)granule_alloc_success_count,
        (unsigned long long)granule_alloc_no_mapping_count,
        (unsigned long long)granule_alloc_no_owner_count,
        (unsigned long long)granule_alloc_capacity_exhausted_count,
        (unsigned long long)grant_reused_bytes,
        (unsigned long long)illegal_transition_count,
        (unsigned long long)epoch_mismatch_reject_count,
        (unsigned long long)stale_grant_token_count,
        drain_pending_dirty, drain_pending_pinned, drain_pending_io, drain_pending_hash, drain_success_count,
        drain_fail_count, drain_rollback_count, drain_priority_flush_count,
        (unsigned long long)reclaimed_mb, reclaim_fail_count,
        ap_idle_reclaim_count, ap_idle_reclaim_mb,
        dtree_calibration_enabled ? "true" : "false", dtree_record_only ? "true" : "false",
        (unsigned long long)dtree_feedback_sample_count,
        (unsigned long long)dtree_feedback_sample_dropped,
        (unsigned long long)dtree_feedback_pending,
        (unsigned long long)dtree_calibration_sample_dropped,
        dtree_last_feedback_available ? "true" : "false",
        dtree_last_feedback_measurement_valid ? "true" : "false",
        dtree_last_feedback_observed_work_mem_kb, dtree_last_feedback_grant_mb,
        (unsigned long long)dtree_last_feedback_spill_bytes, dtree_last_feedback_spill_files,
        dtree_last_feedback_spill_events, (unsigned long long)dtree_last_feedback_session_id,
        (long long)dtree_model_version, (long long)dtree_leaf_id,
        (long long)dtree_calibration_version, dtree_calibration_scale,
        (unsigned long long)dtree_calibration_update_count, dtree_calibrated_leaf_count,
        dtree_scale_min, dtree_scale_max, dtree_scale_avg,
        dtree_rollback_count, dtree_frozen_leaf_count,
        dtree_ewma_underpredict_rate, dtree_ewma_spill_mb, dtree_ewma_runtime_ms,
        allocator_only_mode ? "true" : "false", allocator_only_grant_mb,
        feedback_only_mode ? "true" : "false", feedback_bootstrap_grant_mb, feedback_max_grant_mb,
        feedback_current_grant_mb, feedback_ap_slot_limit, feedback_stable_windows,
        (unsigned long long)feedback_completed_count, (unsigned long long)feedback_admit_count,
        (unsigned long long)feedback_growth_count, (unsigned long long)feedback_backoff_count,
        (unsigned long long)feedback_slot_block_count, feedback_ewma_runtime_ms, feedback_ewma_spill_mb,
        feedback_ewma_peak_mb, feedback_last_action,
        gs_amm_native_auto_mode ? "true" : "false", gs_amm_native_ap_cost_threshold,
        (unsigned long long)native_eligible_count,
        (unsigned long long)native_admit_count, (unsigned long long)native_reject_count,
        (unsigned long long)native_release_count, (unsigned long long)native_error_cleanup_count,
        native_active_grant_count,
        native_current_active ? "true" : "false", (unsigned long long)native_current_grant_id,
        (unsigned long long)native_current_grant_generation, native_current_granted_kb,
        (long long)native_current_model_version, (long long)native_current_leaf_id,
        (long long)native_current_calibration_version, native_current_calibration_scale,
        native_current_raw_bounds_kb[0], native_current_raw_bounds_kb[1], native_current_raw_bounds_kb[2],
        native_current_calibrated_bounds_kb[0], native_current_calibrated_bounds_kb[1],
        native_current_calibrated_bounds_kb[2], gs_amm_memory_mode_name(native_current_memory_mode),
        native_last_reason,
        visible_auto_controller_step_count);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_record_dtree_feedback(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    ArrayType *raw_bounds_array = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *calibrated_bounds_array = PG_GETARG_ARRAYTYPE_P(1);
    int64 model_version = PG_GETARG_INT64(2);
    int64 leaf_id = PG_GETARG_INT64(3);
    double observed_work_mem_kb = PG_GETARG_FLOAT8(4);
    double runtime_ms = PG_GETARG_FLOAT8(5);
    double spill_mb = PG_GETARG_FLOAT8(6);
    double grant_mb = PG_GETARG_FLOAT8(7);
    double tp_drop_ratio = PG_GETARG_FLOAT8(8);
    double io_pressure = PG_GETARG_FLOAT8(9);
    bool backpressure = PG_GETARG_BOOL(10);
    bool error = PG_GETARG_BOOL(11);
    Datum *raw_datums = NULL;
    Datum *calibrated_datums = NULL;
    bool *raw_nulls = NULL;
    bool *calibrated_nulls = NULL;
    int raw_count = 0;
    int calibrated_count = 0;
    double raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double calibrated_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    GsAmmSharedState *state = gs_amm_get_state();
    GsAmmFeedbackRecord feedback;
    char status[2048];

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to record GS AMM dtree feedback")));

    if (ARR_NDIM(raw_bounds_array) != 1 || ARR_ELEMTYPE(raw_bounds_array) != FLOAT8OID ||
        ARR_NDIM(calibrated_bounds_array) != 1 || ARR_ELEMTYPE(calibrated_bounds_array) != FLOAT8OID) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("expected one-dimensional float8[] dtree bounds arrays")));
    }

    deconstruct_array(raw_bounds_array, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd', &raw_datums, &raw_nulls,
        &raw_count);
    deconstruct_array(calibrated_bounds_array, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd', &calibrated_datums,
        &calibrated_nulls, &calibrated_count);
    if (raw_count != GS_AMM_DTREE_BOUND_COUNT || calibrated_count != GS_AMM_DTREE_BOUND_COUNT) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("expected %d dtree bounds values", GS_AMM_DTREE_BOUND_COUNT)));
    }

    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
        if (raw_nulls[i] || calibrated_nulls[i]) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("dtree feedback arrays cannot contain nulls")));
        }
        raw_bounds_kb[i] = DatumGetFloat8(raw_datums[i]);
        calibrated_bounds_kb[i] = DatumGetFloat8(calibrated_datums[i]);
    }
    if (!(observed_work_mem_kb > 0.0))
        observed_work_mem_kb = 0.0;
    if (!(runtime_ms > 0.0))
        runtime_ms = 0.0;
    if (!(spill_mb > 0.0))
        spill_mb = 0.0;
    if (!(grant_mb > 0.0))
        grant_mb = 0.0;
    if (!(tp_drop_ratio > 0.0))
        tp_drop_ratio = 0.0;
    if (tp_drop_ratio > 1.0)
        tp_drop_ratio = 1.0;
    if (!(io_pressure > 0.0))
        io_pressure = 0.0;
    if (io_pressure > 100.0)
        io_pressure = 100.0;

    errno_t feedback_rc = memset_s(&feedback, sizeof(feedback), 0, sizeof(feedback));
    securec_check(feedback_rc, "\0", "\0");
    feedback.session_id = u_sess->session_id;
    feedback.model_version = model_version;
    feedback.leaf_id = leaf_id;
    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
        feedback.raw_bounds_kb[i] = raw_bounds_kb[i];
        feedback.calibrated_bounds_kb[i] = calibrated_bounds_kb[i];
    }
    feedback.observed_work_mem_kb = observed_work_mem_kb;
    feedback.runtime_ms = runtime_ms;
    feedback.spill_mb = spill_mb;
    feedback.grant_mb = grant_mb;
    feedback.tp_drop_ratio = tp_drop_ratio;
    feedback.io_pressure = io_pressure;
    feedback.backpressure = backpressure;
    feedback.error = error;
    GsAmmRecordFeedback(&feedback);

    SpinLockAcquire(&state->mutex);
    double calibration_scale = state->dtree_calibration_table.calibration_scale;
    int64 calibration_version = state->dtree_calibration_table.calibration_version;
    uint64 sample_count = state->dtree_feedback_ring.feedback_sample_count;
    uint64 sample_dropped = state->dtree_feedback_ring.feedback_sample_dropped;
    SpinLockRelease(&state->mutex);

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "dtree_feedback_recorded=true model_version=%lld leaf_id=%lld calibration_version=%lld "
        "calibration_scale=%.10f feedback_sample_count=%llu feedback_sample_dropped=%llu "
        "observed_work_mem_kb=%.3f runtime_ms=%.3f spill_mb=%.3f grant_mb=%.3f "
        "tp_drop_ratio=%.6f io_pressure=%.3f backpressure=%s error=%s",
        (long long)model_version, (long long)leaf_id, (long long)calibration_version, calibration_scale,
        (unsigned long long)sample_count, (unsigned long long)sample_dropped, observed_work_mem_kb,
        runtime_ms, spill_mb, grant_mb, tp_drop_ratio, io_pressure,
        backpressure ? "true" : "false", error ? "true" : "false");
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_set_dtree_calibration_mode(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    bool calibration_enabled = PG_GETARG_BOOL(0);
    bool record_only = PG_GETARG_BOOL(1);
    GsAmmSharedState *state = gs_amm_get_state();
    GsAmmRuntimeConfig effective_config;
    uint64 config_version;
    bool updated;
    char status[256];

    if (!calibration_enabled)
        record_only = true;

    SpinLockAcquire(&state->mutex);
    gs_amm_runtime_config_snapshot_locked(state, &effective_config);
    updated = gs_amm_update_runtime_override_locked(state, effective_config.allocator_only_mode,
        effective_config.allocator_only_grant_mb, calibration_enabled, record_only);
    config_version = state->runtime_config.config_version;
    SpinLockRelease(&state->mutex);
    if (!updated)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("GS AMM runtime configuration version is exhausted")));

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "dtree_calibration_mode_set=true dtree_calibration_enabled=%s dtree_record_only=%s config_version=%llu",
        calibration_enabled ? "true" : "false", record_only ? "true" : "false",
        (unsigned long long)config_version);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_set_allocator_only_mode(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    bool allocator_only_mode = PG_GETARG_BOOL(0);
    int allocator_only_grant_mb = PG_GETARG_INT32(1);
    GsAmmSharedState *state = gs_amm_get_state();
    GsAmmRuntimeConfig effective_config;
    uint64 config_version;
    bool updated;
    char status[256];

    allocator_only_grant_mb = Max(allocator_only_grant_mb, gs_amm_ap_min_grant_mb);

    SpinLockAcquire(&state->mutex);
    gs_amm_runtime_config_snapshot_locked(state, &effective_config);
    updated = gs_amm_update_runtime_override_locked(state, allocator_only_mode, allocator_only_grant_mb,
        effective_config.dtree_calibration_enabled, effective_config.dtree_record_only);
    config_version = state->runtime_config.config_version;
    SpinLockRelease(&state->mutex);
    if (!updated)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("GS AMM runtime configuration version is exhausted")));

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "allocator_only_mode_set=true allocator_only_mode=%s allocator_only_grant_mb=%d config_version=%llu",
        allocator_only_mode ? "true" : "false", allocator_only_grant_mb,
        (unsigned long long)config_version);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_reset_dtree_calibration(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    GsAmmSharedState *state = gs_amm_get_state();
    char status[256];

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be superuser to reset GS AMM dtree calibration")));

    SpinLockAcquire(&state->mutex);
    gs_amm_init_dtree_state(state);
    SpinLockRelease(&state->mutex);

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "dtree_calibration_reset=true calibration_version=1 calibration_scale=1.0000000000 "
        "feedback_sample_count=0 feedback_sample_dropped=0");
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_reset_state(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    GsAmmSharedState *state = gs_amm_get_state();
    char status[512];

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to reset GS AMM state")));

    SpinLockAcquire(&state->mutex);
    if (state->maintenance_resetting) {
        SpinLockRelease(&state->mutex);
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("GS AMM state reset is already in progress")));
    }
    state->maintenance_resetting = true;
    if (state->operation_inflight != 0 || !gs_amm_state_can_reset_locked(state)) {
        state->maintenance_resetting = false;
        SpinLockRelease(&state->mutex);
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("cannot reset GS AMM state while ownership operations are active")));
    }
    if (!gs_amm_advance_tp_sample_generation_locked(state)) {
        state->maintenance_resetting = false;
        SpinLockRelease(&state->mutex);
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("cannot reset GS AMM state after TPS generation exhaustion")));
    }
    state->dynamic_target_mb = gs_amm_dynamic_target_default_mb();
    state->dynamic_used_mb = 0;
    state->active_ap_count = 0;
    state->feedback_current_grant_mb = Max(state->runtime_config.feedback_bootstrap_grant_mb, 1);
    state->feedback_ap_slot_limit = Max(state->runtime_config.feedback_initial_ap_slots, 1);
    state->feedback_stable_windows = 0;
    state->feedback_completed_count = 0;
    state->feedback_admit_count = 0;
    state->feedback_growth_count = 0;
    state->feedback_backoff_count = 0;
    state->feedback_slot_block_count = 0;
    state->feedback_ewma_runtime_ms = 0.0;
    state->feedback_ewma_spill_mb = 0.0;
    state->feedback_ewma_peak_mb = 0.0;
    gs_amm_copy_admission_reason(state->feedback_last_action, "reset");
    state->ap_queue_len = 0;
    state->ap_queue_head = 0;
    state->ap_queue_tail = 0;
    state->ap_queue_admit_count = 0;
    state->ap_queue_timeout_count = 0;
    state->last_queue_wait_ms = 0;
    for (int i = 0; i < GS_AMM_QUEUE_RING_SIZE; i++)
        state->ap_queue_slots[i] = 0;
    state->backpressure_count = 0;
    state->new_ap_guard_block_count = 0;
    state->grant_shrink_count = 0;
    state->grant_debt_mb = 0;
    state->effective_grant_kb = 0;
    state->effective_downgrade_count = 0;
    state->tp_baseline_tps = 0.0;
    state->tp_recent_tps = 0.0;
    state->tp_p95_latency_ms = 0.0;
    state->tp_raw_drop_ratio = 0.0;
    state->tp_window_ms = 0;
    state->tp_sample_count = 0;
    state->tp_baseline_rebase_count = 0;
    state->physical_read_rate = 0.0;
    state->pending_writeback_pages = 0;
    state->dirty_page_ratio = 0.0;
    state->device_io_in_flight = 0;
    state->device_io_ms_rate = 0.0;
    state->device_io_available = false;
    state->temp_spill_mb_rate = 0.0;
    state->hash_multipass_rate = 0.0;
    state->io_guard_latched = false;
    state->io_recovery_stable_windows = 0;
    state->io_recovery_last_window = 0;
    state->io_pressure_observed = 0;
    state->io_window_ms = 0;
    state->tp_sample_next = 0;
    for (int i = 0; i < GS_AMM_TP_WINDOW_SAMPLE_COUNT; i++) {
        state->tp_sample_time[i] = 0;
        state->tp_sample_tps[i] = 0.0;
        state->tp_sample_p95_latency_ms[i] = 0.0;
    }
    state->last_resize_time = 0;
    state->resize_observe_until = 0;
    state->cooldown_until = 0;
    state->recovery_cooldown_until = 0;
    state->guard_block_count = 0;
    state->rollback_count = 0;
    state->recovery_action_count = 0;
    state->recovery_cooldown_block_count = 0;
    state->last_rollback_target_mb = 0;
    state->last_prediction_mb = 0;
    state->last_tp_pressure = 0;
    state->last_io_pressure = 0;
    state->last_grant_mb = 0;
    state->last_effective_grant_kb = 0;
    gs_amm_set_backpressure_reason_locked(state, "none");
    state->last_action = GS_AMM_OBSERVE;
    state->drain_pending_dirty = 0;
    state->drain_pending_pinned = 0;
    state->drain_pending_io = 0;
    state->drain_pending_hash = 0;
    state->drain_success_count = 0;
    state->drain_fail_count = 0;
    state->drain_rollback_count = 0;
    state->drain_priority_flush_count = 0;
    state->reclaimed_mb = 0;
    state->reclaim_fail_count = 0;
    state->illegal_transition_count = 0;
    state->epoch_mismatch_reject_count = 0;
    state->stale_grant_token_count = 0;
    state->ap_idle_reclaim_count = 0;
    state->ap_idle_reclaim_mb = 0;
    state->auto_controller_step_count = 0;
    state->native_eligible_count = 0;
    state->native_admit_count = 0;
    state->native_reject_count = 0;
    state->native_release_count = 0;
    state->native_error_cleanup_count = 0;
    state->native_active_grant_count = 0;
    state->native_current_active = false;
    state->native_current_grant_id = 0;
    state->native_current_grant_generation = 0;
    state->native_current_granted_kb = 0;
    state->native_current_memory_mode = GS_AMM_MEMORY_MODE_NONE;
    state->native_current_model_version = 0;
    state->native_current_leaf_id = 0;
    state->native_current_calibration_version = 0;
    state->native_current_calibration_scale = 0.0;
    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
        state->native_current_raw_bounds_kb[i] = 0.0;
        state->native_current_calibrated_bounds_kb[i] = 0.0;
    }
    gs_amm_copy_admission_reason(state->native_last_reason, "none");
    gs_amm_reinitialize_granule_table_locked(state);
    state->reclaim_syscall_inflight = false;
    state->reclaim_syscall_attempt = 0;
    state->maintenance_resetting = false;
    SpinLockRelease(&state->mutex);

    int saved_work_mem_kb = MyGsAmmSavedWorkMemKb;
    gs_amm_clear_backend_grant_state();
    gs_amm_restore_backend_work_mem(saved_work_mem_kb);
    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "reset=true active_mb=%ld dynamic_target_mb=%d dynamic_used_mb=0 active_ap_count=0 "
        "ap_queue_len=0 last_action=OBSERVE",
        (long)gs_amm_blocks_to_mb(StrategyActiveBufferCount()), gs_amm_dynamic_target_default_mb());
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_set_dynamic_target_mb(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    int target_mb = PG_GETARG_INT32(0);
    GsAmmSharedState *state = gs_amm_get_state();
    int dynamic_target_mb;
    int dynamic_free_mb;
    int dynamic_used_mb;
    char status[512];

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to set GS AMM dynamic target")));

    target_mb = Max(target_mb, gs_amm_ap_min_grant_mb);

    SpinLockAcquire(&state->mutex);
    state->dynamic_target_mb = target_mb;
    dynamic_target_mb = state->dynamic_target_mb;
    dynamic_used_mb = state->dynamic_used_mb;
    dynamic_free_mb = Max(dynamic_target_mb - dynamic_used_mb, 0);
    SpinLockRelease(&state->mutex);

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "set_dynamic_target=true dynamic_target_mb=%d dynamic_used_mb=%d dynamic_free_mb=%d", dynamic_target_mb,
        dynamic_used_mb, dynamic_free_mb);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

static int gs_amm_tp_pressure_from_drop_ratio(double drop_ratio)
{
    int scaled_pressure;

    if (!(drop_ratio > 0.0))
        return 0;
    if (gs_amm_tp_jitter_limit <= 0.0 || drop_ratio >= gs_amm_tp_jitter_limit)
        return 100;

    scaled_pressure = (int)((drop_ratio / gs_amm_tp_jitter_limit) *
                            (double)Max(gs_amm_tp_pressure_guard - 1, 1));
    return Max(0, Min(scaled_pressure, Max(gs_amm_tp_pressure_guard - 1, 0)));
}

static void gs_amm_autorun_controller_from_metrics(int tp_pressure, int io_pressure)
{
    if (!gs_amm_enabled)
        return;

    GsAmmSharedState *state = gs_amm_get_state();
    int active_ap_count;
    int dynamic_used_mb;
    int ap_queue_len;
    int reclaiming_granules;
    bool tp_guard_hot;
    bool io_guard_hot;

    tp_pressure = Max(0, Min(tp_pressure, 100));
    io_pressure = Max(0, Min(io_pressure, 100));

    SpinLockAcquire(&state->mutex);
    active_ap_count = state->active_ap_count;
    dynamic_used_mb = state->dynamic_used_mb;
    ap_queue_len = state->ap_queue_len;
    reclaiming_granules = state->reclaiming_granules;
    tp_guard_hot = gs_amm_tp_drop_guard_hot_locked(state);
    io_guard_hot = gs_amm_io_guard_hot_locked(state);
    if (!tp_guard_hot && !io_guard_hot && active_ap_count == 0 && dynamic_used_mb == 0 &&
        ap_queue_len == 0 && reclaiming_granules == 0) {
        SpinLockRelease(&state->mutex);
        return;
    }
    state->auto_controller_step_count++;
    SpinLockRelease(&state->mutex);

    gs_amm_controller_step_internal(0, tp_pressure, io_pressure, NULL, 0);
}

typedef struct GsAmmNativeIoSignals {
    uint64 physical_read_count;
    uint64 dirty_page_count;
    uint64 pending_writeback_pages;
    uint64 temp_spill_bytes;
    uint64 hash_multipass_count;
} GsAmmNativeIoSignals;

static void GsAmmReadNativeIoSignals(GsAmmSharedState *state, GsAmmNativeIoSignals *signals)
{
    signals->physical_read_count = pg_atomic_read_u64(&state->shared_buffer_physical_read_count);
    signals->dirty_page_count = pg_atomic_read_u64(&state->dirty_page_count);
    signals->pending_writeback_pages = pg_atomic_read_u64(&state->pending_writeback_page_count);
    signals->temp_spill_bytes = pg_atomic_read_u64(&state->ap_temp_spill_bytes);
    signals->hash_multipass_count = pg_atomic_read_u64(&state->ap_hash_multipass_count);
}

void GsAmmPagewriterControllerTick(void)
{
    GsAmmDeviceIoSample device_sample;
    GsAmmNativeIoSignals signals;
    GsAmmTpWindowSnapshot tp_window_snapshot;
    GsAmmSharedState *state;
    TimestampTz now;
    TimestampTz elapsed_us;
    TimestampTz last_tick;
    double elapsed_seconds;
    double tps;
    double physical_read_rate;
    double dirty_page_ratio;
    double temp_spill_mb_rate;
    double hash_multipass_rate;
    double tp_recent_tps;
    double tp_window_p95_latency_ms;
    double tp_raw_drop_ratio;
    double tp_baseline_tps;
    int io_pressure;
    int tp_pressure;
    int active_buffer_blocks;
    uint64 last_commit_count;
    uint64 last_physical_read_count;
    uint64 last_temp_spill_bytes;
    uint64 last_hash_multipass_count;
    bool tp_window_mature;
    bool tp_baseline_rebased;
    bool raw_io_hot;

    if (!gs_amm_enabled) {
        return;
    }

    state = gs_amm_get_state();
    now = GetCurrentTimestamp();
    GsAmmReadNativeIoSignals(state, &signals);
    uint64 commit_count = pg_atomic_read_u64(&state->tp_commit_count);

    SpinLockAcquire(&state->mutex);
    if (state->native_telemetry_last_tick == 0) {
        state->native_telemetry_last_tick = now;
        state->native_telemetry_last_commit_count = commit_count;
        state->native_telemetry_last_physical_read_count = signals.physical_read_count;
        state->native_telemetry_last_temp_spill_bytes = signals.temp_spill_bytes;
        state->native_telemetry_last_hash_multipass_count = signals.hash_multipass_count;
        state->native_telemetry_tick_count++;
        SpinLockRelease(&state->mutex);
        return;
    }

    elapsed_us = now - state->native_telemetry_last_tick;
    if (elapsed_us < 1000000) {
        SpinLockRelease(&state->mutex);
        return;
    }
    last_tick = state->native_telemetry_last_tick;
    last_commit_count = state->native_telemetry_last_commit_count;
    last_physical_read_count = state->native_telemetry_last_physical_read_count;
    last_temp_spill_bytes = state->native_telemetry_last_temp_spill_bytes;
    last_hash_multipass_count = state->native_telemetry_last_hash_multipass_count;
    gs_amm_snapshot_tp_window_locked(state, &tp_window_snapshot);
    SpinLockRelease(&state->mutex);

    /* Diskstats may block on procfs; never hold the shared AMM spinlock while sampling it. */
    GsAmmSampleDeviceIo(&device_sample);

    elapsed_seconds = (double)elapsed_us / 1000000.0;
    tps = (double)(commit_count >= last_commit_count ? commit_count - last_commit_count : 0) /
        elapsed_seconds;
    physical_read_rate = (double)(signals.physical_read_count >= last_physical_read_count ?
                                      signals.physical_read_count - last_physical_read_count : 0) /
        elapsed_seconds;
    temp_spill_mb_rate = (double)(signals.temp_spill_bytes >= last_temp_spill_bytes ?
                                    signals.temp_spill_bytes - last_temp_spill_bytes : 0) /
        (1024.0 * 1024.0 * elapsed_seconds);
    hash_multipass_rate = (double)(signals.hash_multipass_count >= last_hash_multipass_count ?
                                       signals.hash_multipass_count - last_hash_multipass_count : 0) /
        elapsed_seconds;
    active_buffer_blocks = gs_amm_current_active_buffer_blocks(state);
    dirty_page_ratio = active_buffer_blocks > 0 ?
        (double)signals.dirty_page_count / (double)active_buffer_blocks : 0.0;
    io_pressure = gs_amm_compute_io_pressure(physical_read_rate, signals.pending_writeback_pages, dirty_page_ratio,
        device_sample.available ? device_sample.in_flight : 0, device_sample.available ? device_sample.io_ms_rate : 0.0,
        temp_spill_mb_rate, hash_multipass_rate);
    gs_amm_append_tp_window_snapshot(&tp_window_snapshot, now, tps, 0.0);
    gs_amm_compute_tp_window_snapshot(
        &tp_window_snapshot, now, GS_AMM_NATIVE_TP_WINDOW_MS, &tp_recent_tps, &tp_window_p95_latency_ms);
    tp_window_mature =
        gs_amm_tp_window_snapshot_mature(&tp_window_snapshot, now, GS_AMM_NATIVE_TP_WINDOW_MS);
    tp_baseline_tps = gs_amm_tp_baseline_candidate(
        &tp_window_snapshot, now, tp_window_mature, tp_recent_tps, &tp_baseline_rebased);
    if (!tp_window_mature || tp_baseline_tps < GS_AMM_TP_GUARD_MIN_BASELINE_TPS) {
        tp_raw_drop_ratio = 0.0;
    } else {
        tp_raw_drop_ratio = tp_recent_tps > 0.0 ?
            Max((tp_baseline_tps - tp_recent_tps) / tp_baseline_tps, 0.0) : 1.0;
    }
    tp_pressure = tp_window_mature && tp_baseline_tps >= GS_AMM_TP_GUARD_MIN_BASELINE_TPS ?
        gs_amm_tp_pressure_from_drop_ratio(tp_raw_drop_ratio) : 0;

    SpinLockAcquire(&state->mutex);
    if (state->native_telemetry_last_tick != last_tick ||
        state->tp_sample_generation != tp_window_snapshot.generation) {
        SpinLockRelease(&state->mutex);
        return;
    }
    if (!gs_amm_advance_tp_sample_generation_locked(state)) {
        SpinLockRelease(&state->mutex);
        return;
    }
    state->native_telemetry_last_tick = now;
    state->native_telemetry_last_commit_count = commit_count;
    state->native_telemetry_last_physical_read_count = signals.physical_read_count;
    state->native_telemetry_last_temp_spill_bytes = signals.temp_spill_bytes;
    state->native_telemetry_last_hash_multipass_count = signals.hash_multipass_count;
    state->native_telemetry_tick_count++;
    state->tp_sample_next = tp_window_snapshot.sample_next;
    state->tp_sample_count = tp_window_snapshot.sample_count;
    for (int index = 0; index < GS_AMM_TP_WINDOW_SAMPLE_COUNT; index++) {
        state->tp_sample_time[index] = tp_window_snapshot.sample_time[index];
        state->tp_sample_tps[index] = tp_window_snapshot.sample_tps[index];
        state->tp_sample_p95_latency_ms[index] = tp_window_snapshot.sample_p95_latency_ms[index];
    }
    state->tp_baseline_tps = tp_baseline_tps;
    if (tp_baseline_rebased)
        state->tp_baseline_rebase_count++;
    state->tp_recent_tps = tp_recent_tps;
    state->tp_p95_latency_ms = tp_window_p95_latency_ms;
    state->tp_window_ms = GS_AMM_NATIVE_TP_WINDOW_MS;
    state->tp_raw_drop_ratio = tp_raw_drop_ratio;
    state->physical_read_rate = physical_read_rate;
    state->pending_writeback_pages = signals.pending_writeback_pages;
    state->dirty_page_ratio = dirty_page_ratio;
    state->device_io_in_flight = device_sample.available ? device_sample.in_flight : 0;
    state->device_io_ms_rate = device_sample.available ? device_sample.io_ms_rate : 0.0;
    state->device_io_available = device_sample.available;
    state->temp_spill_mb_rate = temp_spill_mb_rate;
    state->hash_multipass_rate = hash_multipass_rate;
    state->io_pressure_observed = io_pressure;
    state->io_window_ms = (int)(elapsed_us / 1000);
    state->last_io_pressure = io_pressure;
    gs_amm_update_io_recovery_locked(state, now, device_sample.available);
    raw_io_hot = io_pressure >= gs_amm_io_pressure_guard;
    if (gs_amm_tp_drop_guard_hot_locked(state) || raw_io_hot)
        state->cooldown_until = gs_amm_timestamp_after_ms(now, gs_amm_resize_cooldown_ms);
    state->last_tp_pressure = tp_pressure;
    SpinLockRelease(&state->mutex);

    GsAmmCommitDeviceIoSample();
    gs_amm_autorun_controller_from_metrics(tp_pressure, io_pressure);
    GsAmmDtreeCalibrationTick();
}

Datum gs_amm_update_tp_metrics(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    double tps = PG_GETARG_FLOAT8(0);
    double p95_latency_ms = PG_GETARG_FLOAT8(1);
    int window_ms = PG_GETARG_INT32(2);
    GsAmmSharedState *state = gs_amm_get_state();
    GsAmmTpWindowSnapshot tp_window_snapshot;
    TimestampTz now = GetCurrentTimestamp();
    double tp_baseline_tps;
    double tp_recent_tps;
    double tp_window_p95_latency_ms;
    double tp_raw_drop_ratio;
    bool tp_window_mature;
    bool tp_baseline_rebased;
    long resize_cooldown_ms;
    const char *resize_guard_state;
    char status[1024];

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to update GS AMM TP metrics")));

    if (!(tps > 0.0))
        tps = 0.0;
    if (!(p95_latency_ms >= 0.0))
        p95_latency_ms = 0.0;
    window_ms = Max(window_ms, 0);

    for (;;) {
        SpinLockAcquire(&state->mutex);
        gs_amm_snapshot_tp_window_locked(state, &tp_window_snapshot);
        SpinLockRelease(&state->mutex);

        gs_amm_append_tp_window_snapshot(&tp_window_snapshot, now, tps, p95_latency_ms);
        gs_amm_compute_tp_window_snapshot(
            &tp_window_snapshot, now, window_ms, &tp_recent_tps, &tp_window_p95_latency_ms);
        tp_window_mature = gs_amm_tp_window_snapshot_mature(&tp_window_snapshot, now, window_ms);
        tp_baseline_tps = gs_amm_tp_baseline_candidate(
            &tp_window_snapshot, now, tp_window_mature, tp_recent_tps, &tp_baseline_rebased);
        if (!tp_window_mature || !(tp_baseline_tps > 0.0)) {
            tp_raw_drop_ratio = 0.0;
        } else {
            tp_raw_drop_ratio = tp_recent_tps > 0.0 ?
                Max((tp_baseline_tps - tp_recent_tps) / tp_baseline_tps, 0.0) : 1.0;
        }

        SpinLockAcquire(&state->mutex);
        if (state->tp_sample_generation != tp_window_snapshot.generation) {
            SpinLockRelease(&state->mutex);
            continue;
        }
        if (!gs_amm_advance_tp_sample_generation_locked(state)) {
            SpinLockRelease(&state->mutex);
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("cannot update GS AMM TP metrics after TPS generation exhaustion")));
        }
        state->tp_sample_next = tp_window_snapshot.sample_next;
        state->tp_sample_count = tp_window_snapshot.sample_count;
        for (int index = 0; index < GS_AMM_TP_WINDOW_SAMPLE_COUNT; index++) {
            state->tp_sample_time[index] = tp_window_snapshot.sample_time[index];
            state->tp_sample_tps[index] = tp_window_snapshot.sample_tps[index];
            state->tp_sample_p95_latency_ms[index] = tp_window_snapshot.sample_p95_latency_ms[index];
        }
        state->tp_baseline_tps = tp_baseline_tps;
        if (tp_baseline_rebased)
            state->tp_baseline_rebase_count++;
        state->tp_recent_tps = tp_recent_tps;
        state->tp_p95_latency_ms = tp_window_p95_latency_ms;
        state->tp_window_ms = window_ms;
        state->tp_raw_drop_ratio = tp_raw_drop_ratio;
        if (gs_amm_tp_drop_guard_hot_locked(state))
            state->cooldown_until = gs_amm_timestamp_after_ms(now, gs_amm_resize_cooldown_ms);
        tp_baseline_tps = state->tp_baseline_tps;
        tp_recent_tps = state->tp_recent_tps;
        tp_raw_drop_ratio = state->tp_raw_drop_ratio;
        resize_cooldown_ms = state->cooldown_until > now ? (long)((state->cooldown_until - now) / 1000) : 0L;
        resize_guard_state = gs_amm_resize_guard_state(state, now);
        SpinLockRelease(&state->mutex);
        break;
    }

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "tp_metrics_updated=true tp_baseline_tps=%.3f tp_recent_tps=%.3f "
        "tp_raw_drop_ratio=%.6f tp_window_ms=%d resize_guard_state=%s resize_cooldown_ms=%ld",
        tp_baseline_tps, tp_recent_tps, tp_raw_drop_ratio, window_ms, resize_guard_state, resize_cooldown_ms);
    securec_check_ss(rc, "\0", "\0");

    gs_amm_autorun_controller_from_metrics(tp_window_mature ? gs_amm_tp_pressure_from_drop_ratio(tp_raw_drop_ratio) : 0, 0);

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_update_io_metrics(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    double physical_read_rate = PG_GETARG_FLOAT8(0);
    int pending_writeback_pages = PG_GETARG_INT32(1);
    double temp_spill_mb_rate = PG_GETARG_FLOAT8(2);
    int hash_multipass_count = PG_GETARG_INT32(3);
    int window_ms = PG_GETARG_INT32(4);
    GsAmmSharedState *state = gs_amm_get_state();
    TimestampTz now = GetCurrentTimestamp();
    int io_pressure_observed;
    bool io_guard_hot;
    const char *resize_guard_state;
    char status[1024];

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to update GS AMM IO metrics")));

    if (!(physical_read_rate > 0.0))
        physical_read_rate = 0.0;
    pending_writeback_pages = Max(pending_writeback_pages, 0);
    if (!(temp_spill_mb_rate > 0.0))
        temp_spill_mb_rate = 0.0;
    hash_multipass_count = Max(hash_multipass_count, 0);
    window_ms = Max(window_ms, 0);
    io_pressure_observed = gs_amm_compute_io_pressure(physical_read_rate, (uint64)pending_writeback_pages, 0.0,
        0, 0.0, temp_spill_mb_rate, (double)hash_multipass_count);

    SpinLockAcquire(&state->mutex);
    state->physical_read_rate = physical_read_rate;
    state->pending_writeback_pages = (uint64)pending_writeback_pages;
    state->device_io_in_flight = 0;
    state->device_io_ms_rate = 0.0;
    state->device_io_available = false;
    state->temp_spill_mb_rate = temp_spill_mb_rate;
    state->hash_multipass_rate = (double)hash_multipass_count;
    state->io_pressure_observed = io_pressure_observed;
    state->io_window_ms = window_ms;
    state->last_io_pressure = io_pressure_observed;
    gs_amm_update_io_recovery_locked(state, now, true);
    if (io_pressure_observed >= gs_amm_io_pressure_guard)
        state->cooldown_until = gs_amm_timestamp_after_ms(now, gs_amm_resize_cooldown_ms);
    io_guard_hot = gs_amm_io_guard_hot_locked(state);
    resize_guard_state = gs_amm_resize_guard_state(state, now);
    SpinLockRelease(&state->mutex);

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "io_metrics_updated=true physical_read_rate=%.6f pending_writeback_pages=%d "
        "temp_spill_mb_rate=%.6f hash_multipass_rate=%d io_pressure_observed=%d "
        "io_guard_hot=%s io_window_ms=%d resize_guard_state=%s",
        physical_read_rate, pending_writeback_pages, temp_spill_mb_rate, hash_multipass_count, io_pressure_observed,
        io_guard_hot ? "true" : "false", window_ms, resize_guard_state);
    securec_check_ss(rc, "\0", "\0");

    gs_amm_autorun_controller_from_metrics(0, io_pressure_observed);

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_resize_shared_buffers(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    int target_mb = PG_GETARG_INT32(0);
    int target_blocks;
    int max_buffers = NORMAL_SHARED_BUFFER_NUM;
    char status[2048];
    GsAmmResizeOutcome outcome;

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to resize GS AMM shared buffers")));
    if (target_mb <= 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("shared buffers target must be positive")));

    target_blocks = gs_amm_mb_to_blocks(target_mb);
    if (target_blocks > max_buffers)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("shared buffers target exceeds the startup maximum envelope")));
    if (!GsAmmOperationBegin())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("cannot resize GS AMM shared buffers during maintenance reset")));

    PG_TRY();
    {
    target_blocks = gs_amm_align_resize_target_blocks(target_blocks, max_buffers);
    target_mb = (int)gs_amm_blocks_to_mb(target_blocks);

    gs_amm_resize_core(target_blocks, &outcome);

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "resize=%s reason=%s active_blocks=%d active_mb=%ld target_blocks=%d target_mb=%d "
        "max_blocks=%d max_mb=%ld resize_serialized=true pending_retire_blocks=%d "
        "resize_deferred=%s resize_granule_mb=%d resize_granule_aligned=true "
        "unsafe_dirty_or_pinned_invalidations=0",
        outcome.decision, outcome.reason, outcome.active_blocks, (long)gs_amm_blocks_to_mb(outcome.active_blocks),
        target_blocks, target_mb, max_buffers, (long)gs_amm_blocks_to_mb(max_buffers), outcome.pending_retire_blocks,
        strcmp(outcome.decision, "deferred") == 0 ? "true" : "false", gs_amm_resize_batch_mb);
    securec_check_ss(rc, "\0", "\0");
    }
    PG_CATCH();
    {
        GsAmmOperationEnd();
        PG_RE_THROW();
    }
    PG_END_TRY();

    GsAmmOperationEnd();
    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_controller_step(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    int ap_demand_mb = PG_GETARG_INT32(0);
    int tp_pressure = PG_GETARG_INT32(1);
    int io_pressure = PG_GETARG_INT32(2);
    char status[2048];

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to drive the GS AMM controller")));

    gs_amm_controller_step_internal(ap_demand_mb, tp_pressure, io_pressure, status, sizeof(status));
    PG_RETURN_TEXT_P(cstring_to_text(status));
}

static void gs_amm_controller_step_internal(
    int ap_demand_mb, int tp_pressure, int io_pressure, char *status, Size status_size)
{
    if (!gs_amm_enabled) {
        if (status != NULL && status_size > 0) {
            int rc = snprintf_s(status, status_size, status_size - 1, "action=OBSERVE reason=amm_disabled");
            securec_check_ss(rc, "\0", "\0");
        }
        return;
    }

    if (!GsAmmOperationBegin()) {
        if (status != NULL && status_size > 0) {
            int rc = snprintf_s(status, status_size, status_size - 1,
                "action=OBSERVE reason=maintenance_resetting");
            securec_check_ss(rc, "\0", "\0");
        }
        return;
    }

    PG_TRY();
    {

    int raw_ap_demand_mb;
    int effective_ap_demand_mb;
    int active_blocks = StrategyActiveBufferCount();
    int max_blocks = NORMAL_SHARED_BUFFER_NUM;
    int active_mb = (int)gs_amm_blocks_to_mb(active_blocks);
    int max_mb = (int)gs_amm_blocks_to_mb(max_blocks);
    GsAmmConfig cfg;
    GsAmmObservation obs;
    GsAmmSimState state0;
    GsAmmPlanResult plan;
    GsAmmResizeOutcome outcome;
    GsAmmSharedState *shared_state = gs_amm_get_state();
    GsAmmGranuleSummary pool_event_before;
    TimestampTz pool_event_started;
    GsAmmRuntimeConfig runtime_config;
    int resize_applied_mb = 0;
    int pre_dynamic_used_mb;
    int pre_dynamic_free_mb;
    int dynamic_target_mb;
    int dynamic_used_mb;
    int dynamic_free_mb;
    int free_granule_mb;
    int active_ap_count;
    int ap_queue_len;
    uint64 ap_queue_head;
    uint64 ap_queue_tail;
    int ap_queue_admit_count;
    int ap_queue_timeout_count;
    int backpressure_count;
    int new_ap_guard_block_count;
    int grant_shrink_count;
    int grant_debt_mb;
    int last_grant_mb;
    int last_effective_grant_kb;
    bool resize_guard_hot;
    bool recovery_cooldown_hot;
    char last_backpressure_reason[32];

    ap_demand_mb = Max(ap_demand_mb, 0);
    raw_ap_demand_mb = ap_demand_mb;
    gs_amm_runtime_config_snapshot(shared_state, &runtime_config);
    effective_ap_demand_mb = gs_amm_effective_controller_demand_mb(raw_ap_demand_mb, &runtime_config);
    tp_pressure = Max(0, Min(tp_pressure, 100));
    io_pressure = Max(0, Min(io_pressure, 100));

    (void)gs_amm_retry_failed_reclaim(shared_state);
    pool_event_started = GetCurrentTimestamp();
    gs_amm_default_config(&cfg, max_mb);

    SpinLockAcquire(&shared_state->mutex);
    gs_amm_summarize_granules_locked(shared_state, &pool_event_before);
    io_pressure = Max(io_pressure, shared_state->io_pressure_observed);
    SpinLockRelease(&shared_state->mutex);

    obs.ap_demand_mb = effective_ap_demand_mb;
    obs.tp_pressure = tp_pressure;
    obs.io_pressure = io_pressure;
    obs.tail_reclaimable = true;
    obs.telemetry_ok = true;

    state0.active_mb = active_mb;
    state0.max_mb = max_mb;
    state0.min_mb = cfg.shared_buffers_min_mb;
    state0.grant_debt_mb = 0;
    state0.tail_reclaimable = obs.tail_reclaimable;
    state0.ap_demand_mb = obs.ap_demand_mb;
    state0.tp_pressure = obs.tp_pressure;
    state0.io_pressure = obs.io_pressure;

    gs_amm_plan_actions(&state0, &obs, &cfg, &plan);

    SpinLockAcquire(&shared_state->mutex);
    if (shared_state->dynamic_target_mb <= 0)
        shared_state->dynamic_target_mb = gs_amm_dynamic_target_default_mb();
    pre_dynamic_used_mb = shared_state->dynamic_used_mb;
    pre_dynamic_free_mb = Max(shared_state->dynamic_target_mb - shared_state->dynamic_used_mb, 0);
    free_granule_mb = gs_amm_free_granule_mb_locked(shared_state);
    resize_guard_hot = gs_amm_recent_tps_drop_blocks_resize_locked(shared_state, GetCurrentTimestamp());
    recovery_cooldown_hot = gs_amm_recovery_cooldown_hot_locked(shared_state, GetCurrentTimestamp());
    if (recovery_cooldown_hot && tp_pressure >= cfg.tp_pressure_guard && active_mb < max_mb)
        shared_state->recovery_cooldown_block_count++;
    SpinLockRelease(&shared_state->mutex);

    if (tp_pressure >= cfg.tp_pressure_guard && active_mb < max_mb)
        plan.chosen_action = recovery_cooldown_hot ?
            (pre_dynamic_used_mb > 0 ? GS_AMM_AP_SHRINK : GS_AMM_BACKPRESSURE) :
            GS_AMM_TP_RECOVERY;
    else if (plan.chosen_action == GS_AMM_TP_RECOVERY && recovery_cooldown_hot)
        plan.chosen_action = pre_dynamic_used_mb > 0 ? GS_AMM_AP_SHRINK : GS_AMM_BACKPRESSURE;
    else if (resize_guard_hot && pre_dynamic_used_mb > 0)
        plan.chosen_action = GS_AMM_AP_SHRINK;
    else if (resize_guard_hot)
        plan.chosen_action = GS_AMM_BACKPRESSURE;
    else if ((tp_pressure >= cfg.tp_pressure_guard || io_pressure >= cfg.io_pressure_guard) && pre_dynamic_used_mb > 0)
        plan.chosen_action = GS_AMM_AP_SHRINK;
    else if (effective_ap_demand_mb > free_granule_mb && active_mb > state0.min_mb)
        plan.chosen_action = GS_AMM_BORROW_FROM_BUFFER;
    else if (effective_ap_demand_mb > pre_dynamic_free_mb && active_mb <= state0.min_mb)
        plan.chosen_action = GS_AMM_BACKPRESSURE;

    outcome.active_blocks = active_blocks;
    if (plan.chosen_action == GS_AMM_BORROW_FROM_BUFFER || plan.chosen_action == GS_AMM_TP_RECOVERY) {
        int step_mb = gs_amm_resize_step_mb(&state0, plan.chosen_action, &cfg);

        if (step_mb > 0) {
            int target_mb = (plan.chosen_action == GS_AMM_BORROW_FROM_BUFFER) ? state0.active_mb - step_mb :
                                                                                 state0.active_mb + step_mb;
            int target_blocks = gs_amm_mb_to_blocks(target_mb);

            target_blocks = gs_amm_align_resize_target_blocks(target_blocks, max_blocks);
            gs_amm_resize_core(target_blocks, &outcome);
            resize_applied_mb = (int)gs_amm_blocks_to_mb(outcome.active_blocks) - active_mb;
        }
    }

    SpinLockAcquire(&shared_state->mutex);
    if (shared_state->dynamic_target_mb <= 0)
        shared_state->dynamic_target_mb = gs_amm_dynamic_target_default_mb();
    shared_state->last_prediction_mb = raw_ap_demand_mb;
    shared_state->last_tp_pressure = tp_pressure;
    shared_state->last_io_pressure = io_pressure;
    shared_state->last_action = plan.chosen_action;
    if (plan.chosen_action == GS_AMM_AP_EXPAND) {
        int free_mb = Max(shared_state->dynamic_target_mb - shared_state->dynamic_used_mb, 0);
        int grant_mb = gs_amm_clamp_grant_mb(effective_ap_demand_mb, free_mb);

        shared_state->effective_grant_kb = 0;
        shared_state->last_effective_grant_kb = 0;
        shared_state->last_grant_mb = grant_mb;
    } else if (plan.chosen_action == GS_AMM_BORROW_FROM_BUFFER) {
        int borrowed_mb = Max(-resize_applied_mb, 0);
        int free_mb;
        int grant_mb;

        if (borrowed_mb > 0)
            shared_state->dynamic_target_mb += borrowed_mb;
        free_mb = Max(shared_state->dynamic_target_mb - shared_state->dynamic_used_mb, 0);
        grant_mb = gs_amm_clamp_grant_mb(effective_ap_demand_mb, free_mb);
        shared_state->effective_grant_kb = 0;
        shared_state->last_effective_grant_kb = 0;
        shared_state->last_grant_mb = grant_mb;
    } else if (plan.chosen_action == GS_AMM_AP_SHRINK || plan.chosen_action == GS_AMM_TP_RECOVERY) {
        int shrink_mb = Min(Max(effective_ap_demand_mb, cfg.resize_rate_limit_mb), Max(shared_state->dynamic_used_mb, 0));
        bool recovered_buffer = plan.chosen_action == GS_AMM_TP_RECOVERY && resize_applied_mb > 0;

        if (recovered_buffer) {
            TimestampTz now = GetCurrentTimestamp();

            shared_state->dynamic_target_mb = Max(gs_amm_ap_min_grant_mb, shared_state->dynamic_target_mb - resize_applied_mb);
            shared_state->recovery_action_count++;
            shared_state->recovery_cooldown_until = gs_amm_timestamp_after_ms(now, gs_amm_tp_recovery_cooldown_ms);
        }
        if (shrink_mb > 0 && shared_state->active_ap_count > 0) {
            int effective_mb;
            int idle_reclaimed_mb = gs_amm_reclaim_unused_ap_granules_locked(shared_state, shrink_mb);

            shared_state->dynamic_used_mb = Max(shared_state->dynamic_used_mb - idle_reclaimed_mb, 0);
            shared_state->grant_shrink_count++;
            shared_state->grant_debt_mb += Max(shrink_mb - idle_reclaimed_mb, 0);
            effective_mb = Max(gs_amm_ap_min_grant_mb, shared_state->dynamic_used_mb / Max(shared_state->active_ap_count, 1));
            shared_state->effective_grant_kb = effective_mb * 1024;
            shared_state->effective_downgrade_count++;
            shared_state->last_effective_grant_kb = shared_state->effective_grant_kb;
            shared_state->last_grant_mb = effective_mb;
        } else if (plan.chosen_action == GS_AMM_TP_RECOVERY) {
            shared_state->effective_grant_kb = 0;
            shared_state->last_effective_grant_kb = 0;
            shared_state->last_grant_mb = 0;
        }
    } else if (plan.chosen_action == GS_AMM_BACKPRESSURE) {
        shared_state->backpressure_count++;
        shared_state->last_grant_mb = 0;
        if (tp_pressure >= cfg.tp_pressure_guard || io_pressure >= cfg.io_pressure_guard) {
            shared_state->effective_grant_kb = 0;
            shared_state->last_effective_grant_kb = shared_state->effective_grant_kb;
        }
    }
    if (plan.chosen_action != GS_AMM_OBSERVE) {
        const char *event_reason = plan.binding_constraint[0] == '\0' ? "none" : plan.binding_constraint;

        gs_amm_record_pool_event_locked(shared_state, plan.chosen_action, event_reason,
            &pool_event_before, pool_event_started);
    }
    dynamic_target_mb = shared_state->dynamic_target_mb;
    dynamic_used_mb = shared_state->dynamic_used_mb;
    dynamic_free_mb = Max(dynamic_target_mb - dynamic_used_mb, 0);
    active_ap_count = shared_state->active_ap_count;
    ap_queue_len = shared_state->ap_queue_len;
    ap_queue_head = shared_state->ap_queue_head;
    ap_queue_tail = shared_state->ap_queue_tail;
    ap_queue_admit_count = shared_state->ap_queue_admit_count;
    ap_queue_timeout_count = shared_state->ap_queue_timeout_count;
    backpressure_count = shared_state->backpressure_count;
    new_ap_guard_block_count = shared_state->new_ap_guard_block_count;
    grant_shrink_count = shared_state->grant_shrink_count;
    grant_debt_mb = shared_state->grant_debt_mb;
    last_grant_mb = shared_state->last_grant_mb;
    last_effective_grant_kb = shared_state->last_effective_grant_kb;
    int reason_rc = snprintf_s(last_backpressure_reason, sizeof(last_backpressure_reason),
        sizeof(last_backpressure_reason) - 1, "%s", shared_state->last_backpressure_reason);
    securec_check_ss(reason_rc, "\0", "\0");
    SpinLockRelease(&shared_state->mutex);

    if (status != NULL && status_size > 0) {
        int rc = snprintf_s(status, status_size, status_size - 1,
            "chosen_action=%s candidate_count=%d rejected_count=%d score=%.4f active_mb=%ld "
            "resize_applied_mb=%d dynamic_target_mb=%d dynamic_used_mb=%d dynamic_free_mb=%d "
            "active_ap_count=%d ap_queue_len=%d ap_queue_head=%llu ap_queue_tail=%llu "
            "ap_queue_admit_count=%d ap_queue_timeout_count=%d backpressure_count=%d "
            "new_ap_guard_block_count=%d last_backpressure_reason=%s grant_shrink_count=%d "
            "grant_debt_mb=%d last_grant_mb=%d last_effective_grant_kb=%d binding_constraint=%s "
            "controller_decisions_observable=true prediction_mb=%d admission_demand_mb=%d "
            "allocator_only_mode=%s allocator_only_grant_mb=%d tp_pressure=%d io_pressure=%d",
            gs_amm_action_name(plan.chosen_action), plan.candidate_count, plan.rejected_count, plan.score,
            (long)gs_amm_blocks_to_mb(StrategyActiveBufferCount()), resize_applied_mb, dynamic_target_mb,
            dynamic_used_mb, dynamic_free_mb, active_ap_count, ap_queue_len, (unsigned long long)ap_queue_head,
            (unsigned long long)ap_queue_tail, ap_queue_admit_count, ap_queue_timeout_count, backpressure_count,
            new_ap_guard_block_count, last_backpressure_reason, grant_shrink_count, grant_debt_mb, last_grant_mb,
            last_effective_grant_kb, plan.binding_constraint[0] == '\0' ? "none" : plan.binding_constraint,
            raw_ap_demand_mb, effective_ap_demand_mb, runtime_config.allocator_only_mode ? "true" : "false",
            runtime_config.allocator_only_grant_mb, tp_pressure, io_pressure);
        securec_check_ss(rc, "\0", "\0");
    }
    }
    PG_CATCH();
    {
        GsAmmOperationEnd();
        PG_RE_THROW();
    }
    PG_END_TRY();

    GsAmmOperationEnd();
}

Datum gs_amm_begin_ap(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    int prediction_mb = PG_GETARG_INT32(0);
    int min_mb = PG_GETARG_INT32(1);
    int max_mb = PG_GETARG_INT32(2);
    int queue_timeout_ms = PG_GETARG_INT32(3);
    GsAmmSharedState *state;
    int dynamic_target_mb;
    int free_mb;
    int admission_request_mb;
    int allocator_only_grant_mb;
    int grant_mb = 0;
    int grant_granules = 0;
    int queue_wait_ms = 0;
    uint64 queue_ticket = 0;
    uint64 grant_id = 0;
    GsAmmGrantToken grant_token = {0, 0};
    bool admitted = false;
    bool queued = false;
    bool backpressure = false;
    bool guard_fast_block = false;
    bool allocator_only_mode;
    GsAmmRuntimeConfig runtime_config;
    const char *block_reason = "";
    char status[1280];

    if (!gs_amm_enabled)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("GS AMM is disabled")));
    state = gs_amm_get_state();
    if (!GsAmmOperationBegin())
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("cannot request a GS AMM AP grant during maintenance reset")));

    PG_TRY();
    {
    prediction_mb = Max(prediction_mb, 0);
    min_mb = Max(min_mb, gs_amm_ap_min_grant_mb);
    max_mb = Max(max_mb, min_mb);
    gs_amm_runtime_config_snapshot(state, &runtime_config);
    allocator_only_mode = runtime_config.allocator_only_mode;
    allocator_only_grant_mb = gs_amm_allocator_only_grant_target_mb(&runtime_config);
    admission_request_mb = gs_amm_effective_ap_request_mb(prediction_mb, min_mb, max_mb, &runtime_config);
    if (queue_timeout_ms < 0)
        queue_timeout_ms = gs_amm_ap_queue_timeout_ms;

    SpinLockAcquire(&state->mutex);
    if (state->dynamic_target_mb <= 0)
        state->dynamic_target_mb = gs_amm_dynamic_target_default_mb();
    dynamic_target_mb = state->dynamic_target_mb;
    free_mb = Max(dynamic_target_mb - state->dynamic_used_mb, 0);
    free_mb = Min(free_mb, gs_amm_free_granule_mb_locked(state));
    block_reason = gs_amm_new_ap_block_reason_locked(state, GetCurrentTimestamp());
    guard_fast_block = block_reason[0] != '\0';
    if (guard_fast_block) {
        free_mb = 0;
        queue_timeout_ms = 0;
        state->new_ap_guard_block_count++;
        gs_amm_set_backpressure_reason_locked(state, block_reason);
    }
    if (state->effective_grant_kb > 0)
        free_mb = Min(free_mb, Max(state->effective_grant_kb / 1024, 0));
    grant_mb = gs_amm_clamp_grant_mb(admission_request_mb, free_mb);
    if (grant_mb >= min_mb) {
        grant_token = gs_amm_next_grant_token_locked(state);
        grant_id = grant_token.grant_id;
        grant_mb = gs_amm_reserve_ap_granules_locked(state, grant_token, grant_mb, &grant_granules);
    }
    if (grant_mb >= min_mb && !gs_amm_activate_ap_granules_locked(state, grant_token)) {
        grant_mb = 0;
        grant_granules = 0;
    }
    if (grant_mb >= min_mb) {
        state->dynamic_used_mb += grant_mb;
        state->active_ap_count++;
        state->last_grant_mb = grant_mb;
        state->last_prediction_mb = prediction_mb;
        if (state->last_action == GS_AMM_OBSERVE)
            state->last_action = GS_AMM_AP_EXPAND;
        admitted = true;
    } else if (queue_timeout_ms > 0 && gs_amm_queue_register_locked(state, &queue_ticket)) {
        state->backpressure_count++;
        state->last_grant_mb = 0;
        state->last_prediction_mb = prediction_mb;
        state->last_action = GS_AMM_BACKPRESSURE;
        gs_amm_set_backpressure_reason_locked(state, "capacity");
        queued = true;
    } else {
        state->backpressure_count++;
        if (queue_timeout_ms > 0)
            state->ap_queue_timeout_count++;
        state->last_grant_mb = 0;
        state->last_prediction_mb = prediction_mb;
        state->last_action = GS_AMM_BACKPRESSURE;
        gs_amm_set_backpressure_reason_locked(state, guard_fast_block ? block_reason : "capacity");
        backpressure = true;
    }
    SpinLockRelease(&state->mutex);

    volatile uint64 live_queue_ticket = queue_ticket;
    volatile int live_queue_wait_ms = queue_wait_ms;
    PG_TRY();
    {
        while (queued && live_queue_ticket > 0 && queue_wait_ms < queue_timeout_ms) {
            int sleep_ms = Min(100, queue_timeout_ms - queue_wait_ms);

            CHECK_FOR_INTERRUPTS();
            pg_usleep((long)sleep_ms * 1000L);
            queue_wait_ms += sleep_ms;
            live_queue_wait_ms = queue_wait_ms;

            SpinLockAcquire(&state->mutex);
            if (gs_amm_queue_is_head_locked(state, live_queue_ticket)) {
            free_mb = Max(state->dynamic_target_mb - state->dynamic_used_mb, 0);
            free_mb = Min(free_mb, gs_amm_free_granule_mb_locked(state));
            block_reason = gs_amm_new_ap_block_reason_locked(state, GetCurrentTimestamp());
            if (block_reason[0] != '\0') {
                free_mb = 0;
                gs_amm_queue_finish_locked(state, live_queue_ticket, false, queue_wait_ms);
                live_queue_ticket = 0;
                state->new_ap_guard_block_count++;
                state->last_grant_mb = 0;
                state->last_prediction_mb = prediction_mb;
                state->last_action = GS_AMM_BACKPRESSURE;
                gs_amm_set_backpressure_reason_locked(state, block_reason);
                backpressure = true;
                queued = false;
            }
            if (state->effective_grant_kb > 0)
                free_mb = Min(free_mb, Max(state->effective_grant_kb / 1024, 0));
            grant_mb = queued ? gs_amm_clamp_grant_mb(admission_request_mb, free_mb) : 0;
            if (grant_mb >= min_mb) {
                grant_token = gs_amm_next_grant_token_locked(state);
                grant_id = grant_token.grant_id;
                grant_mb = gs_amm_reserve_ap_granules_locked(state, grant_token, grant_mb, &grant_granules);
            }
            if (grant_mb >= min_mb && !gs_amm_activate_ap_granules_locked(state, grant_token)) {
                grant_mb = 0;
                grant_granules = 0;
            }
            if (grant_mb >= min_mb) {
                gs_amm_queue_finish_locked(state, live_queue_ticket, true, queue_wait_ms);
                live_queue_ticket = 0;
                state->dynamic_used_mb += grant_mb;
                state->active_ap_count++;
                state->last_grant_mb = grant_mb;
                state->last_prediction_mb = prediction_mb;
                if (state->last_action == GS_AMM_OBSERVE || state->last_action == GS_AMM_BACKPRESSURE)
                    state->last_action = GS_AMM_AP_EXPAND;
                admitted = true;
                queued = false;
            }
            }
            SpinLockRelease(&state->mutex);
        }
    }
    PG_CATCH();
    {
        if (live_queue_ticket > 0) {
            SpinLockAcquire(&state->mutex);
            gs_amm_queue_finish_locked(state, live_queue_ticket, false, live_queue_wait_ms);
            SpinLockRelease(&state->mutex);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (queued && queue_ticket > 0 && !admitted) {
        SpinLockAcquire(&state->mutex);
        gs_amm_queue_finish_locked(state, queue_ticket, false, queue_wait_ms);
        state->last_grant_mb = 0;
        state->last_prediction_mb = prediction_mb;
        state->last_action = GS_AMM_BACKPRESSURE;
        SpinLockRelease(&state->mutex);
        backpressure = true;
    }

    if (admitted) {
        MyGsAmmGrantId = grant_id;
        MyGsAmmGrantGeneration = grant_token.grant_generation;
        MyGsAmmGrantGranules = grant_granules;
        MyGsAmmGrantNative = false;
        PG_TRY();
        {
            gs_amm_apply_backend_grant(grant_mb);
        }
        PG_CATCH();
        {
            (void)GsAmmReleaseGrantToken(grant_token, true);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "admitted=%s queued=%s backpressure=%s granted_mb=%d prediction_mb=%d min_mb=%d max_mb=%d "
        "admission_request_mb=%d allocator_only_mode=%s allocator_only_grant_mb=%d "
        "queue_wait_ms=%d queue_ticket=%llu grant_id=%llu grant_granules=%d backend_work_mem_kb=%d",
        admitted ? "true" : "false", queued ? "true" : "false", backpressure ? "true" : "false", grant_mb,
        prediction_mb, min_mb, max_mb, admission_request_mb, allocator_only_mode ? "true" : "false",
        allocator_only_grant_mb, queue_wait_ms, (unsigned long long)queue_ticket,
        (unsigned long long)grant_id, grant_granules, u_sess->attr.attr_memory.work_mem);
    securec_check_ss(rc, "\0", "\0");
    }
    PG_CATCH();
    {
        GsAmmOperationEnd();
        PG_RE_THROW();
    }
    PG_END_TRY();

    GsAmmOperationEnd();
    PG_RETURN_TEXT_P(cstring_to_text(status));
}

static int gs_amm_select_bound_grant_kb(int free_kb, int cache_bound_kb, int one_pass_bound_kb,
    int multi_pass_bound_kb, int admission_request_kb, int allocator_only_min_kb,
    bool allocator_only_mode, bool feedback_only, GsAmmMemoryMode *memory_mode)
{
    int grant_kb = 0;

    *memory_mode = GS_AMM_MEMORY_MODE_BACKPRESSURE;
    if (allocator_only_mode || feedback_only) {
        grant_kb = Min(free_kb, admission_request_kb);
        if (grant_kb >= allocator_only_min_kb)
            *memory_mode = feedback_only ? GS_AMM_MEMORY_MODE_FEEDBACK_ONLY : GS_AMM_MEMORY_MODE_ALLOCATOR_ONLY;
        else
            grant_kb = 0;
    } else if (free_kb >= cache_bound_kb) {
        grant_kb = cache_bound_kb;
        *memory_mode = GS_AMM_MEMORY_MODE_CACHE;
    } else if (free_kb >= one_pass_bound_kb) {
        grant_kb = one_pass_bound_kb;
        *memory_mode = GS_AMM_MEMORY_MODE_ONEPASS;
    } else if (free_kb >= multi_pass_bound_kb) {
        grant_kb = multi_pass_bound_kb;
        *memory_mode = GS_AMM_MEMORY_MODE_MULTIPASS;
    }
    return grant_kb;
}

/*
 * The dynamic target is a quota, while grants are backed by physical
 * granules.  Before a first grant can be reserved, make one guarded
 * controller pass to create physical capacity when the free list is empty.
 */
static void gs_amm_prepare_admission_granules(int admission_demand_mb)
{
    GsAmmSharedState *state = gs_amm_get_state();
    int free_granule_mb;
    int tp_pressure;
    int io_pressure;
    const char *block_reason;

    admission_demand_mb = Max(admission_demand_mb, gs_amm_ap_min_grant_mb);

    SpinLockAcquire(&state->mutex);
    free_granule_mb = gs_amm_free_granule_mb_locked(state);
    block_reason = gs_amm_new_ap_block_reason_locked(state, GetCurrentTimestamp());
    tp_pressure = gs_amm_tp_pressure_from_drop_ratio(state->tp_raw_drop_ratio);
    io_pressure = state->io_pressure_observed;
    SpinLockRelease(&state->mutex);

    if (free_granule_mb == 0 && block_reason[0] == '\0')
        gs_amm_controller_step_internal(admission_demand_mb, tp_pressure, io_pressure, NULL, 0);
}

static bool gs_amm_admit_bounds_internal(int cache_bound_kb, int one_pass_bound_kb, int multi_pass_bound_kb,
    int queue_timeout_ms, int prediction_mb, GsAmmAdmissionResult *result)
{
    GsAmmSharedState *state;
    int dynamic_target_kb;
    int dynamic_used_kb;
    int free_kb;
    int admission_request_kb;
    int allocator_only_min_kb;
    int allocator_only_grant_mb;
    int grant_kb = 0;
    int grant_mb = 0;
    int grant_granules = 0;
    int queue_wait_ms = 0;
    uint64 queue_ticket = 0;
    uint64 grant_id = 0;
    GsAmmGrantToken grant_token = {0, 0};
    bool admitted = false;
    bool queued = false;
    bool was_queued = false;
    bool backpressure = false;
    bool guard_fast_block = false;
    bool allocator_only_mode;
    bool feedback_only;
    const char *block_reason = "";
    const char *result_reason = "capacity";
    GsAmmMemoryMode memory_mode = GS_AMM_MEMORY_MODE_BACKPRESSURE;
    GsAmmRuntimeConfig runtime_config;
    errno_t rc;

    if (result == NULL)
        return false;

    rc = memset_s(result, sizeof(*result), 0, sizeof(*result));
    securec_check(rc, "\0", "\0");
    result->memory_mode = GS_AMM_MEMORY_MODE_BACKPRESSURE;
    gs_amm_copy_admission_reason(result->reason, result_reason);

    state = gs_amm_get_state();
    gs_amm_runtime_config_snapshot(state, &runtime_config);

    cache_bound_kb = Max(cache_bound_kb, 1);
    one_pass_bound_kb = Max(one_pass_bound_kb, 1);
    multi_pass_bound_kb = Max(multi_pass_bound_kb, 1);
    if (one_pass_bound_kb > cache_bound_kb)
        one_pass_bound_kb = cache_bound_kb;
    if (multi_pass_bound_kb > one_pass_bound_kb)
        multi_pass_bound_kb = one_pass_bound_kb;
    if (queue_timeout_ms < 0)
        queue_timeout_ms = gs_amm_ap_queue_timeout_ms;
    prediction_mb = prediction_mb > 0 ? prediction_mb : Max((cache_bound_kb + 1023) / 1024, 1);
    feedback_only = runtime_config.feedback_only;
    allocator_only_mode = runtime_config.allocator_only_mode && !feedback_only;
    allocator_only_grant_mb = gs_amm_allocator_only_grant_target_mb(&runtime_config);
    admission_request_kb = allocator_only_mode ? gs_amm_effective_ap_request_kb(&runtime_config) : cache_bound_kb;
    allocator_only_min_kb = Min(gs_amm_ap_min_grant_mb, INT_MAX / 1024) * 1024;

    result->prediction_mb = feedback_only ? 0 : prediction_mb;
    result->cache_bound_kb = cache_bound_kb;
    result->one_pass_bound_kb = one_pass_bound_kb;
    result->multi_pass_bound_kb = multi_pass_bound_kb;
    result->admission_request_kb = admission_request_kb;
    result->allocator_only_mode = allocator_only_mode;
    result->allocator_only_grant_mb = allocator_only_grant_mb;
    result->feedback_only = feedback_only;

    if (gs_amm_resolve_buffer_blocks() == NULL) {
        result->backpressure = true;
        gs_amm_copy_admission_reason(result->reason, "buffer_mapping");
        return true;
    }

    if (MyGsAmmGrantId != 0 || MyGsAmmGrantGeneration != 0) {
        result->backpressure = true;
        gs_amm_copy_admission_reason(result->reason, "grant_already_active");
        return true;
    }

    gs_amm_prepare_admission_granules(Max(prediction_mb, (cache_bound_kb + 1023) / 1024));

    SpinLockAcquire(&state->mutex);
    if (state->dynamic_target_mb <= 0)
        state->dynamic_target_mb = gs_amm_dynamic_target_default_mb();
    if (feedback_only)
        admission_request_kb = Max(state->feedback_current_grant_mb, 1) * 1024;
    result->admission_request_kb = admission_request_kb;
    dynamic_target_kb = state->dynamic_target_mb * 1024;
    dynamic_used_kb = state->dynamic_used_mb * 1024;
    free_kb = Max(dynamic_target_kb - dynamic_used_kb, 0);
    free_kb = Min(free_kb, gs_amm_free_granule_mb_locked(state) * 1024);
    block_reason = gs_amm_new_ap_block_reason_locked(state, GetCurrentTimestamp());
    guard_fast_block = block_reason[0] != '\0';
    if (guard_fast_block) {
        free_kb = 0;
        queue_timeout_ms = 0;
        state->new_ap_guard_block_count++;
        gs_amm_set_backpressure_reason_locked(state, block_reason);
    }
    if (feedback_only && state->active_ap_count >= state->feedback_ap_slot_limit) {
        free_kb = 0;
        state->feedback_slot_block_count++;
        gs_amm_set_backpressure_reason_locked(state, "feedback_slot_limit");
        result_reason = "feedback_slot_limit";
    }
    if (state->effective_grant_kb > 0)
        free_kb = Min(free_kb, state->effective_grant_kb);
    grant_kb = gs_amm_select_bound_grant_kb(free_kb, cache_bound_kb, one_pass_bound_kb,
        multi_pass_bound_kb, admission_request_kb, allocator_only_min_kb, allocator_only_mode, feedback_only,
        &memory_mode);
    grant_mb = (grant_kb + 1023) / 1024;
    if (grant_kb > 0) {
        grant_token = gs_amm_next_grant_token_locked(state);
        grant_id = grant_token.grant_id;
        if (gs_amm_reserve_ap_granules_locked(state, grant_token, grant_mb, &grant_granules) < grant_mb) {
            grant_kb = 0;
            grant_mb = 0;
            memory_mode = GS_AMM_MEMORY_MODE_BACKPRESSURE;
        }
    }
    if (grant_kb > 0 && !gs_amm_activate_ap_granules_locked(state, grant_token)) {
        grant_kb = 0;
        grant_mb = 0;
        grant_granules = 0;
        memory_mode = GS_AMM_MEMORY_MODE_BACKPRESSURE;
    }
    if (grant_kb > 0) {
        state->dynamic_used_mb += grant_mb;
        state->active_ap_count++;
        if (feedback_only)
            state->feedback_admit_count++;
        state->last_grant_mb = grant_mb;
        state->last_prediction_mb = prediction_mb;
        if (state->last_action == GS_AMM_OBSERVE)
            state->last_action = GS_AMM_AP_EXPAND;
        admitted = true;
        result_reason = "admitted";
    } else if (queue_timeout_ms > 0 && gs_amm_queue_register_locked(state, &queue_ticket)) {
        state->backpressure_count++;
        state->last_grant_mb = 0;
        state->last_prediction_mb = prediction_mb;
        state->last_action = GS_AMM_BACKPRESSURE;
        gs_amm_set_backpressure_reason_locked(state, "capacity");
        queued = true;
        was_queued = true;
    } else {
        state->backpressure_count++;
        if (queue_timeout_ms > 0)
            state->ap_queue_timeout_count++;
        state->last_grant_mb = 0;
        state->last_prediction_mb = prediction_mb;
        state->last_action = GS_AMM_BACKPRESSURE;
        gs_amm_set_backpressure_reason_locked(state, guard_fast_block ? block_reason : "capacity");
        backpressure = true;
        result_reason = guard_fast_block ? block_reason : "capacity";
    }
    SpinLockRelease(&state->mutex);

    volatile uint64 live_queue_ticket = queue_ticket;
    volatile int live_queue_wait_ms = queue_wait_ms;
    PG_TRY();
    {
        while (queued && live_queue_ticket > 0 && queue_wait_ms < queue_timeout_ms) {
            int sleep_ms = Min(100, queue_timeout_ms - queue_wait_ms);

            CHECK_FOR_INTERRUPTS();
            pg_usleep((long)sleep_ms * 1000L);
            queue_wait_ms += sleep_ms;
            live_queue_wait_ms = queue_wait_ms;

            SpinLockAcquire(&state->mutex);
            if (gs_amm_queue_is_head_locked(state, live_queue_ticket)) {
            dynamic_target_kb = state->dynamic_target_mb * 1024;
            dynamic_used_kb = state->dynamic_used_mb * 1024;
            free_kb = Max(dynamic_target_kb - dynamic_used_kb, 0);
            free_kb = Min(free_kb, gs_amm_free_granule_mb_locked(state) * 1024);
            block_reason = gs_amm_new_ap_block_reason_locked(state, GetCurrentTimestamp());
            if (block_reason[0] != '\0') {
                free_kb = 0;
                gs_amm_queue_finish_locked(state, live_queue_ticket, false, queue_wait_ms);
                live_queue_ticket = 0;
                state->new_ap_guard_block_count++;
                state->last_grant_mb = 0;
                state->last_prediction_mb = prediction_mb;
                state->last_action = GS_AMM_BACKPRESSURE;
                gs_amm_set_backpressure_reason_locked(state, block_reason);
                backpressure = true;
                queued = false;
                result_reason = block_reason;
            }
            if (state->effective_grant_kb > 0)
                free_kb = Min(free_kb, state->effective_grant_kb);
            if (feedback_only && state->active_ap_count >= state->feedback_ap_slot_limit) {
                free_kb = 0;
                state->feedback_slot_block_count++;
                gs_amm_set_backpressure_reason_locked(state, "feedback_slot_limit");
                result_reason = "feedback_slot_limit";
            }
            if (!queued) {
                grant_kb = 0;
                memory_mode = GS_AMM_MEMORY_MODE_BACKPRESSURE;
            } else {
                grant_kb = gs_amm_select_bound_grant_kb(free_kb, cache_bound_kb, one_pass_bound_kb,
                    multi_pass_bound_kb, admission_request_kb, allocator_only_min_kb,
                    allocator_only_mode, feedback_only, &memory_mode);
            }
            grant_mb = (grant_kb + 1023) / 1024;
            if (grant_kb > 0) {
                grant_token = gs_amm_next_grant_token_locked(state);
                grant_id = grant_token.grant_id;
                if (gs_amm_reserve_ap_granules_locked(state, grant_token, grant_mb, &grant_granules) < grant_mb) {
                    grant_kb = 0;
                    grant_mb = 0;
                    memory_mode = GS_AMM_MEMORY_MODE_BACKPRESSURE;
                }
            }
            if (grant_kb > 0 && !gs_amm_activate_ap_granules_locked(state, grant_token)) {
                grant_kb = 0;
                grant_mb = 0;
                grant_granules = 0;
                memory_mode = GS_AMM_MEMORY_MODE_BACKPRESSURE;
            }
            if (grant_kb > 0) {
                gs_amm_queue_finish_locked(state, live_queue_ticket, true, queue_wait_ms);
                live_queue_ticket = 0;
                state->dynamic_used_mb += grant_mb;
                state->active_ap_count++;
                if (feedback_only)
                    state->feedback_admit_count++;
                state->last_grant_mb = grant_mb;
                state->last_prediction_mb = prediction_mb;
                if (state->last_action == GS_AMM_OBSERVE || state->last_action == GS_AMM_BACKPRESSURE)
                    state->last_action = GS_AMM_AP_EXPAND;
                admitted = true;
                queued = false;
                result_reason = "queue_admitted";
            }
            }
            SpinLockRelease(&state->mutex);
        }
    }
    PG_CATCH();
    {
        if (live_queue_ticket > 0) {
            SpinLockAcquire(&state->mutex);
            gs_amm_queue_finish_locked(state, live_queue_ticket, false, live_queue_wait_ms);
            SpinLockRelease(&state->mutex);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (queued && queue_ticket > 0 && !admitted) {
        SpinLockAcquire(&state->mutex);
        gs_amm_queue_finish_locked(state, queue_ticket, false, queue_wait_ms);
        state->last_grant_mb = 0;
        state->last_prediction_mb = prediction_mb;
        state->last_action = GS_AMM_BACKPRESSURE;
        SpinLockRelease(&state->mutex);
        grant_kb = 0;
        grant_mb = 0;
        memory_mode = GS_AMM_MEMORY_MODE_BACKPRESSURE;
        backpressure = true;
        queued = false;
        result_reason = "queue_timeout";
    }

    if (admitted) {
        MyGsAmmGrantId = grant_id;
        MyGsAmmGrantGeneration = grant_token.grant_generation;
        MyGsAmmGrantGranules = grant_granules;
        MyGsAmmGrantNative = false;
        PG_TRY();
        {
            gs_amm_apply_backend_grant_kb(grant_kb);
        }
        PG_CATCH();
        {
            (void)GsAmmReleaseGrantToken(grant_token, true);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }

    result->admitted = admitted;
    result->queued = was_queued;
    result->backpressure = backpressure;
    result->grant_id = admitted ? grant_id : 0;
    result->grant_generation = admitted ? grant_token.grant_generation : 0;
    result->granted_kb = admitted ? grant_kb : 0;
    result->granted_mb = admitted ? grant_mb : 0;
    result->queue_wait_ms = queue_wait_ms;
    result->queue_ticket = queue_ticket;
    result->grant_granules = admitted ? grant_granules : 0;
    result->memory_mode = memory_mode;
    gs_amm_copy_admission_reason(result->reason, result_reason);
    return true;
}

bool GsAmmAdmitBounds(int cache_bound_kb, int one_pass_bound_kb, int multi_pass_bound_kb,
    int queue_timeout_ms, int prediction_mb, GsAmmAdmissionResult *result)
{
    volatile bool admitted = false;

    if (!gs_amm_enabled)
        return false;

    if (!GsAmmOperationBegin())
        return false;

    PG_TRY();
    {
        admitted = gs_amm_admit_bounds_internal(cache_bound_kb, one_pass_bound_kb,
            multi_pass_bound_kb, queue_timeout_ms, prediction_mb, result);
    }
    PG_CATCH();
    {
        GsAmmOperationEnd();
        PG_RE_THROW();
    }
    PG_END_TRY();

    GsAmmOperationEnd();
    return admitted;
}

bool GsAmmAdmitFeedbackOnly(GsAmmAdmissionResult *result)
{
    GsAmmSharedState *state;
    GsAmmRuntimeConfig runtime_config;
    int grant_kb;
    volatile bool admitted = false;

    if (!gs_amm_enabled || result == NULL)
        return false;

    state = gs_amm_get_state();
    gs_amm_runtime_config_snapshot(state, &runtime_config);
    if (!runtime_config.feedback_only)
        return false;

    SpinLockAcquire(&state->mutex);
    grant_kb = Max(state->feedback_current_grant_mb, 1) * 1024;
    SpinLockRelease(&state->mutex);

    if (!GsAmmOperationBegin())
        return false;
    PG_TRY();
    {
        admitted = gs_amm_admit_bounds_internal(grant_kb, grant_kb, grant_kb,
            gs_amm_ap_queue_timeout_ms, 0, result);
    }
    PG_CATCH();
    {
        GsAmmOperationEnd();
        PG_RE_THROW();
    }
    PG_END_TRY();
    GsAmmOperationEnd();
    return admitted;
}

Datum gs_amm_begin_ap_bounds(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    int cache_bound_kb = PG_GETARG_INT32(0);
    int one_pass_bound_kb = PG_GETARG_INT32(1);
    int multi_pass_bound_kb = PG_GETARG_INT32(2);
    int queue_timeout_ms = PG_GETARG_INT32(3);
    GsAmmAdmissionResult result;
    char status[1536];

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to request a GS AMM AP grant")));

    if (!GsAmmAdmitBounds(cache_bound_kb, one_pass_bound_kb, multi_pass_bound_kb, queue_timeout_ms,
        Max((cache_bound_kb + 1023) / 1024, 1), &result))
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("GS AMM admission result is unavailable")));

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "admitted=%s queued=%s backpressure=%s memory_mode=%s granted_kb=%d granted_mb=%d "
        "prediction_mb=%d cache_bound_kb=%d one_pass_bound_kb=%d multi_pass_bound_kb=%d "
        "admission_request_kb=%d allocator_only_mode=%s allocator_only_grant_mb=%d "
        "queue_wait_ms=%d queue_ticket=%llu grant_id=%llu grant_generation=%llu grant_granules=%d "
        "reason=%s backend_work_mem_kb=%d",
        result.admitted ? "true" : "false", result.queued ? "true" : "false",
        result.backpressure ? "true" : "false", gs_amm_memory_mode_name(result.memory_mode),
        result.granted_kb, result.granted_mb, result.prediction_mb, result.cache_bound_kb,
        result.one_pass_bound_kb, result.multi_pass_bound_kb, result.admission_request_kb,
        result.allocator_only_mode ? "true" : "false", result.allocator_only_grant_mb,
        result.queue_wait_ms, (unsigned long long)result.queue_ticket, (unsigned long long)result.grant_id,
        (unsigned long long)result.grant_generation, result.grant_granules, result.reason,
        u_sess->attr.attr_memory.work_mem);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_end_ap(PG_FUNCTION_ARGS)
{
    gs_amm_require_admin_legacy_control();
    GsAmmSharedState *state = gs_amm_get_state();
    int released_mb = MyGsAmmGrantMb;
    GsAmmGrantToken expected_token = {
        GsAmmCurrentBackendGrantId(), GsAmmCurrentBackendGrantGeneration()};
    bool released;
    int dynamic_used_mb;
    int active_ap_count;
    char status[512];

    released = GsAmmReleaseGrantToken(expected_token, true);

    SpinLockAcquire(&state->mutex);
    dynamic_used_mb = state->dynamic_used_mb;
    active_ap_count = state->active_ap_count;
    SpinLockRelease(&state->mutex);

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "released=%s released_mb=%d grant_generation=%llu dynamic_used_mb=%d active_ap_count=%d "
        "backend_work_mem_kb=%d", released ? "true" : "false", released_mb,
        (unsigned long long)expected_token.grant_generation, dynamic_used_mb, active_ap_count,
        u_sess->attr.attr_memory.work_mem);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}
