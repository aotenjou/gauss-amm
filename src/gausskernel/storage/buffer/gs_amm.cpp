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
#include <stdlib.h>

#include "fmgr.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "port.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
#include "storage/gs_amm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "postmaster/pagewriter.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "utils/tuplesort.h"
#include "ddes/dms/ss_dms_bufmgr.h"

extern bool superuser(void);

int gs_amm_shared_buffers_min_mb = 64;
int gs_amm_tp_reserve_mb = 0;
int gs_amm_dynamic_target_mb = 512;
/* Keep ownership changes small enough for low-memory hosts.  The value is a
 * postmaster GUC because it determines the shared-memory granule table. */
int gs_amm_granule_size_mb = 8;
bool gs_amm_enabled = false;
THR_LOCAL int gs_amm_workload_role = GS_AMM_WORKLOAD_TP;
THR_LOCAL int gs_amm_test_ap_cache_label_kb = 0;
THR_LOCAL int gs_amm_test_ap_one_pass_label_kb = 0;
THR_LOCAL int gs_amm_test_ap_multi_pass_label_kb = 0;
THR_LOCAL int gs_amm_test_ap_label_kb = 0;
THR_LOCAL bool gs_amm_test_ap_use_labels = false;
int gs_amm_tp_buffer_miss_threshold_pct = 5;
int gs_amm_ap_borrow_buffer_hit_guard_pct = 3;
int gs_amm_tp_tps_decline_guard_pct = 3;
int gs_amm_tp_cpu_pressure_threshold_pct = 60;
/* A worker-rate surge is retained exclusively to make acceptance tests
 * deterministic on a shared host.  Production recovery uses host CPU. */
THR_LOCAL bool gs_amm_tp_test_mode = false;
THR_LOCAL bool gs_amm_tp_test_worker_surge = false;
#define GS_AMM_TP_TPS_SURGE_GUARD_PCT 50

#define gs_amm_ap_min_grant_mb 4

typedef enum GsAmmAction {
    GS_AMM_OBSERVE = 0,
    GS_AMM_AP_EXPAND,
    GS_AMM_BORROW_FROM_BUFFER,
    GS_AMM_BACKPRESSURE,
    GS_AMM_FAIL_CLOSED,
    GS_AMM_TP_RECOVERY_FREE,
    GS_AMM_TP_RECOVERY_IDLE_AP,
    GS_AMM_TP_DOWNGRADE_PENDING,
    GS_AMM_TP_RECOVERY_DONE,
    GS_AMM_TP_RECOVERY_DEFERRED,
    GS_AMM_TP_BUFFER_BASELINE_DRAIN,
    GS_AMM_ACTION_COUNT
} GsAmmAction;

#define GS_AMM_MIN_GRANULE_MB 1
#define GS_AMM_RECLAIM_RETRY_INTERVAL_MS 1000
#define GS_AMM_MAX_AP_REGISTRY 128
#define GS_AMM_AP_QUEUE_CAPACITY GS_AMM_MAX_AP_REGISTRY
#define GS_AMM_TP_WINDOW_MS 1000
#define GS_AMM_TP_LOW_WINDOWS_BEFORE_DRAIN 3
#define GS_AMM_TP_HOT_CLEAR_WINDOWS 3
#define GS_AMM_TP_TEST_SURGE_LEASE_WINDOWS 3
#define GS_AMM_TP_TEST_MODE_LEASE_WINDOWS 10

typedef enum GsAmmStage {
    GS_AMM_STAGE_CACHE = 0,
    GS_AMM_STAGE_CACHE_ONEPASS,
    GS_AMM_STAGE_FORCE_ONEPASS
} GsAmmStage;

typedef enum GsAmmTpPressureState {
    GS_AMM_TP_PRESSURE_UNKNOWN = 0,
    GS_AMM_TP_PRESSURE_LOW_FLOW,
    GS_AMM_TP_PRESSURE_HOT_RECOVERY,
    GS_AMM_TP_PRESSURE_RECOVERY_WAIT
} GsAmmTpPressureState;

typedef enum GsAmmTpRecoveryPhase {
    GS_AMM_TP_RECOVERY_IDLE = 0,
    GS_AMM_TP_RECOVERY_STOP_AP,
    GS_AMM_TP_RECOVERY_WAIT_AP,
    GS_AMM_TP_RECOVERY_RESTORE_SB,
    GS_AMM_TP_RECOVERY_MULTIPASS
} GsAmmTpRecoveryPhase;

typedef struct GsAmmApRecord {
    bool active;
    uint64 grant_id;
    uint64 grant_generation;
    uint64 lifecycle_generation;
    int cache_bound_kb;
    int one_pass_bound_kb;
    int multi_pass_bound_kb;
    int admission_target_kb;
    int granted_kb;
    int effective_grant_kb;
    uint64 dynamic_granted_bytes;
    uint64 dynamic_used_bytes;
    uint64 used_bytes;
    GsAmmMemoryMode mode;
    GsAmmMemoryMode target_mode;
    uint64 revoke_epoch;
    bool reclaim_pending;
    int reclaim_granule_id;
    ThreadId backend_pid;
    BackendId backend_id;
    bool stop_requested;
} GsAmmApRecord;

typedef enum GsAmmApQueueState {
    GS_AMM_AP_QUEUE_FREE = 0,
    GS_AMM_AP_QUEUE_WAITING,
    GS_AMM_AP_QUEUE_GRANTED
} GsAmmApQueueState;

/* A slot is owned by its waiting backend until it claims or cancels its grant. */
typedef struct GsAmmApQueueSlot {
    GsAmmApQueueState state;
    uint64 ticket;
    TimestampTz queued_at;
    int cache_bound_kb;
    int one_pass_bound_kb;
    int multi_pass_bound_kb;
    int admission_target_kb;
    int prediction_mb;
    int pgprocno;
    ThreadId waiter_pid;
    uint64 waiter_sessionid;
    GsAmmGrantToken grant_token;
    int granted_kb;
    int grant_granules;
    GsAmmMemoryMode memory_mode;
} GsAmmApQueueSlot;

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
} GsAmmRuntimeConfig;

typedef struct GsAmmSharedState {
    slock_t mutex;
    GsAmmRuntimeConfig runtime_config;
    bool maintenance_resetting;
    uint64 operation_inflight;
    bool reclaim_syscall_inflight;
    uint64 reclaim_syscall_attempt;
    int dynamic_target_mb;
    int dynamic_used_mb;
    uint64 dynamic_reserved_bytes;
    uint64 dynamic_allocated_bytes;
    int active_ap_count;
    GsAmmStage stage;
    int ap_registry_count;
    uint64 ap_granted_bytes_total;
    uint64 ap_used_bytes_total;
    uint64 ap_reclaimable_bytes_total;
    int active_target_demand_mb;
    int queued_ap_count;
    int queued_target_demand_mb;
    uint64 next_queue_ticket;
    uint64 ap_queue_admit_count;
    uint64 ap_queue_cancel_count;
    GsAmmApQueueSlot ap_queue[GS_AMM_AP_QUEUE_CAPACITY];
    int pending_demand_mb;
    int last_current_target_mb;
    int last_aggregate_target_mb;
    int last_dynamic_deficit_mb;
    uint64 ap_borrow_count;
    char last_supply_source[24];
    uint64 tp_window_shared_blks_hit;
    uint64 tp_window_shared_blks_read;
    uint64 tp_window_completed_queries;
    uint64 tp_cpu_last_total_jiffies;
    uint64 tp_cpu_last_idle_jiffies;
    int tp_cpu_util_pct;
    bool tp_cpu_util_valid;
    bool tp_cpu_guarded;
    TimestampTz tp_test_mode_until;
    TimestampTz tp_test_worker_surge_until;
    int tp_pressure_pct;
    bool tp_pressure_hot;
    GsAmmTpPressureState tp_pressure_state;
    bool tp_pressure_valid;
    uint64 tp_buffer_hit_baseline_hits;
    uint64 tp_buffer_hit_baseline_accesses;
    int tp_buffer_hit_pct;
    bool tp_buffer_hit_baseline_valid;
    uint64 tp_baseline_tps;
    uint64 tp_recent_tps;
    bool tp_tps_baseline_valid;
    bool tp_tps_guarded;
    bool ap_borrow_buffer_hit_guarded;
    int baseline_active_mb;
    int tp_low_pressure_windows;
    int tp_hot_clear_windows;
    GsAmmTpRecoveryPhase tp_recovery_phase;
    bool ap_multipass_only;
    uint64 tp_ap_stop_requested;
    uint64 tp_ap_stop_completed;
    uint64 tp_sb_restore_granules;
    TimestampTz tp_last_window_at;
    TimestampTz last_ap_borrow_at;
    uint64 tp_recovery_requested_granules;
    uint64 tp_recovered_granules;
    uint64 tp_recovery_deferred_count;
    uint64 tp_restore_attempts;
    TimestampTz tp_restore_retry_after;
    int ap_downgrade_pending;
    uint64 ap_reclaim_poll_count;
    uint64 ap_reclaim_operator_release_bytes;
    GsAmmApRecord ap_registry[GS_AMM_MAX_AP_REGISTRY];
    int backpressure_count;
    int last_prediction_mb;
    int last_grant_mb;
    int last_admission_target_mb;
    char last_backpressure_reason[32];
    GsAmmAction last_action;
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
    bool native_current_label_override;
    char native_last_reason[GS_AMM_ADMISSION_REASON_LENGTH];
    uint64 next_drain_attempt;
    uint64 next_reclaim_attempt;
    uint64 next_grant_id;
    uint64 next_grant_generation;
    GsAmmGranuleMeta granules[1];
} GsAmmSharedState;

static const char *gs_amm_stage_name(GsAmmStage stage)
{
    switch (stage) {
        case GS_AMM_STAGE_CACHE:
            return "cache";
        case GS_AMM_STAGE_CACHE_ONEPASS:
            return "cache_onepass";
        case GS_AMM_STAGE_FORCE_ONEPASS:
            return "force_onepass";
        default:
            return "unknown";
    }
}

static GsAmmApRecord *gs_amm_find_ap_record_locked(
    GsAmmSharedState *state, GsAmmGrantToken token)
{
    for (int index = 0; index < GS_AMM_MAX_AP_REGISTRY; index++) {
        GsAmmApRecord *record = &state->ap_registry[index];

        if (record->active && record->grant_id == token.grant_id &&
            record->grant_generation == token.grant_generation)
            return record;
    }
    return NULL;
}

static void gs_amm_refresh_ap_totals_locked(GsAmmSharedState *state)
{
    uint64 granted = 0;
    uint64 used = 0;
    uint64 dynamic_reserved = 0;
    uint64 dynamic_used = 0;

    state->ap_reclaimable_bytes_total = 0;

    for (int index = 0; index < GS_AMM_MAX_AP_REGISTRY; index++) {
        GsAmmApRecord *record = &state->ap_registry[index];
        uint64 shared_granted = 0;
        uint64 shared_used = 0;

        if (!record->active)
            continue;
        dynamic_reserved += record->dynamic_granted_bytes;
        dynamic_used += record->dynamic_used_bytes;
        for (int granule_index = 0; granule_index < state->total_granules; granule_index++) {
            GsAmmGranuleMeta *granule = &state->granules[granule_index];
            uint64 capacity;

            if (granule->grant_id != record->grant_id ||
                granule->grant_generation != record->grant_generation ||
                (granule->state != GS_AMM_GRANULE_AP_ACTIVE &&
                    granule->state != GS_AMM_GRANULE_AP_RESERVED &&
                    granule->state != GS_AMM_GRANULE_RECLAIMING))
                continue;
            capacity = granule->active_grant_bytes != 0 ? granule->active_grant_bytes :
                (uint64)granule->buffer_count * BLCKSZ;
            shared_granted += capacity;
            shared_used += granule->used_bytes;
        }
        uint64 record_granted = record->dynamic_granted_bytes + shared_granted;
        uint64 record_used = record->dynamic_used_bytes + shared_used;
        record->granted_kb = (int)Min(record_granted / 1024, (uint64)INT_MAX);
        record->used_bytes = record_used;
        granted += record_granted;
        used += record_used;
        /* Dynamic quota is backend-local malloc and is not reclaimable from SB. */
        if (shared_granted > shared_used)
            state->ap_reclaimable_bytes_total += shared_granted - shared_used;
    }
    state->ap_granted_bytes_total = granted;
    state->ap_used_bytes_total = used;
    state->dynamic_reserved_bytes = dynamic_reserved;
    state->dynamic_allocated_bytes = dynamic_used;
    state->dynamic_used_mb = (int)Min((dynamic_reserved + 1024 * 1024 - 1) / (1024 * 1024),
        (uint64)INT_MAX);
}

static int gs_amm_ap_target_bound_kb(const GsAmmApRecord *record)
{
    if (record == NULL)
        return 0;
    switch (record->target_mode) {
        case GS_AMM_MEMORY_MODE_CACHE:
            return record->cache_bound_kb;
        case GS_AMM_MEMORY_MODE_ONEPASS:
            return record->one_pass_bound_kb;
        case GS_AMM_MEMORY_MODE_MULTIPASS:
        case GS_AMM_MEMORY_MODE_BACKPRESSURE:
        case GS_AMM_MEMORY_MODE_NONE:
        default:
            return record->multi_pass_bound_kb;
    }
}

static void gs_amm_refresh_ap_target_totals_locked(GsAmmSharedState *state)
{
    int active_target_mb = 0;

    if (state == NULL)
        return;
    for (int index = 0; index < GS_AMM_MAX_AP_REGISTRY; index++) {
        GsAmmApRecord *record = &state->ap_registry[index];

        if (record->active)
            active_target_mb += (gs_amm_ap_target_bound_kb(record) + 1023) / 1024;
    }
    state->active_target_demand_mb = active_target_mb;
    state->last_aggregate_target_mb = active_target_mb + state->last_current_target_mb;
}

static bool gs_amm_register_ap_locked(GsAmmSharedState *state, GsAmmGrantToken token,
    int cache_bound_kb, int one_pass_bound_kb, int multi_pass_bound_kb, int admission_target_kb,
    int granted_kb,
    uint64 dynamic_granted_bytes, GsAmmMemoryMode mode, GsAmmMemoryMode target_mode,
    ThreadId backend_pid, BackendId backend_id)
{
    GsAmmApRecord *record = gs_amm_find_ap_record_locked(state, token);

    if (record == NULL) {
        for (int index = 0; index < GS_AMM_MAX_AP_REGISTRY; index++) {
            if (!state->ap_registry[index].active) {
                record = &state->ap_registry[index];
                state->ap_registry_count++;
                break;
            }
        }
    }
    if (record == NULL)
        return false;

    record->active = true;
    record->grant_id = token.grant_id;
    record->grant_generation = token.grant_generation;
    record->lifecycle_generation = 0;
    record->cache_bound_kb = cache_bound_kb;
    record->one_pass_bound_kb = one_pass_bound_kb;
    record->multi_pass_bound_kb = multi_pass_bound_kb;
    record->admission_target_kb = admission_target_kb;
    record->granted_kb = granted_kb;
    record->effective_grant_kb = granted_kb;
    record->dynamic_granted_bytes = dynamic_granted_bytes;
    record->dynamic_used_bytes = 0;
    record->used_bytes = 0;
    record->mode = mode;
    record->target_mode = target_mode;
    record->revoke_epoch = 0;
    record->reclaim_pending = false;
    record->reclaim_granule_id = -1;
    record->backend_pid = backend_pid;
    record->backend_id = backend_id;
    record->stop_requested = false;
    gs_amm_refresh_ap_totals_locked(state);
    gs_amm_refresh_ap_target_totals_locked(state);
    return true;
}

static void gs_amm_unregister_ap_locked(GsAmmSharedState *state, GsAmmGrantToken token)
{
    GsAmmApRecord *record = gs_amm_find_ap_record_locked(state, token);

    if (record == NULL)
        return;
    if (record->stop_requested)
        state->tp_ap_stop_completed++;
    if (record->reclaim_pending && state->ap_downgrade_pending > 0)
        state->ap_downgrade_pending--;
    (void)memset(record, 0, sizeof(*record));
    if (state->ap_registry_count > 0)
        state->ap_registry_count--;
    gs_amm_refresh_ap_totals_locked(state);
    gs_amm_refresh_ap_target_totals_locked(state);
}

static void gs_amm_update_ap_used_locked(GsAmmSharedState *state, GsAmmGrantToken token)
{
    GsAmmApRecord *record = gs_amm_find_ap_record_locked(state, token);
    uint64 used = 0;

    if (record == NULL)
        return;
    for (int index = 0; index < state->total_granules; index++) {
        GsAmmGranuleMeta *granule = &state->granules[index];

        if (granule->grant_id == token.grant_id && granule->grant_generation == token.grant_generation &&
            (granule->state == GS_AMM_GRANULE_AP_ACTIVE || granule->state == GS_AMM_GRANULE_AP_RESERVED))
            used += granule->used_bytes;
    }
    record->used_bytes = used;
    gs_amm_refresh_ap_totals_locked(state);
}

static const char *const GS_AMM_ACTION_NAMES[GS_AMM_ACTION_COUNT] = {
    "OBSERVE",
    "AP_EXPAND",
    "BORROW_FROM_BUFFER",
    "BACKPRESSURE",
    "FAIL_CLOSED",
    "TP_RECOVERY_FREE",
    "TP_RECOVERY_IDLE_AP",
    "TP_DOWNGRADE_PENDING",
    "TP_RECOVERY_DONE",
    "TP_RECOVERY_DEFERRED",
    "TP_BUFFER_BASELINE_DRAIN"
};

static const char *gs_amm_memory_mode_name(GsAmmMemoryMode mode)
{
    switch (mode) {
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
static THR_LOCAL bool MyGsAmmCleanupRegistered = false;
static THR_LOCAL bool MyGsAmmTransactionHasNativeAP = false;
static THR_LOCAL uint64 MyGsAmmQueueTicket = 0;

typedef struct GsAmmGrantFreeExtent {
    struct GsAmmGrantFreeExtent *next;
    uint64 size;
    int granule_id;
} GsAmmGrantFreeExtent;

typedef struct GsAmmBackendGrantArena {
    GsAmmGrantToken token;
    GsAmmGrantFreeExtent *free_extents;
} GsAmmBackendGrantArena;

typedef struct GsAmmDynamicAllocation {
    struct GsAmmDynamicAllocation *next;
    void *pointer;
    Size size;
} GsAmmDynamicAllocation;

static THR_LOCAL GsAmmBackendGrantArena MyGsAmmGrantArena = {{0, 0}, NULL};
static THR_LOCAL GsAmmDynamicAllocation *MyGsAmmDynamicAllocations = NULL;

static void gs_amm_backend_cleanup(int code, Datum arg);
static void gs_amm_register_backend_cleanup(void);
static void gs_amm_release_backend_grant(void);
static void gs_amm_set_backpressure_reason_locked(GsAmmSharedState *state, const char *reason);
static int64 gs_amm_blocks_to_mb(int blocks);
static int gs_amm_effective_dynamic_target_mb(void);
static bool gs_amm_prepare_dynamic_capacity(int ap_demand_mb);
static void gs_amm_supply_ap_demand(void);
static void gs_amm_process_ap_queue(void);
static bool gs_amm_try_admit_ap_locked(GsAmmSharedState *state, int cache_bound_kb,
    int one_pass_bound_kb, int multi_pass_bound_kb, int admission_target_kb, int prediction_mb,
    GsAmmGrantToken *grant_token, int *granted_kb, int *grant_granules);
static void gs_amm_request_ap_stop_locked(GsAmmSharedState *state);
static void gs_amm_restore_shared_buffer_after_ap_stop(GsAmmSharedState *state);
static int gs_amm_current_active_buffer_blocks(GsAmmSharedState *state);
static void gs_amm_resize_core(int target_blocks, GsAmmResizeOutcome *out);
static void gs_amm_rebalance_ap_limits_locked(GsAmmSharedState *state);
static int gs_amm_record_target_kb_locked(const GsAmmSharedState *state, const GsAmmApRecord *record);
static GsAmmApRecord *gs_amm_select_ap_growth_candidate_locked(GsAmmSharedState *state);
static bool gs_amm_active_ap_growth_pending_locked(GsAmmSharedState *state);
static int gs_amm_grow_ap_dynamic_locked(GsAmmSharedState *state, GsAmmApRecord *record, int requested_mb);
static int gs_amm_grow_ap_granule_locked(GsAmmSharedState *state, GsAmmApRecord *record, int requested_mb);
static bool gs_amm_granule_owned_by_grant(const GsAmmGranuleMeta *granule, GsAmmGrantToken token);
static uint64 gs_amm_granule_active_capacity_bytes(const GsAmmGranuleMeta *granule);
static int gs_amm_reclaim_unused_ap_granules_locked(
    GsAmmSharedState *state, int requested_mb, int required_granule_id, uint64 required_grant_id,
    uint64 required_grant_generation);
static bool gs_amm_pointer_in_granule(
    const GsAmmGranuleMeta *granule, const char *buffer_blocks, const char *pointer, uint64 size);
static GsAmmApRecord *gs_amm_find_ap_record_locked(GsAmmSharedState *state, GsAmmGrantToken token);
static void gs_amm_release_dynamic_allocations(GsAmmGrantToken token);
static void gs_amm_cancel_waiting_request(void);
static void gs_amm_release_unclaimed_queue_grant(GsAmmGrantToken token);

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

static double gs_amm_dtree_min_bound_kb(void)
{
    return 64.0;
}

static double gs_amm_dtree_bound_kb(double value_mb)
{
    double value_kb;

    if (!(value_mb > 0.0))
        return gs_amm_dtree_min_bound_kb();
    value_kb = value_mb * 1024.0;
    return Max(value_kb, gs_amm_dtree_min_bound_kb());
}

static void gs_amm_init_runtime_config(GsAmmSharedState *state)
{
    state->runtime_config.config_version = 1;
    state->runtime_config.amm_enabled = gs_amm_enabled;
}

void GsAmmShmemInit(void)
{
    bool found = false;

    GsAmmState = (GsAmmSharedState *)ShmemInitStruct("GS AMM Shared State", GsAmmShmemSize(), &found);
    if (!found) {
        errno_t rc = memset_s(GsAmmState, GsAmmShmemSize(), 0, GsAmmShmemSize());
        securec_check(rc, "\0", "\0");
        SpinLockInit(&GsAmmState->mutex);
        GsAmmState->dynamic_target_mb = gs_amm_effective_dynamic_target_mb();
        GsAmmState->stage = GS_AMM_STAGE_CACHE;
        GsAmmState->last_action = GS_AMM_OBSERVE;
        gs_amm_init_runtime_config(GsAmmState);
        gs_amm_init_granule_table(GsAmmState);
        GsAmmState->baseline_active_mb = (int)gs_amm_blocks_to_mb(StrategyActiveBufferCount());
        GsAmmState->tp_pressure_state = GS_AMM_TP_PRESSURE_UNKNOWN;
        GsAmmState->tp_pressure_valid = false;
        GsAmmState->tp_recovery_phase = GS_AMM_TP_RECOVERY_IDLE;
        GsAmmState->ap_multipass_only = false;
        (void)snprintf_s(GsAmmState->last_supply_source, sizeof(GsAmmState->last_supply_source),
            sizeof(GsAmmState->last_supply_source) - 1, "%s", "none");
    }
}

static GsAmmSharedState *gs_amm_get_state(void)
{
    if (GsAmmState == NULL)
        GsAmmShmemInit();
    return GsAmmState;
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
    double raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];

    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++)
        raw_bounds_kb[i] = gs_amm_dtree_bound_kb(raw_bounds_mb[i]);

    detail->model_version = model_version;
    detail->leaf_id = leaf_id;
    detail->test_label_override = false;
    for (int i = 0; i < GS_AMM_DTREE_BOUND_COUNT; i++) {
        detail->bounds_kb[i] = raw_bounds_kb[i];
    }
    detail->admission_target_kb = (int)Min(Max(raw_bounds_kb[0], 1.0), (double)INT_MAX);
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
    state->native_current_label_override = false;
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
    state->native_current_label_override = detail->test_label_override;
    gs_amm_copy_admission_reason(state->native_last_reason, result->reason);
    SpinLockRelease(&state->mutex);
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

static int gs_amm_effective_dynamic_target_mb(void)
{
    return gs_amm_dynamic_target_default_mb();
}

static int gs_amm_protected_buffer_mb(void)
{
    int64 protected_mb = (int64)Max(gs_amm_shared_buffers_min_mb, 1) +
        Max(gs_amm_tp_reserve_mb, 0);

    return (int)Min(protected_mb, (int64)INT_MAX / 2);
}

static const char *gs_amm_tp_pressure_state_name(GsAmmTpPressureState state)
{
    switch (state) {
        case GS_AMM_TP_PRESSURE_LOW_FLOW:
            return "low_flow";
        case GS_AMM_TP_PRESSURE_HOT_RECOVERY:
            return "hot_recovery";
        case GS_AMM_TP_PRESSURE_RECOVERY_WAIT:
            return "recovery_wait";
        case GS_AMM_TP_PRESSURE_UNKNOWN:
        default:
            return "unknown";
    }
}

static const char *gs_amm_tp_recovery_phase_name(GsAmmTpRecoveryPhase phase)
{
    switch (phase) {
        case GS_AMM_TP_RECOVERY_IDLE:
            return "idle";
        case GS_AMM_TP_RECOVERY_STOP_AP:
            return "stop_ap";
        case GS_AMM_TP_RECOVERY_WAIT_AP:
            return "wait_ap_release";
        case GS_AMM_TP_RECOVERY_RESTORE_SB:
            return "restore_sb";
        case GS_AMM_TP_RECOVERY_MULTIPASS:
            return "multipass_admission";
        default:
            return "unknown";
    }
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

static int gs_amm_queue_demand_mb(const GsAmmApQueueSlot *slot, const GsAmmSharedState *state)
{
    uint64 demand_kb;

    if (slot == NULL)
        return 0;
    /* Normal admission reserves the cache target.  Stage transitions and
     * reclaim requests reduce already-admitted APs to one-pass separately;
     * the TP recovery multipass-only phase remains an explicit emergency
     * exception for APs re-entering after shared-buffer restoration. */
    if (state != NULL && state->ap_multipass_only)
        demand_kb = (uint64)Max(slot->multi_pass_bound_kb, 1);
    else
        demand_kb = (uint64)Max(slot->admission_target_kb, 1);
    return (int)Min(Max((demand_kb + 1023) / 1024, (uint64)gs_amm_ap_min_grant_mb),
        (uint64)INT_MAX);
}

static void gs_amm_refresh_ap_queue_totals_locked(GsAmmSharedState *state)
{
    int queue_count = 0;
    int queue_demand_mb = 0;

    if (state == NULL)
        return;
    for (int index = 0; index < GS_AMM_AP_QUEUE_CAPACITY; index++) {
        GsAmmApQueueSlot *slot = &state->ap_queue[index];
        int demand_mb;

        if (slot->state != GS_AMM_AP_QUEUE_WAITING)
            continue;
        demand_mb = gs_amm_queue_demand_mb(slot, state);
        if (queue_count < INT_MAX)
            queue_count++;
        queue_demand_mb = queue_demand_mb <= INT_MAX - demand_mb ?
            queue_demand_mb + demand_mb : INT_MAX;
    }
    state->queued_ap_count = queue_count;
    state->queued_target_demand_mb = queue_demand_mb;
}

static GsAmmApQueueSlot *gs_amm_find_queue_slot_locked(GsAmmSharedState *state, uint64 ticket)
{
    if (state == NULL || ticket == 0)
        return NULL;
    for (int index = 0; index < GS_AMM_AP_QUEUE_CAPACITY; index++) {
        GsAmmApQueueSlot *slot = &state->ap_queue[index];

        if (slot->state != GS_AMM_AP_QUEUE_FREE && slot->ticket == ticket)
            return slot;
    }
    return NULL;
}

static GsAmmApQueueSlot *gs_amm_queue_head_locked(GsAmmSharedState *state)
{
    GsAmmApQueueSlot *head = NULL;

    if (state == NULL)
        return NULL;
    for (int index = 0; index < GS_AMM_AP_QUEUE_CAPACITY; index++) {
        GsAmmApQueueSlot *slot = &state->ap_queue[index];

        if (slot->state != GS_AMM_AP_QUEUE_WAITING)
            continue;
        if (head == NULL || slot->ticket < head->ticket)
            head = slot;
    }
    return head;
}

static bool gs_amm_queue_has_entries_locked(const GsAmmSharedState *state)
{
    if (state == NULL)
        return false;
    for (int index = 0; index < GS_AMM_AP_QUEUE_CAPACITY; index++) {
        if (state->ap_queue[index].state != GS_AMM_AP_QUEUE_FREE)
            return true;
    }
    return false;
}

static bool gs_amm_queue_slot_belongs_to_current_backend(const GsAmmApQueueSlot *slot)
{
    if (slot == NULL || t_thrd.proc == NULL)
        return false;
    return slot->pgprocno == t_thrd.proc->pgprocno &&
        slot->waiter_pid == t_thrd.proc->pid &&
        slot->waiter_sessionid == t_thrd.proc->sessionid;
}

static bool gs_amm_enqueue_ap_request_locked(GsAmmSharedState *state, int cache_bound_kb,
    int one_pass_bound_kb, int multi_pass_bound_kb, int admission_target_kb,
    int prediction_mb, uint64 *ticket)
{
    GsAmmApQueueSlot *slot = NULL;
    errno_t rc;

    if (ticket != NULL)
        *ticket = 0;
    if (state == NULL || t_thrd.proc == NULL || state->next_queue_ticket == PG_UINT64_MAX)
        return false;
    for (int index = 0; index < GS_AMM_AP_QUEUE_CAPACITY; index++) {
        if (state->ap_queue[index].state == GS_AMM_AP_QUEUE_FREE) {
            slot = &state->ap_queue[index];
            break;
        }
    }
    if (slot == NULL)
        return false;

    rc = memset_s(slot, sizeof(*slot), 0, sizeof(*slot));
    securec_check(rc, "\0", "\0");
    state->next_queue_ticket++;
    slot->state = GS_AMM_AP_QUEUE_WAITING;
    slot->ticket = state->next_queue_ticket;
    slot->queued_at = GetCurrentTimestamp();
    slot->cache_bound_kb = cache_bound_kb;
    slot->one_pass_bound_kb = one_pass_bound_kb;
    slot->multi_pass_bound_kb = multi_pass_bound_kb;
    slot->admission_target_kb = admission_target_kb;
    slot->prediction_mb = prediction_mb;
    slot->pgprocno = t_thrd.proc->pgprocno;
    slot->waiter_pid = t_thrd.proc->pid;
    slot->waiter_sessionid = t_thrd.proc->sessionid;
    gs_amm_refresh_ap_queue_totals_locked(state);
    gs_amm_refresh_ap_target_totals_locked(state);
    if (ticket != NULL)
        *ticket = slot->ticket;
    return true;
}

static void gs_amm_wake_queue_waiter(const GsAmmApQueueSlot *slot)
{
    PGPROC *proc;

    if (slot == NULL || slot->pgprocno < 0 || slot->pgprocno >= GLOBAL_ALL_PROCS)
        return;
    proc = g_instance.proc_base_all_procs[slot->pgprocno];
    if (proc != NULL && proc->pid == slot->waiter_pid && proc->sessionid == slot->waiter_sessionid)
        SetLatch(&proc->procLatch);
}

static int gs_amm_record_target_kb_locked(const GsAmmSharedState *state, const GsAmmApRecord *record)
{
    int target_kb;

    if (state == NULL || record == NULL)
        return 0;
    target_kb = gs_amm_ap_target_bound_kb(record);
    if (state->ap_multipass_only)
        target_kb = record->multi_pass_bound_kb;
    /* CACHE_ONEPASS is a recovery policy for APs that have explicitly been
     * downgraded.  It must not silently replace a cache admission target for
     * a newly admitted query: doing so turns a large in-memory sort into an
     * external merge even when the controller has capacity. */
    if (state->stage == GS_AMM_STAGE_FORCE_ONEPASS ||
        (state->stage == GS_AMM_STAGE_CACHE_ONEPASS &&
            record->target_mode == GS_AMM_MEMORY_MODE_ONEPASS &&
            target_kb > record->one_pass_bound_kb))
        target_kb = record->one_pass_bound_kb;
    return Max(target_kb, record->multi_pass_bound_kb);
}

static void gs_amm_rebalance_ap_limits_locked(GsAmmSharedState *state)
{
    gs_amm_refresh_ap_totals_locked(state);
    for (int index = 0; index < GS_AMM_MAX_AP_REGISTRY; index++) {
        GsAmmApRecord *record = &state->ap_registry[index];
        int target_kb;
        int used_kb;

        if (!record->active)
            continue;
        used_kb = (int)Min((uint64)INT_MAX, (record->used_bytes + 1023) / 1024);
        if (record->reclaim_pending) {
            /* A TP revoke must be visible below current use so the operator spills. */
            target_kb = record->one_pass_bound_kb;
            record->target_mode = GS_AMM_MEMORY_MODE_ONEPASS;
        } else {
            target_kb = gs_amm_record_target_kb_locked(state, record);
            target_kb = Max(target_kb, used_kb);
        }
        record->effective_grant_kb = Max(Min(target_kb, Max(record->granted_kb, used_kb)), 1);
        if (record->reclaim_pending)
            record->mode = GS_AMM_MEMORY_MODE_ONEPASS;
        else if (record->effective_grant_kb <= record->one_pass_bound_kb)
            record->mode = GS_AMM_MEMORY_MODE_ONEPASS;
    }
    gs_amm_refresh_ap_totals_locked(state);
    gs_amm_refresh_ap_target_totals_locked(state);
}

static int gs_amm_free_granule_mb_locked(GsAmmSharedState *state);

static int gs_amm_dynamic_free_mb_locked(const GsAmmSharedState *state)
{
    uint64 target_bytes;

    if (state == NULL)
        return 0;
    target_bytes = (uint64)Max(state->dynamic_target_mb, gs_amm_ap_min_grant_mb) * 1024 * 1024;
    if (state->dynamic_reserved_bytes >= target_bytes)
        return 0;
    return (int)Min((target_bytes - state->dynamic_reserved_bytes) / (1024 * 1024),
        (uint64)INT_MAX);
}

static GsAmmApRecord *gs_amm_select_ap_growth_candidate_locked(GsAmmSharedState *state)
{
    GsAmmApRecord *best = NULL;
    int best_deficit_kb = 0;

    if (state == NULL)
        return NULL;
    gs_amm_refresh_ap_totals_locked(state);
    for (int index = 0; index < GS_AMM_MAX_AP_REGISTRY; index++) {
        GsAmmApRecord *record = &state->ap_registry[index];
        int deficit_kb;

        if (!record->active || record->reclaim_pending)
            continue;
        deficit_kb = gs_amm_record_target_kb_locked(state, record) - record->granted_kb;
        if (deficit_kb <= 0)
            continue;
        if (best == NULL || deficit_kb > best_deficit_kb ||
            (deficit_kb == best_deficit_kb && record->grant_id < best->grant_id)) {
            best = record;
            best_deficit_kb = deficit_kb;
        }
    }
    return best;
}

static bool gs_amm_active_ap_growth_pending_locked(GsAmmSharedState *state)
{
    return gs_amm_select_ap_growth_candidate_locked(state) != NULL;
}

static int gs_amm_grow_ap_dynamic_locked(GsAmmSharedState *state, GsAmmApRecord *record, int requested_mb)
{
    int deficit_mb;
    int available_mb;
    int grant_mb;

    if (state == NULL || record == NULL || requested_mb <= 0)
        return 0;
    deficit_mb = Max((gs_amm_record_target_kb_locked(state, record) - record->granted_kb + 1023) / 1024, 0);
    available_mb = gs_amm_dynamic_free_mb_locked(state);
    grant_mb = Min(requested_mb, Min(deficit_mb, available_mb));
    if (grant_mb <= 0)
        return 0;

    record->dynamic_granted_bytes += (uint64)grant_mb * 1024 * 1024;
    gs_amm_refresh_ap_totals_locked(state);
    gs_amm_rebalance_ap_limits_locked(state);
    return grant_mb;
}

static int gs_amm_grow_ap_granule_locked(GsAmmSharedState *state, GsAmmApRecord *record, int requested_mb)
{
    GsAmmGrantToken token;
    int deficit_mb;
    int granule_id = -1;
    int assign_mb;
    GsAmmGranuleMeta *granule = NULL;

    if (state == NULL || record == NULL || requested_mb <= 0)
        return 0;
    deficit_mb = Max((gs_amm_record_target_kb_locked(state, record) - record->granted_kb + 1023) / 1024, 0);
    assign_mb = Min(requested_mb, deficit_mb);
    if (assign_mb <= 0)
        return 0;

    for (int index = state->total_granules - 1; index >= 0; index--) {
        GsAmmGranuleMeta *candidate = &state->granules[index];
        if (candidate->state == GS_AMM_GRANULE_FREE && !candidate->scan_inflight &&
            !candidate->reclaim_inflight &&
            GsAmmGranuleCanCompleteApLifecycle(candidate->generation, candidate->owner_epoch)) {
            granule = candidate;
            granule_id = index;
            break;
        }
    }
    if (granule == NULL)
        return 0;

    token.grant_id = record->grant_id;
    token.grant_generation = record->grant_generation;
    if (!gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_AP_RESERVED))
        return 0;
    granule->grant_id = token.grant_id;
    granule->grant_generation = token.grant_generation;
    granule->reserved_granules = 1;
    granule->active_grant_bytes = (uint64)assign_mb * 1024 * 1024;
    granule->used_bytes = 0;
    granule->alloc_cursor_bytes = 0;
    if (!gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_AP_ACTIVE)) {
        granule->grant_id = 0;
        granule->grant_generation = 0;
        granule->reserved_granules = 0;
        granule->active_grant_bytes = 0;
        (void)gs_amm_publish_granule_state_locked(state, granule, GS_AMM_GRANULE_FREE);
        return 0;
    }
    gs_amm_refresh_granule_counts_locked(state);
    gs_amm_refresh_ap_totals_locked(state);
    gs_amm_rebalance_ap_limits_locked(state);
    (void)granule_id;
    return assign_mb;
}

static void gs_amm_apply_backend_grant_kb(int grant_kb)
{
    MyGsAmmGrantKb = Max(grant_kb, 0);
    MyGsAmmGrantMb = MyGsAmmGrantKb > 0 ? (MyGsAmmGrantKb + 1023) / 1024 : 0;
    gs_amm_register_backend_cleanup();
}

static void gs_amm_clear_backend_grant_state(void)
{
    MyGsAmmGrantMb = 0;
    MyGsAmmGrantKb = 0;
    MyGsAmmGrantId = 0;
    MyGsAmmGrantGeneration = 0;
    MyGsAmmGrantGranules = 0;
    MyGsAmmGrantNative = false;
}

int GsAmmCurrentBackendGrantKB(void)
{
    GsAmmSharedState *state;
    int effective_grant_kb = 0;
    GsAmmGrantToken token = {MyGsAmmGrantId, MyGsAmmGrantGeneration};

    if (MyGsAmmGrantId == 0 || MyGsAmmGrantGeneration == 0)
        return 0;

    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    GsAmmApRecord *record = gs_amm_find_ap_record_locked(state, token);

    if (record != NULL)
        effective_grant_kb = record->effective_grant_kb;
    SpinLockRelease(&state->mutex);

    return effective_grant_kb > 0 ? effective_grant_kb : MyGsAmmGrantKb;
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
    GsAmmSharedState *state;
    GsAmmGrantToken token = {MyGsAmmGrantId, MyGsAmmGrantGeneration};
    uint64 pool_bytes = 0;

    if (!GsAmmGrantTokenIsValid(token))
        return 0;

    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    GsAmmApRecord *record = gs_amm_find_ap_record_locked(state, token);
    if (record != NULL) {
        pool_bytes = record->dynamic_granted_bytes;
        for (int index = 0; index < state->total_granules; index++) {
            GsAmmGranuleMeta *granule = &state->granules[index];
            if (gs_amm_granule_owned_by_grant(granule, token))
                pool_bytes += gs_amm_granule_active_capacity_bytes(granule);
        }
    }
    SpinLockRelease(&state->mutex);
    return pool_bytes;
}

uint64 GsAmmGrantEffectiveMemoryLimit(uint64 grant_id, uint64 requested_max_bytes)
{
    GsAmmSharedState *state;
    int effective_grant_kb = 0;
    GsAmmGrantToken token = {grant_id, MyGsAmmGrantGeneration};
    uint64 effective_bytes;

    if (grant_id == 0 || requested_max_bytes == 0)
        return 0;

    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    if (MyGsAmmGrantId == grant_id) {
        GsAmmApRecord *record = gs_amm_find_ap_record_locked(state, token);

        if (record != NULL)
            effective_grant_kb = record->effective_grant_kb;
    }
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

bool GsAmmGrantDynamicMemoryAvailable(GsAmmGrantToken token, Size size)
{
    GsAmmSharedState *state;
    GsAmmApRecord *record;
    bool available = false;

    if (size == 0 || !gs_amm_grant_token_is_current(token))
        return false;
    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    record = gs_amm_find_ap_record_locked(state, token);
    if (record != NULL && record->dynamic_granted_bytes >= record->dynamic_used_bytes &&
        size <= record->dynamic_granted_bytes - record->dynamic_used_bytes)
        available = true;
    SpinLockRelease(&state->mutex);
    return available;
}

void *GsAmmGrantAllocDynamicMemory(GsAmmGrantToken token, Size size)
{
    GsAmmSharedState *state;
    GsAmmApRecord *record;
    GsAmmDynamicAllocation *allocation;
    void *pointer;

    if (size == 0 || !gs_amm_grant_token_is_current(token))
        return NULL;
    pointer = malloc(size);
    if (pointer == NULL)
        return NULL;

    allocation = (GsAmmDynamicAllocation *)malloc(sizeof(GsAmmDynamicAllocation));
    if (allocation == NULL) {
        free(pointer);
        return NULL;
    }

    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    record = gs_amm_find_ap_record_locked(state, token);
    if (record == NULL || record->dynamic_granted_bytes < record->dynamic_used_bytes ||
        size > record->dynamic_granted_bytes - record->dynamic_used_bytes) {
        SpinLockRelease(&state->mutex);
        free(allocation);
        free(pointer);
        return NULL;
    }
    record->dynamic_used_bytes += size;
    allocation->pointer = pointer;
    allocation->size = size;
    allocation->next = MyGsAmmDynamicAllocations;
    MyGsAmmDynamicAllocations = allocation;
    gs_amm_refresh_ap_totals_locked(state);
    SpinLockRelease(&state->mutex);
    return pointer;
}

bool GsAmmGrantReturnDynamicMemory(GsAmmGrantToken token, void *pointer, Size size)
{
    GsAmmDynamicAllocation *previous = NULL;
    GsAmmDynamicAllocation *allocation;
    GsAmmSharedState *state;
    GsAmmApRecord *record;

    if (pointer == NULL || size == 0 || !gs_amm_grant_token_is_current(token))
        return false;
    for (allocation = MyGsAmmDynamicAllocations; allocation != NULL; allocation = allocation->next) {
        if (allocation->pointer == pointer)
            break;
        previous = allocation;
    }
    if (allocation == NULL || allocation->size != size)
        return false;
    if (previous == NULL)
        MyGsAmmDynamicAllocations = allocation->next;
    else
        previous->next = allocation->next;

    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    record = gs_amm_find_ap_record_locked(state, token);
    /* Usage is accounted at allocation/free transitions.  Returning the
     * malloc block only removes the backend-local allocation descriptor. */
    SpinLockRelease(&state->mutex);
    free(allocation->pointer);
    free(allocation);
    return record != NULL;
}

void GsAmmGrantAccountUsedDynamicMemory(GsAmmGrantToken token, Size size)
{
    GsAmmSharedState *state;
    GsAmmApRecord *record;

    if (size == 0 || !gs_amm_grant_token_is_current(token))
        return;
    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    record = gs_amm_find_ap_record_locked(state, token);
    if (record != NULL) {
        record->dynamic_used_bytes = Min(record->dynamic_used_bytes + size,
            record->dynamic_granted_bytes);
        gs_amm_refresh_ap_totals_locked(state);
    }
    SpinLockRelease(&state->mutex);
}

void GsAmmGrantAccountFreedDynamicMemory(GsAmmGrantToken token, Size size)
{
    GsAmmSharedState *state;
    GsAmmApRecord *record;

    if (size == 0 || !gs_amm_grant_token_is_current(token))
        return;
    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    record = gs_amm_find_ap_record_locked(state, token);
    if (record != NULL) {
        record->dynamic_used_bytes = record->dynamic_used_bytes >= size ?
            record->dynamic_used_bytes - size : 0;
        gs_amm_refresh_ap_totals_locked(state);
    }
    SpinLockRelease(&state->mutex);
}

static void gs_amm_release_dynamic_allocations(GsAmmGrantToken token)
{
    while (MyGsAmmDynamicAllocations != NULL) {
        GsAmmDynamicAllocation *allocation = MyGsAmmDynamicAllocations;

        if (!GsAmmGrantReturnDynamicMemory(token, allocation->pointer, allocation->size))
            break;
    }
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
    char *buffer_blocks = NULL;

    if (!GsAmmGrantTokenIsValid(token) || size == 0)
        return false;
    if (GsAmmGrantDynamicMemoryAvailable(token, size))
        return true;
    buffer_blocks = gs_amm_resolve_buffer_blocks();
    if (buffer_blocks == NULL)
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
                gs_amm_update_ap_used_locked(state, token);
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
    if (ptr != NULL)
        gs_amm_update_ap_used_locked(state, token);
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

bool GsAmmGrantProcessPendingReclaim(void)
{
    GsAmmSharedState *state = GsAmmState;
    GsAmmGrantToken token = {MyGsAmmGrantId, MyGsAmmGrantGeneration};
    GsAmmApRecord *record;
    int granule_id = -1;
    int released_mb;

    if (state == NULL || token.grant_id == 0 || token.grant_generation == 0)
        return false;

    SpinLockAcquire(&state->mutex);
    record = gs_amm_find_ap_record_locked(state, token);
    if (record != NULL && record->reclaim_pending)
        granule_id = record->reclaim_granule_id;
    SpinLockRelease(&state->mutex);
    if (granule_id < 0) {
        uint64 minimum_bytes;

        /* A dynamic-only AP has no shared granule to release.  Once its
         * backend has returned free dynamic chunks, lower the reservation to
         * the one-pass bound and return the quota to the global pool. */
        SpinLockAcquire(&state->mutex);
        record = gs_amm_find_ap_record_locked(state, token);
        if (record == NULL || !record->reclaim_pending) {
            SpinLockRelease(&state->mutex);
            return false;
        }
        minimum_bytes = (uint64)Max(record->one_pass_bound_kb, 1) * 1024;
        if (record->dynamic_used_bytes > minimum_bytes) {
            SpinLockRelease(&state->mutex);
            return false;
        }
        record->dynamic_granted_bytes = Max(minimum_bytes, record->dynamic_used_bytes);
        if (state->ap_downgrade_pending > 0)
            state->ap_downgrade_pending--;
        record->reclaim_pending = false;
        record->reclaim_granule_id = -1;
        record->target_mode = GS_AMM_MEMORY_MODE_ONEPASS;
        record->mode = GS_AMM_MEMORY_MODE_ONEPASS;
        gs_amm_rebalance_ap_limits_locked(state);
        gs_amm_refresh_ap_totals_locked(state);
        state->last_action = GS_AMM_TP_RECOVERY_DONE;
        SpinLockRelease(&state->mutex);
        return true;
    }

    /* The operator has already spilled/rebatched.  Only now may a whole
     * empty physical granule leave this backend's arena. */
    SpinLockAcquire(&state->mutex);
    released_mb = gs_amm_reclaim_unused_ap_granules_locked(
        state, 1, granule_id, token.grant_id, token.grant_generation);
    if (released_mb <= 0)
    {
        SpinLockRelease(&state->mutex);
        return false;
    }

    record = gs_amm_find_ap_record_locked(state, token);
    if (record != NULL) {
        if (record->reclaim_pending && state->ap_downgrade_pending > 0)
            state->ap_downgrade_pending--;
        record->reclaim_pending = false;
        record->reclaim_granule_id = -1;
        record->target_mode = GS_AMM_MEMORY_MODE_ONEPASS;
        record->mode = GS_AMM_MEMORY_MODE_ONEPASS;
    }
    state->tp_recovered_granules++;
    state->last_action = GS_AMM_TP_RECOVERY_DONE;
    gs_amm_rebalance_ap_limits_locked(state);
    gs_amm_refresh_ap_totals_locked(state);
    gs_amm_refresh_granule_counts_locked(state);
    SpinLockRelease(&state->mutex);
    return true;
}

bool GsAmmGrantReclaimPending(void)
{
    GsAmmSharedState *state = GsAmmState;
    GsAmmGrantToken token = {MyGsAmmGrantId, MyGsAmmGrantGeneration};
    bool pending = false;

    if (state == NULL || token.grant_id == 0 || token.grant_generation == 0)
        return false;

    SpinLockAcquire(&state->mutex);
    GsAmmApRecord *record = gs_amm_find_ap_record_locked(state, token);
    pending = record != NULL && record->reclaim_pending;
    SpinLockRelease(&state->mutex);
    return pending;
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
        gs_amm_update_ap_used_locked(state, token);
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
        (void)ap_reclaim;
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
    bool was_enabled;

    if (state == NULL)
        return;

    SpinLockAcquire(&state->mutex);
    was_enabled = state->runtime_config.amm_enabled;
    state->runtime_config.amm_enabled = enabled;
    if (enabled != was_enabled) {
        /* A new AMM epoch gets a fresh TP hit baseline and borrow guard. */
        state->tp_buffer_hit_baseline_hits = 0;
        state->tp_buffer_hit_baseline_accesses = 0;
        state->tp_buffer_hit_pct = 0;
        state->tp_buffer_hit_baseline_valid = false;
        state->tp_pressure_pct = 0;
        state->tp_pressure_valid = false;
        state->tp_pressure_hot = false;
        state->tp_pressure_state = GS_AMM_TP_PRESSURE_UNKNOWN;
        state->tp_hot_clear_windows = 0;
        state->tp_window_shared_blks_hit = 0;
        state->tp_window_shared_blks_read = 0;
        state->tp_cpu_last_total_jiffies = 0;
        state->tp_cpu_last_idle_jiffies = 0;
        state->tp_cpu_util_pct = 0;
        state->tp_cpu_util_valid = false;
        state->tp_cpu_guarded = false;
        state->tp_test_mode_until = 0;
        state->tp_test_worker_surge_until = 0;
        state->tp_baseline_tps = 0;
        state->tp_recent_tps = 0;
        state->tp_tps_baseline_valid = false;
        state->tp_tps_guarded = false;
        state->tp_window_completed_queries = 0;
        state->ap_borrow_buffer_hit_guarded = false;
        state->ap_borrow_count = 0;
        state->last_ap_borrow_at = 0;
        /* Do not carry a previous TP recovery epoch into a new AMM epoch.
         * Otherwise every subsequent AP is admitted against its multi-pass
         * minimum instead of the model's one-pass bound. */
        state->tp_recovery_phase = GS_AMM_TP_RECOVERY_IDLE;
        state->ap_multipass_only = false;
        state->tp_ap_stop_requested = 0;
        state->tp_ap_stop_completed = 0;
        state->tp_sb_restore_granules = 0;
        state->tp_restore_attempts = 0;
        state->tp_restore_retry_after = 0;
    }
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

/*
 * A backend can unregister its AP record after a concurrent reclaim attempt
 * loses the shared reclaim syscall lease.  Such a granule is no longer owned
 * by a live AP, but it must still be reclaimed before shared-buffer restore.
 * Only empty granules without an AP record are eligible here.
 */
static int gs_amm_reclaim_orphan_ap_granules(GsAmmSharedState *state)
{
    int released_mb = 0;

    if (state == NULL)
        return 0;

    for (;;) {
        GsAmmGranuleMeta reclaim_snapshot;
        GsAmmGranuleMeta *granule = NULL;
        GsAmmGrantToken token;
        uint64 reclaim_attempt = 0;
        uint32 reclaim_owner_epoch = 0;
        uint64 reclaimed_mb = 0;
        volatile bool reclaimed = false;

        SpinLockAcquire(&state->mutex);
        if (state->reclaim_syscall_inflight) {
            SpinLockRelease(&state->mutex);
            break;
        }
        for (int i = 0; i < state->total_granules; i++) {
            GsAmmGranuleMeta *candidate = &state->granules[i];

            if ((candidate->state != GS_AMM_GRANULE_AP_ACTIVE &&
                    candidate->state != GS_AMM_GRANULE_AP_RESERVED) ||
                candidate->grant_id == 0 || candidate->grant_generation == 0 ||
                candidate->used_bytes != 0 || candidate->scan_inflight ||
                candidate->reclaim_inflight)
                continue;
            token.grant_id = candidate->grant_id;
            token.grant_generation = candidate->grant_generation;
            if (gs_amm_find_ap_record_locked(state, token) != NULL)
                continue;
            granule = candidate;
            break;
        }
        if (granule == NULL) {
            SpinLockRelease(&state->mutex);
            break;
        }
        if (!gs_amm_begin_reclaim_locked(state, granule, 0, &reclaim_attempt) ||
            !gs_amm_acquire_reclaim_syscall_lease_locked(state, granule, 0,
                granule->owner_epoch, reclaim_attempt)) {
            SpinLockRelease(&state->mutex);
            break;
        }
        reclaim_owner_epoch = granule->owner_epoch;
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
        if (gs_amm_complete_reclaim_locked(state, granule, 0, reclaim_owner_epoch,
            reclaim_attempt, reclaimed, reclaimed_mb) && granule->state == GS_AMM_GRANULE_FREE)
            released_mb += (int)gs_amm_granule_grant_mb(&reclaim_snapshot);
        gs_amm_refresh_granule_counts_locked(state);
        SpinLockRelease(&state->mutex);
        if (!reclaimed)
            break;
    }
    return released_mb;
}

/* A failed buffer drain can leave an unowned granule in RECLAIMING.  Retry it
 * after AP pressure has been removed so the normal restore pass can reuse it. */
static int gs_amm_retry_stalled_buffer_reclaims(GsAmmSharedState *state)
{
    int released_mb = 0;

    if (state == NULL)
        return 0;

    for (;;) {
        GsAmmGranuleMeta reclaim_snapshot;
        GsAmmGranuleMeta *granule = NULL;
        uint64 reclaim_attempt = 0;
        uint64 drain_attempt = 0;
        uint32 reclaim_owner_epoch = 0;
        uint64 reclaimed_mb = 0;
        volatile bool reclaimed = false;

        SpinLockAcquire(&state->mutex);
        if (state->reclaim_syscall_inflight) {
            SpinLockRelease(&state->mutex);
            break;
        }
        for (int i = state->total_granules - 1; i >= 0; i--) {
            GsAmmGranuleMeta *candidate = &state->granules[i];
            GsAmmGrantToken token;

            if (candidate->state != GS_AMM_GRANULE_RECLAIMING ||
                candidate->reclaim_inflight || candidate->scan_inflight ||
                candidate->reclaim_attempt == 0)
                continue;
            /* A failed AP reclaim can retain the old token after its record
             * is unregistered; treat that granule as unowned as well. */
            if (candidate->grant_id != 0 || candidate->grant_generation != 0) {
                token.grant_id = candidate->grant_id;
                token.grant_generation = candidate->grant_generation;
                if (gs_amm_find_ap_record_locked(state, token) != NULL)
                    continue;
            }
            granule = candidate;
            break;
        }
        if (granule == NULL) {
            SpinLockRelease(&state->mutex);
            break;
        }
        reclaim_attempt = granule->reclaim_attempt;
        drain_attempt = granule->drain_attempt;
        reclaim_owner_epoch = granule->owner_epoch;
        if (!gs_amm_acquire_reclaim_syscall_lease_locked(state, granule, drain_attempt,
            reclaim_owner_epoch, reclaim_attempt)) {
            SpinLockRelease(&state->mutex);
            break;
        }
        reclaim_snapshot = *granule;
        SpinLockRelease(&state->mutex);

        PG_TRY();
        {
            reclaimed = gs_amm_reclaim_granule_memory(&reclaim_snapshot, &reclaimed_mb);
        }
        PG_CATCH();
        {
            SpinLockAcquire(&state->mutex);
            (void)gs_amm_complete_reclaim_locked(state, granule, drain_attempt,
                reclaim_owner_epoch, reclaim_attempt, false, 0);
            gs_amm_refresh_granule_counts_locked(state);
            SpinLockRelease(&state->mutex);
            PG_RE_THROW();
        }
        PG_END_TRY();

        SpinLockAcquire(&state->mutex);
        if (gs_amm_complete_reclaim_locked(state, granule, drain_attempt,
            reclaim_owner_epoch, reclaim_attempt, reclaimed, reclaimed_mb) &&
            granule->state == GS_AMM_GRANULE_FREE)
            released_mb += gs_amm_granule_capacity_mb(&reclaim_snapshot);
        gs_amm_refresh_granule_counts_locked(state);
        SpinLockRelease(&state->mutex);
        if (!reclaimed)
            break;
    }
    return released_mb;
}

static int gs_amm_reclaim_unused_ap_granules_locked(
    GsAmmSharedState *state, int requested_mb, int required_granule_id, uint64 required_grant_id,
    uint64 required_grant_generation)
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

            if (required_granule_id >= 0 && i != required_granule_id)
                continue;
            if (required_grant_id != 0 &&
                (candidate->grant_id != required_grant_id ||
                    candidate->grant_generation != required_grant_generation))
                continue;
            if ((candidate->state != GS_AMM_GRANULE_AP_ACTIVE &&
                    candidate->state != GS_AMM_GRANULE_AP_RESERVED) ||
                candidate->used_bytes != 0 ||
                (required_granule_id < 0 && candidate->alloc_cursor_bytes != 0) ||
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
            if (required_granule_id < 0) {
                state->ap_idle_reclaim_count++;
                state->ap_idle_reclaim_mb += granule_mb;
            }
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

    gs_amm_cancel_waiting_request();
    gs_amm_release_backend_grant();
    MyGsAmmCleanupRegistered = false;
}

void GsAmmSessionCleanup(int code, Datum arg)
{
    /* Thread-pool sessions are recycled on the same backend thread, so the
     * process-exit callback is not sufficient to release an AP queue slot or
     * grant.  Keep the cleanup idempotent for both callback paths. */
    gs_amm_backend_cleanup(code, arg);
}

static void gs_amm_register_backend_cleanup(void)
{
    if (MyGsAmmCleanupRegistered)
        return;

    on_proc_exit(gs_amm_backend_cleanup, 0);
    MyGsAmmCleanupRegistered = true;
}

static void gs_amm_release_unclaimed_queue_grant(GsAmmGrantToken token)
{
    GsAmmSharedState *state = GsAmmState;

    if (state == NULL || token.grant_id == 0 || token.grant_generation == 0)
        return;
    (void)gs_amm_release_ap_granules_locked(state, token);
    SpinLockAcquire(&state->mutex);
    gs_amm_unregister_ap_locked(state, token);
    if (state->active_ap_count > 0)
        state->active_ap_count--;
    gs_amm_refresh_ap_totals_locked(state);
    SpinLockRelease(&state->mutex);
}

static void gs_amm_cancel_waiting_request(void)
{
    GsAmmSharedState *state = GsAmmState;
    GsAmmGrantToken token = {0, 0};

    if (state == NULL || MyGsAmmQueueTicket == 0)
        return;
    SpinLockAcquire(&state->mutex);
    GsAmmApQueueSlot *slot = gs_amm_find_queue_slot_locked(state, MyGsAmmQueueTicket);
    /* During thread-pool session teardown the PGPROC identity fields may
     * already have been recycled.  The ticket is thread-local and globally
     * unique, so it is the authoritative ownership check here. */
    if (slot != NULL) {
        if (slot->state == GS_AMM_AP_QUEUE_GRANTED)
            token = slot->grant_token;
        (void)memset_s(slot, sizeof(*slot), 0, sizeof(*slot));
        state->ap_queue_cancel_count++;
        gs_amm_refresh_ap_queue_totals_locked(state);
        gs_amm_refresh_ap_target_totals_locked(state);
    }
    SpinLockRelease(&state->mutex);
    MyGsAmmQueueTicket = 0;
    if (token.grant_id != 0)
        gs_amm_release_unclaimed_queue_grant(token);
}

bool GsAmmReleaseGrantToken(GsAmmGrantToken expected_token)
{
    GsAmmSharedState *state = GsAmmState;
    GsAmmGrantToken backend_token = {MyGsAmmGrantId, MyGsAmmGrantGeneration};
    uint64 grant_id = expected_token.grant_id;
    int released_mb = 0;
    bool had_grant = MyGsAmmGrantId != 0;
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
    /* A final grant release may happen during backend/error cleanup while a
     * cursor-owned sort is still registered.  Drop the thread-local links
     * before the grant's memory becomes invalid. */
    tuplesort_clear_amm_reclaim_states();
    gs_amm_grant_arena_reset(expected_token);
    gs_amm_release_dynamic_allocations(expected_token);
    MyGsAmmGrantGeneration = 0;

    if (state != NULL && grant_id != 0) {
        released_mb = gs_amm_release_ap_granules_locked(state, expected_token);
        SpinLockAcquire(&state->mutex);
        gs_amm_unregister_ap_locked(state, expected_token);
        (void)released_mb;
        if (had_grant && state->active_ap_count > 0)
            state->active_ap_count--;
        /* Once the registry is empty no AP reservation can contribute to
         * dynamic usage.  Keep the global quota consistent even if an
         * earlier reclaim completed after its accounting update. */
        if (state->active_ap_count == 0) {
            state->dynamic_reserved_bytes = 0;
            state->dynamic_allocated_bytes = 0;
            state->dynamic_used_mb = 0;
        }
        gs_amm_refresh_ap_totals_locked(state);
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
                state->native_current_label_override = false;
            }
        }
        SpinLockRelease(&state->mutex);
        gs_amm_restore_buffer_granules_if_disabled(state);
    }

    /* GUC restoration can report errors; never leave a released grant owned by this backend. */
    gs_amm_clear_backend_grant_state();
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

bool GsAmmReleaseGrant(uint64 expected_generation)
{
    GsAmmGrantToken expected_token = {MyGsAmmGrantId, expected_generation};

    return GsAmmReleaseGrantToken(expected_token);
}

static void gs_amm_release_backend_grant(void)
{
    GsAmmGrantToken token = {MyGsAmmGrantId, MyGsAmmGrantGeneration};

    (void)GsAmmReleaseGrantToken(token);
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

static void gs_amm_request_ap_stop_locked(GsAmmSharedState *state)
{
    if (state == NULL)
        return;
    state->tp_recovery_phase = GS_AMM_TP_RECOVERY_WAIT_AP;
    for (int index = 0; index < GS_AMM_MAX_AP_REGISTRY; index++) {
        GsAmmApRecord *record = &state->ap_registry[index];

        if (!record->active || record->stop_requested)
            continue;
        record->stop_requested = true;
        state->tp_ap_stop_requested++;
        if (record->backend_pid != 0)
            (void)SendProcSignal(record->backend_pid, PROCSIG_AMM_AP_STOP, record->backend_id);
    }
    /* A queued AP has not received a grant yet, but it is still an active
     * query blocked in the admission latch.  Cancel it as part of the same
     * TP-priority transition so no waiter can be admitted after recovery. */
    for (int index = 0; index < GS_AMM_AP_QUEUE_CAPACITY; index++) {
        GsAmmApQueueSlot *slot = &state->ap_queue[index];

        if (slot->state == GS_AMM_AP_QUEUE_WAITING && slot->waiter_pid != 0)
            (void)SendProcSignal(slot->waiter_pid, PROCSIG_AMM_AP_STOP, InvalidBackendId);
    }
}

static void gs_amm_restore_shared_buffer_after_ap_stop(GsAmmSharedState *state)
{
    GsAmmResizeOutcome outcome;
    int before_blocks;
    int baseline_blocks;
    int orphan_released_mb;

    if (state == NULL)
        return;

    /* AP cancellation can race the single shared reclaim syscall.  Clean up
     * empty granules whose owner record has already disappeared before
     * calculating the amount of buffer space that can be restored. */
    SpinLockAcquire(&state->mutex);
    if (state->active_ap_count != 0) {
        SpinLockRelease(&state->mutex);
        return;
    }
    if (state->tp_restore_retry_after != 0 &&
        GetCurrentTimestamp() < state->tp_restore_retry_after) {
        SpinLockRelease(&state->mutex);
        return;
    }
    state->tp_restore_attempts++;
    state->tp_recovery_phase = GS_AMM_TP_RECOVERY_RESTORE_SB;
    baseline_blocks = gs_amm_mb_to_blocks(state->baseline_active_mb);
    SpinLockRelease(&state->mutex);

    orphan_released_mb = gs_amm_reclaim_orphan_ap_granules(state);
    (void)orphan_released_mb;
    (void)gs_amm_retry_stalled_buffer_reclaims(state);
    before_blocks = gs_amm_current_active_buffer_blocks(state);
    gs_amm_resize_core(baseline_blocks, &outcome);
    SpinLockAcquire(&state->mutex);
    if (outcome.active_blocks >= baseline_blocks && outcome.active_blocks > before_blocks)
        state->tp_sb_restore_granules += (uint64)((outcome.active_blocks - before_blocks + state->granule_blocks - 1) /
            state->granule_blocks);
    state->ap_multipass_only = true;
    if (outcome.active_blocks >= baseline_blocks) {
        state->tp_recovery_phase = GS_AMM_TP_RECOVERY_MULTIPASS;
        state->tp_restore_retry_after = 0;
        state->last_action = GS_AMM_TP_RECOVERY_DONE;
    } else {
        state->tp_recovery_phase = GS_AMM_TP_RECOVERY_RESTORE_SB;
        state->tp_restore_retry_after = GetCurrentTimestamp() + GS_AMM_RECLAIM_RETRY_INTERVAL_MS * 1000;
        state->tp_recovery_deferred_count++;
        state->last_action = GS_AMM_TP_RECOVERY_DEFERRED;
    }
    SpinLockRelease(&state->mutex);
}

static void gs_amm_resize_core(int target_blocks, GsAmmResizeOutcome *out)
{
    GsAmmSharedState *state = gs_amm_get_state();
    int active_buffers = gs_amm_current_active_buffer_blocks(state);
    int final_active_buffers = active_buffers;
    int pending_retire_blocks = 0;
    bool drain_deferred = false;

    out->decision = "noop";
    out->reason = "none";
    out->target_blocks = target_blocks;
    out->active_blocks = active_buffers;
    out->pending_retire_blocks = 0;
    out->rolled_back = false;

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

    {
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
    GsAmmSharedState *state = gs_amm_get_state();
    char status[4096];
    int active_mb;
    int dynamic_target_mb;
    int dynamic_used_mb;
    int dynamic_pool_free_mb;
    uint64 dynamic_reserved_bytes;
    uint64 dynamic_allocated_bytes;
    int active_ap_count;
    GsAmmStage stage;
    int ap_registry_count;
    uint64 ap_granted_bytes_total;
    uint64 ap_used_bytes_total;
    uint64 ap_reclaimable_bytes_total;
    int active_target_demand_mb;
    int queued_target_demand_mb = 0;
    int current_target_mb;
    int aggregate_target_mb;
    int dynamic_deficit_mb;
    uint64 ap_borrow_count;
    char last_supply_source[24];
    int ap_queue_len = 0;
    uint64 ap_queue_admit_count = 0;
    uint64 ap_queue_cancel_count = 0;
    int free_granules;
    int ap_active_granules;
    int last_prediction_mb;
    int last_grant_mb;
    int last_admission_target_mb;
    GsAmmAction last_action;
    int tp_pressure_pct;
    bool tp_pressure_hot;
    GsAmmTpPressureState tp_pressure_state;
    bool tp_pressure_valid;
    int tp_cpu_util_pct;
    bool tp_cpu_util_valid;
    bool tp_cpu_guarded;
    int baseline_active_mb;
    int tp_buffer_hit_baseline_pct;
    int tp_buffer_hit_pct;
    bool tp_buffer_hit_baseline_valid;
    uint64 tp_baseline_tps;
    uint64 tp_recent_tps;
    bool tp_tps_baseline_valid;
    bool tp_tps_guarded;
    bool ap_borrow_buffer_hit_guarded;
    int effective_dynamic_target_mb;
    int pending_demand_mb = 0;
    char last_backpressure_reason[GS_AMM_ADMISSION_REASON_LENGTH];
    uint64 tp_recovery_requested_granules;
    uint64 tp_recovered_granules;
    uint64 tp_recovery_deferred_count;
    uint64 tp_restore_attempts;
    int ap_downgrade_pending;
    uint64 ap_reclaim_poll_count;
    uint64 ap_reclaim_operator_release_bytes;
    int tp_low_pressure_windows;
    int tp_hot_clear_windows;
    GsAmmTpRecoveryPhase tp_recovery_phase;
    bool ap_multipass_only;
    int64 native_model_version;
    int64 native_leaf_id;
    bool native_label_override;
    char native_last_reason[GS_AMM_ADMISSION_REASON_LENGTH];
    uint64 tp_ap_stop_requested;
    uint64 tp_ap_stop_completed;
    uint64 tp_sb_restore_granules;
    GsAmmGranuleSummary granule_summary;

    SpinLockAcquire(&state->mutex);
    gs_amm_summarize_granules_locked(state, &granule_summary);
    active_mb = (int)gs_amm_blocks_to_mb(granule_summary.buffer_active_blocks);
    if (active_mb <= 0)
        active_mb = (int)gs_amm_blocks_to_mb(StrategyActiveBufferCount());
    dynamic_target_mb = state->dynamic_target_mb;
    dynamic_used_mb = state->dynamic_used_mb;
    dynamic_pool_free_mb = gs_amm_dynamic_free_mb_locked(state);
    dynamic_reserved_bytes = state->dynamic_reserved_bytes;
    dynamic_allocated_bytes = state->dynamic_allocated_bytes;
    active_ap_count = state->active_ap_count;
    stage = state->stage;
    ap_registry_count = state->ap_registry_count;
    ap_granted_bytes_total = state->ap_granted_bytes_total;
    ap_used_bytes_total = state->ap_used_bytes_total;
    ap_reclaimable_bytes_total = state->ap_reclaimable_bytes_total;
    gs_amm_refresh_ap_target_totals_locked(state);
    active_target_demand_mb = state->active_target_demand_mb;
    current_target_mb = state->last_current_target_mb;
    aggregate_target_mb = state->last_aggregate_target_mb + state->queued_target_demand_mb;
    dynamic_deficit_mb = state->last_dynamic_deficit_mb;
    ap_borrow_count = state->ap_borrow_count;
    (void)snprintf_s(last_supply_source, sizeof(last_supply_source),
        sizeof(last_supply_source) - 1, "%s", state->last_supply_source);
    free_granules = state->free_granules;
    ap_active_granules = state->ap_active_granules;
    last_prediction_mb = state->last_prediction_mb;
    last_grant_mb = state->last_grant_mb;
    last_admission_target_mb = state->last_admission_target_mb;
    last_action = state->last_action;
    tp_pressure_pct = state->tp_pressure_pct;
    tp_pressure_hot = state->tp_pressure_hot;
    tp_pressure_state = state->tp_pressure_state;
    tp_pressure_valid = state->tp_pressure_valid;
    tp_cpu_util_pct = state->tp_cpu_util_pct;
    tp_cpu_util_valid = state->tp_cpu_util_valid;
    tp_cpu_guarded = state->tp_cpu_guarded;
    baseline_active_mb = state->baseline_active_mb;
    tp_buffer_hit_baseline_pct = state->tp_buffer_hit_baseline_accesses > 0 ?
        (int)((state->tp_buffer_hit_baseline_hits * 100) /
            state->tp_buffer_hit_baseline_accesses) : 0;
    tp_buffer_hit_pct = state->tp_buffer_hit_pct;
    tp_buffer_hit_baseline_valid = state->tp_buffer_hit_baseline_valid;
    tp_baseline_tps = state->tp_baseline_tps;
    tp_recent_tps = state->tp_recent_tps;
    tp_tps_baseline_valid = state->tp_tps_baseline_valid;
    tp_tps_guarded = state->tp_tps_guarded;
    ap_borrow_buffer_hit_guarded = state->ap_borrow_buffer_hit_guarded;
    effective_dynamic_target_mb = gs_amm_effective_dynamic_target_mb();
    queued_target_demand_mb = state->queued_target_demand_mb;
    pending_demand_mb = state->pending_demand_mb;
    ap_queue_len = state->queued_ap_count;
    ap_queue_admit_count = state->ap_queue_admit_count;
    ap_queue_cancel_count = state->ap_queue_cancel_count;
    (void)snprintf_s(last_backpressure_reason, sizeof(last_backpressure_reason),
        sizeof(last_backpressure_reason) - 1, "%s", state->last_backpressure_reason);
    tp_recovery_requested_granules = state->tp_recovery_requested_granules;
    tp_recovered_granules = state->tp_recovered_granules;
    tp_recovery_deferred_count = state->tp_recovery_deferred_count;
    tp_restore_attempts = state->tp_restore_attempts;
    ap_downgrade_pending = state->ap_downgrade_pending;
    ap_reclaim_poll_count = state->ap_reclaim_poll_count;
    ap_reclaim_operator_release_bytes = state->ap_reclaim_operator_release_bytes;
    tp_low_pressure_windows = state->tp_low_pressure_windows;
    tp_hot_clear_windows = state->tp_hot_clear_windows;
    tp_recovery_phase = state->tp_recovery_phase;
    ap_multipass_only = state->ap_multipass_only;
    native_model_version = state->native_current_model_version;
    native_leaf_id = state->native_current_leaf_id;
    native_label_override = state->native_current_label_override;
    (void)snprintf_s(native_last_reason, sizeof(native_last_reason),
        sizeof(native_last_reason) - 1, "%s", state->native_last_reason);
    tp_ap_stop_requested = state->tp_ap_stop_requested;
    tp_ap_stop_completed = state->tp_ap_stop_completed;
    tp_sb_restore_granules = state->tp_sb_restore_granules;
    SpinLockRelease(&state->mutex);
    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "amm_enabled=%s active_mb=%d shared_buffers_min_mb=%d tp_reserve_mb=%d protected_buffer_mb=%d dynamic_target_mb=%d dynamic_used_mb=%d "
        "dynamic_pool_target_mb=%d dynamic_pool_reserved_mb=%d dynamic_pool_used_mb=%d dynamic_pool_free_mb=%d "
        "active_ap_count=%d stage=%s ap_registry_count=%d ap_granted_bytes_total=%llu "
        "ap_used_bytes_total=%llu ap_reclaimable_bytes_total=%llu ap_queue_len=%d "
        "ap_queue_admit_count=%llu ap_queue_cancel_count=%llu "
        "free_granules=%d buffer_draining_granules=%d reclaiming_granules=%d "
        "ap_reserved_granules=%d ap_active_granules=%d last_prediction_mb=%d last_grant_mb=%d "
        "last_admission_target_mb=%d "
        "active_target_mb=%d current_target_mb=%d queued_target_mb=%d aggregate_target_mb=%d "
        "dynamic_deficit_mb=%d ap_borrow_count=%llu last_supply_source=%s "
        "baseline_active_mb=%d effective_dynamic_target_mb=%d pending_demand_mb=%d "
        "tp_pressure_pct=%d tp_pressure_hot=%s tp_pressure_state=%s tp_pressure_valid=%s "
        "tp_cpu_util_pct=%d tp_cpu_util_valid=%s tp_cpu_guarded=%s "
        "tp_buffer_hit_baseline_pct=%d tp_buffer_hit_pct=%d "
        "tp_buffer_hit_baseline_valid=%s tp_baseline_tps=%llu tp_recent_tps=%llu "
        "tp_tps_baseline_valid=%s tp_tps_guarded=%s ap_borrow_buffer_hit_guarded=%s "
        "last_backpressure_reason=%s "
        "tp_recovery_requested_granules=%llu "
        "tp_recovered_granules=%llu tp_recovery_deferred_count=%llu tp_restore_attempts=%llu ap_downgrade_pending=%d "
        "ap_reclaim_poll_count=%llu ap_reclaim_operator_release_bytes=%llu "
        "tp_low_pressure_windows=%d tp_hot_clear_windows=%d tp_recovery_phase=%s ap_multipass_only=%s "
        "tp_ap_stop_requested=%llu tp_ap_stop_completed=%llu tp_sb_restore_granules=%llu "
        "native_model_version=%lld native_leaf_id=%lld native_label_override=%s native_last_reason=%s last_action=%s",
        gs_amm_enabled ? "true" : "false", active_mb, gs_amm_shared_buffers_min_mb,
        gs_amm_tp_reserve_mb, gs_amm_protected_buffer_mb(), dynamic_target_mb, dynamic_used_mb, dynamic_target_mb,
        (int)Min((dynamic_reserved_bytes + 1024 * 1024 - 1) / (1024 * 1024), (uint64)INT_MAX),
        (int)Min((dynamic_allocated_bytes + 1024 * 1024 - 1) / (1024 * 1024), (uint64)INT_MAX),
        dynamic_pool_free_mb, active_ap_count,
        gs_amm_stage_name(stage), ap_registry_count,
        (unsigned long long)ap_granted_bytes_total,
        (unsigned long long)ap_used_bytes_total,
        (unsigned long long)ap_reclaimable_bytes_total, ap_queue_len,
        (unsigned long long)ap_queue_admit_count, (unsigned long long)ap_queue_cancel_count,
        free_granules, granule_summary.buffer_draining_granules,
        granule_summary.reclaiming_granules, granule_summary.ap_reserved_granules,
        ap_active_granules, last_prediction_mb, last_grant_mb,
        last_admission_target_mb,
        active_target_demand_mb, current_target_mb, queued_target_demand_mb, aggregate_target_mb,
        dynamic_deficit_mb, (unsigned long long)ap_borrow_count, last_supply_source,
        baseline_active_mb, effective_dynamic_target_mb, pending_demand_mb,
        tp_pressure_pct, tp_pressure_hot ? "true" : "false",
        gs_amm_tp_pressure_state_name(tp_pressure_state), tp_pressure_valid ? "true" : "false",
        tp_cpu_util_pct, tp_cpu_util_valid ? "true" : "false", tp_cpu_guarded ? "true" : "false",
        tp_buffer_hit_baseline_pct, tp_buffer_hit_pct,
        tp_buffer_hit_baseline_valid ? "true" : "false",
        (unsigned long long)tp_baseline_tps,
        (unsigned long long)tp_recent_tps,
        tp_tps_baseline_valid ? "true" : "false",
        tp_tps_guarded ? "true" : "false",
        ap_borrow_buffer_hit_guarded ? "true" : "false",
        last_backpressure_reason,
        (unsigned long long)tp_recovery_requested_granules,
        (unsigned long long)tp_recovered_granules,
        (unsigned long long)tp_recovery_deferred_count,
        (unsigned long long)tp_restore_attempts, ap_downgrade_pending,
        (unsigned long long)ap_reclaim_poll_count,
        (unsigned long long)ap_reclaim_operator_release_bytes,
        tp_low_pressure_windows,
        tp_hot_clear_windows,
        gs_amm_tp_recovery_phase_name(tp_recovery_phase),
        ap_multipass_only ? "true" : "false",
        (unsigned long long)tp_ap_stop_requested,
        (unsigned long long)tp_ap_stop_completed,
        (unsigned long long)tp_sb_restore_granules,
        (long long)native_model_version,
        (long long)native_leaf_id,
        native_label_override ? "true" : "false",
        native_last_reason,
        gs_amm_action_name(last_action));
    securec_check_ss(rc, "\0", "\0");
    PG_RETURN_TEXT_P(cstring_to_text(status));
}

static bool gs_amm_prepare_dynamic_capacity(int ap_demand_mb)
{
    GsAmmSharedState *state = gs_amm_get_state();
    int active_blocks;
    TimestampTz now;
    bool can_borrow = false;
    uint64 window_hits;
    uint64 window_reads;
    uint64 window_accesses;
    int window_pressure_pct;
    int baseline_miss_pct;

    if (!gs_amm_enabled)
        return false;
    now = GetCurrentTimestamp();
    SpinLockAcquire(&state->mutex);
    /* A controller tick may not have consumed the current TP window yet.
     * Evaluate the counters in-place before shrinking SB so a newly hot TP
     * workload cannot lose a granule during that interval. */
    window_hits = state->tp_window_shared_blks_hit;
    window_reads = state->tp_window_shared_blks_read;
    window_accesses = window_hits + window_reads;
    window_pressure_pct = window_accesses > 0 ?
        (int)((window_reads * 100) / window_accesses) : 0;
    baseline_miss_pct = state->tp_buffer_hit_baseline_valid &&
        state->tp_buffer_hit_baseline_accesses > 0 ?
        Max(0, 100 - (int)((state->tp_buffer_hit_baseline_hits * 100) /
            state->tp_buffer_hit_baseline_accesses)) : 0;
    if (gs_amm_dynamic_free_mb_locked(state) < ap_demand_mb &&
        gs_amm_dynamic_free_mb_locked(state) + gs_amm_free_granule_mb_locked(state) < ap_demand_mb &&
        state->tp_pressure_valid && !state->tp_pressure_hot &&
        state->tp_pressure_state == GS_AMM_TP_PRESSURE_LOW_FLOW &&
        state->ap_downgrade_pending == 0 &&
        !state->tp_tps_guarded &&
        state->tp_buffer_hit_baseline_valid &&
        !state->ap_borrow_buffer_hit_guarded &&
        !(state->ap_borrow_count > 0 && window_accesses > 0 &&
            gs_amm_tp_buffer_miss_threshold_pct > 0 &&
            window_pressure_pct >= baseline_miss_pct + gs_amm_tp_buffer_miss_threshold_pct) &&
        (state->last_ap_borrow_at == 0 || now - state->last_ap_borrow_at >= GS_AMM_TP_WINDOW_MS * 1000)) {
        state->last_ap_borrow_at = now;
        can_borrow = true;
    }
    if (!can_borrow && state->tp_tps_guarded)
        gs_amm_set_backpressure_reason_locked(state, "tps_guard");
    SpinLockRelease(&state->mutex);
    if (!can_borrow)
        return false;

    active_blocks = gs_amm_current_active_buffer_blocks(state);
    /* A free granule may have appeared after the first snapshot (for example
     * when TP recovery completed).  Recheck the complete supply order before
     * shrinking shared buffers so a lower-priority source is never consumed
     * while a higher-priority source is available. */
    SpinLockAcquire(&state->mutex);
    window_hits = state->tp_window_shared_blks_hit;
    window_reads = state->tp_window_shared_blks_read;
    window_accesses = window_hits + window_reads;
    window_pressure_pct = window_accesses > 0 ?
        (int)((window_reads * 100) / window_accesses) : 0;
    baseline_miss_pct = state->tp_buffer_hit_baseline_valid &&
        state->tp_buffer_hit_baseline_accesses > 0 ?
        Max(0, 100 - (int)((state->tp_buffer_hit_baseline_hits * 100) /
            state->tp_buffer_hit_baseline_accesses)) : 0;
    if (gs_amm_dynamic_free_mb_locked(state) >= ap_demand_mb ||
        gs_amm_dynamic_free_mb_locked(state) + gs_amm_free_granule_mb_locked(state) >= ap_demand_mb ||
        !state->tp_pressure_valid || state->tp_pressure_hot ||
        state->tp_pressure_state != GS_AMM_TP_PRESSURE_LOW_FLOW ||
        state->ap_downgrade_pending > 0 ||
        state->tp_tps_guarded ||
        state->ap_borrow_buffer_hit_guarded || !state->tp_buffer_hit_baseline_valid ||
        (state->ap_borrow_count > 0 && window_accesses > 0 &&
            gs_amm_tp_buffer_miss_threshold_pct > 0 &&
            window_pressure_pct >= baseline_miss_pct + gs_amm_tp_buffer_miss_threshold_pct)) {
        if (state->tp_tps_guarded)
            gs_amm_set_backpressure_reason_locked(state, "tps_guard");
        state->last_ap_borrow_at = 0;
        SpinLockRelease(&state->mutex);
        return false;
    }
    SpinLockRelease(&state->mutex);
    if (ap_demand_mb > 0 && active_blocks > gs_amm_mb_to_blocks(gs_amm_protected_buffer_mb())) {
        GsAmmResizeOutcome outcome;
        int target_blocks = Max(active_blocks - state->granule_blocks,
            gs_amm_mb_to_blocks(gs_amm_protected_buffer_mb()));
        gs_amm_resize_core(gs_amm_align_resize_target_blocks(
            target_blocks, NORMAL_SHARED_BUFFER_NUM), &outcome);
        SpinLockAcquire(&state->mutex);
        if (outcome.decision == NULL || strcmp(outcome.decision, "shrunk") != 0) {
            state->last_ap_borrow_at = 0;
            SpinLockRelease(&state->mutex);
            return false;
        } else {
            state->last_action = GS_AMM_BORROW_FROM_BUFFER;
            state->ap_borrow_count++;
            (void)snprintf_s(state->last_supply_source, sizeof(state->last_supply_source),
                sizeof(state->last_supply_source) - 1, "%s", "shared_buffer");
        }
        SpinLockRelease(&state->mutex);
        return true;
    }
    return false;
}

/* Supply active AP grants before admitting queued APs. */
static void gs_amm_supply_ap_demand(void)
{
    GsAmmSharedState *state = gs_amm_get_state();
    int pending_mb;
    int aggregate_target_mb;
    int granted_mb;
    int dynamic_free_mb;
    int free_granule_mb;
    bool should_borrow = false;
    bool active_growth_pending;

    SpinLockAcquire(&state->mutex);
    gs_amm_refresh_ap_totals_locked(state);
    gs_amm_refresh_ap_target_totals_locked(state);
    /* TP recovery owns the allocator until all requested AP downgrades have
     * been confirmed.  Do not grow an existing AP or consume SB capacity. */
    if (state->tp_pressure_hot || state->tp_pressure_state != GS_AMM_TP_PRESSURE_LOW_FLOW ||
        state->ap_downgrade_pending > 0) {
        state->pending_demand_mb = 0;
        (void)snprintf_s(state->last_supply_source, sizeof(state->last_supply_source),
            sizeof(state->last_supply_source) - 1, "%s", "tp_recovery");
        SpinLockRelease(&state->mutex);
        return;
    }
    active_growth_pending = gs_amm_active_ap_growth_pending_locked(state);
    while (active_growth_pending && (dynamic_free_mb = gs_amm_dynamic_free_mb_locked(state)) > 0) {
        GsAmmApRecord *record = gs_amm_select_ap_growth_candidate_locked(state);
        if (record == NULL || gs_amm_grow_ap_dynamic_locked(state, record, dynamic_free_mb) <= 0)
            break;
        active_growth_pending = gs_amm_active_ap_growth_pending_locked(state);
    }
    while (active_growth_pending && (free_granule_mb = gs_amm_free_granule_mb_locked(state)) > 0) {
        GsAmmApRecord *record = gs_amm_select_ap_growth_candidate_locked(state);
        if (record == NULL || gs_amm_grow_ap_granule_locked(
                state, record, Min(free_granule_mb, gs_amm_configured_granule_mb())) <= 0)
            break;
        active_growth_pending = gs_amm_active_ap_growth_pending_locked(state);
    }
    gs_amm_refresh_ap_totals_locked(state);
    gs_amm_refresh_ap_target_totals_locked(state);
    aggregate_target_mb = state->last_aggregate_target_mb + state->queued_target_demand_mb;
    granted_mb = (int)Min((state->ap_granted_bytes_total + 1024 * 1024 - 1) /
        (1024 * 1024), (uint64)INT_MAX);
    pending_mb = Max(aggregate_target_mb - granted_mb, 0);
    dynamic_free_mb = gs_amm_dynamic_free_mb_locked(state);
    free_granule_mb = gs_amm_free_granule_mb_locked(state);
    state->last_dynamic_deficit_mb = pending_mb;
    state->pending_demand_mb = pending_mb;
    if (pending_mb > 0) {
        if (active_growth_pending) {
            (void)snprintf_s(state->last_supply_source, sizeof(state->last_supply_source),
                sizeof(state->last_supply_source) - 1, "%s", "active_ap");
        } else if (dynamic_free_mb > 0) {
            (void)snprintf_s(state->last_supply_source, sizeof(state->last_supply_source),
                sizeof(state->last_supply_source) - 1, "%s", "dynamic");
        } else if (free_granule_mb > 0) {
            (void)snprintf_s(state->last_supply_source, sizeof(state->last_supply_source),
                sizeof(state->last_supply_source) - 1, "%s", "free_granule");
        } else {
            (void)snprintf_s(state->last_supply_source, sizeof(state->last_supply_source),
                sizeof(state->last_supply_source) - 1, "%s", "queue");
        }
        /* Active APs are the first consumer of every higher-priority source,
         * but an outstanding active deficit must still be allowed to borrow
         * from shared buffers once dynamic and free-granule supply is empty. */
        if (dynamic_free_mb + free_granule_mb < pending_mb)
            should_borrow = true;
    }
    SpinLockRelease(&state->mutex);

    if (should_borrow)
        (void)gs_amm_prepare_dynamic_capacity(Min(pending_mb, gs_amm_configured_granule_mb()));
}

/*
 * Admit a request without publishing a queue slot when it is the only
 * waiter and the current dynamic/free-granule inventory can satisfy its
 * one-pass target.  Keeping this operation under the AMM lock preserves FIFO:
 * callers must only use it while the queue is empty.  Shared-buffer borrowing
 * is deliberately handled outside this helper because resize may sleep and
 * must never run while holding the spinlock.
 */
static bool gs_amm_try_admit_ap_locked(GsAmmSharedState *state, int cache_bound_kb,
    int one_pass_bound_kb, int multi_pass_bound_kb, int admission_target_kb, int prediction_mb,
    GsAmmGrantToken *grant_token, int *granted_kb, int *grant_granules)
{
    int demand_mb;
    int dynamic_free_mb;
    int free_granule_mb;
    int dynamic_grant_kb;
    int shared_grant_kb;
    int shared_grant_mb;
    GsAmmGrantToken token;

    if (state == NULL || grant_token == NULL || granted_kb == NULL || grant_granules == NULL ||
        state->maintenance_resetting || state->active_ap_count >= GS_AMM_MAX_AP_REGISTRY ||
        state->tp_pressure_hot || state->tp_pressure_state != GS_AMM_TP_PRESSURE_LOW_FLOW ||
        state->tp_recovery_phase == GS_AMM_TP_RECOVERY_STOP_AP ||
        state->tp_recovery_phase == GS_AMM_TP_RECOVERY_WAIT_AP ||
        state->tp_recovery_phase == GS_AMM_TP_RECOVERY_RESTORE_SB ||
        state->ap_downgrade_pending > 0)
        return false;
    if (gs_amm_queue_has_entries_locked(state))
        return false;

    demand_mb = (int)Min((Max((uint64)(state->ap_multipass_only ? multi_pass_bound_kb : admission_target_kb), 1) + 1023) / 1024,
        (uint64)INT_MAX);
    demand_mb = Max(demand_mb, gs_amm_ap_min_grant_mb);
    dynamic_free_mb = gs_amm_dynamic_free_mb_locked(state);
    free_granule_mb = gs_amm_free_granule_mb_locked(state);
    if (dynamic_free_mb + free_granule_mb < demand_mb)
        return false;

    token = gs_amm_next_grant_token_locked(state);
    if (token.grant_id == 0 || token.grant_generation == 0)
        return false;
    dynamic_grant_kb = (int)Min((uint64)demand_mb * 1024,
        (uint64)dynamic_free_mb * 1024);
    shared_grant_kb = demand_mb * 1024 - dynamic_grant_kb;
    shared_grant_mb = (shared_grant_kb + 1023) / 1024;
    *grant_granules = 0;

    if (shared_grant_mb > 0 && gs_amm_reserve_ap_granules_locked(
            state, token, shared_grant_mb, grant_granules) < shared_grant_mb)
        return false;
    if (shared_grant_mb > 0 && !gs_amm_activate_ap_granules_locked(state, token)) {
        (void)gs_amm_forward_unwind_ap_granules_locked(state, token);
        gs_amm_refresh_granule_counts_locked(state);
        return false;
    }
    if (!gs_amm_register_ap_locked(state, token, cache_bound_kb, one_pass_bound_kb,
        multi_pass_bound_kb, admission_target_kb, demand_mb * 1024, (uint64)dynamic_grant_kb * 1024,
    state->ap_multipass_only ? GS_AMM_MEMORY_MODE_MULTIPASS : GS_AMM_MEMORY_MODE_CACHE,
        state->ap_multipass_only ? GS_AMM_MEMORY_MODE_MULTIPASS : GS_AMM_MEMORY_MODE_CACHE,
        t_thrd.proc_cxt.MyProcPid, t_thrd.proc->backendId)) {
        (void)gs_amm_forward_unwind_ap_granules_locked(state, token);
        gs_amm_refresh_granule_counts_locked(state);
        return false;
    }
    state->active_ap_count++;
    state->last_prediction_mb = prediction_mb;
    state->last_grant_mb = demand_mb;
    state->last_admission_target_mb = demand_mb;
    state->last_action = GS_AMM_AP_EXPAND;
    (void)snprintf_s(state->last_supply_source, sizeof(state->last_supply_source),
        sizeof(state->last_supply_source) - 1, "%s",
        dynamic_grant_kb == demand_mb * 1024 ? "dynamic" : "free_granule");
    gs_amm_refresh_ap_totals_locked(state);
    gs_amm_refresh_ap_target_totals_locked(state);
    *grant_token = token;
    *granted_kb = demand_mb * 1024;
    return true;
}

/* Admit only the FIFO head and reserve its complete one-pass grant atomically. */
static void gs_amm_process_ap_queue(void)
{
    GsAmmSharedState *state = gs_amm_get_state();

    for (;;) {
        GsAmmApQueueSlot wake_slot;
        int demand_mb;
        int dynamic_free_mb;
        int free_granule_mb;
        int active_buffer_mb;
        int sb_borrowable_mb;
        int64 available_mb;
        int64 demand_kb;
        int64 dynamic_free_kb;
        int dynamic_grant_kb;
        int shared_grant_kb;
        int shared_grant_mb;
        int grant_granules = 0;
        GsAmmGrantToken token;
        bool granted = false;
        bool need_capacity = false;
        bool registry_full = false;
        GsAmmGranuleSummary granule_summary;

        (void)memset_s(&wake_slot, sizeof(wake_slot), 0, sizeof(wake_slot));
        SpinLockAcquire(&state->mutex);
        GsAmmApQueueSlot *head = gs_amm_queue_head_locked(state);
        if (state->tp_pressure_hot || state->tp_pressure_state != GS_AMM_TP_PRESSURE_LOW_FLOW ||
            state->tp_recovery_phase == GS_AMM_TP_RECOVERY_STOP_AP ||
            state->tp_recovery_phase == GS_AMM_TP_RECOVERY_WAIT_AP ||
            state->tp_recovery_phase == GS_AMM_TP_RECOVERY_RESTORE_SB ||
            state->ap_downgrade_pending > 0) {
            gs_amm_set_backpressure_reason_locked(state, "tp_recovery");
            SpinLockRelease(&state->mutex);
            return;
        }
        if (head == NULL) {
            SpinLockRelease(&state->mutex);
            return;
        }
        demand_mb = gs_amm_queue_demand_mb(head, state);
        dynamic_free_mb = gs_amm_dynamic_free_mb_locked(state);
        free_granule_mb = gs_amm_free_granule_mb_locked(state);
        gs_amm_summarize_granules_locked(state, &granule_summary);
        active_buffer_mb = (int)gs_amm_blocks_to_mb(granule_summary.buffer_active_blocks);
        sb_borrowable_mb = Max(active_buffer_mb - gs_amm_protected_buffer_mb(), 0);
        available_mb = (int64)dynamic_free_mb + free_granule_mb + sb_borrowable_mb;
        demand_kb = (int64)demand_mb * 1024;
        dynamic_free_kb = (int64)dynamic_free_mb * 1024;
        registry_full = state->active_ap_count >= GS_AMM_MAX_AP_REGISTRY;
        need_capacity = available_mb < demand_mb;
        /* Convert borrowable SB capacity into a free granule before trying
         * to reserve the queue head.  A queue slot is only left blocked once
         * the SB floor has been reached. */
        if (state->tp_tps_guarded) {
            gs_amm_set_backpressure_reason_locked(state, "tps_guard");
            SpinLockRelease(&state->mutex);
            return;
        }
        if (!state->maintenance_resetting && !registry_full &&
            dynamic_free_mb + free_granule_mb < demand_mb && sb_borrowable_mb > 0) {
            SpinLockRelease(&state->mutex);
            (void)gs_amm_prepare_dynamic_capacity(demand_mb);
            return;
        }
        if (state->maintenance_resetting || registry_full || need_capacity) {
            if (registry_full)
                gs_amm_set_backpressure_reason_locked(state, "registry_full");
            else if (need_capacity)
                gs_amm_set_backpressure_reason_locked(state, "capacity_floor");
            SpinLockRelease(&state->mutex);
            return;
        }
        /* Grant fields use int kB units.  An unrepresentable request cannot
         * be admitted and remains at the FIFO head until it is cancelled. */
        if (demand_kb > INT_MAX) {
            SpinLockRelease(&state->mutex);
            return;
        }

        token = gs_amm_next_grant_token_locked(state);
        if (token.grant_id == 0 || token.grant_generation == 0) {
            SpinLockRelease(&state->mutex);
            return;
        }
        dynamic_grant_kb = (int)Min(demand_kb, dynamic_free_kb);
        shared_grant_kb = (int)(demand_kb - dynamic_grant_kb);
        shared_grant_mb = (shared_grant_kb + 1023) / 1024;
        if (shared_grant_mb == 0 || gs_amm_reserve_ap_granules_locked(
                state, token, shared_grant_mb, &grant_granules) >= shared_grant_mb) {
            if (shared_grant_mb == 0 || gs_amm_activate_ap_granules_locked(state, token)) {
                if (gs_amm_register_ap_locked(state, token, head->cache_bound_kb,
                    head->one_pass_bound_kb, head->multi_pass_bound_kb, head->admission_target_kb,
                    demand_mb * 1024,
                    (uint64)dynamic_grant_kb * 1024,
                    state->ap_multipass_only ? GS_AMM_MEMORY_MODE_MULTIPASS : GS_AMM_MEMORY_MODE_CACHE,
                    state->ap_multipass_only ? GS_AMM_MEMORY_MODE_MULTIPASS : GS_AMM_MEMORY_MODE_CACHE,
                    /* The queue is admitted by the controller/backend that
                     * happens to process the FIFO head, but cancellation must
                     * target the AP backend that owns the waiting slot. */
                    head->waiter_pid,
                    InvalidBackendId)) {
                    state->active_ap_count++;
                    state->last_grant_mb = demand_mb;
                    state->last_admission_target_mb = demand_mb;
                    state->last_prediction_mb = head->prediction_mb;
                    state->last_action = GS_AMM_AP_EXPAND;
                    state->ap_queue_admit_count++;
                    wake_slot = *head;
                    wake_slot.state = GS_AMM_AP_QUEUE_GRANTED;
                    wake_slot.grant_token = token;
                    wake_slot.granted_kb = demand_mb * 1024;
                    wake_slot.grant_granules = grant_granules;
                    wake_slot.memory_mode = state->ap_multipass_only ?
                        GS_AMM_MEMORY_MODE_MULTIPASS : GS_AMM_MEMORY_MODE_CACHE;
                    *head = wake_slot;
                    gs_amm_refresh_ap_totals_locked(state);
                    gs_amm_refresh_ap_queue_totals_locked(state);
                    gs_amm_refresh_ap_target_totals_locked(state);
                    granted = true;
                }
            }
        }
        if (!granted) {
            if (shared_grant_mb > 0)
                (void)gs_amm_forward_unwind_ap_granules_locked(state, token);
            gs_amm_refresh_granule_counts_locked(state);
            SpinLockRelease(&state->mutex);
            return;
        }
        SpinLockRelease(&state->mutex);
        gs_amm_wake_queue_waiter(&wake_slot);
    }
}

static bool gs_amm_request_tp_downgrade_locked(GsAmmSharedState *state)
{
    GsAmmApRecord *best = NULL;
    int best_granule_id = -1;
    uint64 best_used_bytes = PG_UINT64_MAX;

    for (int granule_id = 0; granule_id < state->total_granules; granule_id++) {
        GsAmmGranuleMeta *granule = &state->granules[granule_id];
        GsAmmGrantToken token;
        GsAmmApRecord *record;

        if (granule->state != GS_AMM_GRANULE_AP_ACTIVE || granule->reclaim_inflight ||
            granule->scan_inflight)
            continue;
        token.grant_id = granule->grant_id;
        token.grant_generation = granule->grant_generation;
        record = gs_amm_find_ap_record_locked(state, token);
        if (record == NULL || record->reclaim_pending ||
            record->effective_grant_kb <= record->one_pass_bound_kb)
            continue;
        if (best == NULL || granule->used_bytes < best_used_bytes ||
            (granule->used_bytes == best_used_bytes && token.grant_id < best->grant_id)) {
            best = record;
            best_granule_id = granule_id;
            best_used_bytes = granule->used_bytes;
        }
    }
    /* Dynamic grants do not own a physical granule, but they still consume
     * the global dynamic quota and must participate in TP-driven downgrade. */
    if (best == NULL) {
        for (int index = 0; index < GS_AMM_MAX_AP_REGISTRY; index++) {
            GsAmmApRecord *record = &state->ap_registry[index];
            uint64 minimum_bytes;

            if (!record->active || record->reclaim_pending)
                continue;
            minimum_bytes = (uint64)Max(record->one_pass_bound_kb, 1) * 1024;
            if (record->dynamic_granted_bytes <= minimum_bytes ||
                record->effective_grant_kb <= record->one_pass_bound_kb)
                continue;
            if (best == NULL || record->dynamic_used_bytes < best_used_bytes ||
                (record->dynamic_used_bytes == best_used_bytes && record->grant_id < best->grant_id)) {
                best = record;
                best_granule_id = -1;
                best_used_bytes = record->dynamic_used_bytes;
            }
        }
    }
    if (best == NULL)
        return false;

    best->reclaim_pending = true;
    best->reclaim_granule_id = best_granule_id;
    best->target_mode = GS_AMM_MEMORY_MODE_ONEPASS;
    if (best->revoke_epoch != PG_UINT64_MAX)
        best->revoke_epoch++;
    state->ap_downgrade_pending++;
    state->tp_recovery_requested_granules++;
    gs_amm_rebalance_ap_limits_locked(state);
    return true;
}

static bool gs_amm_expand_one_tp_granule(GsAmmSharedState *state)
{
    int active_blocks = gs_amm_current_active_buffer_blocks(state);
    int baseline_blocks = gs_amm_mb_to_blocks(state->baseline_active_mb);
    int target_blocks = Min(active_blocks + state->granule_blocks, NORMAL_SHARED_BUFFER_NUM);
    int final_blocks;

    if (baseline_blocks > 0)
        target_blocks = Min(target_blocks, baseline_blocks);
    if (target_blocks <= active_blocks)
        return false;
    final_blocks = gs_amm_expand_granules(state, target_blocks, false);
    return final_blocks > active_blocks;
}

void GsAmmRecordTpBufferUsage(uint64 shared_blks_hit, uint64 shared_blks_read,
    uint64 completed_queries)
{
    GsAmmSharedState *state;

    if (!gs_amm_enabled || gs_amm_workload_role != GS_AMM_WORKLOAD_TP)
        return;
    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    if (gs_amm_tp_test_mode)
        state->tp_test_mode_until = GetCurrentTimestamp() +
            GS_AMM_TP_TEST_MODE_LEASE_WINDOWS * GS_AMM_TP_WINDOW_MS * 1000;
    if (gs_amm_tp_test_worker_surge)
        /* The controller runs in pagewriter, so publish the test-only surge
         * marker from the TP backend that owns the test GUC. */
        state->tp_test_worker_surge_until = GetCurrentTimestamp() +
            GS_AMM_TP_TEST_SURGE_LEASE_WINDOWS * GS_AMM_TP_WINDOW_MS * 1000;
    state->tp_window_shared_blks_hit += shared_blks_hit;
    state->tp_window_shared_blks_read += shared_blks_read;
    state->tp_window_completed_queries += completed_queries;
    SpinLockRelease(&state->mutex);
}

void GsAmmRecordApReclaimPoll(uint64 released_bytes)
{
    GsAmmSharedState *state;
    GsAmmGrantToken token;
    GsAmmApRecord *record;

    if (!gs_amm_enabled || gs_amm_workload_role != GS_AMM_WORKLOAD_AP)
        return;
    state = gs_amm_get_state();
    token.grant_id = MyGsAmmGrantId;
    token.grant_generation = MyGsAmmGrantGeneration;
    SpinLockAcquire(&state->mutex);
    record = gs_amm_find_ap_record_locked(state, token);
    if (record != NULL && record->reclaim_pending) {
        state->ap_reclaim_poll_count++;
        state->ap_reclaim_operator_release_bytes += released_bytes;
    }
    SpinLockRelease(&state->mutex);
}

/* Read aggregate host CPU jiffies.  The controller stores consecutive
 * snapshots in shared state so every TP backend observes the same interval. */
static bool gs_amm_read_host_cpu_jiffies(uint64 *total_jiffies, uint64 *idle_jiffies)
{
    FILE *stat_file;
    char line[256];
    uint64 user = 0;
    uint64 nice = 0;
    uint64 system = 0;
    uint64 idle = 0;
    uint64 iowait = 0;
    uint64 irq = 0;
    uint64 softirq = 0;
    uint64 steal = 0;
    int fields;

    if (total_jiffies == NULL || idle_jiffies == NULL)
        return false;
    stat_file = fopen("/proc/stat", "r");
    if (stat_file == NULL)
        return false;
    if (fgets(line, sizeof(line), stat_file) == NULL) {
        fclose(stat_file);
        return false;
    }
    fclose(stat_file);
    fields = sscanf_s(line, "cpu %lu %lu %lu %lu %lu %lu %lu %lu", &user, &nice,
        &system, &idle, &iowait, &irq, &softirq, &steal);
    if (fields < 4)
        return false;
    *total_jiffies = user + nice + system + idle + iowait + irq + softirq + steal;
    *idle_jiffies = idle + iowait;
    return *total_jiffies > 0;
}

void GsAmmControllerTick(void)
{
    GsAmmSharedState *state;
    TimestampTz now;
    uint64 accesses;
    int pressure_pct;
    bool hot;
    int low_pressure_threshold;
    uint64 hits;
    uint64 reads;
    uint64 completed_queries;
    int hit_pct;
    bool hit_guarded = false;
    bool miss_guarded = false;
    bool tps_guarded = false;
    bool previous_hot;
    bool hit_baseline_ready;
    bool cpu_sample_valid = false;
    bool cpu_guarded = false;
    bool recovery_metrics_stable;
    bool test_mode;
    bool test_worker_surge;
    bool tp_sample_available;
    int baseline_miss_pct = 0;
    int cpu_util_pct = 0;
    uint64 cpu_total_jiffies = 0;
    uint64 cpu_idle_jiffies = 0;

    if (!gs_amm_enabled)
        return;
    state = gs_amm_get_state();
    now = GetCurrentTimestamp();
    cpu_sample_valid = gs_amm_read_host_cpu_jiffies(&cpu_total_jiffies, &cpu_idle_jiffies);
    test_mode = gs_amm_tp_test_mode;
    test_worker_surge = gs_amm_tp_test_worker_surge;
    SpinLockAcquire(&state->mutex);
    state->dynamic_target_mb = gs_amm_effective_dynamic_target_mb();
    if (test_mode)
        state->tp_test_mode_until = now +
            GS_AMM_TP_TEST_MODE_LEASE_WINDOWS * GS_AMM_TP_WINDOW_MS * 1000;
    if (test_worker_surge)
        /* A test worker can spend several seconds in one full-table scan.
         * Keep its test-only marker visible to pagewriter until its next
         * completion publishes a fresh lease. */
        state->tp_test_worker_surge_until = now +
            GS_AMM_TP_TEST_SURGE_LEASE_WINDOWS * GS_AMM_TP_WINDOW_MS * 1000;
    if (state->tp_last_window_at != 0 && now - state->tp_last_window_at < GS_AMM_TP_WINDOW_MS * 1000) {
        SpinLockRelease(&state->mutex);
        return;
    }
    state->tp_last_window_at = now;
    /* A test worker publishes a short shared lease.  Pagewriter owns the
     * one-second tick, so a thread-local test GUC alone would be invisible
     * to the process that makes the scheduling decision. */
    test_mode = state->tp_test_mode_until > now;
    test_worker_surge = state->tp_test_worker_surge_until > now;
    if (cpu_sample_valid) {
        if (state->tp_cpu_util_valid && cpu_total_jiffies > state->tp_cpu_last_total_jiffies) {
            uint64 total_delta = cpu_total_jiffies - state->tp_cpu_last_total_jiffies;
            uint64 idle_delta = cpu_idle_jiffies >= state->tp_cpu_last_idle_jiffies ?
                cpu_idle_jiffies - state->tp_cpu_last_idle_jiffies : 0;

            idle_delta = Min(idle_delta, total_delta);
            cpu_util_pct = (int)(((total_delta - idle_delta) * 100) / total_delta);
            state->tp_cpu_util_pct = cpu_util_pct;
        }
        state->tp_cpu_last_total_jiffies = cpu_total_jiffies;
        state->tp_cpu_last_idle_jiffies = cpu_idle_jiffies;
        state->tp_cpu_util_valid = true;
    }
    hits = state->tp_window_shared_blks_hit;
    reads = state->tp_window_shared_blks_read;
    completed_queries = state->tp_window_completed_queries;
    accesses = hits + reads;
    tp_sample_available = accesses > 0 || completed_queries > 0;
    if (accesses == 0) {
        bool empty_window_hot = false;
        bool restore_after_empty = false;

        state->tp_recent_tps = completed_queries;
        state->tp_window_shared_blks_hit = 0;
        state->tp_window_shared_blks_read = 0;
        state->tp_window_completed_queries = 0;
        /* A completed TP query with no buffer counters still provides a TP
         * CPU-pressure sample.  A fully empty pagewriter window does not. */
        if (completed_queries == 0) {
            /* Host CPU is not TP pressure without a TP completion or buffer
             * sample.  Pagewriter executes this tick too, so do not let an
             * unrelated workload preempt AP before TP has started. */
            if (!state->tp_pressure_valid) {
                state->tp_pressure_pct = 0;
                state->tp_buffer_hit_pct = 100;
                state->tp_pressure_valid = true;
                state->tp_pressure_hot = false;
                state->tp_pressure_state = GS_AMM_TP_PRESSURE_LOW_FLOW;
            } else if (state->tp_pressure_hot) {
                state->tp_pressure_hot = false;
                state->tp_pressure_state = GS_AMM_TP_PRESSURE_RECOVERY_WAIT;
                state->tp_hot_clear_windows = 1;
            } else if (state->tp_pressure_state == GS_AMM_TP_PRESSURE_RECOVERY_WAIT) {
                state->tp_hot_clear_windows++;
                if (state->tp_hot_clear_windows >= GS_AMM_TP_HOT_CLEAR_WINDOWS) {
                    state->tp_hot_clear_windows = 0;
                    state->tp_pressure_state = GS_AMM_TP_PRESSURE_LOW_FLOW;
                }
            }
            /* MULTIPASS is a completed recovery state: shared-buffer restore
             * has reached baseline, so queued APs may be supplied using their
             * multipass bounds while the recovery policy remains latched. */
            bool allow_ap_supply = state->tp_pressure_state == GS_AMM_TP_PRESSURE_LOW_FLOW &&
                (state->tp_recovery_phase == GS_AMM_TP_RECOVERY_IDLE ||
                 state->tp_recovery_phase == GS_AMM_TP_RECOVERY_MULTIPASS) &&
                state->ap_downgrade_pending == 0;
            bool restore_pending = !state->tp_pressure_hot && state->active_ap_count == 0 &&
                (state->tp_recovery_phase == GS_AMM_TP_RECOVERY_WAIT_AP ||
                 state->tp_recovery_phase == GS_AMM_TP_RECOVERY_RESTORE_SB);
            SpinLockRelease(&state->mutex);
            if (restore_pending)
                gs_amm_restore_shared_buffer_after_ap_stop(state);
            if (allow_ap_supply) {
                gs_amm_supply_ap_demand();
                gs_amm_process_ap_queue();
            }
            return;
        }
        if (!state->tp_pressure_valid) {
            state->tp_pressure_pct = 0;
            state->tp_buffer_hit_pct = 100;
            state->tp_pressure_valid = true;
            state->tp_pressure_hot = false;
            state->tp_pressure_state = GS_AMM_TP_PRESSURE_LOW_FLOW;
            state->tp_hot_clear_windows = 0;
            /* Do not derive a surge baseline from a counter-less window.
             * The first real buffer sample establishes the TP rate. */
            state->tp_baseline_tps = 0;
            state->tp_tps_baseline_valid = false;
        }
        cpu_guarded = tp_sample_available && state->tp_cpu_util_valid &&
            gs_amm_tp_cpu_pressure_threshold_pct > 0 &&
            state->tp_cpu_util_pct >= gs_amm_tp_cpu_pressure_threshold_pct;
        state->tp_cpu_guarded = cpu_guarded;
        if (test_mode ? test_worker_surge : cpu_guarded) {
            state->tp_hot_clear_windows = 0;
            state->tp_pressure_hot = true;
            state->tp_pressure_state = GS_AMM_TP_PRESSURE_HOT_RECOVERY;
            empty_window_hot = true;
        } else if (state->tp_pressure_hot) {
            state->tp_pressure_hot = false;
            state->tp_pressure_state = GS_AMM_TP_PRESSURE_RECOVERY_WAIT;
            state->tp_hot_clear_windows = 1;
        } else if (state->tp_pressure_state == GS_AMM_TP_PRESSURE_RECOVERY_WAIT) {
            state->tp_hot_clear_windows++;
            if (state->tp_hot_clear_windows >= GS_AMM_TP_HOT_CLEAR_WINDOWS) {
                state->tp_hot_clear_windows = 0;
                state->tp_pressure_state = GS_AMM_TP_PRESSURE_LOW_FLOW;
            }
        }
        restore_after_empty = !state->tp_pressure_hot &&
            (state->tp_recovery_phase == GS_AMM_TP_RECOVERY_WAIT_AP ||
             state->tp_recovery_phase == GS_AMM_TP_RECOVERY_RESTORE_SB) &&
            state->active_ap_count == 0;
        bool allow_ap_supply = state->tp_pressure_valid &&
            state->tp_pressure_state == GS_AMM_TP_PRESSURE_LOW_FLOW &&
            (state->tp_recovery_phase == GS_AMM_TP_RECOVERY_IDLE ||
             state->tp_recovery_phase == GS_AMM_TP_RECOVERY_MULTIPASS) &&
            state->ap_downgrade_pending == 0;
        SpinLockRelease(&state->mutex);
        if (empty_window_hot) {
            SpinLockAcquire(&state->mutex);
            state->tp_recovery_phase = GS_AMM_TP_RECOVERY_STOP_AP;
            gs_amm_request_ap_stop_locked(state);
            bool ap_stopped = state->active_ap_count == 0;
            SpinLockRelease(&state->mutex);
            if (ap_stopped)
                gs_amm_restore_shared_buffer_after_ap_stop(state);
            return;
        }
        if (restore_after_empty)
            gs_amm_restore_shared_buffer_after_ap_stop(state);
        if (allow_ap_supply) {
            gs_amm_supply_ap_demand();
            gs_amm_process_ap_queue();
        }
        return;
    } else {
        pressure_pct = (int)((reads * 100) / accesses);
        hit_pct = (int)((hits * 100) / accesses);
    }
    state->tp_recent_tps = completed_queries;
    if (completed_queries > 0 && (!state->tp_tps_baseline_valid ||
        /* During the low-flow setup window, let the baseline follow the
         * actual two-worker rate even when AP already owns granules.  Once
         * the rate jumps beyond the surge guard it is held for recovery. */
        (state->tp_pressure_state == GS_AMM_TP_PRESSURE_LOW_FLOW &&
            (!state->tp_tps_baseline_valid ||
                completed_queries <= state->tp_baseline_tps *
                    (uint64)(100 + GS_AMM_TP_TPS_SURGE_GUARD_PCT) / 100)))) {
        state->tp_baseline_tps = completed_queries;
        state->tp_tps_baseline_valid = true;
    }
    state->tp_buffer_hit_pct = hit_pct;
    /*
     * A TP table scan naturally changes its hit ratio while it warms its
     * working set.  Until AMM has actually removed an SB granule, that change
     * is not evidence that AP allocation hurt TP.  Keep learning the baseline
     * through this pre-borrow period, then freeze it (apart from genuine
     * recovery) so the first AMM-caused decline is observable.
     */
    hit_baseline_ready = state->tp_buffer_hit_baseline_valid &&
        state->tp_buffer_hit_baseline_accesses > 0;
    if (!hit_baseline_ready || state->ap_borrow_count == 0) {
        state->tp_buffer_hit_baseline_hits = hits;
        state->tp_buffer_hit_baseline_accesses = accesses;
        state->tp_buffer_hit_baseline_valid = true;
    } else if (!state->tp_pressure_hot && state->tp_buffer_hit_baseline_accesses > 0 &&
        (uint64)hit_pct * state->tp_buffer_hit_baseline_accesses >
            state->tp_buffer_hit_baseline_hits * 100) {
        /* Do not let the first cold-start window become the permanent TP
         * baseline.  While pressure is cold, a better hit ratio is evidence
         * that the working set has warmed; retain that window as the new
         * baseline so a later cache eviction is observable. */
        state->tp_buffer_hit_baseline_hits = hits;
        state->tp_buffer_hit_baseline_accesses = accesses;
    }
    if (state->ap_borrow_count > 0 && hit_baseline_ready &&
        gs_amm_ap_borrow_buffer_hit_guard_pct > 0 &&
        state->tp_buffer_hit_baseline_hits > 0 &&
        state->tp_buffer_hit_baseline_accesses > 0) {
        double baseline_ratio = (double)state->tp_buffer_hit_baseline_hits /
            (double)state->tp_buffer_hit_baseline_accesses;
        double current_ratio = (double)hits / (double)accesses;
        double decline_pct = baseline_ratio > 0.0 ?
            (baseline_ratio - current_ratio) / baseline_ratio * 100.0 : 0.0;

        hit_guarded = decline_pct >= (double)gs_amm_ap_borrow_buffer_hit_guard_pct;
        /* The guard follows the current window.  Once hit ratio recovers to
         * its baseline, SB borrowing may resume on a later cold tick. */
        state->ap_borrow_buffer_hit_guarded = hit_guarded;
    }
    state->tp_window_shared_blks_hit = 0;
    state->tp_window_shared_blks_read = 0;
    state->tp_window_completed_queries = 0;
    state->tp_pressure_pct = pressure_pct;
    state->tp_pressure_valid = true;
    if (state->tp_buffer_hit_baseline_valid && state->tp_buffer_hit_baseline_accesses > 0) {
        int baseline_hit_pct = (int)((state->tp_buffer_hit_baseline_hits * 100) /
            state->tp_buffer_hit_baseline_accesses);
        baseline_miss_pct = Max(0, 100 - baseline_hit_pct);
    }
    /* The first valid window establishes normal TP locality.  Compare later
     * windows with that baseline instead of treating a naturally read-heavy
     * warm-up window as hot. */
    if (state->ap_borrow_count > 0 && gs_amm_tp_buffer_miss_threshold_pct > 0 &&
        hit_baseline_ready)
        /* Equality at the configured boundary is normal counter rounding
         * during the first TP window; require a genuine excess before
         * preempting AP. */
        miss_guarded = pressure_pct > baseline_miss_pct + gs_amm_tp_buffer_miss_threshold_pct;
    if (test_mode) {
        tps_guarded = test_worker_surge;
    } else if (state->ap_borrow_count > 0 && gs_amm_tp_tps_decline_guard_pct > 0 &&
        state->tp_tps_baseline_valid && completed_queries > 0) {
        /* A percentage decline cannot be observed reliably until one query
         * is less than the configured percentage of the baseline.  For
         * example, a 3% guard needs at least 34 completed queries; with an
         * eight-query window, a one-query change is already 12.5% and would
         * otherwise turn normal counter quantization into a false hot state.
         */
        uint64 resolution_samples =
            (uint64)((100 + gs_amm_tp_tps_decline_guard_pct - 1) /
                gs_amm_tp_tps_decline_guard_pct);
        if (state->tp_baseline_tps >= resolution_samples) {
            uint64 permitted_tps = state->tp_baseline_tps *
                (uint64)(100 - gs_amm_tp_tps_decline_guard_pct) / 100;

            /* Query counts are integral; the floored permitted value is the
             * first count whose relative decline reaches the configured
             * threshold. */
            tps_guarded = completed_queries <= permitted_tps;
        }
    }
    state->tp_tps_guarded = tps_guarded;
    cpu_guarded = tp_sample_available && state->tp_cpu_util_valid &&
        gs_amm_tp_cpu_pressure_threshold_pct > 0 &&
        state->tp_cpu_util_pct >= gs_amm_tp_cpu_pressure_threshold_pct;
    state->tp_cpu_guarded = cpu_guarded;
    recovery_metrics_stable = !miss_guarded && !hit_guarded && !tps_guarded;
    /* Keep TP protection latched through transient cold samples.  Releasing
     * the latch requires several consecutive low-pressure windows, which
     * prevents AP supply from racing a hot TP workload at the one-second tick
     * boundary. */
    previous_hot = state->tp_pressure_hot;
    if (test_mode ? test_worker_surge : cpu_guarded) {
        state->tp_hot_clear_windows = 0;
        state->tp_pressure_hot = true;
        state->tp_pressure_state = GS_AMM_TP_PRESSURE_HOT_RECOVERY;
    } else if (previous_hot) {
        /* Leave hot recovery only after the first non-hot sample.  The
         * recovery-wait state keeps all AP supply paths closed while the
         * low-pressure hysteresis window is accumulated. */
        state->tp_pressure_hot = false;
        state->tp_pressure_state = GS_AMM_TP_PRESSURE_RECOVERY_WAIT;
        if (recovery_metrics_stable)
            state->tp_hot_clear_windows++;
        else
            state->tp_hot_clear_windows = 0;
    } else {
        state->tp_pressure_hot = false;
        if (state->tp_pressure_state == GS_AMM_TP_PRESSURE_RECOVERY_WAIT) {
            if (recovery_metrics_stable)
                state->tp_hot_clear_windows++;
            else
                state->tp_hot_clear_windows = 0;
            if (state->tp_hot_clear_windows >= GS_AMM_TP_HOT_CLEAR_WINDOWS &&
                state->ap_downgrade_pending == 0) {
                state->tp_hot_clear_windows = 0;
                state->tp_pressure_state = GS_AMM_TP_PRESSURE_LOW_FLOW;
            }
        } else if (state->tp_pressure_state == GS_AMM_TP_PRESSURE_UNKNOWN) {
            state->tp_hot_clear_windows = 0;
            state->tp_pressure_state = GS_AMM_TP_PRESSURE_LOW_FLOW;
        } else {
            state->tp_hot_clear_windows = 0;
            state->tp_pressure_state = GS_AMM_TP_PRESSURE_LOW_FLOW;
        }
    }
    hot = state->tp_pressure_hot;
    if (hit_guarded)
        gs_amm_set_backpressure_reason_locked(state, "buffer_hit_guard");
    else if (tps_guarded)
        gs_amm_set_backpressure_reason_locked(state, "tps_guard");
    low_pressure_threshold = Max(gs_amm_tp_buffer_miss_threshold_pct / 2, 1);
    if (gs_amm_tp_buffer_miss_threshold_pct <= 0) {
        state->tp_low_pressure_windows = 0;
    } else if (state->tp_pressure_state != GS_AMM_TP_PRESSURE_LOW_FLOW) {
        state->tp_low_pressure_windows = 0;
    } else if (pressure_pct < low_pressure_threshold) {
        state->tp_low_pressure_windows++;
    } else {
        state->tp_low_pressure_windows = 0;
    }
    SpinLockRelease(&state->mutex);
    if (!hot) {
        SpinLockAcquire(&state->mutex);
        bool waiting_for_ap = (state->tp_recovery_phase == GS_AMM_TP_RECOVERY_WAIT_AP ||
            state->tp_recovery_phase == GS_AMM_TP_RECOVERY_RESTORE_SB) &&
            state->active_ap_count == 0;
        SpinLockRelease(&state->mutex);
        if (waiting_for_ap)
            gs_amm_restore_shared_buffer_after_ap_stop(state);
    }
    if (!hot && state->tp_pressure_state == GS_AMM_TP_PRESSURE_LOW_FLOW &&
        (state->tp_recovery_phase == GS_AMM_TP_RECOVERY_IDLE ||
         state->tp_recovery_phase == GS_AMM_TP_RECOVERY_MULTIPASS)) {
        gs_amm_supply_ap_demand();
        gs_amm_process_ap_queue();
    }
    if (hot) {
        /* TP owns the memory during a hot window.  Cancel every active AP
         * query first, then wait for normal grant cleanup before resizing SB. */
        SpinLockAcquire(&state->mutex);
        state->tp_recovery_phase = GS_AMM_TP_RECOVERY_STOP_AP;
        (void)snprintf_s(state->last_supply_source, sizeof(state->last_supply_source),
            sizeof(state->last_supply_source) - 1, "%s", "tp_recovery_stop_ap");
        gs_amm_request_ap_stop_locked(state);
        state->last_action = GS_AMM_TP_DOWNGRADE_PENDING;
        bool ap_stopped = state->active_ap_count == 0;
        SpinLockRelease(&state->mutex);
        if (ap_stopped)
            gs_amm_restore_shared_buffer_after_ap_stop(state);
        return;
    }

    /*
     * Cold pressure may return only buffer granules that were added above the
     * startup baseline.  In particular, an initially idle TP workload must
     * not cause the baseline itself to be drained.  Limit the operation to
     * one granule per control window and require a few consecutive cold
     * samples so that a short lull cannot resize the pool.
     */
    {
        bool can_drain_baseline = false;
        int baseline_blocks = 0;

        SpinLockAcquire(&state->mutex);
        if (state->tp_pressure_valid && state->tp_pressure_state == GS_AMM_TP_PRESSURE_LOW_FLOW &&
            state->tp_low_pressure_windows >= GS_AMM_TP_LOW_WINDOWS_BEFORE_DRAIN &&
            state->ap_downgrade_pending == 0) {
            baseline_blocks = gs_amm_mb_to_blocks(state->baseline_active_mb);
            can_drain_baseline = baseline_blocks > 0;
        }
        SpinLockRelease(&state->mutex);

        if (can_drain_baseline) {
            int active_blocks = gs_amm_current_active_buffer_blocks(state);

            if (active_blocks > baseline_blocks) {
                GsAmmResizeOutcome outcome;
                int target_blocks = Max(active_blocks - state->granule_blocks, baseline_blocks);

                gs_amm_resize_core(gs_amm_align_resize_target_blocks(
                    target_blocks, NORMAL_SHARED_BUFFER_NUM), &outcome);
                if (outcome.decision != NULL && strcmp(outcome.decision, "shrunk") == 0) {
                    SpinLockAcquire(&state->mutex);
                    state->last_action = GS_AMM_TP_BUFFER_BASELINE_DRAIN;
                    SpinLockRelease(&state->mutex);
                }
            }
        }
    }
}

Datum gs_amm_begin_ap(PG_FUNCTION_ARGS)
{
    int prediction_mb = Max(PG_GETARG_INT32(0), 1);
    int min_mb = Max(PG_GETARG_INT32(1), 1);
    int max_mb = Max(PG_GETARG_INT32(2), min_mb);
    int queue_timeout_ms = PG_GETARG_INT32(3);
    GsAmmAdmissionResult result;
    char status[1024];

    if (!GsAmmAdmitBounds(max_mb * 1024, prediction_mb * 1024, min_mb * 1024,
        queue_timeout_ms, prediction_mb, &result))
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("GS AMM admission result is unavailable")));
    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "admitted=%s queued=%s backpressure=%s granted_mb=%d prediction_mb=%d grant_id=%llu grant_generation=%llu",
        result.admitted ? "true" : "false", result.queued ? "true" : "false",
        result.backpressure ? "true" : "false", result.granted_mb, prediction_mb,
        (unsigned long long)result.grant_id, (unsigned long long)result.grant_generation);
    securec_check_ss(rc, "\0", "\0");
    PG_RETURN_TEXT_P(cstring_to_text(status));
}

bool GsAmmAdmitBounds(int cache_bound_kb, int one_pass_bound_kb, int multi_pass_bound_kb,
    int queue_timeout_ms, int prediction_mb, GsAmmAdmissionResult *result)
{
    return GsAmmAdmitBoundsWithTarget(cache_bound_kb, one_pass_bound_kb, multi_pass_bound_kb,
        cache_bound_kb, queue_timeout_ms, prediction_mb, result);
}

bool GsAmmAdmitBoundsWithTarget(int cache_bound_kb, int one_pass_bound_kb, int multi_pass_bound_kb,
    int admission_target_kb, int queue_timeout_ms, int prediction_mb, GsAmmAdmissionResult *result)
{
    GsAmmSharedState *state;
    GsAmmApQueueSlot granted_slot;
    GsAmmGrantToken direct_token = {0, 0};
    uint64 ticket = 0;
    TimestampTz queued_at;
    bool claimed = false;
    bool directly_admitted = false;
    GsAmmMemoryMode direct_memory_mode = GS_AMM_MEMORY_MODE_ONEPASS;
    int direct_granted_kb = 0;
    int direct_grant_granules = 0;
    long wait_secs = 0;
    int wait_usecs = 0;
    int wait_ms = 0;
    errno_t rc;

    if (!gs_amm_enabled || gs_amm_workload_role != GS_AMM_WORKLOAD_AP || result == NULL)
        return false;
    (void)queue_timeout_ms;
    rc = memset_s(result, sizeof(*result), 0, sizeof(*result));
    securec_check(rc, "\0", "\0");
    cache_bound_kb = Max(cache_bound_kb, 1);
    one_pass_bound_kb = Max(one_pass_bound_kb, 1);
    multi_pass_bound_kb = Max(multi_pass_bound_kb, 1);
    if (one_pass_bound_kb > cache_bound_kb)
        one_pass_bound_kb = cache_bound_kb;
    if (multi_pass_bound_kb > one_pass_bound_kb)
        multi_pass_bound_kb = one_pass_bound_kb;
    admission_target_kb = Max(admission_target_kb, 1);
    if (admission_target_kb > cache_bound_kb)
        admission_target_kb = cache_bound_kb;
    if (admission_target_kb < multi_pass_bound_kb)
        admission_target_kb = multi_pass_bound_kb;
    prediction_mb = prediction_mb > 0 ? prediction_mb : Max((cache_bound_kb + 1023) / 1024, 1);
    result->prediction_mb = prediction_mb;
    result->cache_bound_kb = cache_bound_kb;
    result->one_pass_bound_kb = one_pass_bound_kb;
    result->multi_pass_bound_kb = multi_pass_bound_kb;
    result->admission_request_kb = admission_target_kb;
    result->backpressure = true;
    result->memory_mode = GS_AMM_MEMORY_MODE_ONEPASS;
    gs_amm_copy_admission_reason(result->reason, "queued");

    if (gs_amm_resolve_buffer_blocks() == NULL) {
        gs_amm_copy_admission_reason(result->reason, "buffer_mapping");
        return true;
    }
    if (MyGsAmmGrantId != 0 || MyGsAmmGrantGeneration != 0) {
        gs_amm_copy_admission_reason(result->reason, "grant_already_active");
        return true;
    }

    /* Install cleanup before publishing the request.  This closes the small
     * exit race where a backend could die after enqueue and before its ticket
     * became visible to the proc-exit callback. */
    gs_amm_register_backend_cleanup();
    state = gs_amm_get_state();
    SpinLockAcquire(&state->mutex);
    directly_admitted = gs_amm_try_admit_ap_locked(state, cache_bound_kb, one_pass_bound_kb,
        multi_pass_bound_kb, admission_target_kb, prediction_mb, &direct_token, &direct_granted_kb,
        &direct_grant_granules);
    direct_memory_mode = state->ap_multipass_only ?
        GS_AMM_MEMORY_MODE_MULTIPASS : GS_AMM_MEMORY_MODE_CACHE;
    SpinLockRelease(&state->mutex);

    if (directly_admitted) {
        MyGsAmmGrantId = direct_token.grant_id;
        MyGsAmmGrantGeneration = direct_token.grant_generation;
        MyGsAmmGrantGranules = direct_grant_granules;
        MyGsAmmGrantNative = false;
        PG_TRY();
        {
            gs_amm_apply_backend_grant_kb(direct_granted_kb);
        }
        PG_CATCH();
        {
            (void)GsAmmReleaseGrantToken(direct_token);
            PG_RE_THROW();
        }
        PG_END_TRY();
        result->admitted = true;
        result->queued = false;
        result->backpressure = false;
        result->grant_id = direct_token.grant_id;
        result->grant_generation = direct_token.grant_generation;
        result->granted_kb = direct_granted_kb;
        result->granted_mb = (direct_granted_kb + 1023) / 1024;
        result->grant_granules = direct_grant_granules;
        result->memory_mode = direct_memory_mode;
        gs_amm_copy_admission_reason(result->reason, "admitted");
        return true;
    }

    /*
     * The request cannot be served from dynamic/free inventory.  Try one
     * cold-state buffer borrow before it becomes a waiter.  This preserves
     * the supply order and avoids queueing a request merely because the
     * controller has not reached its next one-second tick yet.
     */
    (void)gs_amm_prepare_dynamic_capacity(
        Max((admission_target_kb + 1023) / 1024, gs_amm_ap_min_grant_mb));

    SpinLockAcquire(&state->mutex);
    directly_admitted = gs_amm_try_admit_ap_locked(state, cache_bound_kb, one_pass_bound_kb,
        multi_pass_bound_kb, admission_target_kb, prediction_mb, &direct_token, &direct_granted_kb,
        &direct_grant_granules);
    direct_memory_mode = state->ap_multipass_only ?
        GS_AMM_MEMORY_MODE_MULTIPASS : GS_AMM_MEMORY_MODE_CACHE;
    if (!directly_admitted && !gs_amm_enqueue_ap_request_locked(state, cache_bound_kb, one_pass_bound_kb,
        multi_pass_bound_kb, admission_target_kb, prediction_mb, &ticket)) {
        state->backpressure_count++;
        gs_amm_set_backpressure_reason_locked(state, "queue_full");
        gs_amm_copy_admission_reason(result->reason, "queue_full");
        SpinLockRelease(&state->mutex);
        return true;
    }
    SpinLockRelease(&state->mutex);

    if (directly_admitted) {
        MyGsAmmGrantId = direct_token.grant_id;
        MyGsAmmGrantGeneration = direct_token.grant_generation;
        MyGsAmmGrantGranules = direct_grant_granules;
        MyGsAmmGrantNative = false;
        PG_TRY();
        {
            gs_amm_apply_backend_grant_kb(direct_granted_kb);
        }
        PG_CATCH();
        {
            (void)GsAmmReleaseGrantToken(direct_token);
            PG_RE_THROW();
        }
        PG_END_TRY();
        result->admitted = true;
        result->queued = false;
        result->backpressure = false;
        result->grant_id = direct_token.grant_id;
        result->grant_generation = direct_token.grant_generation;
        result->granted_kb = direct_granted_kb;
        result->granted_mb = (direct_granted_kb + 1023) / 1024;
        result->grant_granules = direct_grant_granules;
        result->memory_mode = direct_memory_mode;
        gs_amm_copy_admission_reason(result->reason, "admitted");
        return true;
    }

    SpinLockAcquire(&state->mutex);
    GsAmmApQueueSlot *slot = gs_amm_find_queue_slot_locked(state, ticket);
    if (slot == NULL) {
        SpinLockRelease(&state->mutex);
        gs_amm_copy_admission_reason(result->reason, "queue_cancelled");
        return true;
    }
    queued_at = slot->queued_at;
    MyGsAmmQueueTicket = ticket;
    SpinLockRelease(&state->mutex);

    PG_TRY();
    {
        for (;;) {
            GsAmmApQueueSlot snapshot;
            ResetLatch(&t_thrd.proc->procLatch);
            SpinLockAcquire(&state->mutex);
            slot = gs_amm_find_queue_slot_locked(state, ticket);
            if (slot == NULL || !gs_amm_queue_slot_belongs_to_current_backend(slot)) {
                SpinLockRelease(&state->mutex);
                break;
            }
            if (slot->state == GS_AMM_AP_QUEUE_GRANTED) {
                snapshot = *slot;
                (void)memset_s(slot, sizeof(*slot), 0, sizeof(*slot));
                gs_amm_refresh_ap_queue_totals_locked(state);
                gs_amm_refresh_ap_target_totals_locked(state);
                SpinLockRelease(&state->mutex);
                granted_slot = snapshot;
                claimed = true;
                break;
            }
            SpinLockRelease(&state->mutex);
            (void)WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1L);
            CHECK_FOR_INTERRUPTS();
        }
    }
    PG_CATCH();
    {
        gs_amm_cancel_waiting_request();
        PG_RE_THROW();
    }
    PG_END_TRY();

    MyGsAmmQueueTicket = 0;
    if (!claimed) {
        result->backpressure = true;
        gs_amm_copy_admission_reason(result->reason, "queue_cancelled");
        return true;
    }
    MyGsAmmGrantId = granted_slot.grant_token.grant_id;
    MyGsAmmGrantGeneration = granted_slot.grant_token.grant_generation;
    MyGsAmmGrantGranules = granted_slot.grant_granules;
    MyGsAmmGrantNative = false;
    PG_TRY();
    {
        gs_amm_apply_backend_grant_kb(granted_slot.granted_kb);
    }
    PG_CATCH();
    {
        (void)GsAmmReleaseGrantToken(granted_slot.grant_token);
        PG_RE_THROW();
    }
    PG_END_TRY();
    TimestampDifference(queued_at, GetCurrentTimestamp(), &wait_secs, &wait_usecs);
    wait_ms = wait_secs > INT_MAX / 1000 ? INT_MAX : (int)(wait_secs * 1000 + wait_usecs / 1000);
    result->admitted = true;
    result->queued = true;
    result->backpressure = false;
    result->grant_id = granted_slot.grant_token.grant_id;
    result->grant_generation = granted_slot.grant_token.grant_generation;
    result->granted_kb = granted_slot.granted_kb;
    result->granted_mb = (granted_slot.granted_kb + 1023) / 1024;
    result->queue_wait_ms = wait_ms;
    result->queue_ticket = ticket;
    result->grant_granules = granted_slot.grant_granules;
    result->memory_mode = granted_slot.memory_mode;
    gs_amm_copy_admission_reason(result->reason, "admitted");
    return true;
}

Datum gs_amm_begin_ap_bounds(PG_FUNCTION_ARGS)
{
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
        "admission_request_kb=%d queue_wait_ms=%d queue_ticket=%llu grant_id=%llu grant_generation=%llu grant_granules=%d "
        "reason=%s",
        result.admitted ? "true" : "false", result.queued ? "true" : "false",
        result.backpressure ? "true" : "false", gs_amm_memory_mode_name(result.memory_mode),
        result.granted_kb, result.granted_mb, result.prediction_mb, result.cache_bound_kb,
        result.one_pass_bound_kb, result.multi_pass_bound_kb, result.admission_request_kb,
        result.queue_wait_ms, (unsigned long long)result.queue_ticket, (unsigned long long)result.grant_id,
        (unsigned long long)result.grant_generation, result.grant_granules, result.reason);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum gs_amm_end_ap(PG_FUNCTION_ARGS)
{
    GsAmmSharedState *state = gs_amm_get_state();
    int released_mb = (int)((GsAmmCurrentBackendGrantPoolBytes() + 1024 * 1024 - 1) / (1024 * 1024));
    GsAmmGrantToken expected_token = {
        GsAmmCurrentBackendGrantId(), GsAmmCurrentBackendGrantGeneration()};
    bool released;
    int dynamic_used_mb;
    int active_ap_count;
    char status[512];

    released = GsAmmReleaseGrantToken(expected_token);

    SpinLockAcquire(&state->mutex);
    dynamic_used_mb = state->dynamic_used_mb;
    active_ap_count = state->active_ap_count;
    SpinLockRelease(&state->mutex);

    int rc = snprintf_s(status, sizeof(status), sizeof(status) - 1,
        "released=%s released_mb=%d grant_generation=%llu dynamic_used_mb=%d active_ap_count=%d",
        released ? "true" : "false", released_mb,
        (unsigned long long)expected_token.grant_generation, dynamic_used_mb, active_ap_count);
    securec_check_ss(rc, "\0", "\0");

    PG_RETURN_TEXT_P(cstring_to_text(status));
}
