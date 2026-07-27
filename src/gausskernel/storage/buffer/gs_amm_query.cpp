/* -------------------------------------------------------------------------
 *
 * gs_amm_query.cpp
 *      Native AMM query feature extraction and decision-tree inference.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#include "access/heapam.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "executor/exec/execdesc.h"
#include "executor/executor.h"
#include "knl/knl_thread.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "optimizer/planmem_walker.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "storage/gs_amm.h"
#include "storage/smgr/fd.h"
#include "utils/lsyscache.h"
#include "utils/guc.h"
#include "utils/memprot.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"
#include "utils/workmem_dtree_model.h"

extern uint64 pg_relation_perm_table_size(Relation rel);
extern uint64 pg_relation_table_size(Relation rel);

typedef struct GsAmmWorkMemFeatureContext {
    MethodPlanWalkerContext base;
    MemTuneWorkMemFeatures features;
    double max_plan_rows;
    int max_plan_dop;
    bool has_memory_intensive_node;
} GsAmmWorkMemFeatureContext;

#define GS_AMM_NATIVE_EXECUTOR_STACK_DEPTH 64

typedef struct GsAmmNativeExecutorFrame {
    QueryDesc *query_desc;
    SubTransactionId subxid;
} GsAmmNativeExecutorFrame;

typedef struct GsAmmNativeQueryState {
    knl_session_context *owner_session;
    uint64 owner_session_id;
    QueryDesc *owner_query_desc;
    bool active;
    bool fallback;
    bool feedback_only;
    bool xact_callback_registered;
    int executor_depth;
    GsAmmNativeExecutorFrame executor_frames[GS_AMM_NATIVE_EXECUTOR_STACK_DEPTH];
    SubTransactionId owner_subxid;
    uint64 lifecycle_generation;
    int64 model_version;
    int64 leaf_id;
    double raw_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    double calibrated_bounds_kb[GS_AMM_DTREE_BOUND_COUNT];
    int64 calibration_version;
    double calibration_scale;
    uint64 grant_id;
    uint64 grant_generation;
    int selected_grant_kb;
    int selected_grant_mode;
    int saved_work_mem_kb;
    int fallback_guc_nest_level;
    TimestampTz start_timestamp;
    uint64 start_spill_bytes;
    uint32 start_spill_count;
    uint64 start_guard_count;
} GsAmmNativeQueryState;

typedef struct GsAmmQueryFeedbackAccumulator {
    uint64 lifecycle_generation;
    uint64 operator_peak_bytes;
    uint64 operator_spill_bytes;
    uint64 temp_spill_bytes;
    uint32 operator_spill_events;
    uint32 temp_spill_files;
    int hash_nbatch;
    int hash_multipass_count;
    bool operator_reported;
} GsAmmQueryFeedbackAccumulator;

#define GS_AMM_NATIVE_GRANT_MODE_NONE 0

static THR_LOCAL GsAmmNativeQueryState gs_amm_native_query_state;
static THR_LOCAL GsAmmQueryFeedbackAccumulator gs_amm_query_feedback;

static void gs_amm_clear_native_query_state(void);
static void gs_amm_clear_executor_frames(void);
static void gs_amm_restore_native_work_mem(GsAmmNativeQueryState *state);
static void gs_amm_begin_query_feedback(uint64 lifecycle_generation);
static void gs_amm_clear_query_feedback(uint64 lifecycle_generation);

static bool gs_amm_feedback_operator_walker(Node *node, bool *eligible)
{
    if (node == NULL || eligible == NULL)
        return false;

    switch (nodeTag(node)) {
        case T_HashJoin:
        case T_VecHashJoin:
        case T_Sort:
        case T_VecSort:
        case T_Agg:
        case T_VecAgg:
        case T_WindowAgg:
        case T_VecWindowAgg:
            *eligible = true;
            return true;
        default:
            return plan_tree_walker(node, (MethodWalker)gs_amm_feedback_operator_walker, (void *)eligible);
    }
}

static bool gs_amm_plan_has_feedback_operator(Plan *plan)
{
    bool eligible = false;

    if (plan != NULL)
        (void)gs_amm_feedback_operator_walker((Node *)plan, &eligible);
    return eligible;
}

static uint64 gs_amm_current_session_spill_bytes(void)
{
    int64 spill_bytes = pgstat_get_session_spill_size();

    return spill_bytes > 0 ? (uint64)spill_bytes : 0;
}

static uint32 gs_amm_current_session_spill_count(void)
{
    int spill_count = pgstat_get_session_spill_count();

    return spill_count > 0 ? (uint32)spill_count : 0;
}

static void gs_amm_begin_query_feedback(uint64 lifecycle_generation)
{
    errno_t rc = memset_s(&gs_amm_query_feedback, sizeof(gs_amm_query_feedback), 0,
        sizeof(gs_amm_query_feedback));

    securec_check(rc, "\0", "\0");
    gs_amm_query_feedback.lifecycle_generation = lifecycle_generation;
}

static uint64 gs_amm_saturating_add_u64(uint64 current, uint64 increment)
{
    const uint64 maximum = ~(uint64)0;

    return increment > maximum - current ? maximum : current + increment;
}

static uint32 gs_amm_saturating_add_u32(uint32 current, uint32 increment)
{
    const uint32 maximum = ~(uint32)0;

    return increment > maximum - current ? maximum : current + increment;
}

static void gs_amm_clear_query_feedback(uint64 lifecycle_generation)
{
    errno_t rc;

    if (lifecycle_generation == 0 || gs_amm_query_feedback.lifecycle_generation != lifecycle_generation)
        return;
    rc = memset_s(&gs_amm_query_feedback, sizeof(gs_amm_query_feedback), 0,
        sizeof(gs_amm_query_feedback));
    securec_check(rc, "\0", "\0");
}

static bool gs_amm_query_feedback_is_current(uint64 lifecycle_generation)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;

    return lifecycle_generation != 0 && state->active && !state->fallback &&
        state->lifecycle_generation == lifecycle_generation &&
        gs_amm_query_feedback.lifecycle_generation == lifecycle_generation;
}

uint64 GsAmmCurrentQueryLifecycleGeneration(void)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;

    if (!gs_amm_enabled || !state->active || state->fallback)
        return 0;
    return state->lifecycle_generation;
}

void GsAmmReportOperatorPeak(uint64 lifecycle_generation, uint64 bytes)
{
    if (!gs_amm_query_feedback_is_current(lifecycle_generation))
        return;
    gs_amm_query_feedback.operator_reported = true;
    gs_amm_query_feedback.operator_peak_bytes = Max(gs_amm_query_feedback.operator_peak_bytes, bytes);
}

void GsAmmReportHashBatches(uint64 lifecycle_generation, int nbatch, int multipass_count)
{
    if (!gs_amm_query_feedback_is_current(lifecycle_generation))
        return;
    gs_amm_query_feedback.operator_reported = true;
    gs_amm_query_feedback.hash_nbatch = Max(gs_amm_query_feedback.hash_nbatch, Max(nbatch, 0));
    gs_amm_query_feedback.hash_multipass_count =
        Max(gs_amm_query_feedback.hash_multipass_count, Max(multipass_count, 0));
}

void GsAmmReportQuerySpill(uint64 lifecycle_generation, uint64 bytes, uint32 events)
{
    if (!gs_amm_query_feedback_is_current(lifecycle_generation))
        return;
    gs_amm_query_feedback.operator_reported = true;
    gs_amm_query_feedback.operator_spill_bytes =
        gs_amm_saturating_add_u64(gs_amm_query_feedback.operator_spill_bytes, bytes);
    gs_amm_query_feedback.operator_spill_events =
        gs_amm_saturating_add_u32(gs_amm_query_feedback.operator_spill_events, events);
}

void GsAmmReportTempFileIO(uint64 lifecycle_generation, uint64 bytes, uint32 files)
{
    if (!gs_amm_query_feedback_is_current(lifecycle_generation))
        return;
    gs_amm_query_feedback.temp_spill_bytes =
        gs_amm_saturating_add_u64(gs_amm_query_feedback.temp_spill_bytes, bytes);
    gs_amm_query_feedback.temp_spill_files =
        gs_amm_saturating_add_u32(gs_amm_query_feedback.temp_spill_files, files);
}

static bool gs_amm_snapshot_query_feedback(
    uint64 lifecycle_generation, GsAmmQueryFeedbackAccumulator *snapshot)
{
    if (snapshot == NULL || !gs_amm_query_feedback_is_current(lifecycle_generation))
        return false;

    *snapshot = gs_amm_query_feedback;
    return snapshot->operator_reported;
}

static void gs_amm_push_executor_frame(GsAmmNativeQueryState *state, QueryDesc *query_desc)
{
    GsAmmNativeExecutorFrame *frame;

    if (state->executor_depth >= GS_AMM_NATIVE_EXECUTOR_STACK_DEPTH)
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("native GS AMM executor nesting exceeds %d frames", GS_AMM_NATIVE_EXECUTOR_STACK_DEPTH)));

    frame = &state->executor_frames[state->executor_depth];
    frame->query_desc = query_desc;
    frame->subxid = GetCurrentSubTransactionId();
    state->executor_depth++;
}

static void gs_amm_remove_subxact_executor_frames(GsAmmNativeQueryState *state, SubTransactionId subxid)
{
    int retained = 0;

    for (int index = 0; index < state->executor_depth; index++) {
        GsAmmNativeExecutorFrame *frame = &state->executor_frames[index];

        if (frame->subxid != subxid) {
            if (retained != index)
                state->executor_frames[retained] = *frame;
            retained++;
        }
    }
    for (int index = retained; index < state->executor_depth; index++) {
        state->executor_frames[index].query_desc = NULL;
        state->executor_frames[index].subxid = InvalidSubTransactionId;
    }
    state->executor_depth = retained;
}

static bool gs_amm_pop_executor_frame(GsAmmNativeQueryState *state, QueryDesc *query_desc)
{
    int index;

    for (index = state->executor_depth - 1; index >= 0; index--) {
        if (state->executor_frames[index].query_desc == query_desc)
            break;
    }
    if (index < 0)
        return false;

    for (int next = index + 1; next < state->executor_depth; next++)
        state->executor_frames[next - 1] = state->executor_frames[next];
    state->executor_depth--;
    state->executor_frames[state->executor_depth].query_desc = NULL;
    state->executor_frames[state->executor_depth].subxid = InvalidSubTransactionId;
    return true;
}

static void gs_amm_clear_executor_frames(void)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;
    errno_t rc = memset_s(state->executor_frames, sizeof(state->executor_frames), 0,
        sizeof(state->executor_frames));

    securec_check(rc, "\0", "\0");
    state->executor_depth = 0;
}

static void gs_amm_bind_native_query_session(void)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;

    if (state->owner_session == u_sess && state->owner_session_id == u_sess->session_id)
        return;
    if (state->active)
        ereport(FATAL, (errmsg("native GS AMM state crossed session ownership")));
    gs_amm_clear_native_query_state();
    gs_amm_clear_executor_frames();
    state->xact_callback_registered = false;
    state->owner_session = u_sess;
    state->owner_session_id = u_sess->session_id;
}

static void gs_amm_restore_native_work_mem(GsAmmNativeQueryState *state)
{
    int nest_level;

    if (state == NULL || !state->fallback || state->fallback_guc_nest_level <= 0)
        return;

    nest_level = state->fallback_guc_nest_level;
    state->fallback_guc_nest_level = 0;
    AtEOXact_GUC(true, nest_level);
}

static void gs_amm_finish_native_query(bool error, bool error_cleanup, bool restore_work_mem)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;
    GsAmmFeedbackRecord feedback;
    GsAmmQueryFeedbackAccumulator feedback_snapshot;
    GsAmmGrantToken expected_token;
    uint64 end_spill_bytes;
    uint64 session_spill_bytes;
    uint64 spill_bytes;
    uint32 end_spill_count;
    uint32 session_spill_count;
    uint32 spill_files;
    uint32 spill_events;
    TimestampTz now;
    errno_t rc;

    if (!state->active)
        return;

    if (state->fallback) {
        gs_amm_restore_native_work_mem(state);
        if (error_cleanup)
            GsAmmRecordNativeErrorCleanup();
        gs_amm_clear_native_query_state();
        return;
    }

    if (state->grant_id == 0 || state->grant_generation == 0)
        return;

    rc = memset_s(&feedback_snapshot, sizeof(feedback_snapshot), 0, sizeof(feedback_snapshot));
    securec_check(rc, "\0", "\0");
    (void)gs_amm_snapshot_query_feedback(state->lifecycle_generation, &feedback_snapshot);

    rc = memset_s(&feedback, sizeof(feedback), 0, sizeof(feedback));
    securec_check(rc, "\0", "\0");
    feedback.session_id = u_sess->session_id;
    feedback.model_version = state->model_version;
    feedback.leaf_id = state->leaf_id;
    for (int index = 0; index < GS_AMM_DTREE_BOUND_COUNT; index++) {
        feedback.raw_bounds_kb[index] = state->raw_bounds_kb[index];
        feedback.calibrated_bounds_kb[index] = state->calibrated_bounds_kb[index];
    }
    now = GetCurrentTimestamp();
    feedback.observed_work_mem_kb = (double)feedback_snapshot.operator_peak_bytes / 1024.0;
    feedback.runtime_ms = state->start_timestamp > 0 && now > state->start_timestamp ?
        (double)(now - state->start_timestamp) / 1000.0 : 0.0;
    end_spill_bytes = gs_amm_current_session_spill_bytes();
    session_spill_bytes = end_spill_bytes >= state->start_spill_bytes ?
        end_spill_bytes - state->start_spill_bytes : end_spill_bytes;
    spill_bytes = Max(Max(feedback_snapshot.operator_spill_bytes, feedback_snapshot.temp_spill_bytes),
        session_spill_bytes);
    end_spill_count = gs_amm_current_session_spill_count();
    session_spill_count = end_spill_count >= state->start_spill_count ?
        end_spill_count - state->start_spill_count : end_spill_count;
    spill_files = feedback_snapshot.temp_spill_files;
    spill_events = Max(feedback_snapshot.operator_spill_events, session_spill_count);
    feedback.spill_mb = (double)spill_bytes / (1024.0 * 1024.0);
    feedback.spill_bytes = spill_bytes;
    feedback.spill_files = spill_files;
    feedback.spill_events = spill_events;
    feedback.hash_nbatch = feedback_snapshot.hash_nbatch;
    feedback.hash_multipass_count = feedback_snapshot.hash_multipass_count;
    feedback.grant_mb = (double)state->selected_grant_kb / 1024.0;
    GsAmmGetFeedbackTelemetry(&feedback.tp_drop_ratio, &feedback.io_pressure);
    feedback.backpressure = false;
    feedback.error = error;
    feedback.measurement_valid = feedback_snapshot.operator_reported;
    feedback.feedback_only = state->feedback_only;

    expected_token.grant_id = state->grant_id;
    expected_token.grant_generation = state->grant_generation;
    /* Revoke query ownership before work_mem restoration can raise an error. */
    gs_amm_clear_native_query_state();
    GsAmmRecordApExecutionSignals(
        feedback.spill_bytes, feedback.spill_events, feedback.hash_multipass_count);
    GsAmmRecordFeedback(&feedback);
    if (error_cleanup)
        GsAmmRecordNativeErrorCleanup();
    (void)GsAmmReleaseGrantToken(expected_token, restore_work_mem);
}

static void gs_amm_finish_native_query_noexcept(bool error, bool restore_work_mem)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;
    GsAmmGrantToken expected_token = {state->grant_id, state->grant_generation};

    PG_TRY();
    {
        gs_amm_finish_native_query(error, true, restore_work_mem);
    }
    PG_CATCH();
    {
        FlushErrorState();
        PG_TRY();
        {
            (void)GsAmmReleaseGrantToken(expected_token, false);
        }
        PG_CATCH();
        {
            FlushErrorState();
        }
        PG_END_TRY();
    }
    PG_END_TRY();

    gs_amm_clear_native_query_state();
    gs_amm_clear_executor_frames();
}

static void gs_amm_xact_callback(XactEvent event, void *arg)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;

    (void)arg;
    if (event == XACT_EVENT_ABORT || event == XACT_EVENT_COMMIT) {
        if (state->active)
            gs_amm_finish_native_query_noexcept(event == XACT_EVENT_ABORT, true);
        gs_amm_clear_native_query_state();
        gs_amm_clear_executor_frames();
    }
}

static void gs_amm_subxact_callback(
    SubXactEvent event, SubTransactionId my_subid, SubTransactionId parent_subid, void *arg)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;
    bool owner_aborted;

    (void)parent_subid;
    (void)arg;
    if (event == SUBXACT_EVENT_ABORT_SUB && state->owner_session == u_sess) {
        owner_aborted = state->active && state->owner_subxid == my_subid;
        gs_amm_remove_subxact_executor_frames(state, my_subid);
        if (owner_aborted) {
            gs_amm_finish_native_query_noexcept(true, true);
        } else if (!state->active && state->executor_depth == 0) {
            gs_amm_clear_native_query_state();
        }
    }
}

static void gs_amm_register_xact_callback(void)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;

    if (!state->xact_callback_registered) {
        RegisterXactCallback(gs_amm_xact_callback, NULL);
        RegisterSubXactCallback(gs_amm_subxact_callback, NULL);
        state->xact_callback_registered = true;
    }
}

static bool IsPlanNode(Node *node)
{
    NodeTag tag;

    if (node == NULL)
        return false;

    tag = nodeTag(node);
    return ((tag >= T_BaseResult && tag <= T_Stream) || tag == T_RemoteQuery ||
        (tag >= T_VecResult && tag <= T_VecRemoteQuery));
}

static double gs_amm_nonnegative_finite(double value)
{
    if (!isfinite(value) || value <= 0.0)
        return 0.0;
    return value;
}

static bool gs_amm_workmem_feature_walker(Node *node, GsAmmWorkMemFeatureContext *ctx)
{
    MemTuneWorkMemFeatures &features = ctx->features;

    if (node == NULL)
        return false;

    if (IsPlanNode(node)) {
        Plan *plan = (Plan *)node;

        switch (nodeTag(node)) {
            case T_HashJoin:
            case T_VecHashJoin:
                features.hash_join_nodes += 1.0;
                ctx->has_memory_intensive_node = true;
                break;
            case T_Sort:
            case T_VecSort:
                features.sort_nodes += 1.0;
                ctx->has_memory_intensive_node = true;
                break;
            case T_Agg:
            case T_VecAgg:
                features.aggregate_nodes += 1.0;
                ctx->has_memory_intensive_node = true;
                break;
            case T_WindowAgg:
            case T_VecWindowAgg:
                features.window_nodes += 1.0;
                ctx->has_memory_intensive_node = true;
                break;
            default:
                break;
        }

        if (isfinite(plan->plan_rows) && plan->plan_rows >= 0.0)
            ctx->max_plan_rows = Max(ctx->max_plan_rows, plan->plan_rows);
        features.max_plan_width = Max(features.max_plan_width,
            (double)Max(plan->plan_width, 0));
        ctx->max_plan_dop = Max(ctx->max_plan_dop, Max(plan->dop, 0));
        if (plan->parallel_enabled)
            features.parallel_aware_nodes += 1.0;
    }

    return plan_tree_walker(node, (MethodWalker)gs_amm_workmem_feature_walker, (void *)ctx);
}

static void gs_amm_collect_one_relation(Oid relation_id, double *table_size_mb, double *index_size_mb,
    double *average_width_sum, int *average_width_count)
{
    MemoryContext caller_context = CurrentMemoryContext;
    MemoryContext temporary_context = AllocSetContextCreate(caller_context,
        "native AMM relation feature context", ALLOCSET_DEFAULT_SIZES);
    Relation relation;
    double local_table_size_mb = 0.0;
    double local_index_size_mb = 0.0;
    double local_column_width_sum = 0.0;
    int local_column_width_count = 0;

    (void)MemoryContextSwitchTo(temporary_context);
    relation = try_relation_open(relation_id, AccessShareLock);
    if (relation != NULL) {
        TupleDesc tuple_desc = RelationGetDescr(relation);
        uint64 total_size = pg_relation_perm_table_size(relation);
        uint64 table_size = pg_relation_table_size(relation);

        local_table_size_mb = (double)total_size / (1024.0 * 1024.0);
        if (total_size >= table_size)
            local_index_size_mb = (double)(total_size - table_size) / (1024.0 * 1024.0);

        for (int attr_index = 0; attr_index < tuple_desc->natts; attr_index++) {
            Form_pg_attribute attribute = TupleDescAttr(tuple_desc, attr_index);
            int32 average_width;

            if (attribute->attisdropped || attribute->attnum <= 0)
                continue;
            average_width = get_attavgwidth(relation_id, attribute->attnum, false);
            if (average_width > 0) {
                local_column_width_sum += (double)average_width;
                local_column_width_count++;
            }
        }
        relation_close(relation, AccessShareLock);
    }

    (void)MemoryContextSwitchTo(caller_context);
    MemoryContextDelete(temporary_context);
    *table_size_mb += local_table_size_mb;
    *index_size_mb += local_index_size_mb;
    if (local_column_width_count > 0) {
        *average_width_sum += local_column_width_sum / local_column_width_count;
        (*average_width_count)++;
    }
}

static void gs_amm_collect_relation_features(PlannedStmt *planned_stmt, double *table_size_mb,
    double *index_size_mb, double *average_width_sum, int *average_width_count)
{
    List *seen_relations = NIL;
    ListCell *cell = NULL;

    foreach (cell, planned_stmt->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(cell);

        if (rte == NULL || rte->rtekind != RTE_RELATION || !OidIsValid(rte->relid))
            continue;
        if (list_member_oid(seen_relations, rte->relid))
            continue;
        seen_relations = lappend_oid(seen_relations, rte->relid);
        gs_amm_collect_one_relation(rte->relid, table_size_mb, index_size_mb,
            average_width_sum, average_width_count);
    }

    list_free(seen_relations);
}

static void gs_amm_collect_system_memory(MemTuneWorkMemFeatures &features)
{
    FILE *file = AllocateFile("/proc/meminfo", "r");
    char line[256];
    unsigned long long total_kb = 0;
    unsigned long long available_kb = 0;

    if (file == NULL)
        return;

    while (fgets(line, sizeof(line), file) != NULL) {
        unsigned long long value_kb = 0;

        if (strncmp(line, "MemTotal:", strlen("MemTotal:")) == 0 &&
            sscanf_s(line + strlen("MemTotal:"), "%llu", &value_kb) == 1)
            total_kb = value_kb;
        else if (strncmp(line, "MemAvailable:", strlen("MemAvailable:")) == 0 &&
            sscanf_s(line + strlen("MemAvailable:"), "%llu", &value_kb) == 1)
            available_kb = value_kb;
    }
    FreeFile(file);
    features.system_total_memory_mb = (double)total_kb / 1024.0;
    features.system_available_memory_mb = (double)available_kb / 1024.0;
}

static void gs_amm_collect_runtime_features(MemTuneWorkMemFeatures &features)
{
    TimestampTz transaction_start = GetCurrentTransactionStartTimestamp();
    TimestampTz now = GetCurrentTimestamp();

    features.active_sessions = (double)Max(pgstat_get_current_active_numbackends() - 1, 0);
    features.current_session_private_memory_mb = (double)Max(getSessionMemoryUsageMB(), 0);
    if (transaction_start > 0 && now > transaction_start)
        features.current_transaction_age_sec = (double)(now - transaction_start) / 1000000.0;
    gs_amm_collect_system_memory(features);
    features.memory_pressure_score = GsAmmCurrentMemoryPressureScore();
}

static void gs_amm_release_feature_context(GsAmmWorkMemFeatureContext *ctx)
{
    if (ctx->base.base.init_plans != NIL) {
        list_free(ctx->base.base.init_plans);
        ctx->base.base.init_plans = NIL;
    }
    if (ctx->base.base.traverse_flag != NULL) {
        pfree(ctx->base.base.traverse_flag);
        ctx->base.base.traverse_flag = NULL;
    }
    if (ctx->base.groupTreeList != NIL) {
        list_free(ctx->base.groupTreeList);
        ctx->base.groupTreeList = NIL;
    }
}

bool GsAmmBuildWorkMemFeatures(QueryDesc *query_desc, MemTuneWorkMemFeatures *output_features)
{
    GsAmmWorkMemFeatureContext ctx;
    MemTuneWorkMemFeatures &features = ctx.features;
    PlannedStmt *planned_stmt;
    double average_width_sum = 0.0;
    int average_width_count = 0;

    if (query_desc == NULL || output_features == NULL || query_desc->plannedstmt == NULL ||
        query_desc->plannedstmt->planTree == NULL)
        return false;
    if (!GsAmmOperationBegin())
        return false;

    PG_TRY();
    {
    planned_stmt = query_desc->plannedstmt;
    (void)memset(&ctx, 0, sizeof(ctx));
    exec_init_plan_tree_base(&ctx.base.base, query_desc->plannedstmt);
    ctx.base.plannedStmt = query_desc->plannedstmt;
    ctx.base.dnExec = false;

    (void)gs_amm_workmem_feature_walker((Node *)planned_stmt->planTree, &ctx);
    features.max_plan_rows_log10 = log10(ctx.max_plan_rows + 1.0);
    features.total_cost = gs_amm_nonnegative_finite(planned_stmt->planTree->total_cost);
    features.parallel_workers_planned =
        (double)Max(Max(planned_stmt->query_dop, 0), ctx.max_plan_dop);
    gs_amm_collect_runtime_features(features);
    gs_amm_collect_relation_features(planned_stmt, &features.involved_table_total_size_mb,
        &features.involved_index_total_size_mb, &average_width_sum, &average_width_count);
    if (average_width_count > 0)
        features.average_column_width = average_width_sum / average_width_count;
    features.total_cost_log1p = log1p(Max(features.total_cost, 0.0));

    *output_features = features;
    gs_amm_release_feature_context(&ctx);
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

static void gs_amm_clear_native_query_state(void)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;

    gs_amm_clear_query_feedback(state->lifecycle_generation);
    state->owner_query_desc = NULL;
    state->active = false;
    state->fallback = false;
    state->feedback_only = false;
    state->owner_subxid = InvalidSubTransactionId;
    state->model_version = 0;
    state->leaf_id = 0;
    for (int index = 0; index < GS_AMM_DTREE_BOUND_COUNT; index++) {
        state->raw_bounds_kb[index] = 0.0;
        state->calibrated_bounds_kb[index] = 0.0;
    }
    state->calibration_version = 0;
    state->calibration_scale = 1.0;
    state->grant_id = 0;
    state->grant_generation = 0;
    state->selected_grant_kb = 0;
    state->selected_grant_mode = GS_AMM_NATIVE_GRANT_MODE_NONE;
    state->saved_work_mem_kb = 0;
    state->fallback_guc_nest_level = 0;
    state->start_timestamp = 0;
    state->start_spill_bytes = gs_amm_current_session_spill_bytes();
    state->start_spill_count = gs_amm_current_session_spill_count();
    state->start_guard_count = 0;
}

static int gs_amm_native_bound_kb(double bound_kb)
{
    if (!isfinite(bound_kb) || bound_kb <= 1.0)
        return 1;
    if (bound_kb >= (double)INT_MAX)
        return INT_MAX;
    return (int)ceil(bound_kb);
}

static void gs_amm_native_fail_closed(const char *reason)
{
    GsAmmRecordNativeFailure(reason);
    ereport(ERROR,
        (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
            errmsg("native GS AMM admission failed: %s", reason == NULL ? "unknown" : reason)));
}

static bool gs_amm_begin_native_fallback(
    GsAmmNativeQueryState *state, QueryDesc *query_desc, const char *reason)
{
    char value[32];
    int effective_work_mem_kb;
    int rc;

    if (gs_amm_admission_failure_policy == GS_AMM_ADMISSION_ERROR)
        gs_amm_native_fail_closed(reason);
    GsAmmRecordNativeFailure(reason);
    if (state == NULL || query_desc == NULL || state->active)
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR), errmsg("invalid native GS AMM fallback lifecycle state")));

    state->saved_work_mem_kb = u_sess->attr.attr_memory.work_mem;
    effective_work_mem_kb = Min(state->saved_work_mem_kb, Max(gs_amm_fallback_work_mem_kb, 1));
    state->fallback_guc_nest_level = NewGUCNestLevel();
    rc = snprintf_s(value, sizeof(value), sizeof(value) - 1, "%dkB", effective_work_mem_kb);
    securec_check_ss(rc, "\0", "\0");
    if (set_config_option("work_mem", value, PGC_USERSET, PGC_S_SESSION,
            GUC_ACTION_SAVE, true, ERROR) <= 0) {
        AtEOXact_GUC(true, state->fallback_guc_nest_level);
        state->fallback_guc_nest_level = 0;
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                errmsg("native GS AMM fallback could not apply capped work_mem")));
    }

    state->owner_query_desc = query_desc;
    state->active = true;
    state->fallback = true;
    state->lifecycle_generation++;
    state->selected_grant_kb = effective_work_mem_kb;
    state->selected_grant_mode = GS_AMM_NATIVE_GRANT_MODE_NONE;
    state->start_timestamp = GetCurrentTimestamp();
    state->start_spill_bytes = gs_amm_current_session_spill_bytes();
    state->start_spill_count = gs_amm_current_session_spill_count();
    state->start_guard_count = 0;
    return true;
}

static void gs_amm_release_native_grant_noexcept(void)
{
    GsAmmGrantToken token = {GsAmmCurrentBackendGrantId(), GsAmmCurrentBackendGrantGeneration()};

    if (token.grant_id == 0 || token.grant_generation == 0)
        return;

    PG_TRY();
    {
        (void)GsAmmReleaseGrantToken(token, true);
    }
    PG_CATCH();
    {
        FlushErrorState();
    }
    PG_END_TRY();
}

bool GsAmmExecutorStart(QueryDesc *query_desc, int eflags)
{
    if (!gs_amm_enabled)
        return false;

    GsAmmNativeQueryState *state = &gs_amm_native_query_state;
    MemTuneWorkMemFeatures features;
    double feature_values[MEMTUNE_WORKMEM_FEATURE_COUNT];
    GsAmmDtreeDetail detail;
    GsAmmAdmissionResult admission;
    Plan *root_plan;
    int prediction_mb;
    bool top_level_executor;
    volatile bool native_started = false;
    volatile bool fallback_needed = false;
    const char *volatile fallback_reason = NULL;

    gs_amm_bind_native_query_session();
    gs_amm_register_xact_callback();
    top_level_executor = state->executor_depth == 0;
    gs_amm_push_executor_frame(state, query_desc);
    if (top_level_executor)
        state->owner_subxid = GetCurrentSubTransactionId();
    if (!gs_amm_native_auto_mode || !top_level_executor)
        return false;
    if (query_desc == NULL || query_desc->operation != CMD_SELECT ||
        query_desc->plannedstmt == NULL || query_desc->plannedstmt->planTree == NULL)
        return false;
    if (StreamThreadAmI() || (eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
        return false;
    if (GsAmmCurrentBackendGrantId() != 0)
        return false;

    root_plan = query_desc->plannedstmt->planTree;
    if (root_plan->total_cost < gs_amm_native_ap_cost_threshold)
        return false;
    if (!GsAmmOperationBegin())
        return gs_amm_begin_native_fallback(state, query_desc, "operation_unavailable");

    PG_TRY();
    {
        if (gs_amm_feedback_only_mode) {
            if (gs_amm_plan_has_feedback_operator(root_plan)) {
                errno_t detail_rc;

                GsAmmRecordNativeEligible();
                detail_rc = memset_s(&detail, sizeof(detail), 0, sizeof(detail));
                securec_check(detail_rc, "\0", "\0");
                if (!GsAmmAdmitFeedbackOnly(&admission)) {
                    fallback_needed = true;
                    fallback_reason = "feedback_admission_api";
                } else if (!admission.admitted) {
                    fallback_needed = true;
                    fallback_reason = admission.reason;
                } else {
                    GsAmmRecordNativeAdmission(&detail, &admission);
                    state->owner_query_desc = query_desc;
                    state->active = true;
                    state->feedback_only = true;
                    state->lifecycle_generation++;
                    state->grant_id = admission.grant_id;
                    state->grant_generation = admission.grant_generation;
                    state->selected_grant_kb = admission.granted_kb;
                    state->selected_grant_mode = (int)admission.memory_mode;
                    state->start_timestamp = GetCurrentTimestamp();
                    state->start_spill_bytes = gs_amm_current_session_spill_bytes();
                    state->start_spill_count = gs_amm_current_session_spill_count();
                    state->start_guard_count = 0;
                    gs_amm_begin_query_feedback(state->lifecycle_generation);
                    native_started = true;
                }
            }
        } else if (!GsAmmBuildWorkMemFeatures(query_desc, &features)) {
            fallback_needed = true;
            fallback_reason = "feature_collection";
        } else if (features.hash_join_nodes > 0.0 || features.sort_nodes > 0.0 ||
            features.aggregate_nodes > 0.0 || features.window_nodes > 0.0) {
            GsAmmRecordNativeEligible();

            memtune_workmem_features_to_array(&features, feature_values);
            if (!GsWorkmemDtreePredictDetail(feature_values, &detail)) {
                fallback_needed = true;
                fallback_reason = "dtree_prediction";
            } else {
                prediction_mb = Max((gs_amm_native_bound_kb(detail.calibrated_bounds_kb[0]) + 1023) / 1024, 1);
                (void)GsAmmEvaluateAdmission(prediction_mb);
                if (!GsAmmAdmitBounds(gs_amm_native_bound_kb(detail.calibrated_bounds_kb[0]),
                    gs_amm_native_bound_kb(detail.calibrated_bounds_kb[1]),
                    gs_amm_native_bound_kb(detail.calibrated_bounds_kb[2]), gs_amm_ap_queue_timeout_ms,
                    prediction_mb, &admission)) {
                    fallback_needed = true;
                    fallback_reason = "admission_api";
                } else {
                    if (!admission.admitted) {
                        fallback_needed = true;
                        fallback_reason = admission.reason;
                    } else {
                        GsAmmRecordNativeAdmission(&detail, &admission);
                        state->owner_query_desc = query_desc;
                        state->active = true;
                        state->lifecycle_generation++;
                        state->model_version = detail.model_version;
                        state->leaf_id = detail.leaf_id;
                        for (int index = 0; index < GS_AMM_DTREE_BOUND_COUNT; index++) {
                            state->raw_bounds_kb[index] = detail.raw_bounds_kb[index];
                            state->calibrated_bounds_kb[index] = detail.calibrated_bounds_kb[index];
                        }
                        state->calibration_version = detail.calibration_version;
                        state->calibration_scale = detail.calibration_scale;
                        state->grant_id = admission.grant_id;
                        state->grant_generation = admission.grant_generation;
                        state->selected_grant_kb = admission.granted_kb;
                        state->selected_grant_mode = (int)admission.memory_mode;
                        state->start_timestamp = GetCurrentTimestamp();
                        state->start_spill_bytes = gs_amm_current_session_spill_bytes();
                        state->start_spill_count = gs_amm_current_session_spill_count();
                        state->start_guard_count = 0;
                        gs_amm_begin_query_feedback(state->lifecycle_generation);
                        native_started = true;
                    }
                }
            }
        }
    }
    PG_CATCH();
    {
        int error_code = geterrcode();

        GsAmmOperationEnd();
        gs_amm_release_native_grant_noexcept();
        if (error_code == ERRCODE_QUERY_CANCELED)
            PG_RE_THROW();
        FlushErrorState();
        return gs_amm_begin_native_fallback(state, query_desc, "admission_exception");
    }
    PG_END_TRY();

    GsAmmOperationEnd();
    if (fallback_needed)
        return gs_amm_begin_native_fallback(state, query_desc,
            fallback_reason == NULL ? "admission_failure" : fallback_reason);
    return native_started;
}

void GsAmmExecutorEnd(QueryDesc *query_desc, bool success)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;
    bool owner_finished = state->active && state->owner_query_desc == query_desc;

    (void)gs_amm_pop_executor_frame(state, query_desc);
    if (owner_finished && state->executor_depth == 0) {
        gs_amm_finish_native_query(!success, false, true);
        gs_amm_clear_native_query_state();
        gs_amm_clear_executor_frames();
    } else if (!state->active && state->executor_depth == 0) {
        gs_amm_clear_native_query_state();
    }
}
