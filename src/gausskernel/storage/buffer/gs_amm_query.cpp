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
#include "executor/instrument.h"
#include "knl/knl_thread.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "optimizer/planmem_walker.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "storage/gs_amm.h"
#include "storage/smgr/fd.h"
#include "utils/lsyscache.h"
#include "utils/ammgranule.h"
#include "utils/guc.h"
#include "utils/memprot.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"
#include "utils/tuplesort.h"
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
    bool xact_callback_registered;
    int executor_depth;
    GsAmmNativeExecutorFrame executor_frames[GS_AMM_NATIVE_EXECUTOR_STACK_DEPTH];
    SubTransactionId owner_subxid;
    uint64 lifecycle_generation;
    int64 model_version;
    int64 leaf_id;
    uint64 grant_id;
    uint64 grant_generation;
    bool external_grant;
    int selected_grant_kb;
    int selected_grant_mode;
} GsAmmNativeQueryState;

typedef struct GsAmmTpQueryState {
    knl_session_context *owner_session;
    uint64 owner_session_id;
    QueryDesc *owner_query_desc;
    bool active;
    uint64 start_hit;
    uint64 start_read;
} GsAmmTpQueryState;

#define GS_AMM_NATIVE_GRANT_MODE_NONE 0

static THR_LOCAL GsAmmNativeQueryState gs_amm_native_query_state;
static THR_LOCAL GsAmmTpQueryState gs_amm_tp_query_state;

static void gs_amm_clear_native_query_state(void);
static void gs_amm_clear_executor_frames(void);

static void gs_amm_apply_test_ap_bounds(GsAmmDtreeDetail *detail)
{
    int cache_kb = gs_amm_test_ap_cache_label_kb;
    int one_pass_kb = gs_amm_test_ap_one_pass_label_kb;
    int multi_pass_kb = gs_amm_test_ap_multi_pass_label_kb;

    if (cache_kb == 0 && one_pass_kb == 0 && multi_pass_kb == 0 &&
        gs_amm_test_ap_label_kb > 0) {
        cache_kb = gs_amm_test_ap_label_kb;
        one_pass_kb = gs_amm_test_ap_label_kb;
        multi_pass_kb = gs_amm_test_ap_label_kb;
    }

    if (cache_kb == 0 && one_pass_kb == 0 && multi_pass_kb == 0)
        return;
    if (cache_kb <= 0 || one_pass_kb <= 0 || multi_pass_kb <= 0 ||
        multi_pass_kb > one_pass_kb || one_pass_kb > cache_kb) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("GS AMM AP test bounds must satisfy 0 or multi_pass <= one_pass <= cache")));
    }

    detail->bounds_kb[0] = cache_kb;
    detail->bounds_kb[1] = one_pass_kb;
    detail->bounds_kb[2] = multi_pass_kb;
}

static void gs_amm_clear_tp_query_state(void)
{
    gs_amm_tp_query_state.owner_query_desc = NULL;
    gs_amm_tp_query_state.active = false;
    gs_amm_tp_query_state.start_hit = 0;
    gs_amm_tp_query_state.start_read = 0;
}

uint64 GsAmmCurrentQueryLifecycleGeneration(void)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;

    if (!gs_amm_enabled || !state->active)
        return 0;
    return state->lifecycle_generation;
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
    /* Tuplesort reclaim registration is thread-local, while a thread-pool
     * worker can be reassigned to another session after portal/error cleanup.
     * Those old sort contexts are no longer safe to inspect in this session. */
    tuplesort_clear_amm_reclaim_states();
    gs_amm_clear_native_query_state();
    gs_amm_clear_executor_frames();
    state->xact_callback_registered = false;
    state->owner_session = u_sess;
    state->owner_session_id = u_sess->session_id;
}

static void gs_amm_finish_native_query(bool error, bool error_cleanup)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;
    GsAmmGrantToken expected_token;

    (void)error;
    (void)error_cleanup;

    if (!state->active)
        return;

    /* A SQL caller may hold an explicit AP grant across multiple queries.
     * Executor teardown must detach only this query; gs_amm_end_ap() owns
     * the grant lifetime. */
    if (state->external_grant) {
        gs_amm_clear_native_query_state();
        return;
    }

    if (state->grant_id == 0 || state->grant_generation == 0)
        return;

    expected_token.grant_id = state->grant_id;
    expected_token.grant_generation = state->grant_generation;
    /* Revoke query ownership before releasing its AMM grant. */
    gs_amm_clear_native_query_state();
    (void)GsAmmReleaseGrantToken(expected_token);
}

static void gs_amm_finish_native_query_noexcept(bool error)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;
    GsAmmGrantToken expected_token = {state->grant_id, state->grant_generation};

    PG_TRY();
    {
        gs_amm_finish_native_query(error, true);
    }
    PG_CATCH();
    {
        FlushErrorState();
        PG_TRY();
        {
            (void)GsAmmReleaseGrantToken(expected_token);
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
            gs_amm_finish_native_query_noexcept(event == XACT_EVENT_ABORT);
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
            gs_amm_finish_native_query_noexcept(true);
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
    features.memory_pressure_score = 0.0;
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

    state->owner_query_desc = NULL;
    state->active = false;
    state->owner_subxid = InvalidSubTransactionId;
    state->model_version = 0;
    state->leaf_id = 0;
    state->grant_id = 0;
    state->grant_generation = 0;
    state->external_grant = false;
    state->selected_grant_kb = 0;
    state->selected_grant_mode = GS_AMM_NATIVE_GRANT_MODE_NONE;
}

static int gs_amm_native_bound_kb(double bound_kb)
{
    if (!isfinite(bound_kb) || bound_kb <= 1.0)
        return 1;
    if (bound_kb >= (double)INT_MAX)
        return INT_MAX;
    return (int)ceil(bound_kb);
}

static bool gs_amm_fail_open(const char *reason)
{
    GsAmmRecordNativeFailure(reason);
    return false;
}

static void gs_amm_release_native_grant_noexcept(void)
{
    GsAmmGrantToken token = {GsAmmCurrentBackendGrantId(), GsAmmCurrentBackendGrantGeneration()};

    if (token.grant_id == 0 || token.grant_generation == 0)
        return;

    PG_TRY();
    {
        (void)GsAmmReleaseGrantToken(token);
    }
    PG_CATCH();
    {
        FlushErrorState();
    }
    PG_END_TRY();
}

static bool gs_amm_bind_external_grant_query(GsAmmNativeQueryState *state, QueryDesc *query_desc)
{
    GsAmmGrantToken token = {GsAmmCurrentBackendGrantId(), GsAmmCurrentBackendGrantGeneration()};

    if (state == NULL || query_desc == NULL || token.grant_id == 0 || token.grant_generation == 0 ||
        !GsAmmGrantTokenIsValid(token))
        return false;

    state->owner_query_desc = query_desc;
    state->active = true;
    state->lifecycle_generation++;
    state->grant_id = token.grant_id;
    state->grant_generation = token.grant_generation;
    state->external_grant = true;
    state->selected_grant_kb = GsAmmCurrentBackendGrantKB();
    state->selected_grant_mode = GS_AMM_NATIVE_GRANT_MODE_NONE;
    return true;
}

bool GsAmmApExecutorStart(QueryDesc *query_desc, int eflags)
{
    if (!gs_amm_enabled)
        return false;

    GsAmmNativeQueryState *state = &gs_amm_native_query_state;
    MemTuneWorkMemFeatures features;
    double feature_values[MEMTUNE_WORKMEM_FEATURE_COUNT];
    GsAmmDtreeDetail detail;
    GsAmmAdmissionResult admission;
    int prediction_mb;
    bool top_level_executor;
    bool operation_held = false;
    volatile bool native_started = false;
    volatile bool fallback_needed = false;
    const char *volatile fallback_reason = NULL;

    gs_amm_bind_native_query_session();
    gs_amm_register_xact_callback();
    top_level_executor = state->executor_depth == 0;
    gs_amm_push_executor_frame(state, query_desc);
    if (top_level_executor)
        state->owner_subxid = GetCurrentSubTransactionId();
    if (!top_level_executor)
        return false;
    if (gs_amm_workload_role != GS_AMM_WORKLOAD_AP)
        return false;
    if (query_desc == NULL || query_desc->operation != CMD_SELECT ||
        query_desc->plannedstmt == NULL || query_desc->plannedstmt->planTree == NULL)
        return false;
    if (StreamThreadAmI() || (eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
        return false;
    if (GsAmmCurrentBackendGrantId() != 0) {
        native_started = gs_amm_bind_external_grant_query(state, query_desc);
        return native_started;
    }

    if (!GsAmmOperationBegin())
        return gs_amm_fail_open("operation_unavailable");
    operation_held = true;

    PG_TRY();
    {
        if (!GsAmmBuildWorkMemFeatures(query_desc, &features)) {
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
                gs_amm_apply_test_ap_bounds(&detail);
                prediction_mb = Max((gs_amm_native_bound_kb(detail.bounds_kb[0]) + 1023) / 1024, 1);
                /* Admission may wait indefinitely.  Do not hold an AMM
                 * operation reference while this backend sleeps in FIFO. */
                GsAmmOperationEnd();
                operation_held = false;
                if (!GsAmmAdmitBounds(gs_amm_native_bound_kb(detail.bounds_kb[0]),
                    gs_amm_native_bound_kb(detail.bounds_kb[1]),
                    gs_amm_native_bound_kb(detail.bounds_kb[2]), 0,
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
                        state->grant_id = admission.grant_id;
                        state->grant_generation = admission.grant_generation;
                        state->external_grant = false;
                        state->selected_grant_kb = admission.granted_kb;
                        state->selected_grant_mode = (int)admission.memory_mode;
                        native_started = true;
                    }
                }
            }
        }
    }
    PG_CATCH();
    {
        int error_code = geterrcode();

        if (operation_held)
            GsAmmOperationEnd();
        gs_amm_release_native_grant_noexcept();
        if (error_code == ERRCODE_QUERY_CANCELED)
            PG_RE_THROW();
        FlushErrorState();
        return gs_amm_fail_open("admission_exception");
    }
    PG_END_TRY();

    if (operation_held)
        GsAmmOperationEnd();
    if (fallback_needed)
        return gs_amm_fail_open(fallback_reason == NULL ? "admission_failure" : fallback_reason);
    return native_started;
}

void GsAmmApExecutorEnd(QueryDesc *query_desc, bool success)
{
    GsAmmNativeQueryState *state = &gs_amm_native_query_state;
    bool owner_finished = state->active && state->owner_query_desc == query_desc;

    (void)gs_amm_pop_executor_frame(state, query_desc);
    if (owner_finished && state->executor_depth == 0) {
        gs_amm_finish_native_query(!success, false);
        gs_amm_clear_native_query_state();
        gs_amm_clear_executor_frames();
    } else if (!state->active && state->executor_depth == 0) {
        gs_amm_clear_native_query_state();
    }
}

bool GsAmmApProcessPendingReclaim(void)
{
    /* Reclaim belongs to the backend grant, not to whichever QueryDesc is
     * currently at the executor boundary.  A cursor can yield, sleep, or run
     * nested SQL while its original grant still owns registered sort/hash
     * state, so do not use the native QueryDesc owner as a reclamation gate. */
    if (!gs_amm_enabled || gs_amm_workload_role != GS_AMM_WORKLOAD_AP ||
        GsAmmCurrentBackendGrantId() == 0)
        return false;

    /* Spill live sort results before returning free tuple extents.  The
     * controller only releases an entire physical granule after all live
     * allocations are gone. */
    if (GsAmmGrantReclaimPending())
        (void)tuplesort_process_amm_reclaim();

    return GsAmmGrantProcessPendingReclaim();
}

bool GsAmmTpExecutorStart(QueryDesc *query_desc, int eflags)
{
    GsAmmTpQueryState *state = &gs_amm_tp_query_state;

    (void)eflags;
    if (!gs_amm_enabled || gs_amm_workload_role != GS_AMM_WORKLOAD_TP || query_desc == NULL)
        return false;
    state->owner_session = u_sess;
    state->owner_session_id = u_sess->session_id;
    state->owner_query_desc = query_desc;
    state->active = u_sess->instr_cxt.pg_buffer_usage != NULL;
    if (state->active) {
        state->start_hit = u_sess->instr_cxt.pg_buffer_usage->shared_blks_hit;
        state->start_read = u_sess->instr_cxt.pg_buffer_usage->shared_blks_read;
    }
    return false;
}

void GsAmmTpExecutorEnd(QueryDesc *query_desc, bool success)
{
    GsAmmTpQueryState *state = &gs_amm_tp_query_state;
    uint64 hit;
    uint64 read;

    (void)success;
    if (!state->active || state->owner_query_desc != query_desc ||
        u_sess->instr_cxt.pg_buffer_usage == NULL)
        return;
    hit = u_sess->instr_cxt.pg_buffer_usage->shared_blks_hit;
    read = u_sess->instr_cxt.pg_buffer_usage->shared_blks_read;
    GsAmmRecordTpBufferUsage(hit >= state->start_hit ? hit - state->start_hit : 0,
        read >= state->start_read ? read - state->start_read : 0, 1);
    gs_amm_clear_tp_query_state();
}

bool GsAmmExecutorStart(QueryDesc *query_desc, int eflags)
{
    if (gs_amm_workload_role == GS_AMM_WORKLOAD_AP)
        return GsAmmApExecutorStart(query_desc, eflags);
    return GsAmmTpExecutorStart(query_desc, eflags);
}

void GsAmmExecutorEnd(QueryDesc *query_desc, bool success)
{
    if (gs_amm_workload_role == GS_AMM_WORKLOAD_AP)
        GsAmmApExecutorEnd(query_desc, success);
    else
        GsAmmTpExecutorEnd(query_desc, success);
}
