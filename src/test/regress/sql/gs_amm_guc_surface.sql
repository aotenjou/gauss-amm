SELECT count(*) = 12 AS guc_count_ok
FROM pg_settings
WHERE name LIKE 'gs_amm_%';

SELECT array_agg(name ORDER BY name)::text =
    '{gs_amm_ap_borrow_buffer_hit_guard_pct,gs_amm_dynamic_target_mb,gs_amm_enabled,gs_amm_granule_size_mb,gs_amm_shared_buffers_min_mb,gs_amm_test_ap_cache_label_kb,gs_amm_test_ap_label_kb,gs_amm_test_ap_multi_pass_label_kb,gs_amm_test_ap_one_pass_label_kb,gs_amm_tp_buffer_miss_threshold_pct,gs_amm_tp_tps_decline_guard_pct,gs_amm_workload_role}'
    AS guc_names_ok
FROM pg_settings
WHERE name LIKE 'gs_amm_%';

SHOW gs_amm_workload_role;
SET gs_amm_workload_role = ap;
SHOW gs_amm_workload_role;
RESET gs_amm_workload_role;

BEGIN;
SET LOCAL gs_amm_test_ap_cache_label_kb = 475082;
SET LOCAL gs_amm_test_ap_one_pass_label_kb = 388710;
SET LOCAL gs_amm_test_ap_multi_pass_label_kb = 65536;
SHOW gs_amm_test_ap_cache_label_kb;
SHOW gs_amm_test_ap_one_pass_label_kb;
SHOW gs_amm_test_ap_multi_pass_label_kb;
ROLLBACK;

SHOW gs_amm_tp_buffer_miss_threshold_pct;
SHOW gs_amm_ap_borrow_buffer_hit_guard_pct;
SHOW gs_amm_tp_tps_decline_guard_pct;

SELECT count(*) = 0 AS removed_gucs_absent
FROM pg_settings
WHERE name = ANY (ARRAY[
    'gs_amm_admission_failure_policy',
    'gs_amm_allocator_only_grant_mb',
    'gs_amm_allocator_only_mode',
    'gs_amm_ap_min_grant_mb',
    'gs_amm_ap_queue_limit',
    'gs_amm_ap_queue_timeout_ms',
    'gs_amm_controller_horizon',
    'gs_amm_deadband_mb',
    'gs_amm_dtree_calibration_enabled',
    'gs_amm_dtree_record_only',
    'gs_amm_fallback_work_mem_kb',
    'gs_amm_feedback_bootstrap_grant_mb',
    'gs_amm_feedback_initial_ap_slots',
    'gs_amm_feedback_max_ap_slots',
    'gs_amm_feedback_max_grant_mb',
    'gs_amm_feedback_only_mode',
    'gs_amm_feedback_spill_threshold_mb',
    'gs_amm_feedback_stable_windows',
    'gs_amm_io_pressure_guard',
    'gs_amm_legacy_control_enabled',
    'gs_amm_native_ap_cost_threshold',
    'gs_amm_native_auto_mode',
    'gs_amm_resize_batch_mb',
    'gs_amm_resize_cooldown_ms',
    'gs_amm_resize_observe_window_ms',
    'gs_amm_resize_rate_limit_mb',
    'gs_amm_tp_jitter_limit',
    'gs_amm_tp_pressure_guard',
    'gs_amm_tp_recovery_cooldown_ms'
]);

-- Parameter removal must not affect ordinary SQL execution.
SELECT 1 AS ordinary_query_still_runs;
