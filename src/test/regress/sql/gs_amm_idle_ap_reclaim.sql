-- Run this test with the AMM validation GUCs in the postmaster configuration.
-- A TP recovery step must reclaim AP granules that have not served an
-- allocation, while leaving the running AP lifecycle safe to finish.
SHOW gs_amm_enabled;
SHOW gs_amm_legacy_control_enabled;
SHOW gs_amm_resize_cooldown_ms;
SHOW enable_resource_track;

SELECT position('reset=true' in gs_amm_reset_state()) = 1 AS reset;
SELECT position('chosen_action=BORROW_FROM_BUFFER' in gs_amm_controller_step(64, 0, 0)) = 1 AS borrowed;
SELECT position('chosen_action=BORROW_FROM_BUFFER' in gs_amm_controller_step(128, 0, 0)) = 1 AS borrowed_again;
SELECT position('admitted=true' in gs_amm_begin_ap(68, 68, 68, 0)) = 1 AS admitted;
SELECT position('dynamic_used_mb=68' in gs_amm_status()) > 0
       AND position('ap_active_granules=2' in gs_amm_status()) > 0 AS idle_grant_active;

SELECT position('chosen_action=TP_RECOVERY' in gs_amm_controller_step(0, 100, 0)) = 1 AS recovery;
SELECT position('dynamic_used_mb=4' in gs_amm_status()) > 0
       AND position('ap_active_granules=1' in gs_amm_status()) > 0
       AND position('ap_idle_reclaim_count=1' in gs_amm_status()) > 0
       AND position('ap_idle_reclaim_mb=64' in gs_amm_status()) > 0
       AND position('granule_grant_bytes_consistent=true' in gs_amm_status()) > 0 AS idle_grant_reclaimed;
SELECT position('chosen_action=TP_RECOVERY' in gs_amm_controller_step(0, 100, 0)) = 1 AS repeated_recovery;
SELECT position('dynamic_used_mb=4' in gs_amm_status()) > 0
       AND position('ap_active_granules=1' in gs_amm_status()) > 0
       AND position('ap_idle_reclaim_count=1' in gs_amm_status()) > 0
       AND position('ap_idle_reclaim_mb=64' in gs_amm_status()) > 0
       AND position('granule_grant_bytes_consistent=true' in gs_amm_status()) > 0 AS minimum_capacity_retained;
SELECT max(row_number) AS max_row_number
FROM (
    SELECT row_number() OVER (ORDER BY lpad(g::text, 1024, 'x')) AS row_number
    FROM generate_series(1, 100000) AS g
) ordered;
SELECT position('released=true' in gs_amm_end_ap()) = 1 AS released;
SELECT position('active_ap_count=0' in gs_amm_status()) > 0
       AND position('dynamic_used_mb=0' in gs_amm_status()) > 0 AS lifecycle_cleaned;
SELECT position('reset=true' in gs_amm_reset_state()) = 1 AS restored;
