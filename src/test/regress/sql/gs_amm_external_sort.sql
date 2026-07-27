-- Run this test with the AMM validation GUCs in the postmaster configuration.
-- AMM external sorts must preserve all runs when a tuple allocation forces a
-- mid-input flush.  The aggregate's internal datum sort exercises that path.
SHOW gs_amm_enabled;
SHOW gs_amm_legacy_control_enabled;
SHOW gs_amm_resize_cooldown_ms;
SHOW enable_resource_track;

SELECT position('reset=true' in gs_amm_reset_state()) = 1 AS reset;
SELECT position('chosen_action=BORROW_FROM_BUFFER' in gs_amm_controller_step(64, 0, 0)) = 1 AS borrowed;
SELECT position('admitted=true' in gs_amm_begin_ap(4, 4, 4, 0)) = 1 AS admitted;

SELECT count(*) AS count_all, count(DISTINCT payload) AS count_distinct
FROM (
    SELECT lpad(g::text, 1024, 'x') AS payload
    FROM generate_series(1, 50000) AS g
    ORDER BY payload
) ordered;

SELECT count(*) AS count_all, count(DISTINCT payload) AS count_distinct
FROM (
    SELECT lpad(g::text, 1024, 'x') AS payload
    FROM generate_series(1, 50000) AS g
    ORDER BY payload
) ordered;

SELECT position('released=true' in gs_amm_end_ap()) = 1 AS released;
SELECT position('active_ap_count=0' in gs_amm_status()) > 0
       AND position('ap_active_granules=0' in gs_amm_status()) > 0
       AND position('dynamic_used_mb=0' in gs_amm_status()) > 0 AS grant_cleaned;
SELECT position('reset=true' in gs_amm_reset_state()) = 1 AS restored;
