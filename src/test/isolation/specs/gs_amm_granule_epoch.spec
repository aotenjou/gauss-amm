# Verify that the hard TPS guard rejects a granule shrink before the buffer
# pool changes and leaves the granule state consistent. The AMM validation
# GUCs must be set before this test is run.

setup
{
  DO $$
  BEGIN
    IF current_setting('gs_amm_enabled') <> 'on'
       OR current_setting('gs_amm_legacy_control_enabled') <> 'on'
       OR current_setting('gs_amm_resize_cooldown_ms') <> '0'
    THEN RAISE EXCEPTION 'AMM validation GUCs are not configured'; END IF;
    PERFORM gs_amm_reset_state();
  END $$;
}

teardown
{
  DO $$ BEGIN PERFORM gs_amm_reset_state(); END $$;
}

session "controller"
step "prime_tps" { DO $$ BEGIN PERFORM gs_amm_update_tp_metrics(100.0, 0.0, 0); END $$; }
step "drop_tps" { DO $$ BEGIN PERFORM gs_amm_update_tp_metrics(10.0, 0.0, 0); END $$; }
step "shrink_guarded" {
  DO $$
  DECLARE status text := gs_amm_status();
  BEGIN
    PERFORM gs_amm_resize_shared_buffers(
      split_part(split_part(status, 'max_mb=', 2), ' ', 1)::integer -
      split_part(split_part(status, 'resize_granule_mb=', 2), ' ', 1)::integer);
  END $$;
}
step "check_guard" { DO $$ DECLARE status text := gs_amm_status(); BEGIN IF split_part(split_part(status, 'active_blocks=', 2), ' ', 1) <> split_part(split_part(status, 'max_blocks=', 2), ' ', 1) OR COALESCE(split_part(split_part(status, ' guard_block_count=', 2), ' ', 1)::integer, 0) = 0 OR COALESCE(split_part(split_part(status, ' rollback_count=', 2), ' ', 1)::integer, 0) = 0 OR position('tp_guard_hot=true ' IN status) = 0 OR position('granule_state_consistent=true ' IN status) = 0 OR position('inactive_granule_buffer_violations=0 ' IN status) = 0 THEN RAISE EXCEPTION 'AMM TPS guard did not preserve the buffer pool: %', status; END IF; END $$; }

permutation "prime_tps" "drop_tps" "shrink_guarded" "check_guard"
