import unittest
from unittest.mock import patch

import memory_bounds
import sqlGEM
import generate_memory_bounds_batch
from scripts import train_xgb_memory
from scripts import prepare_combined_xgb_data
import generate_sort_coverage
import generate_ap_sort_coverage
from scripts import collect_five_stage_ap_bounds
from scripts import collect_five_stage_ap_neighborhood
from scripts import train_xgb_one_pass


class MemoryBoundsTests(unittest.TestCase):

    def test_repeated_placeholder_uses_one_sampled_value(self):
        template = "SELECT * FROM gsbench.fact_sales WHERE id >= {{p1}} AND id < {{p1}} + 1000000"
        placeholders = [{"name": "p1", "type": "bigint", "min": 0, "max": 1000}]
        with patch("sqlGEM.random.randint", return_value=321):
            sql = sqlGEM.instantiate_query(template, placeholders)
        self.assertEqual(
            sql,
            "SELECT * FROM gsbench.fact_sales WHERE id >= 321 AND id < 321 + 1000000",
        )

    def test_required_memory_shape_validation(self):
        valid = (
            "SELECT fs.id, fs.payload, a.payload FROM gsbench.fact_sales fs "
            "JOIN gsbench.accounts a ON fs.customer_id = a.customer_id "
            "AND fs.dist_key = a.dist_key WHERE fs.id >= 10 "
            "AND fs.id < 10 + 1000000 AND a.customer_id < 100000"
        )
        self.assertTrue(generate_memory_bounds_batch.has_required_memory_shape(valid))
        self.assertFalse(generate_memory_bounds_batch.has_required_memory_shape(
            valid.replace("a.customer_id < 100000", "a.id < 100000")
        ))
    def test_matches_existing_three_mode_classifier(self):
        multi = {
            "Plan": {
                "Node Type": "Hash Join",
                "Hash Batches": 4,
                "Temp Written Blocks": 100,
                "Temp Read Blocks": 200,
            }
        }
        one = {
            "Plan": {
                "Node Type": "Hash Join",
                "Hash Batches": 2,
                "Temp Written Blocks": 100,
                "Temp Read Blocks": 100,
            }
        }
        cache = {"Plan": {"Node Type": "Sort", "Sort Method": "quicksort", "Sort Space Type": "Memory"}}
        self.assertEqual(memory_bounds.inspect_mode(multi, 2, 1.25)["mode"], "multi_pass")
        self.assertEqual(memory_bounds.inspect_mode(one, 2, 1.25)["mode"], "one_pass")
        self.assertEqual(memory_bounds.inspect_mode(cache, 2, 1.25)["mode"], "cache")

    def test_normalizes_only_one_select_statement(self):
        from generate_memory_bounds_batch import normalize_sql

        self.assertEqual(normalize_sql(" SELECT 1;; \n"), "SELECT 1;")
        self.assertIsNone(normalize_sql("SELECT 1; SELECT 2"))
        self.assertIsNone(normalize_sql("DELETE FROM t"))
        self.assertIsNone(normalize_sql("SELECT {{p1}}"))

    def test_v2_missing_flags_cover_runtime_plan_and_sort_features(self):
        record = {
            "features": {
                "static_plan_features": {
                    "sort_nodes": 1,
                    "sort_input_rows": None,
                    "sort_tuple_width": None,
                    "sort_input_bytes": None,
                    "sort_key_count": None,
                    "plan_node_count": None,
                },
                "runtime_state": {},
                "relation_profile": {"status": "missing", "tables": []},
            }
        }
        values, missing = train_xgb_memory.feature_row_v2(record)
        for name in (
            "active_sessions",
            "current_session_private_memory_mb",
            "system_total_memory_mb",
            "plan_node_count",
            "sort_key_count",
        ):
            self.assertIn(name, missing)
            self.assertEqual(values[f"{name}_missing"], 1.0)

    def test_zero_relation_size_is_missing(self):
        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, *_args):
                return None

            def fetchone(self):
                return (0, 0)

        class Connection:
            def cursor(self):
                return Cursor()

            def rollback(self):
                return None

        profile = memory_bounds.relation_profile(Connection(), "SELECT * FROM gsbench.fact_sales")
        self.assertEqual(profile["status"], "missing")
        self.assertIsNone(profile["involved_table_total_size_mb"])

    def test_unversioned_protocol_is_distinguished_from_native_v2(self):
        self.assertIsNone(train_xgb_memory.row_label_protocol({}))
        self.assertEqual(
            train_xgb_memory.row_label_protocol({"bounds": {"label_protocol": "native-v2"}}),
            "native-v2",
        )

    def test_warning_counts_do_not_treat_missing_features_as_ood(self):
        counts = train_xgb_memory.warning_query_counts([
            [{"code": "MISSING_FEATURE", "feature": "a"}],
            [{"code": "OOD_FEATURE", "feature": "b"}],
            [{"code": "MISSING_FEATURE", "feature": "c"},
             {"code": "OOD_FEATURE", "feature": "d"}],
            [],
        ])
        self.assertEqual(counts["ood_query_count"], 2)
        self.assertEqual(counts["missing_feature_query_count"], 2)

    def test_mixed_features_encode_workload_and_scale(self):
        record = {
            "dataset": "gsbench",
            "dbname": "llm4sqlgen_s20gb",
            "features": {"static_plan_features": {}, "runtime_state": {}, "relation_profile": {}},
        }
        values, missing = train_xgb_memory.feature_row_mixed(record)
        self.assertEqual(values["workload_gsbench"], 1.0)
        self.assertEqual(values["scale_factor"], 20.0)
        self.assertEqual(values["scale_factor_log1p"], __import__("math").log1p(20.0))
        self.assertIn("current_session_private_memory_mb", missing)

    def test_mixed_clean_features_exclude_removed_signals(self):
        removed = {
            "system_available_memory_mb", "memory_pressure_score", "active_sessions",
            "current_session_private_memory_mb", "system_total_memory_mb",
            "scale_factor", "scale_factor_log1p",
        }
        names = train_xgb_memory.FEATURE_NAMES_MIXED_CLEAN
        self.assertTrue(names)
        self.assertTrue(all(not name.startswith("workload_") for name in names))
        self.assertTrue(all(not name.endswith("_missing") for name in names))
        self.assertTrue(removed.isdisjoint(names))

    def test_combined_normalizer_accepts_legacy_top_level_bounds(self):
        row, reason = prepare_combined_xgb_data.normalize_row(
            {"id": "q1", "sql": "SELECT 1", "dataset": "tpch",
             "mem_cache_mb": 4.0, "mem_one_pass_mb": 2.0, "mem_multi_pass_mb": 1.999},
            __import__("pathlib").Path("tpch_sf1.jsonl"), "tpch",
        )
        self.assertIsNone(reason)
        self.assertEqual(row["label_protocol"], "native-v2")
        self.assertEqual(row["bounds"]["cache_mb"], 4.0)
        self.assertEqual(row["bounds"]["multi_pass_mb"], 1.999)
        self.assertEqual(row["multi_pass_status"], "derived")
        self.assertFalse(row["_multi_pass_observed"])

    def test_combined_normalizer_marks_native_three_bound_as_observed(self):
        row, reason = prepare_combined_xgb_data.normalize_row(
            {"id": "q1", "sql": "SELECT 1", "dataset": "gsbench",
             "label_protocol": "native-v2", "label_semantics": "native_executor_boundaries",
             "bounds": {"cache_mb": 4.0, "one_pass_mb": 2.0, "multi_pass_mb": 1.999}},
            __import__("pathlib").Path("gsbench.jsonl"), "gsbench",
        )
        self.assertIsNone(reason)
        self.assertEqual(row["multi_pass_status"], "observed")
        self.assertTrue(row["_multi_pass_observed"])

    def test_calibration_search_reduces_qerror_on_validation_values(self):
        import numpy as np
        gold = np.asarray([[10.0, 5.0, 4.999], [20.0, 10.0, 9.999]])
        pred = np.asarray([[8.0, 4.0, 3.999], [16.0, 8.0, 7.999]])
        self.assertEqual(train_xgb_memory.select_calibration(gold, pred, [1.0, 1.25]), (1.25, 1.25))

    def test_repeated_three_bound_collection_records_stability(self):
        results = [
            ({"mem_cache_mb": 4.0 + i, "mem_one_pass_mb": 2.0 + i,
              "mem_multi_pass_mb": 1.999 + i, "diagnostics": {}}, None)
            for i in range(3)
        ]
        with patch("memory_bounds.collect_three_bounds", side_effect=results):
            result, error = memory_bounds.collect_three_bounds_repeated(object(), "SELECT 1", {}, repeat_count=3)
        self.assertIsNone(error)
        self.assertEqual(result["mem_cache_mb"], 5.0)
        self.assertEqual(result["repeat_count"], 3)
        self.assertEqual(result["boundary_stability_mb"]["cache"], 2.0)

    def test_hinted_search_still_probes_current_boundary(self):
        probes = []

        def predicate(value):
            probes.append(value)
            return value >= 123

        result = memory_bounds.hinted_binary_search(1, 4096, predicate, hint=120)
        self.assertEqual(result, 123)
        self.assertIn(120, probes)
        self.assertIn(123, probes)

    def test_two_bound_collection_allows_one_pass_at_minimum(self):
        static = {"Plan": {"Node Type": "Sort", "Plan Rows": 100, "Plan Width": 64, "Total Cost": 10}}

        def analyzed(_conn, _sql, work_mem_kb, _timeout_ms):
            if work_mem_kb >= 128:
                return {"Plan": {"Node Type": "Sort", "Sort Space Type": "Memory"}}
            return {"Plan": {"Node Type": "Sort", "Sort Space Type": "Disk",
                             "Temp Read Blocks": 10, "Temp Written Blocks": 10}}

        collection = {
            "min_work_mem_kb": 64, "max_work_mem_mb": 1, "upper_max_work_mem_mb": 1,
            "statement_timeout_ms": 1000, "candidate_timeout_ms": 10000,
            "one_pass_max_batches": 2, "one_pass_temp_read_write_ratio": 1.25,
            "label_protocol": "native-v2", "collection_mode": "native",
        }
        with patch("memory_bounds.explain_static", return_value=static), \
             patch("memory_bounds.explain_analyze", side_effect=analyzed):
            result, error = memory_bounds.collect_cache_one_bounds(object(), "SELECT 1", collection)
        self.assertIsNone(error)
        self.assertEqual(result["mem_cache_mb"], 0.125)
        self.assertEqual(result["mem_one_pass_mb"], 0.0625)
        self.assertIsNone(result["mem_multi_pass_mb"])

    def test_sort_coverage_sql_is_select_and_has_targeted_width(self):
        sql = generate_sort_coverage.make_sql(1024, 1000, 4, 1, 0)
        self.assertTrue(sql.lower().startswith("with "))
        self.assertIn("sort_payload_extra", sql.lower())
        self.assertIn("join gsbench.sort_data sd2", sql.lower())
        self.assertIn("order by sort_payload", sql.lower())

    def test_sort_coverage_byte_bins(self):
        self.assertEqual(generate_sort_coverage.byte_bin(0.5), "0-1")
        self.assertEqual(generate_sort_coverage.byte_bin(184.8), "128-256")
        self.assertEqual(generate_sort_coverage.byte_bin(1024), "1024-inf")

    def test_ap_shape_sql_uses_three_sort_keys_without_reusing_ap_interval(self):
        sql = generate_ap_sort_coverage.ap_shape_sql(123, 456)
        self.assertIn("dist_key BETWEEN 123 AND 579", sql)
        self.assertTrue(sql.endswith("ORDER BY payload, sort_key DESC, id"))
        specs = list(generate_ap_sort_coverage.candidate_specs(1, 1_000_000, 3, (1.0,)))
        self.assertTrue(all(start != 1 for _, start, _, _ in specs))

    def test_ap_shape_weight_is_opt_in_and_targeted(self):
        rows = [{"target": "gsbench_ap_shape_coverage"}, {"target": "other"}]
        self.assertEqual(train_xgb_memory.ap_shape_weights(rows, 8.0).tolist(), [8.0, 1.0])

    def test_five_stage_ap_specs_are_distinct_three_key_sorts(self):
        cases = collect_five_stage_ap_bounds.FIVE_STAGE_APS
        self.assertEqual([case["requested_work_mem_mb"] for case in cases], [128, 512])
        self.assertEqual([case["range_end"] for case in cases], [28086, 112347])
        self.assertTrue(all("ORDER BY payload, sort_key DESC, id" in collect_five_stage_ap_bounds.five_stage_sql(
            case["range_start"], case["range_end"]
        ) for case in cases))

    def test_ap_shape_partition_is_ordinal_stable(self):
        rows = [{"query_id": f"gsbench_ap_shape_1gb_{ordinal:03d}"} for ordinal in range(4)]
        train, validation = train_xgb_one_pass.ap_shape_partition(rows)
        self.assertEqual([row["query_id"][-3:] for row in train], ["000", "002"])
        self.assertEqual([row["query_id"][-3:] for row in validation], ["001", "003"])

    def test_five_stage_neighbor_cases_exclude_frozen_sql_and_have_fixed_splits(self):
        cases = collect_five_stage_ap_neighborhood.interval_cases(1, 112347)
        self.assertEqual(len(cases), 8)
        self.assertEqual(sum(case["xgb_split"] == "train" for case in cases), 4)
        self.assertEqual(sum(case["xgb_split"] == "validation" for case in cases), 4)
        frozen = {
            collect_five_stage_ap_bounds.five_stage_sql(case["range_start"], case["range_end"])
            for case in collect_five_stage_ap_bounds.FIVE_STAGE_APS
        }
        self.assertTrue(all(
            collect_five_stage_ap_bounds.five_stage_sql(case["range_start"], case["range_end"]) not in frozen
            for case in cases
        ))

    def test_frozen_one_pass_gate_rejects_underestimate_and_qerror_regression(self):
        rows = [
            {"query_id": "a", "bounds": {"one_pass_mb": 1.0}},
            {"query_id": "b", "bounds": {"one_pass_mb": 2.0}},
        ]
        gate = train_xgb_one_pass.frozen_gate(
            rows,
            __import__("numpy").array([1.0, 2.0]),
            __import__("numpy").array([0.5, 2.0]),
        )
        self.assertFalse(gate["passed"])
        self.assertTrue(gate["queries"][0]["candidate_low_estimate"])

    def test_frozen_one_pass_gate_requires_absolute_qerror_target(self):
        rows = [
            {"query_id": "a", "bounds": {"one_pass_mb": 1.0}},
            {"query_id": "b", "bounds": {"one_pass_mb": 1.0}},
        ]
        gate = train_xgb_one_pass.frozen_gate(
            rows,
            __import__("numpy").array([2.0, 2.0]),
            __import__("numpy").array([1.6, 1.6]),
        )
        self.assertFalse(gate["passed"])
        self.assertEqual(gate["targets"]["per_query_qerror_max"], 1.5)

    def test_p0_plan_features_capture_operator_bytes_and_shape(self):
        payload = {"Plan": {
            "Node Type": "Sort", "Plan Rows": 10, "Plan Width": 20,
            "Sort Key": ["x"],
            "Plans": [{
                "Node Type": "Hash Join", "Plan Rows": 5, "Plan Width": 30,
                "Plans": [
                    {"Node Type": "Seq Scan", "Plan Rows": 100, "Plan Width": 4},
                    {"Node Type": "Hash", "Plan Rows": 7, "Plan Width": 8,
                     "Plans": [{"Node Type": "Seq Scan", "Plan Rows": 7, "Plan Width": 8}]},
                ],
            }],
        }}
        features = memory_bounds.plan_features(payload)
        self.assertEqual(features["join_build_rows_max"], 7.0)
        self.assertEqual(features["join_build_width_max"], 8.0)
        self.assertEqual(features["join_build_bytes_max"], 56.0)
        self.assertEqual(features["join_input_bytes_max"], 456.0)
        self.assertEqual(features["join_output_bytes_max"], 150.0)
        self.assertEqual(features["plan_depth"], 4)
        self.assertEqual(features["plan_bytes_max"], 400.0)
        self.assertEqual(features["seq_scan_nodes"], 2)
        self.assertIsNotNone(features["filter_selectivity_min"])


if __name__ == "__main__":
    unittest.main()
