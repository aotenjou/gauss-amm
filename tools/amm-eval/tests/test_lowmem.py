import json
import tempfile
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lowmem import MemoryCgroupV1, STAGES, inspect_plan, parse_status, percentile, summarize_stage


class LowmemTest(unittest.TestCase):
    def test_status_and_plan_parsers(self):
        status = parse_status("amm_enabled=true ap_queue_len=3 reason=tp_pressure")
        self.assertTrue(status["amm_enabled"])
        self.assertEqual(status["ap_queue_len"], 3)
        plan = inspect_plan({"Node Type": "Sort", "Sort Method": "external merge",
                             "Sort Space Type": "Disk", "Sort Space Used": 123,
                             "Temp Written Blocks": 4})
        self.assertTrue(plan["spill"])
        self.assertTrue(plan["external_merge"])
        self.assertEqual(plan["disk_kb"], 123)

    def test_cgroup_fixture_and_summary(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            for name, value in (("memory.limit_in_bytes", "1073741824"),
                                ("memory.usage_in_bytes", "10"),
                                ("memory.max_usage_in_bytes", "20"),
                                ("memory.failcnt", "0"),
                                ("memory.oom_control", "oom_kill 0"),
                                ("memory.stat", "cache 4\nrss 8\n")):
                (root / name).write_text(value)
            cg = MemoryCgroupV1(root, "case", 1073741824)
            cg.create()
            for name, value in (("memory.limit_in_bytes", "1073741824"),
                                ("memory.usage_in_bytes", "10"),
                                ("memory.max_usage_in_bytes", "20"),
                                ("memory.failcnt", "0"),
                                ("memory.oom_control", "oom_kill 0"),
                                ("memory.stat", "cache 4\nrss 8\n")):
                (cg.path / name).write_text(value)
            self.assertEqual(cg.sample()["cgroup_limit_bytes"], 1073741824)
            for child in cg.path.iterdir():
                child.unlink()
            cg.remove()
            stage = root / "stage"
            stage.mkdir()
            (stage / "tp.jsonl").write_text(json.dumps({"status": "ok", "latency_ms": 10}) + "\n")
            (stage / "ap.jsonl").write_text(json.dumps({"status": "ok", "spill": True}) + "\n")
            (stage / "samples.jsonl").write_text("\n".join([
                json.dumps({"server_rss_bytes": 4, "cgroup_peak_bytes": 8, "cgroup_failcnt": 0, "temp_bytes": 1}),
                json.dumps({"server_rss_bytes": 9, "cgroup_peak_bytes": 12, "cgroup_failcnt": 0, "temp_bytes": 7}),
            ]) + "\n")
            summary = summarize_stage(stage, STAGES[0])
            self.assertEqual(summary["tp"]["p95_ms"], 10)
            self.assertEqual(summary["temp_bytes_delta"], 6)
            self.assertFalse(summary["memory"]["oom"])

    def test_percentile(self):
        self.assertAlmostEqual(percentile([1, 2, 3, 4], .95), 3.85)
        self.assertIsNone(percentile([], .95))


if __name__ == "__main__":
    unittest.main()
