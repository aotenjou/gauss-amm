import json
import tempfile
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from report import generate_report


class ReportTest(unittest.TestCase):
    def test_generates_standard_artifacts_and_jitter(self):
        with tempfile.TemporaryDirectory() as raw:
            run = Path(raw)
            (run / "manifest.json").write_text(json.dumps({
                "run_id": "fixture", "scale": "20", "workload": "gsbench-101",
                "schema": "gsbench_size20", "gsbench": "gsbench run 101",
            }), encoding="utf-8")
            samples = [
                {"epoch": 100.0, "xact_commit": 10, "temp_bytes": 0, "blks_read": 1, "blks_hit": 9},
                {"epoch": 101.0, "xact_commit": 110, "temp_bytes": 1024, "blks_read": 11, "blks_hit": 99,
                 "io_read_bytes": 1000, "io_write_bytes": 200, "cpu_iowait_pct": 1.5, "amm_status": "ok"},
                {"epoch": 102.0, "xact_commit": 260, "temp_bytes": 3072, "blks_read": 26, "blks_hit": 234,
                 "io_read_bytes": 2500, "io_write_bytes": 800, "cpu_iowait_pct": 3.0, "amm_status": "ok"},
            ]
            (run / "samples.jsonl").write_text("\n".join(json.dumps(row) for row in samples) + "\n", encoding="utf-8")
            ap = [
                {"status": "ok", "execution_ms": 12.0, "spill": False, "temp_written_blocks": 0},
                {"status": "ok", "execution_ms": 44.0, "spill": True, "temp_written_blocks": 9},
                {"status": "error", "error": "timeout"},
            ]
            (run / "ap-metrics.jsonl").write_text("\n".join(json.dumps(row) for row in ap) + "\n", encoding="utf-8")
            summary = generate_report(run)
            self.assertEqual(summary["sample_count"], 3)
            self.assertEqual(summary["ap"]["spill_queries"], 1)
            self.assertEqual(summary["ap"]["errors"], 1)
            self.assertAlmostEqual(summary["tps_1s"]["mean"], 125.0)
            for filename in ("report.md", "summary.json", "metrics.csv", "ap-metrics.csv",
                             "tps.svg", "tps-jitter.svg", "io.svg", "ap-execution.svg", "ap-spill.svg"):
                self.assertTrue((run / filename).is_file(), filename)


if __name__ == "__main__":
    unittest.main()
