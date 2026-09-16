# AMM evaluation framework

amm-eval is the reproducible evaluation harness for the AMM fork. A run
produces one self-contained evidence directory and one Markdown report. The
directory contains the exact gsbench configuration, workload parameters,
per-second database/host samples, raw gsbench output, normalized CSV data and
SVG charts.

The standard report covers:

- gsbench dataset scale and workload identity;
- total TPS and one-second TPS jitter (TPS_1S, min/max, p95 and jitter);
- AP admission, active grants, queueing, spill bytes and representative AP
  execution time;
- shared-buffer hit/read traffic, disk throughput, CPU and iowait;
- AMM GUCs and gs_amm_status() snapshots, including unavailable metrics.

## Quick start

The runner assumes that psycopg2 is available to the Python used for the
test and that gsbench is already built. It does not initialize a database or
change server configuration automatically.

    cd /path/to/gauss-amm
    python3 tools/amm-eval/run.py \
      --output-root evaluation-runs \
      --gauss-home "$GAUSSHOME" \
      --gsbench /path/to/gsbench \
      --host /tmp --port 15438 --dbname postgres --user "$USER" \
      --schema gsbench_size20 --scale 20 \
      --workload "gsbench run 101 --workers 2 --duration 60s" \
      --duration 60

For a normal gsbench binary, use --gsbench-bin and --scenario:

    python3 tools/amm-eval/run.py --gsbench-bin /opt/gsbench \
      --scenario 101 --workers 2 --duration 60 --scale 20

To regenerate a report from an existing run:

    python3 tools/amm-eval/report.py evaluation-runs/20260916T120000Z

For a database-free smoke test, feed JSONL samples to the report generator:

    python3 tools/amm-eval/report.py ./sample-run

Missing optional views or AMM functions become null in samples.jsonl and are
listed under report data quality; they are never converted to zero.
