#!/usr/bin/env python3
"""Five-stage AMM acceptance harness for openGauss on the target host.

The driver is meant to run directly on the target host (Ubuntu 20.04,
1 vCPU / ~2 GiB).  It does not use psycopg2 because the retained openGauss
instance may only support its own SASL authentication.  All SQL is executed
through the target's gsql binary via the compatibility loader.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

DEFAULT_GSHOME = "/opt/ammgs/amm-50a1320"
DEFAULT_LOADER = "/opt/ammgs/compat/ld-linux-x86-64.so.2"
DEFAULT_LIBPATH = (
    "{gshome}/lib:/opt/ammgs/compat/lib/x86_64-linux-gnu:"
    "/opt/ammgs/compat/openssl/usr/lib64:/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu"
)
DEFAULT_GSBENCH = "/opt/ammgs/gsbench/gsbench-v1.1.9-linux-amd64/bin/gsbench"
DEFAULT_GSBENCH_DIR = "/opt/ammgs/gsbench"


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def shell(cmd, timeout=90):
    proc = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed rc=%s: %s\nstdout=%s\nstderr=%s" %
            (proc.returncode, cmd, proc.stdout[:500], proc.stderr[:500]))
    return proc.stdout


def make_gsql_base(args):
    libpath = DEFAULT_LIBPATH.format(gshome=args.gshome)
    return (
        "%s --library-path %s %s/bin/gsql -d %s -p %s -h 127.0.0.1 "
        "-U %s -W %s -t -A" %
        (args.loader, libpath, args.gshome, args.dbname, args.port, args.user, args.password)
    )


def gsql_query(base, sql, timeout=90):
    return shell("%s -c \"%s\"" % (base, sql), timeout)


def parse_status(text):
    fields = {}
    for token in text.split():
        if "=" not in token:
            continue
        key, raw = token.split("=", 1)
        if raw.lower() in ("true", "false"):
            fields[key] = raw.lower() == "true"
        else:
            try:
                fields[key] = int(raw)
            except ValueError:
                try:
                    fields[key] = float(raw)
                except ValueError:
                    fields[key] = raw
    return fields


def amm_status(base):
    return parse_status(gsql_query(base, "select pg_catalog.gs_amm_status();"))


def sample(base):
    st = amm_status(base)
    db_text = gsql_query(
        base,
        "select extract(epoch from clock_timestamp())::bigint, "
        "coalesce(sum(xact_commit),0), "
        "coalesce(sum(blks_hit),0), coalesce(sum(blks_read),0), "
        "coalesce(sum(temp_bytes),0), coalesce(sum(temp_files),0) "
        "from pg_stat_database;")
    epoch = int(time.time())
    xact_commit = 0
    blks_hit = 0
    blks_read = 0
    temp_bytes = 0
    temp_files = 0
    try:
        parts = db_text.strip().split("|")
        if len(parts) >= 2:
            epoch = int(float(parts[0]))
            xact_commit = int(parts[1])
        if len(parts) >= 4:
            blks_hit = int(parts[2])
            blks_read = int(parts[3])
        if len(parts) >= 6:
            temp_bytes = int(parts[4])
            temp_files = int(parts[5])
    except Exception:
        pass
    return {
        "ts": now_iso(),
        "epoch": epoch,
        "xact_commit": xact_commit,
        "blks_hit": blks_hit,
        "blks_read": blks_read,
        "temp_bytes": temp_bytes,
        "temp_files": temp_files,
        "tp_buffer_hit_pct": st.get("tp_buffer_hit_pct", st.get("tp_buffer_hit_baseline_pct")),
        "active_mb": st.get("active_mb", 0),
        "ap_borrow_count": st.get("ap_borrow_count", 0),
        "last_grant_mb": st.get("last_grant_mb", 0),
        "last_admission_target_mb": st.get("last_admission_target_mb", 0),
        "ap_active_granules": st.get("ap_active_granules", 0),
        "ap_queue_len": st.get("ap_queue_len", 0),
        "ap_queue_cancel_count": st.get("ap_queue_cancel_count", 0),
        "ap_downgrade_pending": st.get("ap_downgrade_pending", 0),
        "tp_cpu_util_pct": st.get("tp_cpu_util_pct", 0),
        "tp_pressure_hot": bool(st.get("tp_pressure_hot")),
        "tp_pressure_state": st.get("tp_pressure_state", ""),
        "tp_ap_stop_requested": st.get("tp_ap_stop_requested", 0),
        "tp_ap_stop_completed": st.get("tp_ap_stop_completed", 0),
        "tp_sb_restore_granules": st.get("tp_sb_restore_granules", 0),
        "last_backpressure_reason": st.get("last_backpressure_reason", ""),
        "last_supply_source": st.get("last_supply_source", ""),
        "ap_granted_bytes_total": st.get("ap_granted_bytes_total", 0),
        "active_ap_count": st.get("active_ap_count", 0),
        "ap_multipass_only": bool(st.get("ap_multipass_only")),
    }


def inspect_plan_node(node, out):
    if not isinstance(node, dict):
        return
    method = str(node.get("Sort Method", ""))
    space = str(node.get("Sort Space Type", "")).lower()
    out.setdefault("sort_methods", [])
    if method:
        out["sort_methods"].append(method)
    if "external merge" in method.lower():
        out["external_merge"] = True
        out["spill"] = True
    if space == "disk":
        out["spill"] = True
    out["temp_written_blocks"] = out.get("temp_written_blocks", 0) + int(
        node.get("Temp Written Blocks") or 0)
    out["temp_read_blocks"] = out.get("temp_read_blocks", 0) + int(
        node.get("Temp Read Blocks") or 0)
    out["disk_kb"] = out.get("disk_kb", 0) + (
        int(node.get("Sort Space Used") or 0) if space == "disk" else 0)
    for child in node.get("Plans", []):
        inspect_plan_node(child, out)


def parse_explain_output(path):
    text = ""
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        pass
    base_result = {"error": None, "spill": False, "external_merge": False,
                   "sort_methods": [], "temp_written_blocks": 0,
                   "temp_read_blocks": 0, "disk_kb": 0, "total_time_ms": None}
    if not text.strip():
        base_result["error"] = "empty explain output"
        return base_result
    # The JSON document may span many lines.  Extract from the first '['
    # to the last ']' and parse that slice.
    start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end < start:
        base_result["error"] = "explain output has no JSON array"
        return base_result
    try:
        value = json.loads(text[start:end + 1])
    except Exception as exc:
        base_result["error"] = "explain output JSON parse failed: %s" % exc
        return base_result
    if isinstance(value, list):
        value = value[0]
    top = value
    if isinstance(value, dict):
        value = value.get("Plan", value)
    inspect_plan_node(value, base_result)
    if isinstance(top, dict):
        base_result["total_time_ms"] = top.get("Total Runtime") or top.get("Total Time")
    return base_result


def start_gsbench(args, workers, duration, out_file):
    cmd = (
        "runuser -u ammdb -- sh -c 'cd %s && GSBENCH_PASSWORD=%s %s run 101 -c %s/amm.cfg "
        "--workers %s --duration %ss'" %
        (DEFAULT_GSBENCH_DIR, args.password, DEFAULT_GSBENCH, DEFAULT_GSBENCH_DIR,
         workers, duration)
    )
    return subprocess.Popen(cmd, shell=True, text=True, stdout=out_file,
                            stderr=subprocess.STDOUT)


def start_ap(base, ap_sql_file, out_file):
    return subprocess.Popen(
        "%s -f %s > %s 2>&1" % (base, ap_sql_file, out_file),
        shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def tps_from_samples(samples):
    windows = []
    for prev, cur in zip(samples, samples[1:]):
        elapsed = (cur.get("epoch") or 0) - (prev.get("epoch") or 0)
        if elapsed <= 0 or elapsed > 10:
            continue
        delta = max(0, (cur.get("xact_commit") or 0) - (prev.get("xact_commit") or 0))
        windows.append(delta / float(elapsed))
    return windows


def jitter_pct(values):
    if not values or len(values) < 3:
        return None
    mean = sum(values) / len(values)
    if mean <= 0:
        return None
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return (variance ** 0.5) / mean * 100.0


def write_ap_sql(args, work_mem=None):
    path = args.ap_sql_file
    if work_mem is None:
        work_mem = args.ap_work_mem
    role_line = "set gs_amm_workload_role=ap;\n" if args.kernel == "amm" else ""
    operator = getattr(args, "operator", "sort")
    queries = {
        "sort": ("select id, group_id, sort_key, payload from gsbench_20g.sort_data "
                 "where id <= %s order by payload, sort_key desc, id"),
        "hash_join": ("select o.id, i.product_id, i.amount from gsbench_20g.orders o "
                      "join gsbench_20g.order_items i on o.id=i.order_id where o.id <= %s"),
        "hash_agg": ("select group_id, count(*), sum(sort_key) from gsbench_20g.sort_data "
                     "where id <= %s group by group_id"),
        "window_agg": ("select id, group_id, sort_key, row_number() over "
                       "(partition by group_id order by sort_key desc) "
                       "from gsbench_20g.sort_data where id <= %s"),
        "materialize": ("with s as materialized (select id, group_id, sort_key from "
                        "gsbench_20g.sort_data where id <= %s) "
                        "select a.id, b.group_id from s a join s b on a.group_id=b.group_id"),
    }
    q = queries.get(operator, queries["sort"])
    content = (role_line + "set work_mem='%sMB';\n" % work_mem +
               "explain (analyze, buffers, format json) " + q + ";\n") % (args.ap_scan_rows,)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)
    return path
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)
    return path


def run_low_stage(args, base, name, ap_workers, stage_secs, results, ap_scan_rows=None, work_mem=None):
    if ap_scan_rows is not None:
        saved = args.ap_scan_rows
        args.ap_scan_rows = ap_scan_rows
    ap_sql = write_ap_sql(args, work_mem)
    if ap_scan_rows is not None:
        args.ap_scan_rows = saved
    out_files = ["/tmp/five_stage_%s_ap%d.json" % (name, i)
                 for i in range(1, ap_workers + 1)]
    samples = []
    tp_out = open("/tmp/five_stage_%s_tp.out" % name, "w", encoding="utf-8")
    tp = start_gsbench(args, args.tp_low_workers, int(stage_secs), tp_out)
    time.sleep(3)
    aps = [start_ap(base, ap_sql, out_files[i]) for i in range(ap_workers)]
    deadline = time.time() + stage_secs - 1
    while time.time() < deadline:
        try:
            samples.append(sample(base))
        except Exception as exc:
            samples.append({"error": str(exc)})
        time.sleep(2)
    tp.wait(timeout=180)
    tp_out.close()
    for ap in aps:
        try:
            ap.wait(timeout=90)
        except subprocess.TimeoutExpired:
            ap.terminate()
            try:
                ap.wait(timeout=5)
            except subprocess.TimeoutExpired:
                ap.kill()
    ap_summaries = [parse_explain_output(path) for path in out_files]
    results.append({
        "stage": name,
        "tp_workers": args.tp_low_workers,
        "ap_workers": ap_workers,
        "samples": samples,
        "ap": ap_summaries,
    })
    return results[-1]


def run_surge_stage(args, base, stage_secs, results, ap_scan_rows=600000):
    saved = args.ap_scan_rows
    args.ap_scan_rows = ap_scan_rows
    ap_sql = write_ap_sql(args)
    args.ap_scan_rows = saved
    out_files = ["/tmp/five_stage_stage5_ap%d.json" % i for i in (1, 2)]
    samples = []
    tp_out = open("/tmp/five_stage_stage5_tp_low.out", "w", encoding="utf-8")
    tp_low = start_gsbench(args, args.tp_low_workers, int(stage_secs * 0.5), tp_out)
    time.sleep(3)
    aps = [start_ap(base, ap_sql, out_files[i]) for i in range(2)]
    time.sleep(6)
    tp_out.close()
    try:
        tp_low.terminate()
        tp_low.wait(timeout=10)
    except subprocess.TimeoutExpired:
        tp_low.kill()
    tp_high_out = open("/tmp/five_stage_stage5_tp_high.out", "w", encoding="utf-8")
    tp_high = start_gsbench(args, args.tp_high_workers, max(10, int(stage_secs * 0.5)),
                            tp_high_out)
    deadline = time.time() + stage_secs
    while time.time() < deadline:
        try:
            samples.append(sample(base))
        except Exception as exc:
            samples.append({"error": str(exc)})
        time.sleep(2)
    tp_high.wait(timeout=180)
    tp_high_out.close()
    for ap in aps:
        try:
            ap.wait(timeout=90)
        except subprocess.TimeoutExpired:
            ap.terminate()
            try:
                ap.wait(timeout=5)
            except subprocess.TimeoutExpired:
                ap.kill()
    ap_summaries = [parse_explain_output(path) for path in out_files]
    results.append({
        "stage": "stage5_tp_surge",
        "tp_workers": args.tp_high_workers,
        "ap_workers": 2,
        "samples": samples,
        "ap": ap_summaries,
    })
    return results[-1]


def assert_stage(args, res):
    ok = []
    fail = []
    samples = [s for s in res["samples"] if "error" not in s]
    name = res["stage"]

    def check(label, condition, detail=""):
        if condition:
            ok.append(label)
        else:
            fail.append(label + ((": " + detail) if detail else ""))

    if name == "stage1_memory_rich":
        spills = [a for a in res["ap"] if a.get("spill")]
        grants = [s.get("ap_granted_bytes_total", 0) for s in samples]
        check("ap_granted > 0", max(grants, default=0) > 0)
        check("sort_memory_no_spill", len(spills) == 0, "spill=%d" % len(spills))

    elif name == "stage2_drain_sb":
        first_active = samples[0].get("active_mb", 0) if samples else 0
        min_active = min((s.get("active_mb", 0) for s in samples), default=0)
        borrows = [s.get("ap_borrow_count", 0) for s in samples]
        cancels = [s.get("ap_queue_cancel_count", 0) for s in samples]
        spills = [a for a in res["ap"] if a.get("spill")]
        check("ap_borrow_count > 0", max(borrows, default=0) > 0)
        check("active_mb_dropped", min_active < first_active,
              "first=%d min=%d" % (first_active, min_active))
        check("no_ap_cancel", max(cancels, default=0) == 0)
        check("no_spill", len(spills) == 0, "spill=%d" % len(spills))

    elif name == "stage3_floor":
        active = [s.get("active_mb", 0) for s in samples]
        min_active = min(active, default=0)
        cancels = [s.get("ap_queue_cancel_count", 0) for s in samples]
        grants = [s.get("last_grant_mb", 0) for s in samples if s.get("active_ap_count", 0) > 0]
        floor = args.shared_buffers_min_mb
        check("active_mb_at_floor", min_active <= floor + args.granule_mb,
              "min_active=%d floor=%d" % (min_active, floor))
        check("no_ap_cancel", max(cancels, default=0) == 0)
        check("per_session_grant_compressed", grants and max(grants) <= 8,
              "max_grant_mb=%s" % (max(grants) if grants else 0))

    elif name == "stage4_backpressure_queue":
        queues = [s.get("ap_queue_len", 0) for s in samples]
        cancels = [s.get("ap_queue_cancel_count", 0) for s in samples]
        check("ap_queue_len > 0", max(queues, default=0) > 0)
        check("no_ap_cancel", max(cancels, default=0) == 0)

    elif name == "stage5_tp_surge":
        cpu = [s.get("tp_cpu_util_pct", 0) for s in samples]
        cancels = [s.get("ap_queue_cancel_count", 0) for s in samples]
        stops = [s.get("tp_ap_stop_requested", 0) for s in samples]
        restore = [s.get("tp_sb_restore_granules", 0) for s in samples]
        hot = [s.get("tp_pressure_hot") for s in samples]
        check("tp_cpu_util_pct_ge_threshold",
              max(cpu, default=0) >= args.tp_cpu_pressure_threshold_pct,
              "max_cpu=%d" % max(cpu, default=0))
        check("hot_recovery_triggered", any(hot))
        check("tp_sb_restore_granules > 0", max(restore, default=0) > 0)
        check("no_ap_cancel", max(cancels, default=0) == 0)
        check("no_ap_stop", max(stops, default=0) == 0)

    tps = tps_from_samples(samples)
    jit = jitter_pct(tps)
    if jit is None:
        fail.append("tp_jitter_lt_3pct: not enough windows")
    elif jit < 3.0:
        ok.append("tp_jitter_lt_3pct (%.2f%%)" % jit)
    else:
        fail.append("tp_jitter_lt_3pct (%.2f%%)" % jit)

    res["tps_windows"] = tps
    res["tps_jitter_pct"] = jit
    res["assertions"] = {"pass": ok, "fail": fail}
    return res


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--gshome", default=os.environ.get("GAUSSHOME", DEFAULT_GSHOME))
    p.add_argument("--loader", default=DEFAULT_LOADER)
    p.add_argument("--dbname", default="postgres")
    p.add_argument("--port", type=int, default=15439)
    p.add_argument("--user", default="ammdb")
    p.add_argument("--password", default="OpenGauss2026")
    p.add_argument("--tp-low-workers", type=int, default=2)
    p.add_argument("--tp-high-workers", type=int, default=8)
    p.add_argument("--tp-cpu-pct-threshold", dest="tp_cpu_pressure_threshold_pct",
                   type=int, default=60)
    p.add_argument("--shared-buffers-min-mb", dest="shared_buffers_min_mb", type=int, default=128)
    p.add_argument("--granule-mb", type=int, default=8)
    p.add_argument("--stage-seconds", type=float, default=30.0)
    p.add_argument("--ap-scan-rows", type=int, default=600000)
    p.add_argument("--ap-work-mem", type=int, default=64)
    p.add_argument("--operator", choices=["sort", "hash_join", "hash_agg",
                   "window_agg", "materialize"], default="sort")
    p.add_argument("--kernel", choices=["amm", "native"], default="amm")
    p.add_argument("--ap-sql-file", default="/tmp/five_stage_ap.sql")
    p.add_argument("--output", default="/tmp/five_stage_result.json")
    args = p.parse_args()

    base = make_gsql_base(args)

    st = amm_status(base)
    if st.get("ap_multipass_only"):
        print("WARNING: ap_multipass_only=true; low-TP cache admission may clear it.",
              file=sys.stderr)
    if int(st.get("active_mb", 0)) < 240:
        print("WARNING: active_mb=%s; SB is already below baseline." % st.get("active_mb"),
              file=sys.stderr)

    results = []
    run_low_stage(args, base, "stage1_memory_rich", 1, args.stage_seconds, results,
                  ap_scan_rows=150000, work_mem=512)
    run_low_stage(args, base, "stage2_drain_sb", 2, args.stage_seconds, results,
                  ap_scan_rows=300000, work_mem=512)
    run_low_stage(args, base, "stage3_floor", 4, args.stage_seconds, results,
                  ap_scan_rows=600000, work_mem=64)
    run_low_stage(args, base, "stage4_backpressure_queue", 8, args.stage_seconds, results,
                  ap_scan_rows=600000, work_mem=64)
    run_surge_stage(args, base, args.stage_seconds, results)

    for res in results:
        assert_stage(args, res)

    payload = {
        "generated_at": now_iso(),
        "args": vars(args),
        "stages": results,
    }
    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
    print(json.dumps(payload, indent=2, default=str)[:6000])
    failed = sum(len(r["assertions"]["fail"]) for r in results)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
