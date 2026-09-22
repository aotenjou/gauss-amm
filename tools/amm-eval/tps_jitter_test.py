#!/usr/bin/env python3
import argparse, json, subprocess, time, statistics, os, sys
from datetime import datetime, timezone

def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

def shell(cmd, timeout=120):
    return subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=timeout)

def gsql_base(args):
    lib = f"{args.gshome}/lib:/opt/ammgs/compat/lib/x86_64-linux-gnu:/opt/ammgs/compat/openssl/usr/lib64:/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu"
    return (f"{args.loader} --library-path {lib} {args.gshome}/bin/gsql -d {args.dbname} "
            f"-p {args.port} -h 127.0.0.1 -U {args.user} -W {args.password} -t -A")

def query(base, sql, timeout=60):
    r = shell(f'{base} -c "{sql}"', timeout)
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    return r.stdout.strip()

def sample(base):
    txt = query(base, "select extract(epoch from clock_timestamp())::bigint, coalesce(sum(xact_commit),0) from pg_stat_database;")
    parts = txt.split("|")
    return {"epoch": int(float(parts[0])) if parts else int(time.time()),
            "xact": int(parts[1]) if len(parts) > 1 else 0}

def run_gsbench(args, workers, duration, out):
    cmd = (f"runuser -u ammdb -- sh -c 'cd /opt/ammgs/gsbench && GSBENCH_PASSWORD={args.password} "
           f"/opt/ammgs/gsbench/gsbench-v1.1.9-linux-amd64/bin/gsbench run 101 -c {args.gsbench_config} "
           f"--workers {workers} --duration {duration}s'")
    return subprocess.Popen(cmd, shell=True, text=True, stdout=out, stderr=subprocess.STDOUT)

def run_ap_loop(args, ap_sql, out):
    cmd = f"{gsql_base(args)} -f {ap_sql} > {out} 2>&1"
    return subprocess.Popen(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

def jitter(values):
    if len(values) < 3:
        return None
    mean = sum(values)/len(values)
    if mean <= 0:
        return None
    var = sum((v-mean)**2 for v in values)/len(values)
    return (var**0.5)/mean*100.0

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--gshome", default="/opt/ammgs/amm-50a1320")
    p.add_argument("--loader", default="/opt/ammgs/compat/ld-linux-x86-64.so.2")
    p.add_argument("--port", type=int, default=15439)
    p.add_argument("--dbname", default="postgres")
    p.add_argument("--user", default="ammdb")
    p.add_argument("--password", default="OpenGauss2026")
    p.add_argument("--tp-workers", type=int, default=8)
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--ap-sql-file", default="/tmp/amm-apsort.sql")
    p.add_argument("--ap-count", type=int, default=0)
    p.add_argument("--gsbench-config", default="/opt/ammgs/gsbench/amm.cfg")
    p.add_argument("--output", default="/tmp/tps_jitter.json")
    args = p.parse_args()
    base = gsql_base(args)
    out = open("/tmp/tps_jitter_tp.out", "w", encoding="utf-8")
    tp = run_gsbench(args, args.tp_workers, args.duration, out)
    time.sleep(3)
    aps = []
    for i in range(args.ap_count):
        aps.append(run_ap_loop(args, args.ap_sql_file, f"/tmp/tps_jitter_ap{i}.out"))
    samples = []
    end = time.time() + args.duration - 2
    while time.time() < end:
        try:
            samples.append(sample(base))
        except Exception as e:
            samples.append({"error": str(e)})
        time.sleep(2)
    tp.wait(timeout=120)
    for a in aps:
        try:
            a.wait(timeout=60)
        except subprocess.TimeoutExpired:
            a.kill()
    good = [s for s in samples if "error" not in s]
    tps = []
    for prev, cur in zip(good, good[1:]):
        dt = cur["epoch"] - prev["epoch"]
        if dt > 0 and dt <= 10:
            tps.append(max(0, cur["xact"] - prev["xact"]) / dt)
    result = {"samples": good, "tps_windows": tps, "tps_jitter_pct": jitter(tps),
              "tp_workers": args.tp_workers, "ap_count": args.ap_count,
              "duration": args.duration, "tps_mean": sum(tps)/len(tps) if tps else 0}
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps({"tps_mean": result["tps_mean"], "tps_jitter_pct": result["tps_jitter_pct"],
                      "windows": len(tps)}, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
