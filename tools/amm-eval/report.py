#!/usr/bin/env python3
"""Build the standard AMM report from one evidence directory."""
from __future__ import annotations

import csv
import json
import math
import re
import statistics
import sys
from pathlib import Path
from typing import Any, Iterable
from xml.sax.saxutils import escape


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def number(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def percentile(values: Iterable[float], ratio: float) -> float | None:
    items = sorted(values)
    if not items:
        return None
    if len(items) == 1:
        return items[0]
    index = (len(items) - 1) * ratio
    low, high = math.floor(index), math.ceil(index)
    if low == high:
        return items[low]
    return items[low] + (items[high] - items[low]) * (index - low)


def derive_samples(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for row in rows:
        current = dict(row)
        epoch = number(row.get("epoch"))
        if previous is not None:
            dt = max((epoch or 0) - (number(previous.get("epoch")) or 0), 0.001)
            for source, target in (("xact_total", "tps_1s"), ("xact_commit", "tps_1s_fallback"), ("temp_bytes", "temp_bytes_sec"),
                                   ("blks_read", "blks_read_sec"), ("blks_hit", "blks_hit_sec")):
                before, after = number(previous.get(source)), number(row.get(source))
                current[target] = max(0.0, after - before) / dt if before is not None and after is not None else None
            if current.get("tps_1s") is None:
                current["tps_1s"] = current.get("tps_1s_fallback")
            for source, target in (("io_read_bytes", "io_read_bytes_sec"), ("io_write_bytes", "io_write_bytes_sec")):
                before, after = number(previous.get(source)), number(row.get(source))
                current[target] = max(0.0, after - before) / dt if before is not None and after is not None else None
            for source, target in (("disk_read_sectors", "disk_read_bytes_sec"), ("disk_write_sectors", "disk_write_bytes_sec"),
                                   ("disk_busy_ms", "disk_busy_ms_sec")):
                before, after = number(previous.get(source)), number(row.get(source))
                scale = 512 if "sectors" in source else 1
                current[target] = max(0.0, after - before) * scale / dt if before is not None and after is not None else None
        else:
            for key in ("tps_1s", "tps_1s_fallback", "temp_bytes_sec", "blks_read_sec", "blks_hit_sec",
                        "io_read_bytes_sec", "io_write_bytes_sec", "disk_read_bytes_sec",
                        "disk_write_bytes_sec", "disk_busy_ms_sec"):
                current[key] = None
        if current.get("io_read_bytes_sec") is None:
            current["io_read_bytes_sec"] = current.get("disk_read_bytes_sec")
        if current.get("io_write_bytes_sec") is None:
            current["io_write_bytes_sec"] = current.get("disk_write_bytes_sec")
        result.append(current)
        previous = row
    tps = [number(row.get("tps_1s")) for row in result]
    valid = [value for value in tps if value is not None]
    mean = statistics.fmean(valid) if valid else None
    for row in result:
        value = number(row.get("tps_1s"))
        row["tps_jitter"] = abs(value - mean) if value is not None and mean is not None else None
        row["tps_jitter_pct"] = (100 * row["tps_jitter"] / mean) if row["tps_jitter"] is not None and mean else None
    return result


def parse_gsbench_operations(path: Path) -> int | None:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    values = re.findall(r'"metric"\s*:\s*"operations"[^}]*?"actual"\s*:\s*([0-9]+)', text)
    return int(values[-1]) if values else None


def chart(path: Path, title: str, ylabel: str, series: list[tuple[str, list[float | None]]]) -> None:
    width, height, left, right, top, bottom = 960, 360, 64, 20, 42, 52
    values = [value for _, points in series for value in points if value is not None]
    if not values:
        values = [0.0]
    low, high = min(values), max(values)
    if high <= low:
        high = low + 1.0
    plot_w, plot_h = width - left - right, height - top - bottom

    def point(index: int, value: float) -> tuple[float, float]:
        x = left + plot_w * index / max(len(series[0][1]) - 1, 1)
        y = top + (high - value) * plot_h / (high - low)
        return x, y

    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">']
    parts.append('<rect width="100%" height="100%" fill="white"/>')
    parts.append(f'<text x="{left}" y="24" font-family="sans-serif" font-size="16" font-weight="bold">{escape(title)}</text>')
    parts.append(f'<text x="12" y="{top + plot_h / 2}" transform="rotate(-90 12 {top + plot_h / 2})" font-family="sans-serif" font-size="11">{escape(ylabel)}</text>')
    parts.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{width-right}" y2="{top + plot_h}" stroke="#444"/>')
    parts.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="#444"/>')
    colors = ["#1769aa", "#c62828", "#2e7d32", "#6a1b9a"]
    for index, (name, points) in enumerate(series):
        poly: list[str] = []
        for point_index, value in enumerate(points):
            if value is not None:
                x, y = point(point_index, value)
                poly.append(f"{x:.1f},{y:.1f}")
        if len(poly) > 1:
            points_text = " ".join(poly)
            parts.append(f'<polyline fill="none" stroke="{colors[index % len(colors)]}" stroke-width="2" points="{points_text}"/>')
        parts.append(f'<text x="{left + index * 150}" y="{height - 16}" font-family="sans-serif" font-size="11" fill="{colors[index % len(colors)]}">{escape(name)}</text>')
    parts.append(f'<text x="{left}" y="{height - 2}" font-family="sans-serif" font-size="10">sample (1s)</text></svg>')
    path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def generate_report(run_dir: Path) -> dict[str, Any]:
    run_dir = Path(run_dir)
    manifest = read_json(run_dir / "manifest.json", {})
    samples = derive_samples(read_jsonl(run_dir / "samples.jsonl"))
    ap_rows = read_jsonl(run_dir / "ap-metrics.jsonl")
    write_csv(run_dir / "metrics.csv", samples)
    write_csv(run_dir / "ap-metrics.csv", ap_rows)

    tps = [number(row.get("tps_1s")) for row in samples]
    tps_values = [value for value in tps if value is not None]
    jitter = [number(row.get("tps_jitter")) for row in samples]
    jitter_values = [value for value in jitter if value is not None]
    ap_exec = [number(row.get("execution_ms")) for row in ap_rows if row.get("status") == "ok"]
    spills = [row for row in ap_rows if row.get("spill") is True]
    ap_active = [number(row.get("ap_active")) for row in samples]
    ap_load = [number(row.get("ap_exec_ms")) for row in samples]
    summary = {
        "sample_count": len(samples), "ap_query_count": len(ap_rows),
        "gsbench_operations": parse_gsbench_operations(run_dir / "gsbench.log"),
        "tps_1s": {"mean": statistics.fmean(tps_values) if tps_values else None,
                   "min": min(tps_values) if tps_values else None,
                   "max": max(tps_values) if tps_values else None,
                   "p95": percentile(tps_values, .95),
                   "jitter_mean_abs": statistics.fmean(jitter_values) if jitter_values else None,
                   "jitter_p95_abs": percentile(jitter_values, .95)},
        "ap": {"ok": len(ap_exec), "errors": sum(row.get("status") == "error" for row in ap_rows),
               "spill_queries": len(spills), "spill_rate": len(spills) / len(ap_rows) if ap_rows else None,
               "execution_mean_ms": statistics.fmean(ap_exec) if ap_exec else None,
               "execution_p95_ms": percentile(ap_exec, .95), "execution_max_ms": max(ap_exec) if ap_exec else None,
               "temp_written_blocks": sum(number(row.get("temp_written_blocks")) or 0 for row in ap_rows),
               "active_peak": max((value for value in ap_active if value is not None), default=None),
               "load_ms_peak": max((value for value in ap_load if value is not None), default=None)},
        "io": {"read_bytes_sec_max": max((number(row.get("io_read_bytes_sec")) or 0 for row in samples), default=None),
               "write_bytes_sec_max": max((number(row.get("io_write_bytes_sec")) or 0 for row in samples), default=None),
               "busy_ms_sec_max": max((number(row.get("disk_busy_ms_sec")) or 0 for row in samples), default=None),
               "iowait_pct_max": max((number(row.get("cpu_iowait_pct")) or 0 for row in samples), default=None)},
        "data_quality": {"samples_available": bool(samples), "ap_metrics_available": bool(ap_rows),
                         "amm_status_available": any(row.get("amm_status") not in (None, "") for row in samples),
                         "amm_status_samples": sum(row.get("amm_status") not in (None, "") for row in samples),
                         "optional_io_view_available": any(row.get("io_read_bytes") is not None for row in samples)},
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    chart(run_dir / "tps.svg", "gsbench TPS (one-second windows)", "transactions / second", [("TPS_1S", tps)])
    chart(run_dir / "tps-jitter.svg", "TPS one-second jitter", "absolute deviation from mean TPS", [("jitter", jitter)])
    chart(run_dir / "io.svg", "I/O load", "bytes / second", [("read", [number(row.get("io_read_bytes_sec")) for row in samples]), ("write", [number(row.get("io_write_bytes_sec")) for row in samples])])
    chart(run_dir / "ap-execution.svg", "AP execution time", "milliseconds", [("execution_ms", [number(row.get("execution_ms")) for row in ap_rows])])
    chart(run_dir / "ap-spill.svg", "AP spill events", "spill (0/1)", [("spill", [1.0 if row.get("spill") is True else 0.0 for row in ap_rows])])

    quality = summary["data_quality"]
    t = summary["tps_1s"]
    a = summary["ap"]
    lines = [
        "# AMM evaluation report", "", f"- Run: {manifest.get('run_id', run_dir.name)}",
        f"- Started: {manifest.get('start', 'unknown')}", f"- Scale: {manifest.get('scale', 'unknown')}",
        f"- Workload: {manifest.get('workload', 'unknown')}", f"- Schema: {manifest.get('schema', 'unknown')}",
        f"- gsbench command: {manifest.get('gsbench', 'unknown')}", "",
        "## Summary", "", "| Metric | Value |", "|---|---:|",
        f"| TPS mean / min / max | {fmt(t.get('mean'))} / {fmt(t.get('min'))} / {fmt(t.get('max'))} |",
        f"| TPS p95 | {fmt(t.get('p95'))} |",
        f"| TPS 1s jitter mean / p95 | {fmt(t.get('jitter_mean_abs'))} / {fmt(t.get('jitter_p95_abs'))} |",
        f"| AP executions / errors | {a['ok']} / {a['errors']} |",
        f"| AP active peak / sampled execution load peak (ms) | {fmt(a['active_peak'])} / {fmt(a['load_ms_peak'])} |",
        f"| AP spill queries / rate | {a['spill_queries']} / {fmt(a['spill_rate'], percent=True)} |",
        f"| AP execution mean / p95 / max (ms) | {fmt(a['execution_mean_ms'])} / {fmt(a['execution_p95_ms'])} / {fmt(a['execution_max_ms'])} |",
        f"| AP temp written blocks | {fmt(a['temp_written_blocks'])} |", "",
        "## Curves", "", "![TPS](tps.svg)", "", "![TPS jitter](tps-jitter.svg)", "",
        "![I/O](io.svg)", "", "![AP execution](ap-execution.svg)", "", "![AP spill](ap-spill.svg)", "",
        "## Data quality", "",
        f"- Per-second samples: {'available' if quality['samples_available'] else 'missing'}",
        f"- AP metrics: {'available' if quality['ap_metrics_available'] else 'missing'}",
        f"- AMM status: {'available' if quality['amm_status_available'] else 'missing'}",
        f"- AMM status samples: {quality['amm_status_samples']}",
        f"- pg_stat_io: {'available' if quality['optional_io_view_available'] else 'missing (server does not expose it)'}",
        "", "Raw evidence is kept in samples.jsonl, ap-metrics.jsonl, gsbench.log, metrics.csv and ap-metrics.csv.",
    ]
    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


def fmt(value: Any, percent: bool = False) -> str:
    value = number(value)
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%" if percent else f"{value:.2f}"


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: report.py RUN_DIRECTORY")
    generate_report(Path(sys.argv[1]))
