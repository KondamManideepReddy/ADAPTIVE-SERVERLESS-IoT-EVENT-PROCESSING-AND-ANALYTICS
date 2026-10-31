#!/usr/bin/env python3
"""Live demo: run ONE workload with several strategies and print a side-by-side comparison.

  python demo.py                       # real AWS Lambda + DynamoDB, burst workload, naive vs adaptive (~2 min)
  python demo.py --transport mqtt      # same, but through a real Mosquitto broker on localhost:1883
  python demo.py --backend local       # offline fallback: emulated Lambda + SQLite (no AWS needed)
  python demo.py --workload high --strategies naive filter batch adaptive --time-scale 0.5

Each strategy runs in its own process (see iot/experiment.py); results go to results_demo/.
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--backend", default="aws", choices=["aws", "local"])
ap.add_argument("--workload", default="burst", choices=["low", "medium", "high", "burst"])
ap.add_argument("--strategies", nargs="+", default=["naive", "adaptive"], choices=["naive", "filter", "batch", "adaptive"])
ap.add_argument("--time-scale", type=float, default=0.5, help="1.0 = full-length workload (20-24 s), 0.5 = half")
ap.add_argument("--transport", default="inproc", choices=["inproc", "mqtt"])
ap.add_argument("--out", default="results_demo")
ap.add_argument("--seed", type=int, default=7)
ap.add_argument("--aws-region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-2")
ap.add_argument("--aws-function", default=os.environ.get("AWS_FUNCTION", "iot-demo"))
ap.add_argument("--dynamodb-table", default=os.environ.get("DYNAMODB_TABLE", "iot-demo-events"))
a = ap.parse_args()

root = Path(__file__).parent
print(f"\n=== {a.workload} workload | backend={a.backend} | transport={a.transport} | strategies: {', '.join(a.strategies)} ===\n")

for st in a.strategies:
    cmd = [sys.executable, "-m", "iot.experiment", "--workload", a.workload, "--strategy", st, "--seed", str(a.seed),
           "--out", a.out, "--time-scale", str(a.time_scale), "--backend", a.backend, "--transport", a.transport]
    if a.backend == "aws":
        cmd += ["--aws-region", a.aws_region, "--aws-function", a.aws_function, "--dynamodb-table", a.dynamodb_table]
    if subprocess.run(cmd, cwd=root).returncode:
        sys.exit(f"run failed: {' '.join(cmd)}")

rows = []
for st in a.strategies:
    rows.append(json.loads((root / a.out / "runs" / f"{a.workload}__{st}__none__s{a.seed}.json").read_text()))

lines = [
    ("events published", "events_published", "{:.0f}"),
    ("Lambda invocations", "invocations", "{:.0f}"),
    ("cold starts", "cold_starts", "{:.0f}"),
    ("DB write calls", "db_api_calls", "{:.0f}"),
    ("rows stored in DB", "db_rows", "{:.0f}"),
    ("latency p50 (ms)", "latency_p50_ms", "{:.0f}"),
    ("latency p95 (ms)", "latency_p95_ms", "{:.0f}"),
    ("throughput (events/s)", "throughput_eps", "{:.1f}"),
    ("alarm recall", "anomaly_recall", "{:.3f}"),
    ("estimated cost (USD)", "cost_total_usd", "{:.5f}"),
    ("est. cost per M events (USD)", "cost_usd_per_million_events", "{:.2f}"),
]
w = 30
print("\n" + "".join(["metric".ljust(w)] + [r["strategy"].rjust(14) for r in rows]))
print("-" * (w + 14 * len(rows)))
for label, key, fmt in lines:
    print("".join([label.ljust(w)] + [(fmt.format(r[key]) if key in r and r[key] == r[key] else "n/a").rjust(14) for r in rows]))
print("\nReading it: fewer invocations / cold starts / DB calls and lower cost, with alarm recall still 1.000 and bounded latency,")
print("is what the adaptive strategy is designed to achieve. (Run python analyze.py for the full tables and charts.)")
