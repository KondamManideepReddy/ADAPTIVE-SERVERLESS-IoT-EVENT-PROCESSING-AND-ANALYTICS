#!/usr/bin/env python3
"""Run the full experiment matrix (each run in a fresh subprocess) and collect results/summary.csv."""
import argparse
import itertools
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--workloads", nargs="+", default=["low", "medium", "high", "burst"])
ap.add_argument("--strategies", nargs="+", default=["naive", "filter", "batch", "adaptive"])
ap.add_argument("--networks", nargs="+", default=["none"])
ap.add_argument("--seeds", nargs="+", type=int, default=[7])
ap.add_argument("--out", default="results")
ap.add_argument("--time-scale", type=float, default=1.0)
ap.add_argument("--extra", nargs=argparse.REMAINDER, default=[], help="extra args for iot.experiment (after --extra)")
a = ap.parse_args()

for seed, net, wl, st in itertools.product(a.seeds, a.networks, a.workloads, a.strategies):
    cmd = [sys.executable, "-m", "iot.experiment", "--workload", wl, "--strategy", st, "--network", net,
           "--seed", str(seed), "--out", a.out, "--time-scale", str(a.time_scale)] + a.extra
    r = subprocess.run(cmd, cwd=Path(__file__).parent)
    if r.returncode:
        sys.exit(f"run failed: {' '.join(cmd)}")

rows = []
for f in sorted(Path(a.out, "runs").glob("*.json")):
    d = json.loads(f.read_text())
    d.pop("params", None)
    rows.append(d)
pd.DataFrame(rows).to_csv(Path(a.out, "summary.csv"), index=False)
print(f"wrote {a.out}/summary.csv ({len(rows)} runs)")
