#!/usr/bin/env python3
"""Analyse results/summary.csv: tables (report.md) + charts (png) + burst timeline."""
import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from iot.config import Params, cost_usd

ap = argparse.ArgumentParser()
ap.add_argument("--results", default="results")
ap.add_argument("--network-results", default="results_net")
a = ap.parse_args()
R = Path(a.results)

WL = ["low", "medium", "high", "burst"]
ST = ["naive", "filter", "batch", "adaptive"]
# categorical slots 1-4 of the reference palette (adjacent-pair validated); direct values also in report.md table
COL = {"naive": "#2a78d6", "filter": "#eb6834", "batch": "#1baf7a", "adaptive": "#eda100"}
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e6e5e1"

raw = pd.read_csv(R / "summary.csv")
BACKEND = ", ".join(sorted(raw.backend.astype(str).unique())) if "backend" in raw else "local"
P = Params()
_lam, _ddb = cost_usd(P, raw.billed_ms_total, raw.invocations, raw.db_items_written)
raw["cost_per_m"] = (_lam + _ddb) / raw.events_published * 1e6          # estimated USD per million events
df = raw[raw.network == "none"].groupby(["workload", "strategy"], as_index=False).mean(numeric_only=True)
df["gb_s_per_m"] = df.billed_ms_total / 1e3 * 0.25 / df.events_published * 1e6   # 256 MB function
rates = {"low": "5/s", "medium": "50/s", "high": "200/s", "burst": "5→400→5/s"}


def panel(ax, metric, title, log=False, unit=""):
    x = np.arange(len(WL))
    w = 0.2
    for i, s in enumerate(ST):
        v = [df[(df.workload == k) & (df.strategy == s)][metric].mean() for k in WL]
        ax.bar(x + (i - 1.5) * w, v, w * 0.85, color=COL[s], label=s)
    ax.set_xticks(x, [f"{k}\n{rates[k]}" for k in WL], fontsize=8, color=MUTED)
    ax.set_title(title + (f" ({unit})" if unit else ""), fontsize=10, color=INK, loc="left")
    if log:
        ax.set_yscale("log")
    ax.grid(axis="y", color=GRID, lw=0.8)
    ax.set_axisbelow(True)
    for sp in ("top", "right", "left"):
        ax.spines[sp].set_visible(False)
    ax.tick_params(axis="y", labelsize=8, colors=MUTED, length=0)


fig, axs = plt.subplots(2, 4, figsize=(18, 7.5))
panel(axs[0, 0], "latency_p95_ms", "End-to-end latency p95", unit="ms")
panel(axs[0, 1], "invocations", "Function invocations", log=True)
panel(axs[0, 2], "cold_starts", "Cold starts")
panel(axs[1, 0], "db_items_written", "Rows written to DB", log=True)
panel(axs[1, 1], "db_api_calls", "DB round trips", log=True)
panel(axs[1, 2], "gb_s_per_m", "Compute per million events", unit="GB-seconds")
panel(axs[0, 3], "cost_per_m", "Estimated cost per million events", unit="USD")
panel(axs[1, 3], "throughput_eps", "Throughput", unit="events/s")
h, l = axs[0, 0].get_legend_handles_labels()
fig.legend(h, l, loc="upper center", ncol=4, frameon=False, fontsize=10)
fig.tight_layout(rect=(0, 0, 1, 0.94))
fig.savefig(R / "comparison.png", dpi=140)

# ---- burst timeline: invocations/s and in-flight batch size ----
fig, axs = plt.subplots(1, 2, figsize=(13, 4))
for s in ST:
    f = next(R.glob(f"runs/burst__{s}__none__s*.invocations.csv"), None)
    if f is None:
        continue
    d = pd.read_csv(f)
    d["sec"] = d.t_s.astype(int)
    d["sec"] -= d["sec"].min()
    axs[0].plot(d.groupby("sec").size(), color=COL[s], lw=2, label=s)
    lat = pd.read_csv(str(f).replace("invocations", "latencies"))["latency_ms"]
    axs[1].plot(np.sort(lat.values), np.linspace(0, 100, len(lat)), color=COL[s], lw=2, label=s)
axs[0].set_title("Burst: function invocations per second", fontsize=10, loc="left")
axs[1].set_title("Burst: latency CDF (x = ms, y = % of events)", fontsize=10, loc="left")
axs[0].set_yscale("log")
axs[1].set_xlim(0, None)
for ax in axs:
    ax.grid(color=GRID, lw=0.8)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
axs[0].legend(frameon=False)
fig.tight_layout()
fig.savefig(R / "burst_timeline.png", dpi=140)


# ---- markdown report ----
def table(cols, names, fmt):
    lines = []
    for wl in WL:
        sub = df[df.workload == wl].set_index("strategy").reindex(ST)
        lines.append(f"\n**{wl}** ({rates[wl]})\n")
        lines.append("| strategy | " + " | ".join(names) + " |")
        lines.append("|---|" + "---|" * len(names))
        for s in ST:
            lines.append(f"| {s} | " + " | ".join(f.format(sub.loc[s, c]) for c, f in zip(cols, fmt)) + " |")
    return "\n".join(lines)


cols = ["latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "throughput_eps", "invocations", "cold_starts",
        "cold_start_ms_total", "db_api_calls", "db_items_written", "gb_s_per_m", "cost_per_m"]
names = ["p50 ms", "p95 ms", "p99 ms", "throughput ev/s", "invocations", "cold starts", "cold ms total",
         "DB calls", "DB rows", "GB-s / M events", "est. USD / M events"]
fmt = ["{:.0f}", "{:.0f}", "{:.0f}", "{:.1f}", "{:.0f}", "{:.0f}", "{:.0f}", "{:.0f}", "{:.0f}", "{:.0f}", "{:.3f}"]
fid = table(["suppressed", "agg_covered", "db_rows", "storage_reduction_pct", "anomaly_recall",
             "mean_temp_rel_err_pct", "incomplete"],
            ["suppressed", "events folded into aggregates", "DB rows", "storage reduction %", "alarm recall",
             "mean-temp error %", "incomplete"], ["{:.0f}", "{:.0f}", "{:.0f}", "{:.1f}", "{:.3f}", "{:.3f}", "{:.0f}"])
backend_note = ("real AWS Lambda + DynamoDB" if BACKEND == "aws" else
                "local Lambda emulator + SQLite (assumed constants, not real AWS)" if BACKEND == "local" else BACKEND)
out = ["# Experiment results\n", f"Backend: **{BACKEND}** ({backend_note})\n",
       f"Cost = Lambda requests (${P.price_lambda_per_m_req}/M) + Lambda compute (${P.price_lambda_gb_s}/GB-s) + DynamoDB writes "
       f"(${P.price_ddb_per_m_wru}/M WRU); assumed prices, edit in `iot/config.py`. Free tier ignored.\n", "![comparison](comparison.png)\n![burst](burst_timeline.png)\n",
       "## Performance and cost\n", table(cols, names, fmt), "\n\n## Data fidelity\n", fid]

net = Path(a.network_results, "summary.csv")
if net.exists():
    n = pd.read_csv(net)
    n = n.groupby(["network", "strategy"], as_index=False).mean(numeric_only=True)
    out.append("\n\n## Burst workload under degraded network\n\n| network | strategy | p50 ms | p95 ms | p99 ms | "
               "invocations | cold starts | DB rows | incomplete |\n|---|---|---|---|---|---|---|---|---|")
    for _, r in n.iterrows():
        out.append(f"| {r.network} | {r.strategy} | {r.latency_p50_ms:.0f} | {r.latency_p95_ms:.0f} | "
                   f"{r.latency_p99_ms:.0f} | {r.invocations:.0f} | {r.cold_starts:.0f} | "
                   f"{r.db_items_written:.0f} | {r.incomplete:.0f} |")
(R / "report.md").write_text("\n".join(out) + "\n", encoding="utf-8")
print("wrote", R / "report.md", R / "comparison.png", R / "burst_timeline.png")
