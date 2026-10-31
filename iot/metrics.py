"""Thread-safe metrics collector + summary (latency, throughput, cold starts, invocations, DB ops, GB-seconds)."""
import threading
import time

import numpy as np

from .config import cost_usd


class Metrics:
    def __init__(self, p):
        self.p = p
        self.lock = threading.Lock()
        self.published = self.received = self.suppressed = self.failed = 0
        self.raw_persisted = self.agg_covered = 0
        self.anom_published = 0
        self.truth_n = 0
        self.truth_temp_sum = 0.0
        self.lat_ms = []                       # per-event end-to-end latency (publish -> DB commit acknowledged)
        self.inv_log = []                      # per-invocation records
        self.invocations = self.cold_starts = 0
        self.cold_ms = self.billed_ms = self.exec_ms = self.wait_ms = 0.0
        self.db_calls = self.db_items = 0
        self.first_pub = None
        self.last_commit = None
        self.t0 = time.time()

    def on_publish(self, ev, anom):
        with self.lock:
            self.published += 1
            self.anom_published += int(anom)
            self.truth_n += 1
            self.truth_temp_sum += ev["temp"]
            if self.first_pub is None:
                self.first_pub = ev["ts"]

    def on_received(self):
        with self.lock:
            self.received += 1

    def on_suppressed(self, n=1):
        with self.lock:
            self.suppressed += n

    def on_failed(self, n):
        with self.lock:
            self.failed += n

    def on_invoke(self, info, ts_list, commit_t, n_raw, n_agg):
        resp = info["response"]
        with self.lock:
            self.invocations += 1
            self.cold_starts += int(info["cold"])
            self.cold_ms += info["init_ms"]
            self.billed_ms += info["billed_ms"]
            self.exec_ms += info["exec_ms"]
            self.wait_ms += info["wait_ms"]
            self.db_calls += resp["api_calls"]
            self.db_items += resp["items"]
            self.raw_persisted += n_raw
            self.agg_covered += n_agg
            self.lat_ms.extend((commit_t - t) * 1e3 for t in ts_list)
            self.last_commit = commit_t
            self.inv_log.append((commit_t - self.t0, len(ts_list), int(info["cold"]), info["exec_ms"],
                                 info["billed_ms"], resp["items"], resp["api_calls"]))

    def accounted(self):
        with self.lock:
            return self.raw_persisted + self.agg_covered + self.suppressed + self.failed

    def summary(self, runtime_max_active=None):
        p = self.p
        with self.lock:
            lat = np.array(self.lat_ms) if self.lat_ms else np.array([np.nan])
            done = self.raw_persisted + self.agg_covered
            span = max((self.last_commit or 0) - (self.first_pub or 0), 1e-9)
            gb_s = (self.billed_ms / 1e3) * (p.memory_mb / 1024)
            c_lam, c_ddb = cost_usd(p, self.billed_ms, self.invocations, self.db_items)
            return {
                "events_published": self.published, "events_received": self.received,
                "suppressed": self.suppressed, "raw_persisted": self.raw_persisted,
                "agg_covered": self.agg_covered, "failed": self.failed,
                "incomplete": self.published - (done + self.suppressed + self.failed),
                "anomalies_published": self.anom_published,
                "latency_mean_ms": float(np.nanmean(lat)), "latency_p50_ms": float(np.nanpercentile(lat, 50)),
                "latency_p95_ms": float(np.nanpercentile(lat, 95)), "latency_p99_ms": float(np.nanpercentile(lat, 99)),
                "latency_max_ms": float(np.nanmax(lat)),
                "span_s": span, "throughput_eps": done / span,
                "invocations": self.invocations, "invocations_per_1k_events": 1000 * self.invocations / max(self.published, 1),
                "cold_starts": self.cold_starts, "cold_start_ms_total": self.cold_ms,
                "cold_start_pct_of_invocations": 100 * self.cold_starts / max(self.invocations, 1),
                "billed_ms_total": self.billed_ms,
                "lambda_exec_mean_ms": self.exec_ms / max(self.invocations, 1),
                "throttle_wait_ms_total": self.wait_ms,
                "max_concurrency": runtime_max_active,
                "db_api_calls": self.db_calls, "db_items_written": self.db_items,
                "compute_gb_seconds": gb_s, "compute_gb_s_per_million_events": gb_s / max(self.published, 1) * 1e6,
                "cost_lambda_usd": c_lam, "cost_ddb_usd": c_ddb, "cost_total_usd": c_lam + c_ddb,
                "cost_usd_per_million_events": (c_lam + c_ddb) / max(self.published, 1) * 1e6,
                "truth_mean_temp": self.truth_temp_sum / max(self.truth_n, 1),
            }
