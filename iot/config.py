"""Central configuration: workloads, network profiles, tunables, price assumptions."""
import math
from dataclasses import dataclass

# Workload = list of phases: (duration_s, mean_rate_events_per_s, anomaly_probability)
WORKLOADS = {
    "low":    [(20, 5, 0.01)],
    "medium": [(20, 50, 0.01)],
    "high":   [(20, 200, 0.02)],
    # normal operation -> abnormal-condition burst (many devices alarm) -> recovery
    "burst":  [(8, 5, 0.0), (8, 400, 0.15), (8, 5, 0.0)],
}

# In-process network emulation (for real MQTT use infra/netem.sh instead).
NETWORKS = {
    "none":   {},
    "lossy":  {"delay_ms": 40, "jitter_ms": 20, "loss": 0.05, "retry_ms": 300},
    # uplink down for 5 s: devices buffer, then flood the broker on reconnect
    "outage": {"delay_ms": 20, "jitter_ms": 5, "outages": [(6, 11)]},
}

ANOM_TEMP = 50.0   # deg C: above this a reading is an alarm and is never dropped/aggregated
ANOM_VIB = 5.0


@dataclass
class Params:
    # --- prices (USD). ASSUMPTIONS: us-east-1, x86, on-demand. Check the AWS pricing pages for YOUR region and edit. ---
    price_lambda_gb_s: float = 0.0000166667      # per GB-second of compute
    price_lambda_per_m_req: float = 0.20          # per million invocations
    price_ddb_per_m_wru: float = 0.625            # DynamoDB on-demand, per million write request units (1 WRU = 1 KB item)
    # --- devices / strategies ---
    n_devices: int = 50
    deadband_temp: float = 0.5      # filter strategy: min change to forward
    deadband_hum: float = 2.0
    heartbeat_s: float = 5.0        # filter strategy: forward at least this often per device
    fixed_batch: int = 50           # fixed-batch strategy
    fixed_wait_s: float = 0.3
    budget_s: float = 0.3           # adaptive: max time an event may wait in the gateway buffer
    max_batch: int = 200
    agg_threshold_eps: float = 100  # adaptive: above this rate, normal readings are aggregated
    gw_workers: int = 128
    # --- Lambda (local emulator) ---
    memory_mb: int = 256
    cold_start_ms: float = 350      # typical Python runtime init + DB connect
    invoke_rtt_ms: float = 15       # invoke round trip (not counted as compute)
    max_concurrency: int = 50
    warm_ttl_s: float = 300
    # --- DB (emulated network round trip to a database server) ---
    db_latency_ms: float = 5        # per DB round trip (one multi-row INSERT per invocation)
    # --- run control ---
    drain_timeout_s: float = 45


def cost_usd(p, billed_ms, invocations, items):
    """Estimated processing cost: Lambda requests + Lambda compute + DynamoDB writes. Ignores free tier, data transfer,
    CloudWatch Logs and storage. Items are < 1 KB, so 1 item = 1 write request unit."""
    lam = billed_ms / 1e3 * (p.memory_mb / 1024) * p.price_lambda_gb_s + invocations / 1e6 * p.price_lambda_per_m_req
    ddb = items / 1e6 * p.price_ddb_per_m_wru
    return lam, ddb
