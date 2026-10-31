"""Run ONE experiment (workload x strategy x network) and write results.

  python -m iot.experiment --workload burst --strategy adaptive --network none
  python -m iot.experiment --backend faas --faas-url http://gateway:8080/function/iot-processor --transport mqtt ...
"""
import argparse
import csv
import dataclasses
import json
import os
import sqlite3
import sys
import threading
import time
import uuid
from pathlib import Path

from .config import NETWORKS, WORKLOADS, Params


def verify_sqlite(path):
    c = sqlite3.connect(path)
    rows, n, tsum, anom = c.execute("SELECT COUNT(*), COALESCE(SUM(n),0), COALESCE(SUM(temp_sum),0), "
                                    "COALESCE(SUM(anom),0) FROM events").fetchone()
    return rows, n, tsum, anom


def verify_postgres(dsn, run):
    import psycopg2
    c = psycopg2.connect(dsn)
    cur = c.cursor()
    cur.execute("SELECT COUNT(*), COALESCE(SUM(n),0), COALESCE(SUM(temp_sum),0), COALESCE(SUM(anom),0) "
                "FROM events WHERE run=%s", (run,))
    return cur.fetchone()


def verify_dynamodb(table, region, run):
    import boto3
    from boto3.dynamodb.conditions import Attr
    t = boto3.resource("dynamodb", region_name=region).Table(table)
    rows = n = tsum = anom = 0
    kw = dict(FilterExpression=Attr("run").eq(run), ConsistentRead=True)     # consistent: see every write just made
    while True:
        r = t.scan(**kw)
        for it in r["Items"]:
            rows += 1
            n += int(it["n"])
            tsum += float(it["temp_sum"])
            anom += int(it["anom"])
        if "LastEvaluatedKey" not in r:
            return rows, n, tsum, anom
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def force_cold_start(function_name, region):
    """Change a harmless environment variable so AWS discards every warm execution environment of the function:
    the next invocations are guaranteed cold starts. Without this, containers warmed by earlier runs are reused and
    the per-run cold-start count depends on run order instead of on the strategy."""
    import uuid

    import boto3
    c = boto3.client("lambda", region_name=region)
    env = dict(c.get_function_configuration(FunctionName=function_name).get("Environment", {}).get("Variables", {}))
    env["COLD_NONCE"] = uuid.uuid4().hex
    c.update_function_configuration(FunctionName=function_name, Environment={"Variables": env})
    c.get_waiter("function_updated_v2").wait(FunctionName=function_name)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", required=True, choices=WORKLOADS)
    ap.add_argument("--strategy", required=True, choices=["naive", "filter", "batch", "adaptive"])
    ap.add_argument("--network", default="none", choices=NETWORKS)
    ap.add_argument("--backend", default="local", choices=["local", "faas", "aws"])
    ap.add_argument("--transport", default="inproc", choices=["inproc", "mqtt"])
    ap.add_argument("--out", default="results")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--time-scale", type=float, default=1.0, help="scale phase durations (quick smoke tests)")
    ap.add_argument("--faas-url", default=os.environ.get("FAAS_URL", "http://localhost:8080/function/iot-processor"))
    ap.add_argument("--aws-function", default=os.environ.get("AWS_FUNCTION", "iot-demo"))
    ap.add_argument("--aws-region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-2")
    ap.add_argument("--dynamodb-table", default=os.environ.get("DYNAMODB_TABLE", "iot-demo-events"))
    ap.add_argument("--fresh-containers", action=argparse.BooleanOptionalAction, default=None,
                    help="aws backend: force cold containers at the start of every run (default: on for --backend aws)")
    ap.add_argument("--aws-max-concurrency", type=int, default=50,
                    help="client-side cap on parallel invocations; keep it <= your Lambda concurrency quota (0 = no cap)")
    ap.add_argument("--postgres-dsn", default=os.environ.get("POSTGRES_DSN", ""), help="verification DSN for --backend faas")
    ap.add_argument("--mqtt-host", default=os.environ.get("MQTT_HOST", "localhost"))
    ap.add_argument("--mqtt-port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    ap.add_argument("--mqtt-ca"), ap.add_argument("--mqtt-cert"), ap.add_argument("--mqtt-key")  # optional TLS
    ap.add_argument("--prom-port", type=int, default=0, help="expose Prometheus metrics on this port")
    a = ap.parse_args()

    p = Params()
    fresh = a.fresh_containers if a.fresh_containers is not None else (a.backend == "aws")
    run_id = f"{a.workload}__{a.strategy}__{a.network}__s{a.seed}"
    # Shared databases (DynamoDB / PostgreSQL) keep rows between runs: tag every row with a unique value so that
    # verification only counts THIS execution, even when the same experiment is repeated into the same table.
    run_tag = run_id if a.backend == "local" else f"{run_id}__{uuid.uuid4().hex[:8]}"
    out = Path(a.out)
    (out / "runs").mkdir(parents=True, exist_ok=True)
    (out / "db").mkdir(parents=True, exist_ok=True)

    # storage backend must be configured before the handler module is imported
    if a.backend == "local":
        db_path = out / "db" / f"{run_id}.sqlite"
        for suffix in ("", "-wal", "-shm"):
            Path(str(db_path) + suffix).unlink(missing_ok=True)
        os.environ.update(STORE_BACKEND="sqlite", SQLITE_PATH=str(db_path), SIM_DB_LATENCY_MS=str(p.db_latency_ms))
    else:
        pass  # storage is configured on the function itself (STORE_BACKEND=postgres / dynamodb)

    from .gateway import Gateway
    from .lambda_runtime import AwsLambda, HttpFaas, LocalLambda
    from .metrics import Metrics
    from .simulator import Simulator
    from .strategies import STRATEGIES
    from .transport import InProcessBroker, MqttTransport

    metrics = Metrics(p)
    if a.backend == "local":
        runtime = LocalLambda(p)
    elif a.backend == "aws":
        if fresh:
            force_cold_start(a.aws_function, a.aws_region)
        runtime = AwsLambda(a.aws_function, a.aws_region, a.aws_max_concurrency)
    else:
        runtime = HttpFaas(a.faas_url)

    if a.transport == "inproc":
        pub = sub = InProcessBroker(NETWORKS[a.network], seed=a.seed)
        pub.start()
    else:
        pub = MqttTransport(a.mqtt_host, a.mqtt_port, f"sim-{run_id}", ca=a.mqtt_ca, cert=a.mqtt_cert, key=a.mqtt_key)
        sub = MqttTransport(a.mqtt_host, a.mqtt_port, f"gw-{run_id}", ca=a.mqtt_ca, cert=a.mqtt_cert, key=a.mqtt_key)
        pub.start(), sub.start()

    if a.prom_port:
        try:
            from prometheus_client import Gauge, start_http_server
            start_http_server(a.prom_port)
            gauges = {k: Gauge(f"iot_{k}", k) for k in ("events_published", "invocations", "cold_starts",
                                                        "db_api_calls", "db_items_written", "latency_p95_ms")}

            def export():
                while True:
                    s = metrics.summary()
                    for k, g in gauges.items():
                        g.set(s[k] if s[k] == s[k] else 0)
                    time.sleep(1)
            threading.Thread(target=export, daemon=True).start()
        except ImportError:
            print("prometheus_client not installed; skipping exporter", file=sys.stderr)

    gw = Gateway(sub, runtime, STRATEGIES[a.strategy], metrics, p, run_tag)
    gw.start()
    phases = [(d * a.time_scale, r, pa) for d, r, pa in WORKLOADS[a.workload]]
    print(f"[{run_id}] running {sum(d for d, _, _ in phases):.0f}s ...", flush=True)
    Simulator(pub.publish, metrics, p, seed=a.seed).run(phases)

    deadline = time.time() + p.drain_timeout_s            # drain: wait until every event is accounted for
    while time.time() < deadline and (metrics.accounted() < metrics.published):
        time.sleep(0.1)
    gw.stop()
    for t in {id(pub): pub, id(sub): sub}.values():
        t.stop()

    s = metrics.summary(getattr(runtime, "max_active", None))
    if a.backend == "local":
        rows, n, tsum, anom = verify_sqlite(str(db_path))
    elif a.backend == "aws":
        rows, n, tsum, anom = verify_dynamodb(a.dynamodb_table, a.aws_region, run_tag)
    else:
        rows, n, tsum, anom = verify_postgres(a.postgres_dsn, run_tag)
    s.update(db_rows=rows, db_readings_represented=n, db_anomalies_stored=int(anom),
             anomaly_recall=(anom / s["anomalies_published"]) if s["anomalies_published"] else 1.0,
             mean_temp_db=(tsum / n) if n else float("nan"))
    s["mean_temp_rel_err_pct"] = 100 * abs(s["mean_temp_db"] - s["truth_mean_temp"]) / s["truth_mean_temp"]
    s["storage_reduction_pct"] = 100 * (1 - rows / max(s["events_published"], 1))
    s.update(run_id=run_id, run_tag=run_tag, workload=a.workload, strategy=a.strategy, network=a.network, backend=a.backend,
             seed=a.seed, fresh_containers=bool(fresh and a.backend == "aws"), params=dataclasses.asdict(p))
    (out / "runs" / f"{run_id}.json").write_text(json.dumps(s, indent=2))
    with open(out / "runs" / f"{run_id}.latencies.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["latency_ms"])
        w.writerows([[x] for x in metrics.lat_ms])
    with open(out / "runs" / f"{run_id}.invocations.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t_s", "events", "cold", "exec_ms", "billed_ms", "items", "api_calls"])
        w.writerows(metrics.inv_log)
    print(f"[{run_id}] events={s['events_published']} inv={s['invocations']} cold={s['cold_starts']} "
          f"db_items={s['db_items_written']} p95={s['latency_p95_ms']:.0f}ms incomplete={s['incomplete']} "
          f"anom_recall={s['anomaly_recall']:.3f} GB-s={s['billed_ms_total']/1e3*p.memory_mb/1024:.1f} "
          f"cost=${s['cost_total_usd']:.5f}", flush=True)


if __name__ == "__main__":
    main()
