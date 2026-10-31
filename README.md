# Adaptive Serverless IoT Event Processing and Analytics

**Course project, CS741 Cloud, NITK | Python, MQTT (Mosquitto), AWS Lambda, DynamoDB, CloudWatch, Pandas**

IoT devices produce sensor events whose rate swings from a trickle to sudden bursts. Processing every event with its own
Lambda call is slow and expensive under load: many invocations, many cold starts, many database writes. This project builds
the full event pipeline on **real AWS Lambda + DynamoDB**, compares four processing strategies under four workloads, and shows
that an **adaptive strategy** (rate-driven batching + aggregation) keeps latency bounded while cutting invocations, cold starts,
database operations and cost, without losing a single alarm.

---

## 1. Architecture

```
 50 simulated        MQTT / in-process        Edge ingest gateway             AWS (ap-southeast-2)
 sensors            +------------+        +-----------------------+       +-----------------------------+
 (Poisson,   ------>|  Broker    |------->| strategy: naive |     | HTTPS | Lambda  "iot-demo"          |
 alarms)            | (Mosquitto)|        | filter | batch |      |------>| (Python 3.12, 256 MB)       |
                    +------------+        | adaptive              |       |   |                         |
                                          +-----------------------+       |   v  BatchWriteItem (<=25)  |
                                                     |                    | DynamoDB "iot-demo-events"  |
                                                     v                    +--------------+--------------+
                                    metrics (latency, invocations, ...)                  |
                                                     |                       CloudWatch: Lambda metrics,
                                                     v                       REPORT lines (duration, init, memory)
                                      results/*.csv|json --> analyze.py (Pandas) --> report.md + charts
```

| Component | File | Role |
|---|---|---|
| Sensor simulator | `iot/simulator.py` | 50 devices, Poisson arrivals, temperature / humidity / vibration, injected alarms |
| Event broker | `iot/transport.py` | in-process broker (with delay / loss / outage emulation) **or** real Mosquitto over MQTT |
| Gateway + strategies | `iot/gateway.py`, `iot/strategies.py` | decides *how* events are grouped before calling Lambda |
| Lambda runtime client | `iot/lambda_runtime.py` | invokes the real function with boto3; reads cold start, duration and billed duration from the Lambda `REPORT` line |
| Lambda function | `lambda_fn/handler.py`, `lambda_fn/store.py` | validates the batch, writes it to DynamoDB in `BatchWriteItem` calls; idempotent (same key = overwrite) |
| One experiment run | `iot/experiment.py` | one workload x strategy x network; verifies the data in DynamoDB afterwards |
| Experiment matrix | `run_experiments.py` | every run in a fresh process; writes `summary.csv` |
| Analytics | `analyze.py` | Pandas tables, charts, burst timeline, cost estimate |
| AWS setup / teardown | `aws/deploy.py` | creates / deletes the DynamoDB table, IAM role, Lambda function |
| Live demo | `demo.py` | side-by-side comparison of strategies in about two minutes |

## 2. Strategies (the design decision)

| Strategy | What it does | Purpose |
|---|---|---|
| `naive` | 1 event = 1 Lambda call = 1 write | baseline (what a plain IoT-rule-to-Lambda setup does) |
| `filter` | forwards a reading only if temperature changed by >= 0.5 C or humidity by >= 2 %, or 5 s passed (heartbeat); alarms always pass | cuts volume, but still one call per forwarded event |
| `batch` | fixed micro-batch: flush at 50 events or 300 ms | cuts calls, but the setting is static |
| `adaptive` (proposed) | measures the arrival rate over a 0.5 s window; target batch = rate x 0.3 s latency budget (1..200 events); flushes when the target is reached **or** the oldest event has waited 0.3 s. Above 100 events/s, normal readings are **aggregated per device** (count, sum, min, max); alarms are always kept raw | few events: send immediately, low latency. Burst: large batches and aggregation, so the cost stays flat while latency stays bounded by the 0.3 s budget |

An **alarm** is a reading with temperature > 50 C or vibration > 5. No strategy may drop or aggregate an alarm; the verification step checks this (`anomaly_recall`).

## 3. Experiments

| Workload | Event rate | Alarm probability |
|---|---|---|
| `low` | 5 /s for 20 s | 1 % |
| `medium` | 50 /s for 20 s | 1 % |
| `high` | 200 /s for 20 s | 2 % |
| `burst` | 5 /s (8 s) -> **400 /s (8 s)** -> 5 /s (8 s) | 15 % during the burst |

Network profiles (applied between devices and gateway): `none`, `lossy` (40 ms +/- 20 ms delay, 5 % loss with retry),
`outage` (uplink down for 5 s, then devices flush their buffer).

**Metrics per run:** end-to-end latency (publish to acknowledged DynamoDB write; p50 / p95 / p99), throughput, Lambda invocations,
cold starts and cold-start time, DynamoDB write calls and items written, Lambda compute (billed GB-seconds),
estimated cost in USD, and correctness: alarm recall, error of the mean temperature, storage reduction.

**Fairness and correctness measures**

- every run is a separate process with a fixed random seed;
- **fresh Lambda containers at the start of every run** (the script changes a harmless environment variable so AWS discards warm containers); otherwise cold-start counts would depend on run order;
- every row carries a unique run tag, and after each run the table is scanned to verify that all events and alarms arrived;
- cold starts, duration, billed duration and memory come from AWS's own `REPORT` log line, not from the client.

## 4. Setup (Windows, PowerShell in VS Code)

Prerequisites: Python 3.11+ (tick "Add to PATH" when installing), VS Code, an AWS account, and an IAM user with access keys
(`aws configure` once, or create `C:\Users\<you>\.aws\credentials`).
**Region:** the code defaults to `ap-southeast-2` (Sydney). Some student accounts only allow certain regions; change it with
`--region` / `--aws-region` or the `AWS_REGION` environment variable.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1          # if scripts are disabled: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
pip install -r requirements.txt
python -c "import boto3; print('AWS account:', boto3.client('sts').get_caller_identity()['Account'])"   # checks credentials
python aws\deploy.py deploy           # DynamoDB table + IAM role + Lambda function + smoke test (about 1 minute)
```
The deploy ends with `smoke test: OK` and a REPORT line (this shows a real cold start).

## 5. Run

**Quick live comparison (about 2 minutes; use this for the demonstration)**
```powershell
python demo.py                        # burst workload, naive vs adaptive, on real AWS
python demo.py --strategies naive filter batch adaptive --workload high
python demo.py --backend local        # offline fallback: emulated Lambda + SQLite, no AWS needed
```

**Full experiments (about 45 minutes; plug in the laptop and disable sleep)**
```powershell
python run_experiments.py --out results_aws --seeds 1 2 3 --extra --backend aws
python run_experiments.py --out results_aws_net --workloads burst --networks lossy outage --extra --backend aws
python analyze.py --results results_aws --network-results results_aws_net
```
`--extra` must be the last option. Three seeds give averaged results. Add `--transport mqtt` after `--extra` to publish through a
real Mosquitto broker (start it first: `& "C:\Program Files\mosquitto\mosquitto.exe" -c infra\mosquitto.conf -v` in a second
terminal; install from mosquitto.org). The `lossy` / `outage` profiles are in-process emulations, so run those without `--transport mqtt`;
on a real MQTT path use `infra/netem.sh` (Linux `tc/netem`).

**Outputs** (`results_aws/`): `summary.csv` (one row per run), `report.md` (tables), `comparison.png` (8 panels),
`burst_timeline.png`, and `runs/` (per-run JSON, per-event latencies, per-invocation records).

**Offline baseline:** `results_emulator/` and `results_emulator_net/` hold results from the built-in Lambda emulator (assumed
cold start 350 ms). They are only a reference; the AWS results are the ones to report.

## 6. Monitoring on AWS

1. **Lambda -> Functions -> iot-demo -> Monitor**: Invocations, Duration, Error count and success rate, Throttles, Total concurrent executions.
2. **CloudWatch -> Logs -> Logs Insights**, log group `/aws/lambda/iot-demo`:
```
filter @type = "REPORT"
| stats count(*) as invocations, count(@initDuration) as coldStarts, avg(@initDuration) as avgInitMs,
        avg(@duration) as avgDurationMs, max(@duration) as maxDurationMs, max(@maxMemoryUsed / 1000 / 1000) as maxMemoryMB
```
3. **DynamoDB -> Tables -> iot-demo-events -> Monitor**: write usage (the pipeline's load). *Read* usage is only the verification scan after each run, not pipeline load.
4. Optional Prometheus exporter: `pip install -r requirements-optional.txt`, then add `--prom-port 9100` after `--extra` (Grafana: `docker compose up`).

The totals in CloudWatch should match the sum of the `invocations` and `cold_starts` columns of the `summary.csv` files for the same time window.


## 7. Limitations (be upfront about these)

- The client (simulator + gateway) runs on one laptop, so measured latency includes the internet round trip to the AWS region; compare strategies with each other, not with other machines.
- Prices in `iot/config.py` are assumptions (us-east-1, x86); check the current AWS price list for your region.
- The client caps parallel invocations at 50 (`--aws-max-concurrency`), below the account's Lambda quota.
- The `lossy` / `outage` profiles emulate the device-to-gateway hop in-process; real packet-level faults need `infra/netem.sh` on Linux.
- Optional parts not needed for the AWS demo: the Docker stack (`docker-compose.yml`: Mosquitto, CPU / memory-limited edge container, Prometheus, Grafana) and the PostgreSQL / open-FaaS back ends.

## 8. Cost and cleanup

A full run costs a few cents at most (about 30,000 invocations and a few tens of thousands of DynamoDB writes). Delete everything when done:
```powershell
python aws\deploy.py destroy          # asks for confirmation: deletes function, table, role and log group
python aws\deploy.py status           # all three should say "not found"
```
To demonstrate later, `python aws\deploy.py deploy` recreates everything in about a minute.

