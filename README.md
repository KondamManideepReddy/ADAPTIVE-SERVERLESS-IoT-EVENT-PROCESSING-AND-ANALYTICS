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
 50 simulated        MQTT (port 1883)         Edge ingest gateway             AWS (ap-southeast-2)
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
| Event broker | `iot/transport.py` | **real Mosquitto over MQTT (the default for all main experiments)**; an in-process broker with delay / loss / outage emulation is used only for the network-fault runs |
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

Network profiles (applied between devices and gateway, in-process broker only): `none`, `lossy` (40 ms +/- 20 ms delay, 5 % loss with retry),
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

**Event broker (required before every run): Mosquitto.** Install the Windows 64-bit build from mosquitto.org (allow the firewall prompt). Start the broker in a **second terminal**, in the project folder, and leave it open:
```powershell
& "C:\Program Files\mosquitto\mosquitto.exe" -c infra\mosquitto.conf -v
```
Its window prints a line for every client that connects, which is your evidence that events travel over MQTT. If it says `Address already in use`, Mosquitto already runs as a Windows service and you can skip this step. If the broker is not running, `demo.py` and `run_experiments.py` stop with a clear message; they never fall back silently.

## 5. Run

**Quick live comparison (about 2 minutes; use this for the demonstration)**
```powershell
python demo.py                        # Mosquitto (MQTT) -> gateway -> real AWS, burst workload, naive vs adaptive
python demo.py --strategies naive filter batch adaptive --workload high
python demo.py --backend local        # emulated Lambda + SQLite, no AWS needed (still uses Mosquitto)
python demo.py --transport inproc     # only if Mosquitto cannot be used; results then say 'inproc'
```

**Full experiments (about 45 minutes; plug in the laptop and disable sleep)**
```powershell
python run_experiments.py --out results_aws --seeds 1 2 3 --extra --backend aws
python run_experiments.py --out results_aws_net --workloads burst --networks lossy outage --extra --backend aws
python analyze.py --results results_aws --network-results results_aws_net
```
`--extra` must be the last option. Three seeds give averaged results. Every run with `--network none` publishes through Mosquitto; the `lossy` / `outage` runs automatically use the in-process broker, because those faults are emulated inside it (on a real MQTT path use `infra/netem.sh`, Linux `tc/netem`). Each result row records the broker it used (`transport` column), and `report.md` states it.

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

## 7. Demonstration script (about 10 minutes)

| Time | What to do | What to say |
|---|---|---|
| 0:00 | show the architecture diagram | "Sensors publish to a broker, an edge gateway groups events, Lambda processes them and DynamoDB stores them. The gateway strategy is the thing we tune." |
| 1:30 | AWS console, region Sydney: Lambda `iot-demo`, DynamoDB `iot-demo-events` | "This is real AWS, deployed by `aws/deploy.py`." |
| 2:30 | start Mosquitto (second terminal), then run `python demo.py` | show the broker window filling with client connections; while it runs explain naive vs adaptive and the 5 -> 400 -> 5 events/s burst |
| 5:00 | read the comparison table | invocations and cold starts, DB write calls, cost per million events, p95 latency (bounded by the 0.3 s budget), **alarm recall 1.000** |
| 6:30 | open `results_aws\report.md`, `comparison.png`, `burst_timeline.png` | how every metric changes as the load grows from low to burst; where each strategy wins or loses |
| 8:00 | Lambda Monitor graphs, Logs Insights table, DynamoDB write usage | "Independent evidence from AWS: no errors, no throttles, cold-start count and time, memory used." |
| 9:00 | network-fault table, limitations | lossy link and 5 s outage: nothing is lost, the outage produces a flood that adaptive absorbs |

**Likely questions**

- *Why not just batch?* Fixed batches cannot match both low and high rates: a fixed wait adds latency at low rate, a fixed size is too small in a burst. Adaptive sizes the batch from the measured rate and bounds latency with a time budget; it also aggregates when the rate is high.
- *Does aggregation lose data?* Normal readings are reduced to count, sum, min and max per device and batch, so means and extremes are preserved (checked by the mean-temperature error). Alarms are never aggregated (alarm recall 1.000).
- *What is a cold start and why do you control it?* A new Lambda container must start the runtime and open connections (hundreds of ms). AWS reuses warm containers between runs, so each run starts with fresh containers; otherwise the count depends on run order.
- *Why Mosquitto and not AWS IoT Core?* The statement allows either. An IoT Core rule normally calls Lambda once per message, which is the naive strategy we want to improve; with a local Mosquitto broker the gateway groups events before Lambda is called. IoT Core is a natural future extension.
- *Why DynamoDB?* Serverless, on-demand billing, key (device, sort key) suits time-series writes, `BatchWriteItem` writes up to 25 items per call. SQLite cannot be shared between Lambda containers.
- *How do you know no data was lost?* After each run the table is scanned for that run's unique tag: events represented, alarms stored, mean temperature are compared with the ground truth. Writes are idempotent, so retries are safe.
- *What is the cost model?* Lambda requests + Lambda GB-seconds (billed duration, includes init) + DynamoDB write units, at assumed us-east-1 prices (`iot/config.py`); free tier ignored.

## 8. Limitations (be upfront about these)

- The client (simulator + gateway) runs on one laptop, so measured latency includes the internet round trip to the AWS region; compare strategies with each other, not with other machines.
- Prices in `iot/config.py` are assumptions (us-east-1, x86); check the current AWS price list for your region.
- The client caps parallel invocations at 50 (`--aws-max-concurrency`), below the account's Lambda quota.
- The Mosquitto broker runs on the same laptop as the simulator and gateway; the `lossy` / `outage` profiles emulate the device-to-gateway hop inside the in-process broker, so those runs do not use Mosquitto. Real packet-level faults need `infra/netem.sh` on Linux.
- Optional parts: the Docker stack (`docker-compose.yml`: Mosquitto, CPU / memory-limited edge container, Prometheus, Grafana) and the PostgreSQL / open-FaaS back ends.

## 9. Cost and cleanup

A full run costs a few cents at most (about 30,000 invocations and a few tens of thousands of DynamoDB writes). Delete everything when done:
```powershell
python aws\deploy.py destroy          # asks for confirmation: deletes function, table, role and log group
python aws\deploy.py status           # all three should say "not found"
```
To demonstrate later, `python aws\deploy.py deploy` recreates everything in about a minute.

## 10. Troubleshooting (Windows)

| Problem | Fix |
|---|---|
| `Activate.ps1 cannot be loaded` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, then activate again |
| a `.ps1` or `.py` file opens in Notepad | the terminal is Command Prompt: use `.venv\Scripts\activate.bat`, or switch the VS Code terminal to PowerShell |
| `AccessDeniedException ... explicit deny in a service control policy` | your organization restricts regions / services; try `--region` with an allowed region (here: `ap-southeast-2`) |
| `MQTT broker not reachable at localhost:1883` | start Mosquitto in a second terminal (section 4); if the port is busy it is already running as a service |
| `Function not found` at the start of a run | run `python aws\deploy.py deploy` first |
| `UnicodeEncodeError` writing the report | use the current `analyze.py` (writes UTF-8) |
| `incomplete` > 0 in a run line | the system could not drain the backlog in time (usually the naive strategy at high load); a valid result, mention it |
| `No module named ...` | the virtual environment is not active: `.\.venv\Scripts\Activate.ps1` |
