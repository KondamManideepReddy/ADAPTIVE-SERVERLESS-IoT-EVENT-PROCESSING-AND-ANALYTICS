"""Lambda back ends with one interface: invoke(payload) -> info dict.

LocalLambda : emulator (container pool, cold starts, concurrency limit, billed-duration accounting)
              running the *real* handler code in lambda_fn/.
AwsLambda   : REAL AWS Lambda through boto3 (cold start, duration and billed duration are read from the REPORT line
              that Lambda returns with LogType=Tail). Needs the function from aws/deploy.py and DynamoDB.
HttpFaas    : any open-source FaaS exposing an HTTP function (OpenFaaS, Knative, Fission, OpenWhisk web action);
              cold start is detected from the function's own response ("cold" flag, set by lambda_fn/server.py).
info = {cold, init_ms, exec_ms, billed_ms, wait_ms, response}
"""
import base64
import importlib
import json
import math
import random
import re
import sys
import threading
import time
from pathlib import Path

LAMBDA_DIR = Path(__file__).resolve().parent.parent / "lambda_fn"


class _Container:
    __slots__ = ("initialized", "last_used")

    def __init__(self):
        self.initialized = False
        self.last_used = time.time()


class LocalLambda:
    def __init__(self, p):
        sys.path.insert(0, str(LAMBDA_DIR))
        self.mod = importlib.import_module("handler")
        self.p = p
        self.sem = threading.BoundedSemaphore(p.max_concurrency)
        self.idle, self.lock = [], threading.Lock()
        self.active = 0
        self.max_active = 0
        self.rng = random.Random(11)

    def _take(self):
        now = time.time()
        with self.lock:
            self.idle = [c for c in self.idle if now - c.last_used < self.p.warm_ttl_s]
            c = self.idle.pop() if self.idle else _Container()   # most-recently-used first, like Lambda
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            return c

    def invoke(self, payload):
        t_req = time.time()
        self.sem.acquire()                      # emulates reserved-concurrency limit (queues instead of 429)
        wait_ms = (time.time() - t_req) * 1e3
        c = self._take()
        try:
            time.sleep(self.p.invoke_rtt_ms / 1e3)
            cold, init_ms = False, 0.0
            if not c.initialized:
                t = time.time()
                time.sleep(self.p.cold_start_ms * (0.8 + 0.4 * self.rng.random()) / 1e3)
                self.mod.init()
                c.initialized, cold = True, True
                init_ms = (time.time() - t) * 1e3
            t = time.time()
            resp = self.mod.handler(json.loads(json.dumps(payload)), None)
            exec_ms = (time.time() - t) * 1e3
            return {"cold": cold, "init_ms": init_ms, "exec_ms": exec_ms,
                    "billed_ms": math.ceil(exec_ms) + (init_ms if cold else 0.0),   # init is billed (Aug-2025 change)
                    "wait_ms": wait_ms, "response": resp}
        finally:
            c.last_used = time.time()
            with self.lock:
                self.active -= 1
                self.idle.append(c)
            self.sem.release()


class HttpFaas:
    def __init__(self, url):
        self.url, self.max_active = url, None

    def invoke(self, payload):
        import urllib.request
        t = time.time()
        req = urllib.request.Request(self.url, json.dumps(payload).encode(), {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            body = json.loads(r.read())
        total = (time.time() - t) * 1e3
        resp, cold, init = body["response"], body.get("cold", False), body.get("init_ms", 0.0)
        return {"cold": cold, "init_ms": init, "exec_ms": resp["exec_ms"],
                "billed_ms": math.ceil(resp["exec_ms"]) + init, "wait_ms": max(total - resp["exec_ms"] - init, 0.0),
                "response": resp}


def parse_report(log_text):
    """Parse the Lambda 'REPORT ...' log line -> {'Duration': ms, 'Billed Duration': ms, 'Init Duration': ms, ...}."""
    for line in reversed(log_text.splitlines()):
        if line.startswith("REPORT"):
            return {k.strip(): float(v) for k, v in re.findall(r"([A-Za-z ]+): ([\d.]+) (?:ms|MB)", line)}
    return {}


class AwsLambda:
    """Synchronous invocation of a real AWS Lambda function. Same interface as LocalLambda."""

    def __init__(self, function_name, region=None, max_concurrency=10, client=None):
        if client is None:
            import boto3
            from botocore.config import Config
            client = boto3.client("lambda", region_name=region, config=Config(
                retries={"max_attempts": 8, "mode": "adaptive"},     # retries throttling (HTTP 429) with backoff
                max_pool_connections=max(max_concurrency, 10) + 10, read_timeout=65, connect_timeout=10))
        self.client, self.fn = client, function_name
        self.sem = threading.BoundedSemaphore(max_concurrency) if max_concurrency else None
        self.lock, self.active, self.max_active = threading.Lock(), 0, 0

    def invoke(self, payload):
        t_req = time.time()
        if self.sem:
            self.sem.acquire()                  # client-side cap = your account's concurrency quota
        wait_ms = (time.time() - t_req) * 1e3
        try:
            with self.lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            t = time.time()
            r = self.client.invoke(FunctionName=self.fn, InvocationType="RequestResponse", LogType="Tail",
                                   Payload=json.dumps(payload).encode())
            total_ms = (time.time() - t) * 1e3
            body = r["Payload"].read()
            if r.get("FunctionError"):
                raise RuntimeError(f"Lambda error: {body[:300]!r}")
            resp = json.loads(body)
            rep = parse_report(base64.b64decode(r.get("LogResult", "")).decode("utf-8", "replace"))
            init_ms = rep.get("Init Duration", 0.0)
            exec_ms = rep.get("Duration", resp.get("exec_ms", 0.0))
            # The REPORT line's "Billed Duration" ALREADY includes the INIT phase on cold starts (observed on real AWS:
            # Duration 124 ms + Init 505 ms -> Billed 630 ms), so do not add init_ms again.
            billed = rep.get("Billed Duration", math.ceil(exec_ms) + init_ms)
            return {"cold": "Init Duration" in rep, "init_ms": init_ms, "exec_ms": exec_ms, "billed_ms": billed,
                    "wait_ms": wait_ms + max(total_ms - exec_ms - init_ms, 0.0), "response": resp}
        finally:
            with self.lock:
                self.active -= 1
            if self.sem:
                self.sem.release()
