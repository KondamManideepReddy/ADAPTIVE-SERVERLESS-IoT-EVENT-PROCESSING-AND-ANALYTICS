"""Lambda handler: validates/enriches a batch of readings + pre-aggregated windows and persists them
with a single batched write. Accepts:
  {"records": [...raw events...], "aggregates": [...windows...], "run": "<id>"}   (gateway invocation)
  a single raw event {"device","ts","temp",...}                                   (native IoT Rule baseline)
Idempotent (same primary key on retry), so at-least-once delivery is safe.
"""
import os
import time

import store

ANOM_TEMP, ANOM_VIB = 50.0, 5.0


def init():
    store.warm()


def _raw(r, run):
    anom = 1 if (r["temp"] > ANOM_TEMP or r["vib"] > ANOM_VIB) else 0
    return {"device": r["device"], "sk": f"{r['ts']:.6f}#{r.get('id', 0)}", "kind": "raw", "ts": r["ts"],
            "wend": r["ts"], "n": 1, "temp_sum": r["temp"], "temp_min": r["temp"], "temp_max": r["temp"],
            "hum_sum": r["hum"], "vib_max": r["vib"], "anom": anom, "run": run}


def _agg(a, run):
    return {"device": a["device"], "sk": f"agg#{a['wstart']:.6f}", "kind": "agg", "ts": a["wstart"],
            "wend": a["wend"], "n": a["n"], "temp_sum": a["temp_sum"], "temp_min": a["temp_min"],
            "temp_max": a["temp_max"], "hum_sum": a["hum_sum"], "vib_max": a["vib_max"], "anom": 0, "run": run}


def handler(event, context=None):
    t = time.time()
    run = event.get("run", "adhoc")
    if "records" not in event and "device" in event:      # direct IoT Rule payload
        event = {"records": [event], "aggregates": []}
    items = [_raw(r, run) for r in event.get("records", [])]
    items += [_agg(a, run) for a in event.get("aggregates", [])]
    calls = store.write_items(items)
    return {"items": len(items), "api_calls": calls, "raw": len(event.get("records", [])),
            "agg": len(event.get("aggregates", [])), "exec_ms": (time.time() - t) * 1e3}


# On real AWS Lambda, run init() at import time so it happens in the INIT phase (reported as "Init Duration" and
# counted as a cold start). The local emulator does not set this variable and calls init() itself.
if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
    init()
