"""Event-processing strategies (all run in the edge/ingest gateway between broker and Lambda).

naive    : 1 event -> 1 invocation -> 1 PutItem                       (baseline; what an IoT Rule->Lambda does)
filter   : dead-band + heartbeat filter, then 1 event -> 1 invocation (reduces volume, not fan-out)
batch    : fixed size/time micro-batching (50 events or 300 ms)       (static)
adaptive : rate-driven batch size (rate x latency budget), time-bounded flush, and above a rate
           threshold normal readings are aggregated per device per batch (alarms always kept raw).

dispatch(payload, covered_publish_timestamps, n_raw_items, n_events_covered_by_aggregates)
"""
import threading
import time
from collections import deque

from .simulator import is_anomaly


class Strategy:
    name = "base"

    def __init__(self, dispatch, metrics, p):
        self.dispatch, self.metrics, self.p = dispatch, metrics, p

    def on_event(self, ev):
        raise NotImplementedError

    def start(self):
        pass

    def stop(self):
        pass


def _strip(ev):
    return {k: ev[k] for k in ("id", "device", "ts", "temp", "hum", "vib")}


class Naive(Strategy):
    name = "naive"

    def on_event(self, ev):
        self.dispatch({"records": [_strip(ev)], "aggregates": []}, [ev["ts"]], 1, 0)


class Filter(Strategy):
    name = "filter"

    def __init__(self, *a):
        super().__init__(*a)
        self.last, self.lock = {}, threading.Lock()

    def on_event(self, ev):
        p, now = self.p, time.time()
        with self.lock:
            prev = self.last.get(ev["device"])
            keep = (is_anomaly(ev) or prev is None or abs(ev["temp"] - prev[0]) >= p.deadband_temp
                    or abs(ev["hum"] - prev[1]) >= p.deadband_hum or now - prev[2] >= p.heartbeat_s)
            if keep:
                self.last[ev["device"]] = (ev["temp"], ev["hum"], now)
        if keep:
            self.dispatch({"records": [_strip(ev)], "aggregates": []}, [ev["ts"]], 1, 0)
        else:
            self.metrics.on_suppressed(1)


class _Buffered(Strategy):
    def __init__(self, *a):
        super().__init__(*a)
        self.buf, self.lock = [], threading.Lock()
        self._stop, self._t = threading.Event(), None

    def start(self):
        self._t = threading.Thread(target=self._tick, name=f"{self.name}-flush", daemon=True)
        self._t.start()

    def _tick(self):
        while not self._stop.is_set():
            time.sleep(0.005)
            with self.lock:
                self._maybe_flush(time.time())

    def stop(self):
        self._stop.set()
        if self._t:
            self._t.join(timeout=2)
        with self.lock:
            if self.buf:
                self._flush(False)

    def on_event(self, ev):
        with self.lock:
            self._note(ev)
            self.buf.append(ev)
            self._maybe_flush(time.time())

    def _note(self, ev):
        pass

    def _maybe_flush(self, now):
        raise NotImplementedError

    def _flush(self, aggregate):
        evs, self.buf = self.buf, []
        if not evs:
            return
        raws, groups = [], {}
        for e in evs:
            if aggregate and not is_anomaly(e):
                groups.setdefault(e["device"], []).append(e)
            else:
                raws.append(e)
        aggs = [{"device": d, "wstart": min(x["ts"] for x in g), "wend": max(x["ts"] for x in g), "n": len(g),
                 "temp_sum": sum(x["temp"] for x in g), "temp_min": min(x["temp"] for x in g),
                 "temp_max": max(x["temp"] for x in g), "hum_sum": sum(x["hum"] for x in g),
                 "vib_max": max(x["vib"] for x in g)} for d, g in groups.items()]
        self.dispatch({"records": [_strip(e) for e in raws], "aggregates": aggs}, [e["ts"] for e in evs],
                      len(raws), len(evs) - len(raws))


class FixedBatch(_Buffered):
    name = "batch"

    def _maybe_flush(self, now):
        if self.buf and (len(self.buf) >= self.p.fixed_batch or now - self.buf[0]["_rx"] >= self.p.fixed_wait_s):
            self._flush(False)


class Adaptive(_Buffered):
    name = "adaptive"

    def __init__(self, *a):
        super().__init__(*a)
        self.arrivals = deque()

    def _note(self, ev):
        self.arrivals.append(ev["_rx"])

    def rate(self, now):
        while self.arrivals and self.arrivals[0] < now - 0.5:
            self.arrivals.popleft()
        return len(self.arrivals) / 0.5

    def _maybe_flush(self, now):
        if not self.buf:
            return
        p = self.p
        rate = self.rate(now)
        target = min(max(int(rate * p.budget_s), 1), p.max_batch)     # events expected inside the latency budget
        if len(self.buf) >= target or now - self.buf[0]["_rx"] >= p.budget_s:
            self._flush(aggregate=rate >= p.agg_threshold_eps)


STRATEGIES = {c.name: c for c in (Naive, Filter, FixedBatch, Adaptive)}
