"""Ingest gateway: subscribes to the broker, applies a strategy, invokes Lambda, records metrics."""
import json
import time
from concurrent.futures import ThreadPoolExecutor


class Gateway:
    def __init__(self, transport, runtime, strategy_cls, metrics, p, run_id):
        self.transport, self.runtime, self.metrics, self.p, self.run_id = transport, runtime, metrics, p, run_id
        self.pool = ThreadPoolExecutor(max_workers=p.gw_workers, thread_name_prefix="gw")
        self.strategy = strategy_cls(self.dispatch, metrics, p)

    def start(self):
        self.strategy.start()
        self.transport.subscribe("sensors/+", self._on_message)

    def _on_message(self, topic, payload):
        ev = json.loads(payload)
        ev["_rx"] = time.time()
        self.metrics.on_received()
        self.strategy.on_event(ev)

    def dispatch(self, payload, ts_list, n_raw, n_agg):
        payload["run"] = self.run_id
        self.pool.submit(self._invoke, payload, ts_list, n_raw, n_agg)

    def _invoke(self, payload, ts_list, n_raw, n_agg):
        for attempt in range(3):
            try:
                info = self.runtime.invoke(payload)
                break
            except Exception as e:                      # noqa: BLE001 - retry any invoke failure
                last = e
                time.sleep(0.2 * (attempt + 1))
        else:
            print("invoke failed:", last)
            self.metrics.on_failed(len(ts_list))
            return
        self.metrics.on_invoke(info, ts_list, time.time(), n_raw, n_agg)

    def stop(self):
        self.strategy.stop()
        self.pool.shutdown(wait=True)
