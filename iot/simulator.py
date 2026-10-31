"""Python IoT sensor simulator with configurable (phased) workloads."""
import json
import math
import random
import time

from .config import ANOM_TEMP, ANOM_VIB


def is_anomaly(ev) -> bool:
    return ev["temp"] > ANOM_TEMP or ev["vib"] > ANOM_VIB


class Simulator:
    def __init__(self, publish, metrics, p, seed=7):
        self.publish, self.metrics, self.p = publish, metrics, p
        self.rng = random.Random(seed)
        self.devices = [f"dev-{i:03d}" for i in range(p.n_devices)]
        self.base_t = [self.rng.uniform(20, 30) for _ in self.devices]
        self.seq = 0

    def _reading(self, i, now, anomalous):
        ph = i * 0.7
        temp = self.base_t[i] + 2 * math.sin(now / 40 + ph) + self.rng.gauss(0, 0.1)
        hum = 50 + 5 * math.sin(now / 55 + ph) + self.rng.gauss(0, 0.3)
        vib = abs(self.rng.gauss(0.5, 0.15))
        if anomalous:
            temp, vib = self.rng.uniform(55, 90), self.rng.uniform(5.5, 9)
        return round(temp, 3), round(hum, 3), round(vib, 3)

    def _emit(self, p_anom):
        i = self.rng.randrange(len(self.devices))
        now = time.time()
        temp, hum, vib = self._reading(i, now, self.rng.random() < p_anom)
        self.seq += 1
        ev = {"id": self.seq, "device": self.devices[i], "ts": now, "temp": temp, "hum": hum, "vib": vib}
        self.metrics.on_publish(ev, is_anomaly(ev))
        self.publish(f"sensors/{ev['device']}", json.dumps(ev).encode())

    def run(self, phases):
        """Poisson arrivals per phase. Blocks until all phases finish."""
        for dur, rate, p_anom in phases:
            end = time.time() + dur
            nxt = time.time()
            while True:
                now = time.time()
                if now >= end:
                    break
                if nxt <= now:
                    self._emit(p_anom)
                    nxt += self.rng.expovariate(rate)
                else:
                    time.sleep(min(nxt - now, 0.002))
