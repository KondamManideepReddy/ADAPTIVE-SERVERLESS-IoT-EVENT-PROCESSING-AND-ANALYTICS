"""Event-broker transports.

InProcessBroker : zero-dependency broker with delay / loss / outage emulation (default, used for experiments).
MqttTransport   : real MQTT client for Mosquitto (docker-compose; optional TLS on 8883).
Both expose start(), stop(), publish(topic, bytes), subscribe(pattern, callback(topic, bytes)).
"""
import heapq
import itertools
import random
import threading
import time


def topic_match(pattern: str, topic: str) -> bool:
    p, t = pattern.split("/"), topic.split("/")
    for i, seg in enumerate(p):
        if seg == "#":
            return True
        if i >= len(t):
            return False
        if seg != "+" and seg != t[i]:
            return False
    return len(p) == len(t)


class InProcessBroker:
    def __init__(self, net=None, seed=3):
        self.net = net or {}
        self.subs = []
        self._heap = []
        self._seq = itertools.count()
        self._cv = threading.Condition()
        self._rng = random.Random(seed)
        self._running = False
        self._thread = None
        self.t0 = time.time()

    def start(self):
        self.t0 = time.time()
        self._running = True
        self._thread = threading.Thread(target=self._loop, name="broker", daemon=True)
        self._thread.start()

    def stop(self):
        with self._cv:
            self._running = False
            self._cv.notify_all()
        if self._thread:
            self._thread.join(timeout=2)

    def subscribe(self, pattern, cb):
        self.subs.append((pattern, cb))

    def publish(self, topic, payload: bytes):
        now = time.time()
        n = self.net
        delay = 0.0
        if n:
            delay = (n.get("delay_ms", 0) + self._rng.uniform(-1, 1) * n.get("jitter_ms", 0)) / 1e3
            while self._rng.random() < n.get("loss", 0):      # QoS1 retransmission after timeout
                delay += n.get("retry_ms", 300) / 1e3
        at = now + max(delay, 0.0)
        for s, e in n.get("outages", []):                      # device-side buffering during uplink outage
            if self.t0 + s <= now < self.t0 + e:
                at = self.t0 + e + max(delay, 0.0)
        with self._cv:
            heapq.heappush(self._heap, (at, next(self._seq), topic, payload))
            self._cv.notify()

    def _loop(self):
        while True:
            with self._cv:
                while self._running and (not self._heap or self._heap[0][0] > time.time()):
                    timeout = None if not self._heap else max(self._heap[0][0] - time.time(), 0)
                    self._cv.wait(timeout)
                if not self._running:
                    return
                _, _, topic, payload = heapq.heappop(self._heap)
            for pat, cb in self.subs:
                if topic_match(pat, topic):
                    cb(topic, payload)


class MqttTransport:
    """Real MQTT. NOTE: not exercised in the build sandbox (no broker available) - test against your Mosquitto."""

    def __init__(self, host="localhost", port=1883, client_id=None, qos=1, ca=None, cert=None, key=None):
        self.host, self.port, self.qos = host, port, qos
        self.client_id, self.ca, self.cert, self.key = client_id, ca, cert, key
        self.subs = []
        self._connected = threading.Event()
        self.c = None

    def start(self):
        import paho.mqtt.client as mqtt
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id or "")
        if self.ca:
            self.c.tls_set(ca_certs=self.ca, certfile=self.cert, keyfile=self.key)
        self.c.max_inflight_messages_set(2000)
        self.c.max_queued_messages_set(0)

        def on_connect(client, userdata, flags, reason_code, properties=None):
            for pat, _ in self.subs:
                client.subscribe(pat, self.qos)
            self._connected.set()

        def on_message(client, userdata, msg):
            for pat, cb in self.subs:
                if topic_match(pat, msg.topic):
                    cb(msg.topic, msg.payload)

        self.c.on_connect, self.c.on_message = on_connect, on_message
        self.c.connect(self.host, self.port, keepalive=30)
        self.c.loop_start()
        if not self._connected.wait(10):
            raise RuntimeError(f"MQTT connect to {self.host}:{self.port} timed out")

    def stop(self):
        if self.c:
            self.c.loop_stop()
            self.c.disconnect()

    def subscribe(self, pattern, cb):
        self.subs.append((pattern, cb))
        if self._connected.is_set():
            self.c.subscribe(pattern, self.qos)

    def publish(self, topic, payload: bytes):
        self.c.publish(topic, payload, qos=self.qos)
