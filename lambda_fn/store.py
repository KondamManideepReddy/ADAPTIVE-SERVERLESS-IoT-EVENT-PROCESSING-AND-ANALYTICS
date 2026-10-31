"""Storage layer: SQLite (default, local), PostgreSQL (docker-compose) or DynamoDB (real AWS Lambda).

Item schema (single table, PK=device, SK=sk):
  kind  'raw' | 'agg'      n  readings represented      temp_sum/min/max, hum_sum, vib_max
  anom  1 if alarm reading  ts / wend  time span         run  experiment id
write_items() returns the number of DB round trips issued (one multi-row statement = 1 call).
"""
import os
import sqlite3
import threading
import time

BACKEND = os.environ.get("STORE_BACKEND", "sqlite")
_local = threading.local()

_SCHEMA = """CREATE TABLE IF NOT EXISTS events(
  device TEXT, sk TEXT, kind TEXT, ts REAL, wend REAL, n INTEGER,
  temp_sum REAL, temp_min REAL, temp_max REAL, hum_sum REAL, vib_max REAL,
  anom INTEGER, run TEXT, PRIMARY KEY(device, sk))"""
_COLS = ["device", "sk", "kind", "ts", "wend", "n", "temp_sum", "temp_min", "temp_max",
         "hum_sum", "vib_max", "anom", "run"]


def _sqlite():
    c = getattr(_local, "c", None)
    if c is None:
        c = sqlite3.connect(os.environ["SQLITE_PATH"], timeout=60, isolation_level=None)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute(_SCHEMA)
        _local.c = c
    return c


def _pg():
    c = getattr(_local, "pg", None)
    if c is None:
        import psycopg2
        c = psycopg2.connect(os.environ["POSTGRES_DSN"])
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute(_SCHEMA.replace("REAL", "DOUBLE PRECISION"))
        _local.pg = c
    return c


def _ddb():
    t = getattr(_local, "ddb", None)
    if t is None:
        import boto3                      # preinstalled in the AWS Lambda Python runtime
        t = boto3.resource("dynamodb").Table(os.environ["DYNAMODB_TABLE"])
        _local.ddb = t
    return t


def _ddb_item(it):
    """DynamoDB rejects Python floats: convert to Decimal, drop None."""
    from decimal import Decimal
    return {k: (Decimal(repr(v)) if isinstance(v, float) else v) for k, v in it.items() if v is not None}


def warm():
    """Called once per container (init phase): open the connection."""
    if BACKEND == "sqlite":
        _sqlite()
    elif BACKEND == "dynamodb":
        _ddb()
    else:
        _pg()


def write_items(items):
    if not items:
        return 0
    if BACKEND == "dynamodb":
        # BatchWriteItem takes <= 25 items per request; batch_writer chunks, retries unprocessed items and
        # de-duplicates keys. Same (device, sk) key on retry just overwrites => idempotent.
        with _ddb().batch_writer(overwrite_by_pkeys=["device", "sk"]) as bw:
            for it in items:
                bw.put_item(Item=_ddb_item(it))
        return -(-len(items) // 25)                     # number of BatchWriteItem calls
    rows = [tuple(it.get(k) for k in _COLS) for it in items]
    if BACKEND == "sqlite":
        c = _sqlite()
        c.execute("BEGIN IMMEDIATE")
        c.executemany(f"INSERT OR REPLACE INTO events VALUES({','.join('?' * len(_COLS))})", rows)
        c.execute("COMMIT")
        # emulate a networked DB round trip (one call per invocation, batched insert)
        time.sleep(float(os.environ.get("SIM_DB_LATENCY_MS", "5")) / 1e3)
    else:
        from psycopg2.extras import execute_values
        with _pg().cursor() as cur:
            execute_values(cur, f"INSERT INTO events({','.join(_COLS)}) VALUES %s "
                           f"ON CONFLICT (device, sk) DO UPDATE SET n=EXCLUDED.n", rows)
    return 1
