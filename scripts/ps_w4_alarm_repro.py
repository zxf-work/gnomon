#!/usr/bin/env python3
"""Minimal reproduction of W4-P2: the W4 runner's client-side SIGALRM cap on
PostgreSQL. A statement past the alarm keeps running on the server; this shows what the next call on the same
connection gets, against the server-side cap (statement_timeout) the fixed runner uses.

    ps_w4_alarm_repro.py      (PGHOST / PGPORT / PGUSER from the kit's lib/ports.sh)
"""
import json
import os
import signal
import time

import psycopg


class Alarm(Exception):
    pass


def conn():
    return psycopg.connect(host=os.environ.get("PGHOST", "127.0.0.1"), port=int(os.environ.get("PGPORT", 15432)),
                           user=os.environ.get("PGUSER", "postgres"), dbname="postgres", autocommit=True)


def call(c, sql):
    t = time.perf_counter()
    try:
        signal.alarm(2)
        r = c.execute(sql).fetchall()
        signal.alarm(0)
        out = {"ok": True, "rows": r}
    except Alarm:
        out = {"ok": False, "error": "client alarm (2 s)"}
    except Exception as e:  # noqa: BLE001
        signal.alarm(0)
        out = {"ok": False, "error": f"{type(e).__name__}: {str(e)[:200]}"}
    out.update(sql=sql, s=round(time.perf_counter() - t, 2))
    return out


signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(Alarm()))
res = {}
c = conn()     # the runner before the fix: the client alarm only
res["client_alarm"] = [call(c, "SELECT pg_sleep(8)"), call(c, "SELECT 1"), call(c, "SELECT 1")]
c2 = conn()
res["client_alarm_server_view"] = c2.execute(
    "SELECT state, left(query, 40) FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(8)%'").fetchall()
c3 = conn()    # the fix: statement_timeout at the cap (1 s here), the alarm only a backstop
c3.execute("SET statement_timeout = '1s'")
res["server_cap"] = [call(c3, "SELECT pg_sleep(8)"), call(c3, "SELECT 1")]
print(json.dumps(res, indent=1, default=str))
