# Measures actual HTTP requests: baseline (N+1) vs paginated (cursor).
# Seeds its own test data -- just point it at a running metadata service.

import os
import sys
import time
import json

os.environ.setdefault("METAFLOW_SERVICE_URL", "http://localhost:8080")

import requests

# --- HTTP call counter via monkey-patch ---

_original_get = requests.Session.get
_call_log = []


def _counting_get(self, url, **kwargs):
    _call_log.append(url)
    return _original_get(self, url, **kwargs)


requests.Session.get = _counting_get

FLOW_ID = "HelloFlow"
NUM_RUNS = 100
PAGE_SIZE = 50


def seed_data():
    """Seed test data directly into postgres."""
    try:
        import psycopg2
    except ImportError:
        print("  pip install psycopg2-binary to enable seeding")
        return

    db_host = os.environ.get("MF_METADATA_DB_HOST", "localhost")
    db_port = os.environ.get("MF_METADATA_DB_PORT", "5432")
    db_user = os.environ.get("MF_METADATA_DB_USER", "postgres")
    db_pass = os.environ.get("MF_METADATA_DB_PSWD", "postgres")
    db_name = os.environ.get("MF_METADATA_DB_NAME", "postgres")

    conn = psycopg2.connect(
        host=db_host, port=int(db_port),
        user=db_user, password=db_pass, dbname=db_name,
    )
    cur = conn.cursor()

    # check existing
    cur.execute("SELECT COUNT(*) FROM runs_v3 WHERE flow_id = %s", (FLOW_ID,))
    existing = cur.fetchone()[0]
    if existing >= NUM_RUNS:
        print("  already have %d runs, skipping seed" % existing)
        cur.close()
        conn.close()
        return

    now_ms = int(time.time() * 1000)

    # create flow
    cur.execute(
        """INSERT INTO flows_v3 (flow_id, user_name, ts_epoch, tags, system_tags)
           VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
           ON CONFLICT (flow_id) DO NOTHING""",
        (FLOW_ID, "demo", now_ms,
         json.dumps(["demo"]), json.dumps(["runtime:dev"])),
    )

    # create runs
    for i in range(NUM_RUNS):
        ts = now_ms - i * 60000
        cur.execute(
            """INSERT INTO runs_v3
               (flow_id, user_name, ts_epoch, tags, system_tags, run_id, last_heartbeat_ts)
               VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)""",
            (FLOW_ID, "demo", ts,
             json.dumps(["demo"]), json.dumps(["runtime:dev"]),
             "run-%d" % i, ts + 30000),
        )

    conn.commit()
    cur.close()
    conn.close()
    print("  seeded %d runs for %s" % (NUM_RUNS, FLOW_ID))


def reset():
    _call_log.clear()


def run_baseline():
    """Simulate list(Flow('HelloFlow').runs()) -- the N+1 pattern."""
    reset()
    start = time.perf_counter()

    session = requests.Session()
    base = os.environ["METAFLOW_SERVICE_URL"].rstrip("/")

    # 1 call: list all runs
    resp = session.get("%s/flows/%s/runs" % (base, FLOW_ID))
    runs = resp.json()

    # N calls: fetch each run individually (what the client iterator does)
    for run in runs:
        run_id = run.get("run_id") or str(run.get("run_number", ""))
        session.get("%s/flows/%s/runs/%s" % (base, FLOW_ID, run_id))

    elapsed = time.perf_counter() - start
    return len(_call_log), len(runs), elapsed


def run_paginated():
    """Simulate paginated fetch using _limit and _cursor."""
    reset()
    start = time.perf_counter()

    session = requests.Session()
    base = os.environ["METAFLOW_SERVICE_URL"].rstrip("/")

    all_runs = []
    url = "%s/flows/%s/runs?_limit=%d" % (base, FLOW_ID, PAGE_SIZE)
    pages = 0
    while url:
        resp = session.get(url)
        page = resp.json()
        all_runs.extend(page)
        pages += 1
        cursor = resp.headers.get("X-Next-Cursor")
        if cursor:
            url = "%s/flows/%s/runs?_limit=%d&_cursor=%s" % (
                base, FLOW_ID, PAGE_SIZE, cursor)
        else:
            url = None

    elapsed = time.perf_counter() - start
    return len(_call_log), len(all_runs), pages, elapsed


def main():
    base = os.environ["METAFLOW_SERVICE_URL"].rstrip("/")
    print()
    print("Metaflow HTTP Call Counter")
    print("Service: %s" % base)

    # check service
    try:
        resp = requests.get(base + "/ping")
        version = resp.headers.get("METADATA_SERVICE_VERSION", "unknown")
        print("Version: %s" % version)
    except Exception as e:
        print("Service not reachable: %s" % e)
        sys.exit(1)

    # seed data
    print()
    print("--- SEED ---")
    seed_data()

    # baseline
    print()
    print("--- BASELINE ---")
    b_calls, b_runs, b_time = run_baseline()
    print("  %d HTTP calls for %d runs (%.0f ms)" % (b_calls, b_runs, b_time * 1000))

    # paginated
    resp = requests.get(base + "/flows/%s/runs?_limit=3" % FLOW_ID)
    pagination_works = len(resp.json()) <= 3 and len(resp.json()) > 0

    if pagination_works:
        print()
        print("--- PAGINATED ---")
        p_calls, p_runs, p_pages, p_time = run_paginated()
        print("  %d HTTP calls across %d pages for %d runs (%.0f ms)" % (
            p_calls, p_pages, p_runs, p_time * 1000))

        print()
        print("--- COMPARISON ---")
        print("  Baseline:   %3d calls  %6.0f ms" % (b_calls, b_time * 1000))
        print("  Paginated:  %3d calls  %6.0f ms" % (p_calls, p_time * 1000))
        print("  Reduction:  %dx fewer calls" % max(b_calls // max(p_calls, 1), 1))
    else:
        print()
        print("  pagination not available -- run this against the pagination branch")

    print()


if __name__ == "__main__":
    main()
