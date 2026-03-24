"""
Benchmark: paginated vs unbounded metadata-service endpoints.

Seeds 10,000 runs into a local Postgres instance, then measures response
time and payload size across paginated, filtered, and unbounded queries.

Prerequisites:
    docker compose -f docker-compose.benchmark.yml up -d
    pip install requests psycopg2-binary

Usage:
    python benchmarks/benchmark_pagination.py
"""

import json
import os
import random
import subprocess
import sys
import time

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

try:
    import psycopg2
except ImportError:
    print("pip install psycopg2-binary")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SERVICE_URL = os.environ.get("METAFLOW_SERVICE_URL", "http://localhost:8080")
DB_HOST = os.environ.get("MF_METADATA_DB_HOST", "localhost")
DB_PORT = int(os.environ.get("MF_METADATA_DB_PORT", 5432))
DB_USER = os.environ.get("MF_METADATA_DB_USER", "postgres")
DB_PASS = os.environ.get("MF_METADATA_DB_PSWD", "postgres")
DB_NAME = os.environ.get("MF_METADATA_DB_NAME", "postgres")

NUM_RUNS = 10000
FLOW_ID = "TestFlow"
THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000

TAG_POOLS = {
    "env": ["env:prod", "env:staging", "env:dev"],
    "team": ["team:ml", "team:data", "team:infra", "team:platform"],
    "user": ["user:alice", "user:bob", "user:carol", "user:dave"],
}

SYSTEM_TAG_POOLS = [
    "runtime:dev",
    "runtime:production",
    "metaflow_version:2.12.0",
    "metaflow_version:2.11.5",
]


# ---------------------------------------------------------------------------
# Database seeding
# ---------------------------------------------------------------------------

def make_tags():
    tags = [random.choice(TAG_POOLS["env"])]
    tags.append(random.choice(TAG_POOLS["team"]))
    tags.append(random.choice(TAG_POOLS["user"]))
    return json.dumps(tags)


def make_system_tags():
    tags = [random.choice(SYSTEM_TAG_POOLS)]
    return json.dumps(tags)


def wait_for_postgres(max_retries=30):
    print("waiting for postgres at %s:%d..." % (DB_HOST, DB_PORT))
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT,
                user=DB_USER, password=DB_PASS, dbname=DB_NAME,
            )
            conn.close()
            print("  postgres is ready")
            return
        except psycopg2.OperationalError:
            time.sleep(1)
    print("  postgres not reachable after %d retries" % max_retries)
    sys.exit(1)


def wait_for_service(max_retries=30):
    print("waiting for metadata service at %s..." % SERVICE_URL)
    session = requests.Session()
    for i in range(max_retries):
        try:
            resp = session.get(SERVICE_URL.rstrip("/") + "/ping")
            if resp.status_code == 200:
                version = resp.headers.get("METADATA_SERVICE_VERSION", "unknown")
                print("  service is up (version: %s)" % version)
                return
        except requests.ConnectionError:
            pass
        time.sleep(1)
    print("  service not reachable after %d retries" % max_retries)
    sys.exit(1)


def seed_data():
    """Insert flow + 10,000 runs directly into postgres."""
    wait_for_postgres()

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS, dbname=DB_NAME,
    )
    cur = conn.cursor()

    # check if data already seeded
    cur.execute(
        "SELECT COUNT(*) FROM runs_v3 WHERE flow_id = %s", (FLOW_ID,)
    )
    existing = cur.fetchone()[0]
    if existing >= NUM_RUNS:
        print("already have %d runs for %s, skipping seed" % (existing, FLOW_ID))
        cur.close()
        conn.close()
        return

    print("seeding %d runs for flow '%s'..." % (NUM_RUNS, FLOW_ID))

    # ensure flow exists
    cur.execute(
        """INSERT INTO flows_v3 (flow_id, user_name, ts_epoch, tags, system_tags)
           VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
           ON CONFLICT (flow_id) DO NOTHING""",
        (FLOW_ID, "benchmark", int(time.time() * 1000),
         json.dumps(["benchmark"]), json.dumps(["runtime:dev"])),
    )

    # seed runs with realistic spread
    now_ms = int(time.time() * 1000)
    batch = []
    for i in range(NUM_RUNS):
        ts = now_ms - random.randint(0, THIRTY_DAYS_MS)
        tags = make_tags()
        sys_tags = make_system_tags()
        batch.append((FLOW_ID, "benchmark", ts, tags, sys_tags))

        if len(batch) >= 500:
            _insert_batch(cur, batch)
            batch = []
            if (i + 1) % 2000 == 0:
                print("  inserted %d/%d runs..." % (i + 1, NUM_RUNS))

    if batch:
        _insert_batch(cur, batch)

    conn.commit()
    cur.close()
    conn.close()

    print("  seeding complete: %d runs" % NUM_RUNS)


def _insert_batch(cur, batch):
    args_str = ",".join(
        cur.mogrify(
            "(%s, %s, %s, %s::jsonb, %s::jsonb)", row
        ).decode("utf-8")
        for row in batch
    )
    cur.execute(
        "INSERT INTO runs_v3 (flow_id, user_name, ts_epoch, tags, system_tags) "
        "VALUES " + args_str
    )


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

def measure(session, url, label):
    start = time.perf_counter()
    resp = session.get(url)
    elapsed_ms = (time.perf_counter() - start) * 1000
    payload_kb = len(resp.content) / 1024.0

    if resp.status_code != 200:
        return {
            "label": label,
            "status": resp.status_code,
            "rows": 0,
            "payload_kb": 0,
            "latency_ms": round(elapsed_ms, 1),
        }

    body = resp.json()
    rows = len(body) if isinstance(body, list) else 1

    return {
        "label": label,
        "status": resp.status_code,
        "rows": rows,
        "payload_kb": round(payload_kb, 1),
        "latency_ms": round(elapsed_ms, 1),
    }


def find_deep_cursor(session, base_url, page_size=50, target_page=100):
    """Walk forward to page N and return the cursor value."""
    url = "%s/flows/%s/runs?_limit=%d" % (base_url, FLOW_ID, page_size)
    for i in range(target_page):
        resp = session.get(url)
        if resp.status_code != 200:
            return None
        cursor = resp.headers.get("X-Next-Cursor")
        if not cursor:
            return None
        url = "%s/flows/%s/runs?_limit=%d&_cursor=%s" % (
            base_url, FLOW_ID, page_size, cursor
        )
    return cursor


def run_benchmarks():
    wait_for_service()

    session = requests.Session()
    base = SERVICE_URL.rstrip("/")
    results = []

    # 1. unbounded
    results.append(measure(
        session,
        "%s/flows/%s/runs" % (base, FLOW_ID),
        "unbounded (no params)",
    ))

    # 2. paginated page 1
    results.append(measure(
        session,
        "%s/flows/%s/runs?_limit=50" % (base, FLOW_ID),
        "paginated page 1 (_limit=50)",
    ))

    # 3. deep page (page 100)
    print("walking to page 100 for deep cursor...")
    deep_cursor = find_deep_cursor(session, base)
    if deep_cursor:
        results.append(measure(
            session,
            "%s/flows/%s/runs?_limit=50&_cursor=%s" % (base, FLOW_ID, deep_cursor),
            "deep page 100 (_limit=50&_cursor=...)",
        ))
    else:
        results.append({
            "label": "deep page 100 (_limit=50&_cursor=...)",
            "status": "N/A",
            "rows": 0,
            "payload_kb": 0,
            "latency_ms": 0,
        })

    # 4. filtered unbounded
    results.append(measure(
        session,
        "%s/flows/%s/runs?_tags=env:prod" % (base, FLOW_ID),
        "filtered (_tags=env:prod)",
    ))

    # 5. filtered + paginated
    results.append(measure(
        session,
        "%s/flows/%s/runs?_limit=50&_tags=env:prod" % (base, FLOW_ID),
        "filtered+paginated (_limit=50&_tags=env:prod)",
    ))

    # print markdown table
    print()
    print("## Benchmark Results (%d runs seeded)" % NUM_RUNS)
    print()
    print("| Endpoint | Status | Rows | Payload (KB) | Latency (ms) |")
    print("|----------|--------|------|-------------|-------------|")
    for r in results:
        print("| %s | %s | %s | %s | %s |" % (
            r["label"], r["status"], r["rows"],
            r["payload_kb"], r["latency_ms"],
        ))
    print()


def main():
    seed_data()
    run_benchmarks()


if __name__ == "__main__":
    main()
