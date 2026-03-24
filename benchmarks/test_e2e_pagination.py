"""
End-to-end pagination test.

Seeds 500 runs, then walks through all pages via X-Next-Cursor headers
and verifies: total count, no duplicates, and ts_epoch DESC ordering.

Prerequisites:
    docker compose -f docker-compose.benchmark.yml up -d
    pip install requests psycopg2-binary

Usage:
    python benchmarks/test_e2e_pagination.py
"""

import json
import os
import random
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

NUM_RUNS = 500
FLOW_ID = "E2ETestFlow"
PAGE_SIZE = 50


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def wait_for_postgres(max_retries=30):
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT,
                user=DB_USER, password=DB_PASS, dbname=DB_NAME,
            )
            conn.close()
            return
        except psycopg2.OperationalError:
            time.sleep(1)
    print("FAIL: postgres not reachable")
    sys.exit(1)


def wait_for_service(max_retries=30):
    session = requests.Session()
    for i in range(max_retries):
        try:
            resp = session.get(SERVICE_URL.rstrip("/") + "/ping")
            if resp.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(1)
    print("FAIL: metadata service not reachable at %s" % SERVICE_URL)
    sys.exit(1)


def seed_runs():
    """Insert flow + 500 runs with distinct ts_epoch values."""
    wait_for_postgres()

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS, dbname=DB_NAME,
    )
    cur = conn.cursor()

    # clean up any previous test data
    cur.execute("DELETE FROM runs_v3 WHERE flow_id = %s", (FLOW_ID,))
    cur.execute("DELETE FROM flows_v3 WHERE flow_id = %s", (FLOW_ID,))

    # create flow
    now_ms = int(time.time() * 1000)
    cur.execute(
        """INSERT INTO flows_v3 (flow_id, user_name, ts_epoch, tags, system_tags)
           VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)""",
        (FLOW_ID, "e2e_test", now_ms,
         json.dumps(["test"]), json.dumps(["runtime:dev"])),
    )

    # insert runs with strictly decreasing ts_epoch so ordering is deterministic
    # each run gets a unique ts_epoch to avoid tie-breaking ambiguity
    batch = []
    for i in range(NUM_RUNS):
        ts = now_ms - (i * 1000)  # 1 second apart, newest first
        run_id = "e2e-run-%d" % i
        tags = json.dumps(["test", "batch:%d" % (i // 100)])
        sys_tags = json.dumps(["runtime:dev"])
        batch.append((FLOW_ID, "e2e_test", ts, tags, sys_tags, run_id, ts))

    args_str = ",".join(
        cur.mogrify(
            "(%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)", row
        ).decode("utf-8")
        for row in batch
    )
    cur.execute(
        "INSERT INTO runs_v3 "
        "(flow_id, user_name, ts_epoch, tags, system_tags, run_id, last_heartbeat_ts) "
        "VALUES " + args_str
    )

    conn.commit()
    cur.close()
    conn.close()


def check(name, passed):
    status = "PASS" if passed else "FAIL"
    print("  [%s] %s" % (status, name))
    return passed


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_paginated_walk():
    """Walk all pages via X-Next-Cursor, collect all records."""
    session = requests.Session()
    base = SERVICE_URL.rstrip("/")

    all_records = []
    page_count = 0
    url = "%s/flows/%s/runs?_limit=%d" % (base, FLOW_ID, PAGE_SIZE)

    while url:
        resp = session.get(url)
        if resp.status_code != 200:
            print("  FAIL: page %d returned %d: %s" % (
                page_count + 1, resp.status_code, resp.text[:200]))
            return None
        page = resp.json()
        page_count += 1
        all_records.extend(page)

        cursor = resp.headers.get("X-Next-Cursor")
        if cursor:
            url = "%s/flows/%s/runs?_limit=%d&_cursor=%s" % (
                base, FLOW_ID, PAGE_SIZE, cursor)
        else:
            url = None

    print("  fetched %d records across %d pages" % (len(all_records), page_count))
    return all_records


def run_tests():
    print("seeding %d runs for flow '%s'..." % (NUM_RUNS, FLOW_ID))
    seed_runs()

    print("running pagination e2e tests...\n")
    wait_for_service()

    records = test_paginated_walk()
    if records is None:
        print("\n  FAIL: could not complete paginated walk")
        return False

    all_passed = True

    # check 1: total count matches seeded data
    all_passed &= check(
        "total records == %d (got %d)" % (NUM_RUNS, len(records)),
        len(records) == NUM_RUNS,
    )

    # check 2: no duplicates (by run_number)
    run_numbers = [r["run_number"] for r in records]
    unique_run_numbers = set(run_numbers)
    all_passed &= check(
        "no duplicates (%d unique out of %d)" % (
            len(unique_run_numbers), len(run_numbers)),
        len(unique_run_numbers) == len(run_numbers),
    )

    # check 3: ordering is ts_epoch DESC (each ts_epoch >= next)
    ts_epochs = [r["ts_epoch"] for r in records]
    is_descending = all(
        ts_epochs[i] >= ts_epochs[i + 1]
        for i in range(len(ts_epochs) - 1)
    )
    all_passed &= check(
        "ordering is ts_epoch DESC",
        is_descending,
    )

    # check 4: page count is correct (ceil(500/50) = 10)
    expected_pages = (NUM_RUNS + PAGE_SIZE - 1) // PAGE_SIZE
    actual_pages = (len(records) + PAGE_SIZE - 1) // PAGE_SIZE
    all_passed &= check(
        "expected ~%d pages" % expected_pages,
        actual_pages == expected_pages,
    )

    # check 5: verify the unpaginated endpoint still returns all records
    session = requests.Session()
    base = SERVICE_URL.rstrip("/")
    resp = session.get("%s/flows/%s/runs" % (base, FLOW_ID))
    if resp.status_code == 200:
        unpaginated = resp.json()
        all_passed &= check(
            "unpaginated endpoint returns all %d records (got %d)" % (
                NUM_RUNS, len(unpaginated)),
            len(unpaginated) == NUM_RUNS,
        )
    else:
        all_passed &= check(
            "unpaginated endpoint returns 200 (got %d)" % resp.status_code,
            False,
        )

    print()
    if all_passed:
        print("ALL CHECKS PASSED")
    else:
        print("SOME CHECKS FAILED")
    return all_passed


def cleanup():
    """Remove test data."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASS, dbname=DB_NAME,
        )
        cur = conn.cursor()
        cur.execute("DELETE FROM runs_v3 WHERE flow_id = %s", (FLOW_ID,))
        cur.execute("DELETE FROM flows_v3 WHERE flow_id = %s", (FLOW_ID,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass


def main():
    try:
        passed = run_tests()
    finally:
        cleanup()
        print("cleaned up test data")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
