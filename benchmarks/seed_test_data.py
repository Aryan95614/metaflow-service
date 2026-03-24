"""
Seed test data for pagination benchmarks.

Creates a flow with N runs so the benchmark script has data to paginate over.
Usage:
    python benchmarks/seed_test_data.py --url http://localhost:8080 --runs 500
"""

import argparse
import json
import os
import sys
import time

try:
    import requests
except ImportError:
    print("requires: pip install requests")
    sys.exit(1)


def seed_data(base_url, flow_id, num_runs):
    session = requests.Session()
    base = base_url.rstrip("/")

    # check service is reachable
    try:
        resp = session.get(base + "/ping")
        resp.raise_for_status()
        print("service is up (version: %s)" % resp.headers.get(
            "METADATA_SERVICE_VERSION", "unknown"
        ))
    except Exception as e:
        print("cannot reach service at %s: %s" % (base_url, e))
        sys.exit(1)

    # create the flow
    flow_data = {
        "flow_id": flow_id,
        "user_name": "benchmark",
        "tags": ["benchmark"],
        "system_tags": ["runtime:dev"],
    }
    resp = session.post(base + "/flows/%s/flow" % flow_id, json=flow_data)
    if resp.status_code in (200, 201, 409):
        print("flow '%s' ready" % flow_id)
    else:
        print("failed to create flow: %d %s" % (resp.status_code, resp.text))
        sys.exit(1)

    # create runs
    created = 0
    for i in range(num_runs):
        run_data = {
            "user_name": "benchmark",
            "tags": ["benchmark", "batch:%d" % (i // 100)],
            "system_tags": ["runtime:dev"],
        }
        resp = session.post(base + "/flows/%s/run" % flow_id, json=run_data)
        if resp.status_code in (200, 201):
            created += 1
            if created % 50 == 0:
                print("  created %d/%d runs..." % (created, num_runs))
        else:
            print("  failed at run %d: %d %s" % (i, resp.status_code, resp.text[:100]))

    print("done: %d runs created for flow '%s'" % (created, flow_id))


def main():
    parser = argparse.ArgumentParser(description="Seed benchmark data")
    parser.add_argument(
        "--url",
        default=os.environ.get("METAFLOW_SERVICE_URL", "http://localhost:8080"),
        help="metadata service URL",
    )
    parser.add_argument(
        "--flow",
        default="BenchmarkFlow",
        help="flow_id to create",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=500,
        help="number of runs to create",
    )
    args = parser.parse_args()
    seed_data(args.url, args.flow, args.runs)


if __name__ == "__main__":
    main()
