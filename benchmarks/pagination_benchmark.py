"""
Benchmark: paginated vs unpaginated list endpoint performance.

Requires a running metadata service with test data.
Usage:
    python benchmarks/pagination_benchmark.py --url http://localhost:8080 --flow test_flow

Set up test data first by creating a flow with many runs, then run this
script to compare response times and payload sizes.
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


def measure_request(session, url, label, headers=None):
    """Make a GET request and return timing + payload info."""
    start = time.time()
    resp = session.get(url, headers=headers or {})
    elapsed = time.time() - start
    body = resp.json() if resp.status_code == 200 else []
    size = len(resp.content)
    return {
        "label": label,
        "status": resp.status_code,
        "elapsed_ms": round(elapsed * 1000, 1),
        "records": len(body) if isinstance(body, list) else 1,
        "payload_bytes": size,
        "headers": dict(resp.headers),
    }


def unpaginated_fetch(session, base_url, path):
    """Single request, no pagination params."""
    url = base_url.rstrip("/") + path
    return measure_request(session, url, "unpaginated")


def paginated_fetch(session, base_url, path, page_size):
    """Walk through all pages using _limit and _cursor."""
    url = base_url.rstrip("/") + path
    sep = "&" if "?" in url else "?"

    results = []
    total_bytes = 0
    total_ms = 0
    page_count = 0
    page_url = "%s%s_limit=%s" % (url, sep, page_size)

    while True:
        start = time.time()
        resp = session.get(page_url)
        elapsed = time.time() - start

        if resp.status_code != 200:
            print("  page %d failed: %d %s" % (page_count + 1, resp.status_code, resp.text[:100]))
            break

        body = resp.json()
        page_count += 1
        total_ms += elapsed * 1000
        total_bytes += len(resp.content)

        if isinstance(body, list):
            results.extend(body)
        else:
            results.append(body)

        cursor = resp.headers.get("X-Next-Cursor")
        if not cursor:
            break
        page_url = "%s%s_limit=%s&_cursor=%s" % (url, sep, page_size, cursor)

    return {
        "label": "paginated (page_size=%d)" % page_size,
        "status": 200,
        "elapsed_ms": round(total_ms, 1),
        "records": len(results),
        "payload_bytes": total_bytes,
        "pages": page_count,
    }


def run_benchmark(base_url, flow_id, page_sizes):
    session = requests.Session()

    # check service is up
    try:
        resp = session.get(base_url.rstrip("/") + "/ping")
        version = resp.headers.get("METADATA_SERVICE_VERSION", "unknown")
        print("service version: %s" % version)
    except Exception as e:
        print("cannot reach service at %s: %s" % (base_url, e))
        sys.exit(1)

    endpoints = [
        ("/flows", "all flows"),
        ("/flows/%s/runs" % flow_id, "runs for %s" % flow_id),
    ]

    for path, description in endpoints:
        print("\n--- %s (%s) ---" % (description, path))

        # unpaginated baseline
        result = unpaginated_fetch(session, base_url, path)
        print("  unpaginated: %d records, %d bytes, %.1f ms" % (
            result["records"], result["payload_bytes"], result["elapsed_ms"]
        ))
        baseline_records = result["records"]

        if baseline_records == 0:
            print("  no data, skipping paginated tests")
            continue

        # paginated at different page sizes
        for ps in page_sizes:
            result = paginated_fetch(session, base_url, path, ps)
            print("  page_size=%d: %d records across %d pages, %d bytes, %.1f ms" % (
                ps, result["records"], result.get("pages", 0),
                result["payload_bytes"], result["elapsed_ms"]
            ))
            if result["records"] != baseline_records:
                print("  WARNING: record count mismatch (%d vs %d)" % (
                    result["records"], baseline_records
                ))


def main():
    parser = argparse.ArgumentParser(description="Pagination benchmark")
    parser.add_argument(
        "--url",
        default=os.environ.get("METAFLOW_SERVICE_URL", "http://localhost:8080"),
        help="metadata service URL",
    )
    parser.add_argument(
        "--flow",
        default="BenchmarkFlow",
        help="flow_id to benchmark runs endpoint against",
    )
    parser.add_argument(
        "--page-sizes",
        default="10,25,50,100",
        help="comma-separated page sizes to test",
    )
    args = parser.parse_args()

    page_sizes = [int(x.strip()) for x in args.page_sizes.split(",")]
    run_benchmark(args.url, args.flow, page_sizes)


if __name__ == "__main__":
    main()
