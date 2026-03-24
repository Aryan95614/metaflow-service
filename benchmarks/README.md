# Pagination Benchmarks

Measures metadata-service response time and payload size across unbounded,
paginated, and filtered queries with 10,000 seeded runs.

## Setup

```bash
# from the repo root
cd benchmarks

# start postgres + migration + metadata service
docker compose -f docker-compose.benchmark.yml up -d

# wait ~15s for migrations to run, then:
pip install requests psycopg2-binary
python benchmark_pagination.py
```

The script will:
1. Wait for postgres and the metadata service to be ready
2. Seed 10,000 runs with varied tags and timestamps spread across 30 days
3. Hit five endpoints and print a markdown results table

## Endpoints tested

| Endpoint | What it measures |
|----------|-----------------|
| `GET /flows/TestFlow/runs` | Unbounded fetch (current behavior, no params) |
| `GET /flows/TestFlow/runs?_limit=50` | First page of paginated results |
| `GET /flows/TestFlow/runs?_limit=50&_cursor=...` | Deep page (page 100) to show cursor stability |
| `GET /flows/TestFlow/runs?_tags=env:prod` | Tag-filtered unbounded fetch |
| `GET /flows/TestFlow/runs?_limit=50&_tags=env:prod` | Tag-filtered + paginated |

## Teardown

```bash
docker compose -f docker-compose.benchmark.yml down -v
```

## Notes

- Requires the pagination branch (`feat/cursor-pagination-prototype`) for
  `_limit`/`_cursor` params and the tag filtering branch
  (`feat/server-side-tag-filtering`) for `_tags` to work. Without those
  branches, the parameterized endpoints will return the full unbounded
  response (which is still useful as a baseline comparison).
- The seeder inserts directly into postgres, bypassing the API, so it
  runs in a few seconds regardless of service performance.
- Re-running the script skips seeding if 10,000 runs already exist.
