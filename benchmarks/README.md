# Cursor Pagination Demo: HTTP Call Reduction

Measures the actual HTTP request count for `list(Flow('HelloFlow').runs())` with and without cursor-based pagination. The baseline makes 1 + N calls (list all runs, then fetch each run individually). With pagination, it makes ceil(N / page_size) calls using `X-Next-Cursor` headers.

## Prerequisites

- Python 3.8+
- Docker (for PostgreSQL)
- `pip install requests psycopg2-binary`

## Setup

```bash
# 1. start postgres
docker run -d --name mf-postgres -p 5432:5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres postgres:11

# 2. wait for postgres
sleep 5

# 3. apply schema migrations
for f in services/migration_service/migration_files/*.sql; do
  sed -n '/+goose Up/,/+goose Down/p' "$f" | grep -v "goose" | grep -v "^SELECT" | \
    docker exec -i mf-postgres psql -U postgres -d postgres 2>/dev/null
done

# 4. start the metadata service (from the repo root)
MF_METADATA_DB_HOST=localhost MF_METADATA_DB_PORT=5432 \
MF_METADATA_DB_USER=postgres MF_METADATA_DB_PSWD=postgres \
MF_METADATA_DB_NAME=postgres MF_METADATA_PORT=8080 \
MF_METADATA_HOST=0.0.0.0 \
python -c "
import pkg_resources
orig = pkg_resources.require
def fake(n):
    if 'metadata' in str(n).lower():
        class F: version='2.5.0'
        return [F()]
    return orig(n)
pkg_resources.require = fake
from services.metadata_service.server import main
main()
" &

# 5. wait for service
sleep 3
curl http://localhost:8080/ping
```

## Run the demo

```bash
python benchmarks/http_call_counter.py
```

The script seeds 100 runs automatically if they don't exist.

## Expected output

```
--- BASELINE ---
  101 HTTP calls for 100 runs (100 ms)

--- PAGINATED ---
  2 HTTP calls across 2 pages for 100 runs (4 ms)

--- COMPARISON ---
  Baseline:   101 calls   100 ms
  Paginated:    2 calls     4 ms
  Reduction:  50x fewer calls
```

## Cleanup

```bash
kill $(lsof -ti:8080) 2>/dev/null
docker stop mf-postgres && docker rm mf-postgres
```

## Context

- [PR #9: Cursor-based pagination](https://github.com/saikonen/metaflow-service/pull/9)
- [PR #10: Server-side tag filtering](https://github.com/saikonen/metaflow-service/pull/10)
