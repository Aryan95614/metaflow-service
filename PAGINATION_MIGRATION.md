# Pagination Migration Guide

## What changed

All metadata-service list endpoints now accept optional `_limit` and `_cursor` query params for cursor-based pagination. If you don't send them, nothing changes. Same response as before.

## How to use it

Add `_limit=N` to any list endpoint:

```
GET /flows?_limit=100
GET /flows/MyFlow/runs?_limit=50
```

Response comes back ordered by `ts_epoch DESC`. If there's more data, the response includes an `X-Next-Cursor` header with the ts_epoch to pass on the next request.

To get the next page, pass the cursor back:

```
GET /flows?_limit=100&_cursor=1710500000000
```

Quick pagination loop:

```python
results = []
cursor = None

while True:
    params = {"_limit": 100}
    if cursor:
        params["_cursor"] = cursor

    resp = http_get("/flows/MyFlow/runs", params=params)
    results.extend(resp.json())

    cursor = resp.headers.get("X-Next-Cursor")
    if not cursor:
        break
```

If you don't send `_limit` or `_cursor`, the server behaves exactly like before. No new headers, full result set, nothing changes.

## Endpoints

All list endpoints support pagination:

- `GET /flows`
- `GET /flows/{flow_id}/runs`
- `GET .../steps`
- `GET .../tasks`
- `GET .../artifacts` (run, step, and task level)
- `GET .../metadata` (run and task level)

Single-resource GETs like `GET /flows/{flow_id}` are not affected.

## Validation

- `_limit` must be a non-negative integer, otherwise 400
- `_limit=0` returns everything (same as not sending it)
- `_cursor` without `_limit` returns 400
- `_cursor` must be a non-negative integer, otherwise 400

## Things to watch out for

**No random page access.** You can't jump to page N, you have to iterate forward from the start.

**ts_epoch ties.** If two records have the same ts_epoch they could theoretically end up on different pages. Rare in practice since timestamps are millisecond-precision, but possible during bulk inserts.

**Artifact filtering.** The artifact endpoints run `filter_artifacts_for_latest_attempt` *after* the page slice, so you might get fewer than `_limit` items back even when `X-Next-Cursor` is present. Check the header, not the array length.
