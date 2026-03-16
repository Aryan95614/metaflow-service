# Cursor-Based Pagination Migration Guide

## Overview

The metadata service now supports optional cursor-based pagination on all list endpoints. Previously, every list query returned the full result set in a single response. For tables with millions of rows (e.g., artifacts, metadata), this creates unbounded memory usage on both the server and client, and can cause request timeouts or OOM kills.

With this change, clients can request results in fixed-size pages using `_limit` and `_cursor` query parameters. Results are ordered by `ts_epoch DESC` (most recent first). The feature is fully opt-in: existing clients that do not send these parameters receive the same unbounded responses as before.

## For API Consumers (SDK/Client Developers)

### Opting In

Add `_limit=N` to any list endpoint to receive at most N results per request:

```
GET /flows?_limit=100
GET /flows/MyFlow/runs?_limit=50
```

### Paginating Through Results

When there are more results beyond the current page, the response includes two headers:

- `X-Next-Cursor` -- the `ts_epoch` value to pass on the next request
- `X-Has-More` -- `"true"` if more pages exist, `"false"` if this is the last page

Pass the cursor value back as `_cursor` on your next request:

```
GET /flows?_limit=100&_cursor=1710500000000
```

### Detecting the Last Page

The last page is reached when either:

- `X-Has-More` is `"false"`, or
- `X-Has-More` and `X-Next-Cursor` headers are absent (this also covers the case where `_limit` was not provided at all)

### Complete Pagination Loop (Pseudocode)

```python
results = []
cursor = None

while True:
    params = {"_limit": 100}
    if cursor is not None:
        params["_cursor"] = cursor

    response = http_get("/flows/MyFlow/runs", params=params)
    page = response.json()
    results.extend(page)

    has_more = response.headers.get("X-Has-More", "false")
    if has_more != "true":
        break

    cursor = response.headers["X-Next-Cursor"]
```

### Backward Compatibility

If you do not send `_limit` or `_cursor`, the server behaves identically to the pre-pagination version. No response headers related to pagination are emitted, and the full result set is returned. No client changes are required.

## Affected Endpoints

All GET list endpoints in the metadata service are paginated:

| Endpoint | Description |
|---|---|
| `GET /flows` | List all flows |
| `GET /flows/{flow_id}/runs` | List runs for a flow |
| `GET /flows/{flow_id}/runs/{run_number}/steps` | List steps for a run |
| `GET /flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks` | List tasks for a step |
| `GET /flows/{flow_id}/runs/{run_number}/artifacts` | List artifacts for a run |
| `GET /flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts` | List artifacts for a step |
| `GET /flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts` | List artifacts for a task |
| `GET /flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/attempt/{attempt_id}/artifacts` | List artifacts for a task attempt |
| `GET /flows/{flow_id}/runs/{run_number}/metadata` | List metadata for a run |
| `GET /flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata` | List metadata for a task |

Single-resource GET endpoints (e.g., `GET /flows/{flow_id}`, `GET /flows/{flow_id}/runs/{run_number}`) are not affected.

## Response Header Reference

| Header | Description |
|---|---|
| `X-Next-Cursor` | The `ts_epoch` value for the next page. Only present when `_limit` is provided and there are more results. |
| `X-Has-More` | `"true"` or `"false"`. Only present when `_limit` is provided. |
| `METADATA_SERVICE_VERSION` | The metadata service version string. Always present (unchanged). |

## How It Works Internally

The server uses the limit+1 trick: when `_limit=N` is requested, it fetches N+1 rows from the database. If N+1 rows are returned, the server knows there are more results. It strips the extra row, returns only N rows, and sets `X-Has-More: true` with `X-Next-Cursor` set to the `ts_epoch` of the last returned row. If N or fewer rows are returned, `X-Has-More` is `"false"` and `X-Next-Cursor` is omitted.

## Parameter Validation

| Rule | Behavior |
|---|---|
| `_limit` must be a non-negative integer | Non-numeric values return HTTP 400 |
| `_limit=0` | Returns all records (same as omitting `_limit`) |
| `_cursor` must be a non-negative integer | Non-numeric values return HTTP 400 |
| `_cursor` provided without `_limit` | Returns HTTP 400 |

## Known Limitations

### No Random Page Access

This is cursor-based pagination, not offset-based. You cannot jump to an arbitrary page number. You must iterate from the beginning to reach a specific position in the result set.

### ts_epoch Ties

The cursor is based on `ts_epoch`. If multiple records share the same `ts_epoch` value, they may be split across page boundaries. In practice, this is rare because `ts_epoch` is set at insert time with millisecond granularity, but it can happen during bulk inserts.

### Artifact Endpoints and filter_artifacts_for_latest_attempt

Several artifact list endpoints (`/artifacts` at the run, step, and task level) apply `filter_artifacts_for_latest_attempt` after fetching paginated results from the database. This post-fetch filter removes artifacts from non-latest attempts. As a result, the number of items in the response body may be less than `_limit`, even when `X-Has-More` is `"true"`. Clients should not rely on the response array length equaling `_limit` to detect the last page; always use the `X-Has-More` header.

## Rollback

If you need to revert a client to pre-pagination behavior, simply stop sending the `_limit` and `_cursor` query parameters. The server ignores unknown query parameters on older versions, so a client that sends these parameters against an older metadata service will receive the full unpaginated response without errors.
