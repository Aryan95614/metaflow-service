import asyncio
import json
import uuid
import random

import time

from .utils import (
    cli, db,
    assert_api_get_response, assert_api_post_response, compare_partial,
    add_flow, add_run, add_metadata, assert_api_patch_response
)
from services.data.postgres_async_db import RUN_INACTIVE_CUTOFF_TIME
import pytest

pytestmark = [pytest.mark.integration_tests]


async def _run_numbers(cli, flow_id, query=""):
    # the metadata service serves json as text/plain, so parse the body ourselves
    resp = await cli.get("/flows/{flow_id}/runs{q}".format(flow_id=flow_id, q=query))
    assert resp.status == 200
    return {r["run_number"] for r in json.loads(await resp.text())}


async def test_run_post(cli, db):
    # create flow to add runs for.
    _flow = (await add_flow(db)).body

    payload = {
        "user_name": "test_user",
        "tags": ["a_tag", "b_tag"],
        "system_tags": ["runtime:test"]
    }
    _run = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/run".format(**_flow),
        payload=payload,
        status=200  # why 200 instead of 201?
    )

    # Record should be found in DB
    _found = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body

    compare_partial(_found, payload)

    # Posting on a non-existent flow_id should result in error
    await assert_api_post_response(
        cli,
        path="/flows/NonExistentFlow/run",
        payload=payload,
        status=404
    )


async def test_run_post_has_initial_heartbeat_with_supported_version(cli, db):
    # create flow to add runs for.
    _flow = (await add_flow(db)).body

    # No initial heartbeat without client version info
    _run = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/run".format(**_flow),
        payload={
            "user_name": "test_user",
            "tags": ["a_tag", "b_tag"],
            "system_tags": ["runtime:test"]
        },
        status=200  # why 200 instead of 201?
    )

    assert _run["last_heartbeat_ts"] is None

    # No initial heartbeat with non-heartbeating client version
    _run = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/run".format(**_flow),
        payload={
            "user_name": "test_user",
            "tags": ["a_tag", "b_tag"],
            "system_tags": ["runtime:test", "metaflow_version:2.0.5"]
        },
        status=200  # why 200 instead of 201?
    )

    assert _run["last_heartbeat_ts"] is None

    # Should have initial heartbeat with heartbeating client version
    _run = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/run".format(**_flow),
        payload={
            "user_name": "test_user",
            "tags": ["a_tag", "b_tag"],
            "system_tags": ["runtime:test", "metaflow_version:2.2.12"]
        },
        status=200  # why 200 instead of 201?
    )

    assert _run["last_heartbeat_ts"] is not None



async def test_run_heartbeat_post(cli, db):
    # create flow to add runs for.
    _flow = (await add_flow(db)).body
    # create run to update heartbeat on.
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    assert _run["last_heartbeat_ts"] == None

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/heartbeat".format(**_run),
        status=200   # why 200 instead of 201?
    )

    # Record should be found in DB
    _found = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body

    assert _found["last_heartbeat_ts"] is not None

    # should get 404 for non-existent run
    await assert_api_post_response(
        cli,
        path="/flows/NonExistentFlow/runs/{run_number}/heartbeat".format(**_run),
        status=404
    )

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/1234/heartbeat".format(**_run),
        status=404
    )


async def test_runs_get(cli, db):
    # create a flow for the test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body

    # add runs to the flow
    _first_run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _second_run = (await add_run(db, flow_id=_flow["flow_id"])).body

    # try to get all the created runs
    await assert_api_get_response(cli, "/flows/{flow_id}/runs".format(**_first_run),
                                  data=[_second_run, _first_run], data_is_unordered_list_of_dicts=True)

    # getting runs for non-existent flow should return empty list
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs", status=200, data=[])


async def test_runs_get_status_filter(cli, db):
    # a run with a fresh heartbeat reads as "running"; one without ever reads as "failed".
    # that's enough to exercise filtering without seeding attempt metadata.
    _flow = (await add_flow(db, "StatusFlow", "test_user-1", ["a_tag"], ["runtime:test"])).body

    _running = (await add_run(db, flow_id=_flow["flow_id"],
                              last_heartbeat_ts=int(time.time()))).body
    _failed = (await add_run(db, flow_id=_flow["flow_id"])).body

    flow_id = _flow["flow_id"]
    assert await _run_numbers(cli, flow_id, "?status=running") == {_running["run_number"]}
    assert await _run_numbers(cli, flow_id, "?status=failed") == {_failed["run_number"]}
    # repeated param is an OR over statuses
    assert await _run_numbers(cli, flow_id, "?status=running&status=failed") == \
        {_running["run_number"], _failed["run_number"]}
    # no filter still returns everything
    assert await _run_numbers(cli, flow_id) == {_running["run_number"], _failed["run_number"]}


async def test_runs_get_status_filter_completed(cli, db):
    # a run whose 'end' step recorded a successful attempt reads as "completed".
    # this is the path that actually exercises the attempt-metadata join.
    _flow = (await add_flow(db, "DoneFlow", "test_user-1", ["a_tag"], ["runtime:test"])).body

    _done = (await add_run(db, flow_id=_flow["flow_id"])).body
    await add_metadata(db, flow_id=_done["flow_id"], run_number=_done["run_number"],
                       step_name="end", task_id=1,
                       metadata={"field_name": "attempt_ok", "value": "true"})
    _bare = (await add_run(db, flow_id=_flow["flow_id"])).body

    flow_id = _flow["flow_id"]
    assert await _run_numbers(cli, flow_id, "?status=completed") == {_done["run_number"]}
    assert await _run_numbers(cli, flow_id, "?status=failed") == {_bare["run_number"]}


async def test_runs_get_status_filter_rejects_unknown(cli, db):
    _flow = (await add_flow(db, "BadFlow", "test_user-1", ["a_tag"], ["runtime:test"])).body
    await add_run(db, flow_id=_flow["flow_id"])

    resp = await cli.get("/flows/{flow_id}/runs?status=bogus".format(**_flow))
    assert resp.status == 400


async def test_runs_get_status_filter_classification(cli, db):
    # one run per branch of the status expression, asserted as three disjoint buckets.
    _flow = (await add_flow(db, "MatrixFlow", "test_user-1", ["a_tag"], ["runtime:test"])).body
    now = int(time.time())

    async def end_attempt_ok(run, ok):
        await add_metadata(db, flow_id=run["flow_id"], run_number=run["run_number"],
                           step_name="end", task_id=1,
                           metadata={"field_name": "attempt_ok", "value": str(ok)})

    # a successful end attempt wins even over a fresh heartbeat
    _completed = (await add_run(db, flow_id=_flow["flow_id"], last_heartbeat_ts=now)).body
    await end_attempt_ok(_completed, True)
    # an explicitly unsuccessful end attempt
    _failed_attempt = (await add_run(db, flow_id=_flow["flow_id"])).body
    await end_attempt_ok(_failed_attempt, False)
    # a heartbeat too old to still count as running
    _stale = (await add_run(db, flow_id=_flow["flow_id"],
                            last_heartbeat_ts=now - RUN_INACTIVE_CUTOFF_TIME - 60)).body
    # a recent heartbeat with no end attempt
    _running = (await add_run(db, flow_id=_flow["flow_id"], last_heartbeat_ts=now)).body

    flow_id = _flow["flow_id"]
    assert await _run_numbers(cli, flow_id, "?status=completed") == {_completed["run_number"]}
    assert await _run_numbers(cli, flow_id, "?status=running") == {_running["run_number"]}
    assert await _run_numbers(cli, flow_id, "?status=failed") == \
        {_failed_attempt["run_number"], _stale["run_number"]}


async def test_runs_get_status_filter_retrying(cli, db):
    # the first CASE branch: a run whose 'end' step failed (attempt_ok=false) but has a
    # *newer* 'attempt' record is being retried, so it reads as running, not failed.
    # The branch turns on a strict ts_epoch comparison between the two metadata rows, so
    # stamp their timestamps explicitly rather than trusting insert order.
    _flow = (await add_flow(db, "RetryFlow", "test_user-1", ["a_tag"], ["runtime:test"])).body
    flow_id = _flow["flow_id"]
    _retrying = (await add_run(db, flow_id=flow_id)).body

    async def stamp_end_metadata(field_name, value, ts):
        md = (await add_metadata(db, flow_id=flow_id, run_number=_retrying["run_number"],
                                 step_name="end", task_id=1,
                                 metadata={"field_name": field_name, "value": value})).body
        await db.metadata_table_postgres.update_row(
            filter_dict={"id": md["id"]}, update_dict={"ts_epoch": ts})

    # end step failed at T=1000, then a new attempt started at T=2000 (the retry)
    await stamp_end_metadata("attempt_ok", "false", 1000)
    await stamp_end_metadata("attempt", "1", 2000)

    assert await _run_numbers(cli, flow_id, "?status=running") == {_retrying["run_number"]}
    assert await _run_numbers(cli, flow_id, "?status=failed") == set()


async def test_runs_get_status_filter_composes_with_pagination(cli, db):
    # a filter has to ride on top of pagination: a limited page of "failed" runs must
    # be the matching rows taken *after* filtering, not a limited slice that then gets
    # filtered down.
    _flow = (await add_flow(db, "PageFlow", "test_user-1", ["a_tag"], ["runtime:test"])).body

    failed = [(await add_run(db, flow_id=_flow["flow_id"])).body for _ in range(3)]
    await add_run(db, flow_id=_flow["flow_id"], last_heartbeat_ts=int(time.time()))  # running

    page = (await db.run_table_postgres.get_all_runs(
        _flow["flow_id"], statuses=["failed"], limit=2, order=["run_number DESC"])).body

    assert len(page) == 2
    assert all(r["status"] == "failed" for r in page)
    newest_two = sorted((r["run_number"] for r in failed), reverse=True)[:2]
    assert [r["run_number"] for r in page] == newest_two


async def test_runs_get_time_range_filter(cli, db):
    # ts_epoch is stamped by the DB, so read it back and use the observed min/max as
    # bounds. Bounds are inclusive; a window outside the data returns nothing.
    _flow = (await add_flow(db, "TimeFlow", "test_user-1", ["a_tag"], ["runtime:test"])).body
    flow_id = _flow["flow_id"]

    runs = [(await add_run(db, flow_id=flow_id)).body for _ in range(3)]
    epochs = [int(r["ts_epoch"]) for r in runs]
    lo, hi = min(epochs), max(epochs)
    everyone = {r["run_number"] for r in runs}

    # inclusive bounds: an endpoint exactly on the data still matches it
    assert await _run_numbers(cli, flow_id, "?ts_from=%d" % lo) == everyone
    assert await _run_numbers(cli, flow_id, "?ts_to=%d" % hi) == everyone
    assert await _run_numbers(cli, flow_id, "?ts_from=%d&ts_to=%d" % (lo, hi)) == everyone
    # windows that sit entirely outside the data are empty, not an error
    assert await _run_numbers(cli, flow_id, "?ts_from=%d" % (hi + 1)) == set()
    assert await _run_numbers(cli, flow_id, "?ts_to=%d" % (lo - 1)) == set()
    # no bound still returns everything
    assert await _run_numbers(cli, flow_id) == everyone


async def test_runs_get_time_range_partitions_runs(cli, db):
    # stamp deterministic timestamps so the window genuinely splits the set.
    _flow = (await add_flow(db, "StampFlow", "test_user-1", ["a_tag"], ["runtime:test"])).body
    flow_id = _flow["flow_id"]

    async def run_at(ts):
        run = (await add_run(db, flow_id=flow_id)).body
        await db.run_table_postgres.update_row(
            filter_dict={"flow_id": flow_id, "run_number": run["run_number"]},
            update_dict={"ts_epoch": ts})
        return run["run_number"]

    old = await run_at(1000)
    mid = await run_at(2000)
    new = await run_at(3000)

    assert await _run_numbers(cli, flow_id, "?ts_from=2000") == {mid, new}
    assert await _run_numbers(cli, flow_id, "?ts_to=2000") == {old, mid}
    assert await _run_numbers(cli, flow_id, "?ts_from=1500&ts_to=2500") == {mid}


async def test_runs_get_time_range_rejects_non_integer(cli, db):
    _flow = (await add_flow(db, "BadTimeFlow", "test_user-1", ["a_tag"], ["runtime:test"])).body
    await add_run(db, flow_id=_flow["flow_id"])

    # a programmatic consumer passing a malformed bound should fail loud, not be
    # silently ignored (same stance as an unknown status).
    assert (await cli.get("/flows/{flow_id}/runs?ts_from=notanumber".format(**_flow))).status == 400
    assert (await cli.get("/flows/{flow_id}/runs?ts_to=2026-01-01".format(**_flow))).status == 400


async def test_runs_get_status_and_time_range_compose(cli, db):
    # status and time-range are ANDed, so a run must satisfy both to be returned.
    _flow = (await add_flow(db, "ComposeFlow", "test_user-1", ["a_tag"], ["runtime:test"])).body
    flow_id = _flow["flow_id"]

    async def run_at(ts, **kw):
        run = (await add_run(db, flow_id=flow_id, **kw)).body
        await db.run_table_postgres.update_row(
            filter_dict={"flow_id": flow_id, "run_number": run["run_number"]},
            update_dict={"ts_epoch": ts})
        return run["run_number"]

    now = int(time.time())
    old_failed = await run_at(1000)                          # failed, before window
    new_failed = await run_at(3000)                          # failed, inside window
    new_running = await run_at(3000, last_heartbeat_ts=now)  # running, inside window

    # status alone catches both failed runs regardless of time
    assert await _run_numbers(cli, flow_id, "?status=failed") == {old_failed, new_failed}
    # adding a time bound narrows it to the failed run inside the window
    assert await _run_numbers(cli, flow_id, "?status=failed&ts_from=2000") == {new_failed}
    # the running run is inside the window but excluded by the status predicate
    assert await _run_numbers(cli, flow_id, "?status=running&ts_from=2000") == {new_running}
    # a status that matches but a window that does not yields nothing
    assert await _run_numbers(cli, flow_id, "?status=running&ts_to=2000") == set()


async def test_run_get(cli, db):
    # create flow for test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body

    # add run to flow for testing
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body

    # try to get created flow
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}".format(**_run), data=_run)

    # non-existent flow or run should return 404
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234".format(**_run), status=404)
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}".format(**_run), status=404)


async def test_run_mutate_user_tags(cli, db):
    # create flow for test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body

    # add run to flow for testing
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body

    async def assert_tags_unchanged():
        _run_in_db = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body
        assert sorted(_run_in_db["system_tags"]) == sorted(_run["system_tags"])
        assert sorted(_run_in_db["tags"]) == sorted(_run["tags"])

    async def assert_tags_in_db(tags):
        _run_in_db = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body
        assert all(tag in _run_in_db["tags"] for tag in tags)

    async def assert_tags_not_in_db(tags):
        _run_in_db = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body
        assert all(tag not in _run_in_db["tags"] for tag in tags)

    # try invalid inputs (like tag lists that are not lists, or tag values that are not string)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                    payload={"tags_to_add": "so_meta"}, status=400)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                    payload={"tags_to_add": [5]}, status=400)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                    payload={"tags_to_remove": "so_meta"}, status=400)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                    payload={"tags_to_remove": [5]}, status=400)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/__NOT_A_RUN__/tag/mutate'.format(**_run),
                                    payload={"tags_to_add": ["user_tag"]}, status=404)

    # try to remove system tags - it should not work
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                    payload={"tags_to_remove": _run["system_tags"]}, status=422)
    await assert_tags_unchanged()

    # try to add system tags - it should be no-op (but no error)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                    payload={"tags_to_add": _run["system_tags"]}, status=200)
    await assert_tags_unchanged()

    # try to add user tags
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                    payload={"tags_to_add": ["coca-cola", "pepsi"]}, status=200)
    await assert_tags_in_db(["coca-cola", "pepsi"])

    # try to remove user tags
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                    payload={"tags_to_remove": ["coca-cola", "pepsi"]}, status=200)
    await assert_tags_not_in_db(["coca-cola", "pepsi"])

    # try to replace user tags
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                    payload={"tags_to_add": ["coca-cola", "pepsi"]}, status=200)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                    payload={"tags_to_add": ["sprite", "pepsi"],
                                             "tags_to_remove": ["coca-cola", "pepsi"]}, status=200)
    await assert_tags_in_db(["sprite", "pepsi"])
    await assert_tags_not_in_db(["coca-cola"])


async def test_run_mutate_user_tags_concurrency(cli, db):
    # create flow for test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body

    # add run to flow for testing.  Start with 0 user tags
    _run = (await add_run(db, flow_id=_flow["flow_id"], tags=[])).body

    async def _add_tag_request_with_retries(path, tag_to_add):
        attempts = 0
        delay = 0.2
        payload = {"tags_to_add": [tag_to_add]}
        r = random.Random(json.dumps(payload))
        for _ in range(10):
            attempts += 1
            response = await cli.patch(path, json=payload)
            if response.status == 200:
                # Parse the response, to make sure that it is the JSON format we
                # expect, AND the response correctly reflects the presence of the
                # tag being added
                data = json.loads(await response.text())
                assert tag_to_add in data["tags"]
                return attempts
            # 409 temporarily conflicting with another mutate request
            # See https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/409
            elif response.status == 409:
                delay *= r.uniform(1.0, 1.2)
                await asyncio.sleep(delay)
            else:
                raise AssertionError("Unexpected status %d" % response.status)
        raise AssertionError("Retries exhausted")

    # confirm that concurrent requests get serialized correctly, if they retry
    # in other words, no mutations get lost
    expected_tag_set = set()
    awaitables = []
    for i in range(50):
        a_tag = str(i)
        expected_tag_set.add(a_tag)
        awaitables.append(_add_tag_request_with_retries('/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run),
                                                        a_tag))
    attempt_counts = await asyncio.gather(*awaitables)
    assert sum(attempt_counts) > 50

    _run_in_db = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body
    assert sorted(_run_in_db["tags"]) == sorted(expected_tag_set)

