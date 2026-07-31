"""Unit tests for the run-tags join optimization.

The step/task/artifact read API returns the ancestral run's tags, not the row's
own stored tags. Handler-facing reads opt in via with_run_tags=True, embedding
a LEFT JOIN to runs_v3 in the query -- replacing the per-request get_run +
deepcopy in apply_run_tags_to_db_response. Default DB-layer reads are unchanged.
"""

from unittest import mock

from services.data.db_utils import DBResponse, DBPagination
from services.data.postgres_async_db import (
    AsyncPostgresTable,
    AsyncStepTablePostgres,
    AsyncTaskTablePostgres,
    AsyncArtifactTablePostgres,
    _run_tags_join,
    _run_tag_qualified_columns,
    RUN_TABLE_NAME,
    STEP_TABLE_NAME,
    TASK_TABLE_NAME,
)

_EMPTY_PAGE = (
    DBResponse(response_code=200, body=[]),
    DBPagination(limit=0, offset=0, count=0, page=1, next_cursor_record=None),
)


def test_join_references_runs_table():
    join = _run_tags_join(STEP_TABLE_NAME)
    assert RUN_TABLE_NAME in join
    assert "run_for_tags" in join


def test_join_qualifies_both_tables():
    join = _run_tags_join(TASK_TABLE_NAME)
    assert "tasks_v3.flow_id = run_for_tags.flow_id" in join
    assert "tasks_v3.run_number = run_for_tags.run_number" in join


def test_qualified_columns_pull_tags_from_join():
    keys = ["flow_id", "run_number", "tags", "system_tags"]
    col_sql = ", ".join(_run_tag_qualified_columns(STEP_TABLE_NAME, keys))
    assert "steps_v3.flow_id AS flow_id" in col_sql
    assert "steps_v3.run_number AS run_number" in col_sql
    assert "run_for_tags.tags AS tags" in col_sql
    assert "run_for_tags.system_tags AS system_tags" in col_sql


def test_default_select_columns_are_the_plain_keys():
    # Default DB-layer reads must stay byte-identical to the pre-optimization
    # behavior: unqualified keys, no run-tags redirection.
    for table_cls in (
        AsyncStepTablePostgres,
        AsyncTaskTablePostgres,
        AsyncArtifactTablePostgres,
    ):
        table = table_cls.__new__(table_cls)
        assert list(table.select_columns) == list(table.keys)


def test_opted_in_select_columns_redirect_tags():
    table = AsyncStepTablePostgres.__new__(AsyncStepTablePostgres)
    table._with_run_tags = True
    col_sql = ", ".join(table.select_columns)
    assert "run_for_tags.tags AS tags" in col_sql
    assert "run_for_tags.system_tags AS system_tags" in col_sql
    assert "steps_v3.step_name AS step_name" in col_sql


async def test_get_records_with_run_tags_forces_enable_joins():
    table = AsyncTaskTablePostgres.__new__(AsyncTaskTablePostgres)
    with mock.patch.object(
        AsyncPostgresTable, "find_records", new_callable=mock.AsyncMock
    ) as mock_super:
        mock_super.return_value = _EMPTY_PAGE
        await table.get_records(filter_dict={"flow_id": "f"}, with_run_tags=True)
        assert mock_super.call_args.kwargs["enable_joins"] is True


async def test_get_records_default_does_not_join():
    table = AsyncStepTablePostgres.__new__(AsyncStepTablePostgres)
    with mock.patch.object(
        AsyncPostgresTable, "find_records", new_callable=mock.AsyncMock
    ) as mock_super:
        mock_super.return_value = _EMPTY_PAGE
        await table.get_records(filter_dict={"flow_id": "f"})
        assert "enable_joins" not in mock_super.call_args.kwargs


async def test_get_filtered_tasks_paginated_honors_the_flag():
    table = AsyncTaskTablePostgres.__new__(AsyncTaskTablePostgres)
    with mock.patch.object(
        AsyncPostgresTable, "find_records", new_callable=mock.AsyncMock
    ) as mock_super:
        mock_super.return_value = _EMPTY_PAGE
        await table.get_filtered_tasks_paginated(
            conditions=["flow_id = %s"], values=["f"], limit=50, with_run_tags=True
        )
        assert mock_super.call_args.kwargs["enable_joins"] is True

        await table.get_filtered_tasks_paginated(
            conditions=["flow_id = %s"], values=["f"], limit=50
        )
        assert "enable_joins" not in mock_super.call_args.kwargs


async def test_artifact_paginated_sql_embeds_join_only_when_opted_in():
    """The custom paginated SQL templates bypass find_records, so the join must
    be embedded in the template itself -- and only when with_run_tags is set."""
    table = AsyncArtifactTablePostgres.__new__(AsyncArtifactTablePostgres)
    with mock.patch.object(
        AsyncArtifactTablePostgres, "execute_sql", new_callable=mock.AsyncMock
    ) as mock_exec:
        mock_exec.return_value = _EMPTY_PAGE

        await table.get_artifacts_in_runs_paginated(flow_id="f", run_id="1", limit=50)
        default_sql = mock_exec.call_args.kwargs["select_sql"]
        assert "run_for_tags" not in default_sql

        await table.get_artifacts_in_runs_paginated(
            flow_id="f", run_id="1", limit=50, with_run_tags=True
        )
        join_sql = mock_exec.call_args.kwargs["select_sql"]
        assert "run_for_tags.tags AS tags" in join_sql
        assert "artifact_v3.flow_id" in join_sql
