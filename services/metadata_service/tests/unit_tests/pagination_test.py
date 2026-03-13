import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from multidict import CIMultiDict, CIMultiDictProxy

from services.data.db_utils import DBResponse, DBPagination
from services.metadata_service.api.run import RunApi
from services.metadata_service.api.flow import FlowApi
from services.metadata_service.api.step import StepApi
from services.metadata_service.api.task import TaskApi
from services.metadata_service.api.metadata import MetadataApi
from services.metadata_service.api.artifact import ArtificatsApi


def _make_request(path, match_info=None, query=None):
    """Build a mocked aiohttp request with path params and query string."""
    if query is None:
        query = {}
    raw_headers = [(b"Host", b"localhost")]
    qs = "&".join("{}={}".format(k, v) for k, v in query.items())
    full_path = "{}?{}".format(path, qs) if qs else path
    req = make_mocked_request("GET", full_path, headers=CIMultiDict())
    if match_info:
        req.match_info.update(match_info)
    return req


def _make_db_response(records, code=200):
    return DBResponse(response_code=code, body=records)


def _make_pagination():
    return DBPagination(limit=0, offset=0, count=0, page=1)


def _sample_records(n, base_ts=1000000):
    """Generate n sample run-like records with descending ts_epoch."""
    return [
        {"flow_id": "TestFlow", "run_number": i, "ts_epoch": base_ts - i * 100,
         "user_name": "user", "tags": [], "system_tags": []}
        for i in range(n)
    ]


class TestRunPagination:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.app = web.Application()
        with patch("services.metadata_service.api.run.AsyncPostgresDB") as mock_db_cls:
            mock_instance = MagicMock()
            mock_db_cls.get_instance.return_value = mock_instance
            self.mock_table = MagicMock()
            mock_instance.run_table_postgres = self.mock_table
            self.api = RunApi(self.app)

    @pytest.mark.asyncio
    async def test_no_params_returns_all(self):
        records = _sample_records(5)
        self.mock_table.get_all_runs = AsyncMock(
            return_value=_make_db_response(records))

        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"})
        resp = await self.api.get_all_runs(req)

        assert resp.status == 200
        body = json.loads(resp.text)
        assert len(body) == 5
        assert "X-Has-More" not in resp.headers

    @pytest.mark.asyncio
    async def test_limit_returns_page_with_headers(self):
        records = _sample_records(4)
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response(records), _make_pagination()))

        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"},
                            query={"_limit": "3"})
        resp = await self.api.get_all_runs(req)

        assert resp.status == 200
        body = json.loads(resp.text)
        assert len(body) == 3
        assert resp.headers["X-Has-More"] == "true"
        assert "X-Next-Cursor" in resp.headers

    @pytest.mark.asyncio
    async def test_last_page_has_more_false(self):
        records = _sample_records(2)
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response(records), _make_pagination()))

        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"},
                            query={"_limit": "5"})
        resp = await self.api.get_all_runs(req)

        assert resp.status == 200
        body = json.loads(resp.text)
        assert len(body) == 2
        assert resp.headers["X-Has-More"] == "false"
        assert "X-Next-Cursor" not in resp.headers

    @pytest.mark.asyncio
    async def test_cursor_without_limit_returns_400(self):
        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"},
                            query={"_cursor": "999999"})
        resp = await self.api.get_all_runs(req)

        assert resp.status == 400
        body = json.loads(resp.text)
        assert "error" in body
        assert "_limit is required" in body["error"]

    @pytest.mark.asyncio
    async def test_invalid_limit_returns_400(self):
        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"},
                            query={"_limit": "abc"})
        resp = await self.api.get_all_runs(req)

        assert resp.status == 400
        body = json.loads(resp.text)
        assert "error" in body
        assert "_limit" in body["error"]

    @pytest.mark.asyncio
    async def test_invalid_cursor_returns_400(self):
        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"},
                            query={"_limit": "10", "_cursor": "not_a_number"})
        resp = await self.api.get_all_runs(req)

        assert resp.status == 400
        body = json.loads(resp.text)
        assert "error" in body
        assert "_cursor" in body["error"]

    @pytest.mark.asyncio
    async def test_negative_limit_returns_400(self):
        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"},
                            query={"_limit": "-5"})
        resp = await self.api.get_all_runs(req)

        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_empty_result_with_cursor(self):
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response([]), _make_pagination()))

        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"},
                            query={"_limit": "10", "_cursor": "100"})
        resp = await self.api.get_all_runs(req)

        assert resp.status == 200
        body = json.loads(resp.text)
        assert body == []
        assert resp.headers["X-Has-More"] == "false"

    @pytest.mark.asyncio
    async def test_cursor_passed_to_find_records(self):
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response([]), _make_pagination()))

        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"},
                            query={"_limit": "10", "_cursor": "500000"})
        await self.api.get_all_runs(req)

        call_kwargs = self.mock_table.find_records.call_args[1]
        assert "ts_epoch < %s" in call_kwargs["conditions"]
        assert 500000 in call_kwargs["values"]
        assert call_kwargs["order"] == ["ts_epoch DESC"]
        assert call_kwargs["limit"] == 11


class TestFlowPagination:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.app = web.Application()
        with patch("services.metadata_service.api.flow.AsyncPostgresDB") as mock_db_cls:
            mock_instance = MagicMock()
            mock_db_cls.get_instance.return_value = mock_instance
            self.mock_table = MagicMock()
            mock_instance.flow_table_postgres = self.mock_table
            self.api = FlowApi(self.app)

    @pytest.mark.asyncio
    async def test_no_params_returns_all(self):
        records = [{"flow_id": "A", "ts_epoch": 100, "user_name": "u", "tags": [], "system_tags": []}]
        self.mock_table.get_all_flows = AsyncMock(
            return_value=_make_db_response(records))

        req = _make_request("/flows")
        resp = await self.api.get_all_flows(req)

        assert resp.status == 200
        assert "X-Has-More" not in resp.headers

    @pytest.mark.asyncio
    async def test_cursor_without_limit_returns_400(self):
        req = _make_request("/flows", query={"_cursor": "999"})
        resp = await self.api.get_all_flows(req)
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_invalid_limit_returns_400(self):
        req = _make_request("/flows", query={"_limit": "xyz"})
        resp = await self.api.get_all_flows(req)
        assert resp.status == 400


class TestStepPagination:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.app = web.Application()
        with patch("services.metadata_service.api.step.AsyncPostgresDB") as mock_db_cls:
            mock_instance = MagicMock()
            mock_db_cls.get_instance.return_value = mock_instance
            self.mock_table = MagicMock()
            self.mock_run_table = MagicMock()
            mock_instance.step_table_postgres = self.mock_table
            mock_instance.run_table_postgres = self.mock_run_table
            self.api = StepApi(self.app)

    @pytest.mark.asyncio
    async def test_no_params_returns_all(self):
        records = [{"flow_id": "F", "run_number": 1, "step_name": "s",
                     "ts_epoch": 100, "user_name": "u", "tags": [], "system_tags": []}]
        db_resp = _make_db_response(records)
        self.mock_table.get_steps = AsyncMock(return_value=db_resp)
        self.mock_run_table.get_run = AsyncMock(
            return_value=_make_db_response({"tags": [], "system_tags": []}))

        req = _make_request("/flows/F/runs/1/steps",
                            match_info={"flow_id": "F", "run_number": "1"})
        resp = await self.api.get_steps(req)

        assert resp.status == 200
        assert "X-Has-More" not in resp.headers

    @pytest.mark.asyncio
    async def test_cursor_without_limit_returns_400(self):
        req = _make_request("/flows/F/runs/1/steps",
                            match_info={"flow_id": "F", "run_number": "1"},
                            query={"_cursor": "999"})
        resp = await self.api.get_steps(req)
        assert resp.status == 400


class TestTaskPagination:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.app = web.Application()
        with patch("services.metadata_service.api.task.AsyncPostgresDB") as mock_db_cls:
            mock_instance = MagicMock()
            mock_db_cls.get_instance.return_value = mock_instance
            self.mock_table = MagicMock()
            self.mock_run_table = MagicMock()
            mock_instance.task_table_postgres = self.mock_table
            mock_instance.run_table_postgres = self.mock_run_table
            self.api = TaskApi(self.app)

    @pytest.mark.asyncio
    async def test_no_params_returns_all(self):
        records = [{"flow_id": "F", "run_number": 1, "step_name": "s",
                     "task_id": 1, "ts_epoch": 100, "user_name": "u",
                     "tags": [], "system_tags": []}]
        db_resp = _make_db_response(records)
        self.mock_table.get_tasks = AsyncMock(return_value=db_resp)
        self.mock_run_table.get_run = AsyncMock(
            return_value=_make_db_response({"tags": [], "system_tags": []}))

        req = _make_request("/flows/F/runs/1/steps/s/tasks",
                            match_info={"flow_id": "F", "run_number": "1", "step_name": "s"})
        resp = await self.api.get_tasks(req)

        assert resp.status == 200
        assert "X-Has-More" not in resp.headers

    @pytest.mark.asyncio
    async def test_cursor_without_limit_returns_400(self):
        req = _make_request("/flows/F/runs/1/steps/s/tasks",
                            match_info={"flow_id": "F", "run_number": "1", "step_name": "s"},
                            query={"_cursor": "999"})
        resp = await self.api.get_tasks(req)
        assert resp.status == 400


class TestMetadataPagination:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.app = web.Application()
        with patch("services.metadata_service.api.metadata.AsyncPostgresDB") as mock_db_cls:
            mock_instance = MagicMock()
            mock_db_cls.get_instance.return_value = mock_instance
            self.mock_table = MagicMock()
            mock_instance.metadata_table_postgres = self.mock_table
            self.api = MetadataApi(self.app)

    @pytest.mark.asyncio
    async def test_no_params_get_metadata(self):
        records = [{"flow_id": "F", "ts_epoch": 100}]
        self.mock_table.get_metadata = AsyncMock(
            return_value=_make_db_response(records))

        req = _make_request("/flows/F/runs/1/steps/s/tasks/1/metadata",
                            match_info={"flow_id": "F", "run_number": "1",
                                        "step_name": "s", "task_id": "1"})
        resp = await self.api.get_metadata(req)

        assert resp.status == 200
        assert "X-Has-More" not in resp.headers

    @pytest.mark.asyncio
    async def test_cursor_without_limit_get_metadata(self):
        req = _make_request("/flows/F/runs/1/steps/s/tasks/1/metadata",
                            match_info={"flow_id": "F", "run_number": "1",
                                        "step_name": "s", "task_id": "1"},
                            query={"_cursor": "999"})
        resp = await self.api.get_metadata(req)
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_no_params_get_metadata_by_run(self):
        records = [{"flow_id": "F", "ts_epoch": 100}]
        self.mock_table.get_metadata_in_runs = AsyncMock(
            return_value=_make_db_response(records))

        req = _make_request("/flows/F/runs/1/metadata",
                            match_info={"flow_id": "F", "run_number": "1"})
        resp = await self.api.get_metadata_by_run(req)

        assert resp.status == 200
        assert "X-Has-More" not in resp.headers

    @pytest.mark.asyncio
    async def test_paginated_metadata_by_run(self):
        records = _sample_records(4)
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response(records), _make_pagination()))

        req = _make_request("/flows/F/runs/1/metadata",
                            match_info={"flow_id": "F", "run_number": "1"},
                            query={"_limit": "3"})
        resp = await self.api.get_metadata_by_run(req)

        assert resp.status == 200
        body = json.loads(resp.text)
        assert len(body) == 3
        assert resp.headers["X-Has-More"] == "true"
        assert "X-Next-Cursor" in resp.headers


def _artifact_records(specs, base_ts=1000000):
    """Build artifact records from (task_id, attempt_id) tuples.

    Each record gets a descending ts_epoch so the ordering matches
    what the DB would return with ORDER BY ts_epoch DESC.
    """
    records = []
    for i, (tid, aid) in enumerate(specs):
        records.append({
            "flow_id": "F", "run_number": 1, "step_name": "s",
            "task_id": tid, "attempt_id": aid,
            "name": "art_{}".format(i),
            "ts_epoch": base_ts - i * 100,
            "tags": [], "system_tags": [],
        })
    return records


class TestArtifactPaginationByTask:

    _path = "/flows/F/runs/1/steps/s/tasks/1/artifacts"
    _match = {"flow_id": "F", "run_number": "1",
              "step_name": "s", "task_id": "1"}

    @pytest.fixture(autouse=True)
    def setup(self):
        self.app = web.Application()
        with patch("services.metadata_service.api.artifact.AsyncPostgresDB") as mock_db_cls:
            mock_instance = MagicMock()
            mock_db_cls.get_instance.return_value = mock_instance
            self.mock_table = MagicMock()
            self.mock_run_table = MagicMock()
            mock_instance.artifact_table_postgres = self.mock_table
            mock_instance.run_table_postgres = self.mock_run_table
            self.api = ArtificatsApi(self.app)
        self.mock_run_table.get_run = AsyncMock(
            return_value=_make_db_response({"tags": [], "system_tags": []}))

    @pytest.mark.asyncio
    async def test_no_params_returns_filtered_artifacts(self):
        """Non-paginated path still filters for latest attempt."""
        records = _artifact_records([
            (1, 0), (1, 1), (1, 1),
        ])
        self.mock_table.get_artifact_in_task = AsyncMock(
            return_value=_make_db_response(records))

        req = _make_request(self._path, match_info=self._match)
        resp = await self.api.get_artifacts_by_task(req)

        assert resp.status == 200
        body = json.loads(resp.text)
        # attempt 0 filtered out, only attempt 1 artifacts remain
        assert len(body) == 2
        assert all(a["attempt_id"] == 1 for a in body)

    @pytest.mark.asyncio
    async def test_filter_reduces_page_but_cursor_reflects_raw(self):
        """The key interaction: filter runs after trim.

        DB returns 4 raw records (limit+1 trick with _limit=3).
        Raw records include mixed attempts. After trim to 3, the
        filter removes old-attempt artifacts. The response body is
        smaller than page_limit, but X-Has-More and X-Next-Cursor
        still reflect the raw boundary.
        """
        # 4 records: task 1 has attempts 0 and 1
        # After trim to 3 we keep first 3; after filter, attempt 0 is dropped
        raw = _artifact_records([
            (1, 1),   # ts=1000000 - kept by filter
            (1, 0),   # ts=999900  - dropped by filter (older attempt)
            (1, 1),   # ts=999800  - kept by filter, this is records[-1] after trim
            (1, 1),   # ts=999700  - the +1 overflow record, proves has_more
        ])
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response(raw), _make_pagination()))

        req = _make_request(self._path, match_info=self._match,
                            query={"_limit": "3"})
        resp = await self.api.get_artifacts_by_task(req)

        assert resp.status == 200
        body = json.loads(resp.text)

        # filter removed the attempt=0 record from the trimmed page
        assert len(body) == 2
        assert all(a["attempt_id"] == 1 for a in body)

        # pagination headers reflect raw records, not filtered
        assert resp.headers["X-Has-More"] == "true"
        # cursor should be ts_epoch of 3rd raw record (index 2), which is 999800
        assert resp.headers["X-Next-Cursor"] == "999800"

    @pytest.mark.asyncio
    async def test_last_page_no_cursor(self):
        """Fewer records than limit+1 means last page."""
        raw = _artifact_records([
            (1, 1),   # ts=1000000
            (1, 1),   # ts=999900
        ])
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response(raw), _make_pagination()))

        req = _make_request(self._path, match_info=self._match,
                            query={"_limit": "5"})
        resp = await self.api.get_artifacts_by_task(req)

        assert resp.status == 200
        body = json.loads(resp.text)
        assert len(body) == 2
        assert resp.headers["X-Has-More"] == "false"
        assert "X-Next-Cursor" not in resp.headers

    @pytest.mark.asyncio
    async def test_cursor_without_limit_returns_400(self):
        req = _make_request(self._path, match_info=self._match,
                            query={"_cursor": "999"})
        resp = await self.api.get_artifacts_by_task(req)
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_cursor_passed_to_find_records(self):
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response([]), _make_pagination()))

        req = _make_request(self._path, match_info=self._match,
                            query={"_limit": "10", "_cursor": "500000"})
        await self.api.get_artifacts_by_task(req)

        call_kwargs = self.mock_table.find_records.call_args[1]
        assert "ts_epoch < %s" in call_kwargs["conditions"]
        assert 500000 in call_kwargs["values"]
        assert call_kwargs["limit"] == 11

    @pytest.mark.asyncio
    async def test_filter_is_page_local(self):
        """Filter only sees the trimmed page, not global state.

        If the page contains only attempt 0 records for a task, the
        filter treats attempt 0 as the latest *within this page* and
        keeps them all. The overflow record (attempt 1) was discarded
        before filtering, so the filter has no knowledge of it.
        """
        raw = _artifact_records([
            (1, 0),   # ts=1000000 - attempt 0, kept (latest on this page)
            (1, 0),   # ts=999900  - attempt 0, kept (latest on this page)
            (1, 1),   # ts=999800  - overflow, trimmed before filter runs
        ])
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response(raw), _make_pagination()))

        req = _make_request(self._path, match_info=self._match,
                            query={"_limit": "2"})
        resp = await self.api.get_artifacts_by_task(req)

        assert resp.status == 200
        body = json.loads(resp.text)
        # filter sees attempt 0 as max on this page, keeps both
        assert len(body) == 2
        assert all(a["attempt_id"] == 0 for a in body)
        assert resp.headers["X-Has-More"] == "true"


class TestArtifactPaginationByStep:

    _path = "/flows/F/runs/1/steps/s/artifacts"
    _match = {"flow_id": "F", "run_number": "1", "step_name": "s"}

    @pytest.fixture(autouse=True)
    def setup(self):
        self.app = web.Application()
        with patch("services.metadata_service.api.artifact.AsyncPostgresDB") as mock_db_cls:
            mock_instance = MagicMock()
            mock_db_cls.get_instance.return_value = mock_instance
            self.mock_table = MagicMock()
            self.mock_run_table = MagicMock()
            mock_instance.artifact_table_postgres = self.mock_table
            mock_instance.run_table_postgres = self.mock_run_table
            self.api = ArtificatsApi(self.app)
        self.mock_run_table.get_run = AsyncMock(
            return_value=_make_db_response({"tags": [], "system_tags": []}))

    @pytest.mark.asyncio
    async def test_paginated_with_filter(self):
        raw = _artifact_records([
            (1, 1),   # kept
            (2, 0),   # dropped — task 2 has attempt 1 later in list
            (2, 1),   # kept
            (1, 1),   # overflow
        ])
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response(raw), _make_pagination()))

        req = _make_request(self._path, match_info=self._match,
                            query={"_limit": "3"})
        resp = await self.api.get_artifacts_by_step(req)

        assert resp.status == 200
        body = json.loads(resp.text)
        assert len(body) == 2
        assert resp.headers["X-Has-More"] == "true"

    @pytest.mark.asyncio
    async def test_cursor_without_limit_returns_400(self):
        req = _make_request(self._path, match_info=self._match,
                            query={"_cursor": "999"})
        resp = await self.api.get_artifacts_by_step(req)
        assert resp.status == 400


class TestArtifactPaginationByRun:

    _path = "/flows/F/runs/1/artifacts"
    _match = {"flow_id": "F", "run_number": "1"}

    @pytest.fixture(autouse=True)
    def setup(self):
        self.app = web.Application()
        with patch("services.metadata_service.api.artifact.AsyncPostgresDB") as mock_db_cls:
            mock_instance = MagicMock()
            mock_db_cls.get_instance.return_value = mock_instance
            self.mock_table = MagicMock()
            self.mock_run_table = MagicMock()
            mock_instance.artifact_table_postgres = self.mock_table
            mock_instance.run_table_postgres = self.mock_run_table
            self.api = ArtificatsApi(self.app)
        self.mock_run_table.get_run = AsyncMock(
            return_value=_make_db_response({"tags": [], "system_tags": []}))

    @pytest.mark.asyncio
    async def test_paginated_with_filter(self):
        raw = _artifact_records([
            (1, 1),
            (1, 0),   # dropped by filter
            (1, 1),
            (1, 1),   # overflow
        ])
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response(raw), _make_pagination()))

        req = _make_request(self._path, match_info=self._match,
                            query={"_limit": "3"})
        resp = await self.api.get_artifacts_by_run(req)

        assert resp.status == 200
        body = json.loads(resp.text)
        assert len(body) == 2
        assert resp.headers["X-Has-More"] == "true"
        assert resp.headers["X-Next-Cursor"] == "999800"

    @pytest.mark.asyncio
    async def test_last_page_no_overflow(self):
        raw = _artifact_records([
            (1, 1),
            (1, 1),
        ])
        self.mock_table.find_records = AsyncMock(
            return_value=(_make_db_response(raw), _make_pagination()))

        req = _make_request(self._path, match_info=self._match,
                            query={"_limit": "5"})
        resp = await self.api.get_artifacts_by_run(req)

        assert resp.status == 200
        assert resp.headers["X-Has-More"] == "false"
        assert "X-Next-Cursor" not in resp.headers

    @pytest.mark.asyncio
    async def test_cursor_without_limit_returns_400(self):
        req = _make_request(self._path, match_info=self._match,
                            query={"_cursor": "999"})
        resp = await self.api.get_artifacts_by_run(req)
        assert resp.status == 400
