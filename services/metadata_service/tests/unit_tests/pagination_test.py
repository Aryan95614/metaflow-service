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
        body = json.loads(resp.body)
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
        body = json.loads(resp.body)
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
        body = json.loads(resp.body)
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
        body = json.loads(resp.body)
        assert "error" in body
        assert "_limit is required" in body["error"]

    @pytest.mark.asyncio
    async def test_invalid_limit_returns_400(self):
        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"},
                            query={"_limit": "abc"})
        resp = await self.api.get_all_runs(req)

        assert resp.status == 400
        body = json.loads(resp.body)
        assert "error" in body
        assert "_limit" in body["error"]

    @pytest.mark.asyncio
    async def test_invalid_cursor_returns_400(self):
        req = _make_request("/flows/TestFlow/runs",
                            match_info={"flow_id": "TestFlow"},
                            query={"_limit": "10", "_cursor": "not_a_number"})
        resp = await self.api.get_all_runs(req)

        assert resp.status == 400
        body = json.loads(resp.body)
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
        body = json.loads(resp.body)
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
        body = json.loads(resp.body)
        assert len(body) == 3
        assert resp.headers["X-Has-More"] == "true"
        assert "X-Next-Cursor" in resp.headers
