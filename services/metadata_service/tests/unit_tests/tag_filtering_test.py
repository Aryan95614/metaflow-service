import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from multidict import CIMultiDict, CIMultiDictProxy

from services.data.db_utils import DBResponse, DBPagination
from services.metadata_service.api.run import RunApi
from services.metadata_service.api.flow import FlowApi


def _make_request(query_params=None, match_info=None):
    request = MagicMock()
    if query_params is None:
        query_params = {}
    multi = CIMultiDict()
    for k, v in query_params.items():
        if isinstance(v, list):
            for item in v:
                multi.add(k, item)
        else:
            multi.add(k, v)
    request.query = CIMultiDictProxy(multi)
    request.match_info = match_info or {}
    return request


def _run_record(flow_id="TestFlow", run_number=1, tags=None, system_tags=None):
    return {
        "flow_id": flow_id,
        "run_number": run_number,
        "user_name": "testuser",
        "ts_epoch": 1000000,
        "tags": tags or [],
        "system_tags": system_tags or [],
        "last_heartbeat_ts": None,
    }


def _flow_record(flow_id="TestFlow", tags=None, system_tags=None):
    return {
        "flow_id": flow_id,
        "user_name": "testuser",
        "ts_epoch": 1000000,
        "tags": tags or [],
        "system_tags": system_tags or [],
    }


class TestRunTagFiltering:
    @pytest.fixture
    def run_api(self):
        app = MagicMock()
        app.router = MagicMock()
        app.router.add_route = MagicMock()
        with patch("services.metadata_service.api.run.AsyncPostgresDB"):
            api = RunApi(app)
        api._async_table = MagicMock()
        return api

    async def test_no_tag_param_returns_all_records(self, run_api):
        records = [_run_record(run_number=1), _run_record(run_number=2)]
        run_api._async_table.get_all_runs = AsyncMock(
            return_value=DBResponse(response_code=200, body=records)
        )
        request = _make_request(match_info={"flow_id": "TestFlow"})

        result = await run_api.get_all_runs.__wrapped__.__wrapped__(run_api, request)

        run_api._async_table.get_all_runs.assert_awaited_once_with("TestFlow")
        assert result.response_code == 200
        assert len(result.body) == 2

    async def test_single_tag_calls_find_records(self, run_api):
        records = [_run_record(run_number=1, tags=["env:prod"])]
        run_api._async_table.find_records = AsyncMock(
            return_value=(DBResponse(response_code=200, body=records),
                          DBPagination(limit=0, offset=0, count=1, page=1))
        )
        request = _make_request(
            query_params={"_tag": "env:prod"},
            match_info={"flow_id": "TestFlow"},
        )

        result = await run_api.get_all_runs.__wrapped__.__wrapped__(run_api, request)

        run_api._async_table.find_records.assert_awaited_once()
        call_args = run_api._async_table.find_records.call_args
        conditions = call_args.kwargs.get("conditions", call_args[1].get("conditions", call_args[0][0] if call_args[0] else None))
        values = call_args.kwargs.get("values", call_args[1].get("values", call_args[0][1] if len(call_args[0]) > 1 else None))

        assert "flow_id = %s" in conditions
        assert any("?&" in c for c in conditions)
        assert "TestFlow" in values
        assert "env:prod" in values
        assert result.response_code == 200

    async def test_multiple_tags_all_passed_to_query(self, run_api):
        run_api._async_table.find_records = AsyncMock(
            return_value=(DBResponse(response_code=200, body=[]),
                          DBPagination(limit=0, offset=0, count=0, page=1))
        )
        request = _make_request(
            query_params={"_tag": ["env:prod", "user:alice"]},
            match_info={"flow_id": "TestFlow"},
        )

        result = await run_api.get_all_runs.__wrapped__.__wrapped__(run_api, request)

        call_args = run_api._async_table.find_records.call_args
        conditions = call_args.kwargs.get("conditions", call_args[1].get("conditions"))
        values = call_args.kwargs.get("values", call_args[1].get("values"))

        tag_condition = [c for c in conditions if "?&" in c][0]
        assert "%s,%s" in tag_condition or tag_condition.count("%s") == 2
        assert "env:prod" in values
        assert "user:alice" in values

    async def test_no_matching_records_returns_empty(self, run_api):
        run_api._async_table.find_records = AsyncMock(
            return_value=(DBResponse(response_code=200, body=[]),
                          DBPagination(limit=0, offset=0, count=0, page=1))
        )
        request = _make_request(
            query_params={"_tag": "nonexistent"},
            match_info={"flow_id": "TestFlow"},
        )

        result = await run_api.get_all_runs.__wrapped__.__wrapped__(run_api, request)

        assert result.response_code == 200
        assert result.body == []

    async def test_tag_filter_uses_combined_tags_system_tags(self, run_api):
        run_api._async_table.find_records = AsyncMock(
            return_value=(DBResponse(response_code=200, body=[]),
                          DBPagination(limit=0, offset=0, count=0, page=1))
        )
        request = _make_request(
            query_params={"_tag": "runtime:dev"},
            match_info={"flow_id": "TestFlow"},
        )

        await run_api.get_all_runs.__wrapped__.__wrapped__(run_api, request)

        call_args = run_api._async_table.find_records.call_args
        conditions = call_args.kwargs.get("conditions", call_args[1].get("conditions"))

        tag_condition = [c for c in conditions if "?&" in c][0]
        assert "tags||system_tags" in tag_condition


class TestFlowTagFiltering:
    @pytest.fixture
    def flow_api(self):
        app = MagicMock()
        app.router = MagicMock()
        app.router.add_route = MagicMock()
        with patch("services.metadata_service.api.flow.AsyncPostgresDB"):
            api = FlowApi(app)
        api._async_table = MagicMock()
        return api

    async def test_no_tag_param_returns_all_flows(self, flow_api):
        records = [_flow_record("FlowA"), _flow_record("FlowB")]
        flow_api._async_table.get_all_flows = AsyncMock(
            return_value=DBResponse(response_code=200, body=records)
        )
        request = _make_request()

        result = await flow_api.get_all_flows.__wrapped__.__wrapped__(flow_api, request)

        flow_api._async_table.get_all_flows.assert_awaited_once()
        assert result.response_code == 200
        assert len(result.body) == 2

    async def test_single_tag_filters_flows(self, flow_api):
        records = [_flow_record("FlowA", tags=["team:ml"])]
        flow_api._async_table.find_records = AsyncMock(
            return_value=(DBResponse(response_code=200, body=records),
                          DBPagination(limit=0, offset=0, count=1, page=1))
        )
        request = _make_request(query_params={"_tag": "team:ml"})

        result = await flow_api.get_all_flows.__wrapped__.__wrapped__(flow_api, request)

        flow_api._async_table.find_records.assert_awaited_once()
        call_args = flow_api._async_table.find_records.call_args
        conditions = call_args.kwargs.get("conditions", call_args[1].get("conditions"))
        values = call_args.kwargs.get("values", call_args[1].get("values"))

        assert any("?&" in c for c in conditions)
        assert "team:ml" in values
        assert result.response_code == 200

    async def test_multiple_tags_filters_flows(self, flow_api):
        flow_api._async_table.find_records = AsyncMock(
            return_value=(DBResponse(response_code=200, body=[]),
                          DBPagination(limit=0, offset=0, count=0, page=1))
        )
        request = _make_request(query_params={"_tag": ["team:ml", "env:prod"]})

        result = await flow_api.get_all_flows.__wrapped__.__wrapped__(flow_api, request)

        call_args = flow_api._async_table.find_records.call_args
        values = call_args.kwargs.get("values", call_args[1].get("values"))

        assert "team:ml" in values
        assert "env:prod" in values

    async def test_no_matching_flows_returns_empty(self, flow_api):
        flow_api._async_table.find_records = AsyncMock(
            return_value=(DBResponse(response_code=200, body=[]),
                          DBPagination(limit=0, offset=0, count=0, page=1))
        )
        request = _make_request(query_params={"_tag": "nonexistent"})

        result = await flow_api.get_all_flows.__wrapped__.__wrapped__(flow_api, request)

        assert result.response_code == 200
        assert result.body == []
