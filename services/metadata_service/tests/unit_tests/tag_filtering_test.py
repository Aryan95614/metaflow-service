import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from multidict import CIMultiDict, CIMultiDictProxy

from services.data.db_utils import DBResponse, DBPagination
from services.metadata_service.api.utils import tag_conditions
from services.metadata_service.api.run import RunApi
from services.metadata_service.api.flow import FlowApi


def _make_request(query_params=None, match_info=None):
    request = MagicMock()
    multi = CIMultiDict()
    for k, v in (query_params or {}).items():
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


def _mock_find_records(body):
    return AsyncMock(
        return_value=(
            DBResponse(response_code=200, body=body),
            DBPagination(limit=0, offset=0, count=len(body), page=1),
        )
    )


class TestTagConditionsUtility:
    def test_empty_query_returns_empty(self):
        conditions, values = tag_conditions(CIMultiDictProxy(CIMultiDict()))
        assert conditions == []
        assert values == []

    def test_single_tag(self):
        query = CIMultiDictProxy(CIMultiDict({"_tags": "env:prod"}))
        conditions, values = tag_conditions(query)
        assert len(conditions) == 1
        assert "COALESCE" in conditions[0]
        assert "?&" in conditions[0]
        assert values == ["env:prod"]

    def test_comma_separated_tags(self):
        query = CIMultiDictProxy(CIMultiDict({"_tags": "env:prod,user:alice"}))
        conditions, values = tag_conditions(query)
        assert len(conditions) == 1
        assert conditions[0].count("%s") == 2
        assert values == ["env:prod", "user:alice"]

    def test_whitespace_in_tags_is_trimmed(self):
        query = CIMultiDictProxy(CIMultiDict({"_tags": " env:prod , user:alice "}))
        conditions, values = tag_conditions(query)
        assert values == ["env:prod", "user:alice"]

    def test_empty_tags_string_returns_empty(self):
        query = CIMultiDictProxy(CIMultiDict({"_tags": ""}))
        conditions, values = tag_conditions(query)
        assert conditions == []
        assert values == []

    def test_only_commas_returns_empty(self):
        query = CIMultiDictProxy(CIMultiDict({"_tags": ",,,"}))
        conditions, values = tag_conditions(query)
        assert conditions == []
        assert values == []

    def test_special_characters_in_tags(self):
        query = CIMultiDictProxy(CIMultiDict({"_tags": "user:alice@corp,runtime:dev-v2.1"}))
        conditions, values = tag_conditions(query)
        assert values == ["user:alice@corp", "runtime:dev-v2.1"]

    def test_coalesce_null_safety(self):
        query = CIMultiDictProxy(CIMultiDict({"_tags": "sometag"}))
        conditions, _ = tag_conditions(query)
        assert "COALESCE(tags, '[]'::jsonb)" in conditions[0]
        assert "COALESCE(system_tags, '[]'::jsonb)" in conditions[0]


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

    async def test_no_tags_param_returns_all_records(self, run_api):
        records = [_run_record(run_number=1), _run_record(run_number=2)]
        run_api._async_table.get_all_runs = AsyncMock(
            return_value=DBResponse(response_code=200, body=records)
        )
        request = _make_request(match_info={"flow_id": "TestFlow"})

        result = await run_api.get_all_runs.__wrapped__.__wrapped__(run_api, request)

        run_api._async_table.get_all_runs.assert_awaited_once_with("TestFlow")
        assert result.response_code == 200
        assert len(result.body) == 2

    async def test_tags_param_calls_find_records(self, run_api):
        records = [_run_record(run_number=1, tags=["env:prod"])]
        run_api._async_table.find_records = _mock_find_records(records)
        request = _make_request(
            query_params={"_tags": "env:prod"},
            match_info={"flow_id": "TestFlow"},
        )

        result = await run_api.get_all_runs.__wrapped__.__wrapped__(run_api, request)

        run_api._async_table.find_records.assert_awaited_once()
        call_kwargs = run_api._async_table.find_records.call_args.kwargs
        assert "flow_id = %s" in call_kwargs["conditions"]
        assert "TestFlow" in call_kwargs["values"]
        assert "env:prod" in call_kwargs["values"]
        assert result.response_code == 200

    async def test_comma_separated_tags(self, run_api):
        run_api._async_table.find_records = _mock_find_records([])
        request = _make_request(
            query_params={"_tags": "env:prod,user:alice"},
            match_info={"flow_id": "TestFlow"},
        )

        await run_api.get_all_runs.__wrapped__.__wrapped__(run_api, request)

        call_kwargs = run_api._async_table.find_records.call_args.kwargs
        assert "env:prod" in call_kwargs["values"]
        assert "user:alice" in call_kwargs["values"]

    async def test_no_matches_returns_empty(self, run_api):
        run_api._async_table.find_records = _mock_find_records([])
        request = _make_request(
            query_params={"_tags": "nonexistent"},
            match_info={"flow_id": "TestFlow"},
        )

        result = await run_api.get_all_runs.__wrapped__.__wrapped__(run_api, request)

        assert result.response_code == 200
        assert result.body == []

    async def test_system_tags_only_match(self, run_api):
        records = [_run_record(run_number=1, system_tags=["runtime:dev"])]
        run_api._async_table.find_records = _mock_find_records(records)
        request = _make_request(
            query_params={"_tags": "runtime:dev"},
            match_info={"flow_id": "TestFlow"},
        )

        result = await run_api.get_all_runs.__wrapped__.__wrapped__(run_api, request)

        assert result.response_code == 200
        assert len(result.body) == 1


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

    async def test_no_tags_param_returns_all_flows(self, flow_api):
        records = [_flow_record("FlowA"), _flow_record("FlowB")]
        flow_api._async_table.get_all_flows = AsyncMock(
            return_value=DBResponse(response_code=200, body=records)
        )
        request = _make_request()

        result = await flow_api.get_all_flows.__wrapped__.__wrapped__(flow_api, request)

        flow_api._async_table.get_all_flows.assert_awaited_once()
        assert result.response_code == 200
        assert len(result.body) == 2

    async def test_tags_param_filters_flows(self, flow_api):
        records = [_flow_record("FlowA", tags=["team:ml"])]
        flow_api._async_table.find_records = _mock_find_records(records)
        request = _make_request(query_params={"_tags": "team:ml"})

        result = await flow_api.get_all_flows.__wrapped__.__wrapped__(flow_api, request)

        flow_api._async_table.find_records.assert_awaited_once()
        assert result.response_code == 200

    async def test_no_matching_flows_returns_empty(self, flow_api):
        flow_api._async_table.find_records = _mock_find_records([])
        request = _make_request(query_params={"_tags": "nonexistent"})

        result = await flow_api.get_all_flows.__wrapped__.__wrapped__(flow_api, request)

        assert result.response_code == 200
        assert result.body == []
