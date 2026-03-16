import json

from .utils import (
    cli, db,
    add_flow, add_run,
)
import pytest

pytestmark = [pytest.mark.integration_tests]


async def test_runs_get_without_tags_returns_all(cli, db):
    _flow = (await add_flow(db, flow_id="TagTestFlow")).body
    await add_run(db, flow_id="TagTestFlow", tags=["a"], system_tags=["sys:one"])
    await add_run(db, flow_id="TagTestFlow", tags=["b"], system_tags=["sys:two"])

    response = await cli.get("/flows/TagTestFlow/runs")
    assert response.status == 200
    body = json.loads(await response.text())
    assert len(body) == 2


async def test_runs_get_filter_single_tag(cli, db):
    _flow = (await add_flow(db, flow_id="TagTestFlow")).body
    await add_run(db, flow_id="TagTestFlow", tags=["env:prod"], system_tags=["runtime:dev"])
    await add_run(db, flow_id="TagTestFlow", tags=["env:staging"], system_tags=["runtime:dev"])

    response = await cli.get("/flows/TagTestFlow/runs?_tags=env:prod")
    assert response.status == 200
    body = json.loads(await response.text())
    assert len(body) == 1
    assert "env:prod" in body[0]["tags"]


async def test_runs_get_filter_multiple_tags(cli, db):
    _flow = (await add_flow(db, flow_id="TagTestFlow")).body
    await add_run(db, flow_id="TagTestFlow", tags=["env:prod", "team:ml"], system_tags=["runtime:dev"])
    await add_run(db, flow_id="TagTestFlow", tags=["env:prod"], system_tags=["runtime:dev"])

    response = await cli.get("/flows/TagTestFlow/runs?_tags=env:prod,team:ml")
    assert response.status == 200
    body = json.loads(await response.text())
    assert len(body) == 1
    assert "team:ml" in body[0]["tags"]


async def test_runs_get_filter_matches_system_tags(cli, db):
    _flow = (await add_flow(db, flow_id="TagTestFlow")).body
    await add_run(db, flow_id="TagTestFlow", tags=["user_tag"], system_tags=["runtime:dev"])
    await add_run(db, flow_id="TagTestFlow", tags=["user_tag"], system_tags=["runtime:prod"])

    response = await cli.get("/flows/TagTestFlow/runs?_tags=runtime:dev")
    assert response.status == 200
    body = json.loads(await response.text())
    assert len(body) == 1


async def test_runs_get_filter_no_matches(cli, db):
    _flow = (await add_flow(db, flow_id="TagTestFlow")).body
    await add_run(db, flow_id="TagTestFlow", tags=["env:prod"], system_tags=["runtime:dev"])

    response = await cli.get("/flows/TagTestFlow/runs?_tags=nonexistent")
    assert response.status == 200
    body = json.loads(await response.text())
    assert len(body) == 0


async def test_flows_get_without_tags_returns_all(cli, db):
    await add_flow(db, flow_id="FlowA", tags=["team:ml"], system_tags=["runtime:dev"])
    await add_flow(db, flow_id="FlowB", tags=["team:infra"], system_tags=["runtime:prod"])

    response = await cli.get("/flows")
    assert response.status == 200
    body = json.loads(await response.text())
    assert len(body) == 2


async def test_flows_get_filter_single_tag(cli, db):
    await add_flow(db, flow_id="FlowA", tags=["team:ml"], system_tags=["runtime:dev"])
    await add_flow(db, flow_id="FlowB", tags=["team:infra"], system_tags=["runtime:dev"])

    response = await cli.get("/flows?_tags=team:ml")
    assert response.status == 200
    body = json.loads(await response.text())
    assert len(body) == 1
    assert body[0]["flow_id"] == "FlowA"


async def test_flows_get_filter_no_matches(cli, db):
    await add_flow(db, flow_id="FlowA", tags=["team:ml"], system_tags=["runtime:dev"])

    response = await cli.get("/flows?_tags=nonexistent")
    assert response.status == 200
    body = json.loads(await response.text())
    assert len(body) == 0
