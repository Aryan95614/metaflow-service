import json
import asyncio

from aiohttp import web
from multidict import MultiDict
from services.data import FlowRow
from services.data.postgres_async_db import AsyncPostgresDB
from services.utils import read_body
from services.metadata_service.api.utils import format_response, \
    handle_exceptions, http_500, METADATA_SERVICE_HEADER, METADATA_SERVICE_VERSION


class FlowApi(object):
    _flow_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route("GET", "/flows", self.get_all_flows)
        app.router.add_route("GET", "/flows/{flow_id}", self.get_flow)
        app.router.add_route("POST", "/flows/{flow_id}", self.create_flow)
        self._async_table = AsyncPostgresDB.get_instance().flow_table_postgres

    @format_response
    @handle_exceptions
    async def create_flow(self, request):
        """
        ---
        description: create/register a flow
        tags:
        - Flow
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "body"
          in: "body"
          description: "body"
          required: true
          schema:
            type: object
            properties:
                user_name:
                    type: string
                tags:
                    type: object
                system_tags:
                    type: object

        produces:
        - 'text/plain'
        responses:
            "200":
                description: successfully created flow row
            "409":
                description: CONFLICT record exists
        """
        flow_name = request.match_info.get("flow_id")

        body = await read_body(request.content)
        user = body.get("user_name")
        tags = body.get("tags")
        system_tags = body.get("system_tags")
        flow = FlowRow(
            flow_id=flow_name, user_name=user, tags=tags, system_tags=system_tags
        )
        return await self._async_table.add_flow(flow)

    @format_response
    @handle_exceptions
    async def get_flow(self, request):
        """
        ---
        description: Get flow by id
        tags:
        - Flow
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Return flow
            "404":
                description: flow not found
            "405":
                description: invalid HTTP Method
        """

        flow_name = request.match_info.get("flow_id")
        return await self._async_table.get_flow(flow_name)

    async def get_all_flows(self, request):
        """
        ---
        description: Get all flows, with optional cursor-based pagination
        tags:
        - Flow
        parameters:
        - name: "_limit"
          in: "query"
          description: "page size (0 or absent for all)"
          required: false
          type: "integer"
        - name: "_cursor"
          in: "query"
          description: "ts_epoch to paginate from"
          required: false
          type: "integer"
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Returned all registered flows
            "405":
                description: invalid HTTP Method
        """
        try:
            _limit = request.query.get("_limit")
            _cursor = request.query.get("_cursor")

            if _limit is None and _cursor is None:
                db_response = await self._async_table.get_all_flows()
                return web.Response(
                    status=db_response.response_code,
                    body=json.dumps(db_response.body),
                    headers=MultiDict(
                        {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))

            if _cursor is not None and _limit is None:
                return web.Response(
                    status=400,
                    body=json.dumps(
                        {"error": "_limit is required when using _cursor"}),
                    headers=MultiDict(
                        {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))

            try:
                page_limit = int(_limit) if _limit else 0
                if _limit is not None and page_limit < 0:
                    raise ValueError()
            except ValueError:
                return web.Response(
                    status=400,
                    body=json.dumps(
                        {"error": "Invalid value for _limit: must be a positive integer"}),
                    headers=MultiDict(
                        {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))

            try:
                cursor_value = int(_cursor) if _cursor is not None else None
            except ValueError:
                return web.Response(
                    status=400,
                    body=json.dumps(
                        {"error": "Invalid value for _cursor: must be an integer"}),
                    headers=MultiDict(
                        {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))

            conditions = []
            values = []

            if cursor_value is not None:
                conditions.append("ts_epoch < %s")
                values.append(cursor_value)

            fetch_limit = page_limit + 1 if page_limit > 0 else 0

            db_response, _ = await self._async_table.find_records(
                conditions=conditions,
                values=values,
                order=["ts_epoch DESC"],
                limit=fetch_limit,
            )

            if db_response.response_code != 200:
                return web.Response(
                    status=db_response.response_code,
                    body=json.dumps(db_response.body),
                    headers=MultiDict(
                        {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))

            records = db_response.body
            headers = {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}

            if page_limit > 0:
                has_more = len(records) > page_limit
                if has_more:
                    records = records[:page_limit]
                    headers["X-Next-Cursor"] = str(records[-1]["ts_epoch"])
                headers["X-Has-More"] = str(has_more).lower()

            return web.Response(
                status=200,
                body=json.dumps(records),
                headers=MultiDict(headers))
        except Exception as err:
            error_response = http_500(str(err))
            return web.Response(
                status=error_response.response_code,
                body=json.dumps(error_response.body),
                headers=MultiDict(
                    {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))
