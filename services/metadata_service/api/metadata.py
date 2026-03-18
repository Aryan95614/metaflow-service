import json
import asyncio

from aiohttp import web
from multidict import MultiDict
from services.data.db_utils import translate_run_key, translate_task_key
from services.utils import read_body
from services.metadata_service.api.utils import format_response, \
    handle_exceptions, http_500, parse_pagination_params, paginate_response, \
    METADATA_SERVICE_HEADER, METADATA_SERVICE_VERSION
from services.data.postgres_async_db import AsyncPostgresDB


class MetadataApi(object):
    _metadata_table = None
    lock = asyncio.Lock()

    def __init__(self, app):
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/"
            "tasks/{task_id}/metadata",
            self.get_metadata,
        )
        app.router.add_route(
            "GET",
            "/flows/{flow_id}/runs/{run_number}/metadata",
            self.get_metadata_by_run,
        )
        app.router.add_route(
            "POST",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/"
            "tasks/{task_id}/metadata",
            self.create_metadata,
        )
        self._db = AsyncPostgresDB.get_instance()
        self._async_table = AsyncPostgresDB.get_instance().metadata_table_postgres

    async def get_metadata(self, request):
        """
        ---
        description: get all metadata associated with the specified task, with optional cursor-based pagination
        tags:
        - Metadata
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "string"
        - name: "step_name"
          in: "path"
          description: "step_name"
          required: true
          type: "string"
        - name: "task_id"
          in: "path"
          description: "task_id"
          required: true
          type: "string"
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
                description: successful operation
            "405":
                description: invalid HTTP Method
        """
        try:
            flow_name = request.match_info.get("flow_id")
            run_number = request.match_info.get("run_number")
            step_name = request.match_info.get("step_name")
            task_id = request.match_info.get("task_id")
            parsed = parse_pagination_params(request.query)

            if parsed is None:
                db_response = await self._async_table.get_metadata(
                    flow_name, run_number, step_name, task_id
                )
                return web.Response(
                    status=db_response.response_code,
                    body=json.dumps(db_response.body),
                    headers=MultiDict(
                        {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))

            if isinstance(parsed, web.Response):
                return parsed

            page_limit, cursor_value = parsed
            run_key, run_value = translate_run_key(run_number)
            task_key, task_value = translate_task_key(task_id)
            conditions = ["flow_id = %s", "{} = %s".format(run_key),
                          "step_name = %s", "{} = %s".format(task_key)]
            values = [flow_name, run_value, step_name, task_value]

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

            records, headers = paginate_response(db_response.body, page_limit)

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

    async def get_metadata_by_run(self, request):
        """
        ---
        description: get all metadata associated with the specified run, with optional cursor-based pagination
        tags:
        - Metadata
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "string"
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
                description: successful operation
            "405":
                description: invalid HTTP Method
        """
        try:
            flow_name = request.match_info.get("flow_id")
            run_number = request.match_info.get("run_number")
            parsed = parse_pagination_params(request.query)

            if parsed is None:
                db_response = await self._async_table.get_metadata_in_runs(
                    flow_name, run_number
                )
                return web.Response(
                    status=db_response.response_code,
                    body=json.dumps(db_response.body),
                    headers=MultiDict(
                        {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))

            if isinstance(parsed, web.Response):
                return parsed

            page_limit, cursor_value = parsed
            run_key, run_value = translate_run_key(run_number)
            conditions = ["flow_id = %s", "{} = %s".format(run_key)]
            values = [flow_name, run_value]

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

            records, headers = paginate_response(db_response.body, page_limit)

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

    async def create_metadata(self, request):
        """
        ---
        description: persist metadata
        tags:
        - Metadata
        parameters:
        - name: "flow_id"
          in: "path"
          description: "flow_id"
          required: true
          type: "string"
        - name: "run_number"
          in: "path"
          description: "run_number"
          required: true
          type: "string"
        - name: "step_name"
          in: "path"
          description: "step_name"
          required: true
          type: "string"
        - name: "task_id"
          in: "path"
          description: "task_id"
          required: true
          type: "string"
        - name: "body"
          in: "body"
          description: "body"
          required: true
          schema:
            type: array
            items:
                type: object
                properties:
                    field_name:
                        type: string
                    value:
                        type: string
                    type:
                        type: string
                    user_name:
                        type: string
                    tags:
                        type: object
                    system_tags:
                        type: object
        produces:
        - 'text/plain'
        responses:
            "202":
                description: successful operation.
            "405":
                description: invalid HTTP Method
        """
        flow_name = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        task_id = request.match_info.get("task_id")

        body = await read_body(request.content)
        count = 0
        try:
            run_number, run_id = await self._db.get_run_ids(flow_name, run_number)
            task_id, task_name = await self._db.get_task_ids(flow_name, run_number,
                                                             step_name, task_id)
        except Exception:
            return web.Response(status=400, body=json.dumps(
                {"message": "need to register run_id and task_id first"}))

        for datum in body:
            values = {
                "flow_id": flow_name,
                "run_number": run_number,
                "run_id": run_id,
                "step_name": step_name,
                "task_id": task_id,
                "task_name": task_name,
                "field_name": datum.get("field_name", " "),
                "value": datum.get("value", " "),
                "type": datum.get("type", " "),
                "user_name": datum.get("user_name"),
                "tags": datum.get("tags"),
                "system_tags": datum.get("system_tags"),
            }
            metadata_response = await self._async_table.add_metadata(**values)
            if metadata_response.response_code == 200:
                count = count + 1

        result = {"metadata_created": count}

        return web.Response(body=json.dumps(result))
