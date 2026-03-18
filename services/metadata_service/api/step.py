import json

from aiohttp import web
from multidict import MultiDict
from services.data import StepRow
from services.data.db_utils import translate_run_key
from services.data.tagging_utils import apply_run_tags_to_db_response
from services.utils import read_body
from services.metadata_service.api.utils import format_response, \
    handle_exceptions, http_500, parse_pagination_params, paginate_response, \
    METADATA_SERVICE_HEADER, METADATA_SERVICE_VERSION
from services.data.postgres_async_db import AsyncPostgresDB


class StepApi(object):
    _step_table = None

    def __init__(self, app):
        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/steps", self.get_steps
        )

        app.router.add_route(
            "GET", "/flows/{flow_id}/runs/{run_number}/steps/{step_name}", self.get_step
        )
        app.router.add_route(
            "POST",
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/step",
            self.create_step,
        )
        self._async_table = AsyncPostgresDB.get_instance().step_table_postgres
        self._async_run_table = AsyncPostgresDB.get_instance().run_table_postgres
        self._db = AsyncPostgresDB.get_instance()

    async def get_steps(self, request):
        """
        ---
        description: get all steps associated with the specified run, with optional cursor-based pagination
        tags:
        - Steps
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
                description: successful operation. returned all steps
            "405":
                description: invalid HTTP Method
        """
        try:
            flow_id = request.match_info.get("flow_id")
            run_number = request.match_info.get("run_number")
            parsed = parse_pagination_params(request.query)

            if parsed is None:
                db_response = await self._async_table.get_steps(flow_id, run_number)
                db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
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
            values = [flow_id, run_value]

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

            db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
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

    @format_response
    @handle_exceptions
    async def get_step(self, request):
        """
        ---
        description: get specified step.
                    "tags" and "system_tags" values will be persisted to DB, but will not be
                    returned by read endpoints - the related run's "tags" and "system_tags" will
                    be returned instead.
        tags:
        - Steps
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
        produces:
        - text/plain
        responses:
            "200":
                description: successful operation. Returned specified step
            "404":
                description: step not found
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")
        db_response = await self._async_table.get_step(flow_id, run_number, step_name)
        db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
        return db_response

    @format_response
    @handle_exceptions
    async def create_step(self, request):
        """
        ---
        description: Create step.
        tags:
        - Steps
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
        - text/plain
        responses:
            "200":
                description: successful operation. Registered step
            "405":
                description: invalid HTTP Method
        """
        flow_id = request.match_info.get("flow_id")
        run_number = request.match_info.get("run_number")
        step_name = request.match_info.get("step_name")

        body = await read_body(request.content)
        user = body.get("user_name", "")
        tags = body.get("tags")
        system_tags = body.get("system_tags")

        run_number, run_id = await self._db.get_run_ids(flow_id, run_number)

        step_row = StepRow(
            flow_id, run_number, run_id, user, step_name, tags=tags,
            system_tags=system_tags
        )

        db_response = await self._async_table.add_step(step_row)
        db_response = await apply_run_tags_to_db_response(flow_id, run_number, self._async_run_table, db_response)
        return db_response
