import json
from functools import wraps

import pkg_resources
import collections
from aiohttp import web
from multidict import MultiDict

from services.utils import get_traceback_str

version = pkg_resources.require("metadata_service")[0].version
METADATA_SERVICE_VERSION = version
METADATA_SERVICE_HEADER = 'METADATA_SERVICE_VERSION'

ServiceResponse = collections.namedtuple("ServiceResponse", "response_code body")


def format_response(func):
    """handle formatting"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        db_response = await func(*args, **kwargs)
        return web.Response(status=db_response.response_code,
                            body=json.dumps(db_response.body),
                            headers=MultiDict(
                                {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))

    return wrapper


def web_response(status: int, body):
    return web.Response(status=status,
                        body=json.dumps(body),
                        headers=MultiDict(
                            {"Content-Type": "application/json",
                             METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))


def http_500(msg, traceback_str=None):
    # NOTE: worth considering if we want to expose tracebacks in the future in the api messages.
    if traceback_str is None:
        traceback_str = get_traceback_str()
    body = {
        'traceback': traceback_str,
        'detail': msg,
        'status': 500,
        'title': 'Internal Server Error',
        'type': 'about:blank'
    }

    return ServiceResponse(500, body)


def handle_exceptions(func):
    """Catch exceptions and return appropriate HTTP error."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as err:
            return http_500(str(err))

    return wrapper


def parse_pagination_params(query):
    _limit = query.get("_limit")
    _cursor = query.get("_cursor")

    if _limit is None and _cursor is None:
        return None

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

    return page_limit, cursor_value


def paginate_records(records, page_limit):
    headers = {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}
    if page_limit > 0 and len(records) > page_limit:
        records = records[:page_limit]
        headers["X-Next-Cursor"] = str(records[-1]["ts_epoch"])
    return records, headers
