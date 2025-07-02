# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any, Dict
from unittest.mock import MagicMock

import httpx


def create_mock_response(
    status_code: int = 200,
    headers: Dict[str, str] | None = None,
    content: str = "",
    json_data: Dict[str, Any] | None = None,
) -> MagicMock:
    """Create a mock httpx.Response object."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = status_code
    mock_response.headers = headers or {}
    mock_response.text = content

    if json_data:
        mock_response.json.return_value = json_data

    if 200 <= status_code < 300:
        mock_response.raise_for_status.return_value = None
    else:
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}", request=MagicMock(), response=mock_response
        )

    return mock_response


def create_success_response(content: str = "OK") -> MagicMock:
    """Create a successful HTTP response (200)."""
    return create_mock_response(status_code=200, content=content)


def create_server_error_response(status_code: int = 500) -> MagicMock:
    """Create a server error HTTP response (5xx)."""
    return create_mock_response(status_code=status_code, content=f"Internal Server Error ({status_code})")


def create_client_error_response(status_code: int = 400) -> MagicMock:
    """Create a client error HTTP response (4xx)."""
    return create_mock_response(status_code=status_code, content=f"Client Error ({status_code})")


def create_timeout_exception() -> Exception:
    """Create a timeout exception."""
    return httpx.TimeoutException("Request timed out")


def create_connection_error() -> Exception:
    """Create a connection error exception."""
    return httpx.ConnectError("Connection failed")


def create_mock_async_client():
    """Create a mock httpx.AsyncClient."""
    mock_client = MagicMock(spec=httpx.AsyncClient)

    # Set up context manager behavior
    mock_client.__aenter__ = MagicMock(return_value=mock_client)
    mock_client.__aexit__ = MagicMock(return_value=None)

    return mock_client


def create_mock_sync_client():
    """Create a mock httpx.Client."""
    mock_client = MagicMock(spec=httpx.Client)

    # Set up context manager behavior
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=None)

    return mock_client


# Common response scenarios
SUCCESS_RESPONSE = create_success_response()
SERVER_ERROR_RESPONSE = create_server_error_response(500)
BAD_GATEWAY_RESPONSE = create_server_error_response(502)
SERVICE_UNAVAILABLE_RESPONSE = create_server_error_response(503)
GATEWAY_TIMEOUT_RESPONSE = create_server_error_response(504)
BAD_REQUEST_RESPONSE = create_client_error_response(400)
UNAUTHORIZED_RESPONSE = create_client_error_response(401)
FORBIDDEN_RESPONSE = create_client_error_response(403)
NOT_FOUND_RESPONSE = create_client_error_response(404)
