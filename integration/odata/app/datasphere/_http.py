# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Shared HTTP GET helper: bounded retry on transient failures, typed error on non-2xx."""

from __future__ import annotations

import logging
import math
import time

import httpx

from app.datasphere.errors import ODataRequestError, bounded_body

log = logging.getLogger(__name__)

_RETRYABLE_STATUS = {429, 502, 503, 504}
_MAX_RETRIES = 3
# Upper bound on a server-requested Retry-After, so one response cannot stall the scan indefinitely.
_MAX_RETRY_AFTER_SECONDS = 60.0


def _retry_after(resp: httpx.Response) -> float | None:
    value = resp.headers.get("retry-after")
    if not value:
        return None
    try:
        delay = float(value)
    except ValueError:
        return None
    if not math.isfinite(delay) or delay < 0:
        return None
    return min(delay, _MAX_RETRY_AFTER_SECONDS)


def _message_from_json(body: object) -> tuple[str | None, str | None]:
    """Extract ``(message, correlation_id)`` from an OData JSON error body, if present."""
    if not isinstance(body, dict):
        return None, None
    err = body.get("error")
    err = err if isinstance(err, dict) else {}
    message = err.get("message") or body.get("message")
    # OData v4 allows message to be an object ({"lang": .., "value": ..}); normalize to a string.
    if isinstance(message, dict):
        message = message.get("value")
    correlation_id = err.get("correlationId") or body.get("correlationId")
    return (str(message) if message else None), (str(correlation_id) if correlation_id else None)


def error_from_response(resp: httpx.Response) -> ODataRequestError:
    correlation_id = resp.headers.get("x-correlationid") or resp.headers.get("x-request-id")
    message = f"HTTP {resp.status_code}"
    try:
        detail, body_correlation_id = _message_from_json(resp.json())
    except ValueError:
        detail, body_correlation_id = None, None
    correlation_id = body_correlation_id or correlation_id
    if detail:
        message = detail
    else:
        # No usable message: keep the raw body so error codes and details are not lost.
        text = bounded_body(resp.text)
        if text:
            message = f"{message}: {text}"
    return ODataRequestError(
        message,
        url=str(resp.request.url),
        http_status=resp.status_code,
        correlation_id=correlation_id,
    )


def get_with_retry(
    client: httpx.Client,
    path: str,
    *,
    headers: dict,
    params: dict | None = None,
) -> httpx.Response:
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = client.get(path, headers=headers, params=params)
        except httpx.TransportError as exc:
            if attempt < _MAX_RETRIES:
                time.sleep(min(2**attempt, 8))
                continue
            raise ODataRequestError(f"Transport error: {exc}", url=path) from exc
        except httpx.HTTPStatusError as exc:
            # Raised inside an auth flow, e.g. the OAuth2 token endpoint answering 401/503.
            resp = exc.response
            if resp.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
                delay = _retry_after(resp) or min(2**attempt, 8)
                log.warning(
                    "retryable auth response, backing off",
                    extra={"status": resp.status_code, "delay": delay, "url": str(resp.request.url)},
                )
                time.sleep(delay)
                continue
            raise error_from_response(resp) from exc

        if resp.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
            delay = _retry_after(resp) or min(2**attempt, 8)
            log.warning(
                "retryable response, backing off",
                extra={"status": resp.status_code, "delay": delay, "url": str(resp.request.url)},
            )
            time.sleep(delay)
            continue

        if not resp.is_success:
            raise error_from_response(resp)
        return resp

    raise ODataRequestError("Exhausted retries", url=path)  # pragma: no cover
