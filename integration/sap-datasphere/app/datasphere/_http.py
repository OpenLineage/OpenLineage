# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Shared HTTP GET helper: bounded retry on transient failures, typed error on non-2xx."""

from __future__ import annotations

import logging
import time

import httpx

from app.datasphere.errors import ODataRequestError

log = logging.getLogger(__name__)

_RETRYABLE_STATUS = {429, 502, 503, 504}
_MAX_RETRIES = 3


def _retry_after(resp: httpx.Response) -> float | None:
    value = resp.headers.get("retry-after")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def error_from_response(resp: httpx.Response) -> ODataRequestError:
    correlation_id = resp.headers.get("x-correlationid") or resp.headers.get("x-request-id")
    message = f"HTTP {resp.status_code}"
    try:
        body = resp.json()
        if isinstance(body, dict):
            err = body.get("error", body)
            if isinstance(err, dict):
                message = err.get("message") or body.get("message") or message
                correlation_id = err.get("correlationId") or correlation_id
            else:
                message = body.get("message") or message
    except Exception:
        text = resp.text.strip()
        if text:
            message = f"{message}: {text[:300]}"
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
