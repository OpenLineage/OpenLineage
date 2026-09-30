# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import httpx
import pytest

from app.datasphere._http import _MAX_RETRY_AFTER_SECONDS, _retry_after, error_from_response
from app.lineage.emitter import _expand


def _resp(status, **kwargs):
    return httpx.Response(status, request=httpx.Request("GET", "https://h/x"), **kwargs)


@pytest.mark.parametrize("value", ["-1", "nan", "inf", "soon"])
def test_retry_after_rejects_invalid_values(value):
    assert _retry_after(_resp(429, headers={"retry-after": value})) is None


def test_retry_after_is_clamped():
    assert _retry_after(_resp(429, headers={"retry-after": "86400"})) == _MAX_RETRY_AFTER_SECONDS
    assert _retry_after(_resp(429, headers={"retry-after": "2"})) == 2.0


def test_error_keeps_body_when_json_has_no_message():
    err = error_from_response(_resp(400, json={"error": {"code": "E42", "details": []}}))
    assert err.message.startswith("HTTP 400: ") and "E42" in err.message


def test_error_normalizes_object_message():
    err = error_from_response(_resp(400, json={"error": {"message": {"lang": "en", "value": "bad filter"}}}))
    assert err.message == "bad filter"


def test_unresolved_transport_placeholder_raises():
    with pytest.raises(ValueError, match="OPENLINEAGE_API_KEY"):
        _expand({"auth": {"api_key": "${OPENLINEAGE_API_KEY}"}}, {})
