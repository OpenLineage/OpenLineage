# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
from unittest.mock import patch

from openlineage.airflow.adapter import OpenLineageAdapter


@patch.dict(os.environ, {
    "MARQUEZ_URL": "http://marquez:5000",
    "MARQUEZ_API_KEY": "api-key"
})
def test_create_client_from_marquez_url():
    client = OpenLineageAdapter().get_or_create_openlineage_client()
    assert client.transport.url == "http://marquez:5000"
    assert "Authorization" in client.transport.session.headers
    assert client.transport.session.headers["Authorization"] == "Bearer api-key"


@patch.dict(os.environ, {
    "OPENLINEAGE_URL": "http://ol-api:5000",
    "OPENLINEAGE_API_KEY": "api-key"
})
def test_create_client_from_ol_env():
    client = OpenLineageAdapter().get_or_create_openlineage_client()

    assert client.transport.url == "http://ol-api:5000"
    assert "Authorization" in client.transport.session.headers
    assert client.transport.session.headers["Authorization"] == "Bearer api-key"
