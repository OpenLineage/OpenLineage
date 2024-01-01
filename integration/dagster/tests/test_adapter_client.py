# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from openlineage.dagster.adapter import OpenLineageAdapter


def test_custom_client_config():
    os.environ["OPENLINEAGE_URL"] = "http://ol-api:5000"
    os.environ["OPENLINEAGE_API_KEY"] = "api-key"
    adapter = OpenLineageAdapter()
    assert adapter._client.transport.url == "http://ol-api:5000"
    del os.environ["OPENLINEAGE_URL"]
    del os.environ["OPENLINEAGE_API_KEY"]
