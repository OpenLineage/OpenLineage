# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from airflow.models import Connection
from openlineage.airflow.extractors.base import BaseExtractor
from urllib.parse import urlparse, parse_qs

AIRFLOW_CONN_ID = "test_db"
AIRFLOW_CONN_URI = "postgres://localhost:5432/testdb"
CONN_ID = "database://login:password@host:123/role?param=value&param_secret=hideit"


def test_get_connection_uri():
    conn = Connection(conn_id=AIRFLOW_CONN_ID, uri=AIRFLOW_CONN_URI)
    assert BaseExtractor.get_connection_uri(conn) == AIRFLOW_CONN_URI


def test_get_connection_filter_qs_params():
    conn = Connection(conn_id="snowflake", uri=CONN_ID)

    class TestExtractor(BaseExtractor):
        _whitelist_query_params = ["param"]

    uri = TestExtractor.get_connection_uri(conn)
    parsed = urlparse(uri)
    qs_dict = parse_qs(parsed.query)
    assert "param" in qs_dict
    assert "param_secret" not in qs_dict
    assert not parsed.username
    assert not parsed.password
    assert uri == "database://host:123/role?param=value"
