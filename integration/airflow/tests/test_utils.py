# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import json
from urllib.parse import parse_qs, urlparse

import pendulum
import datetime
from airflow.models import Connection, DAG as AIRFLOW_DAG
from airflow.operators.dummy import DummyOperator
from pkg_resources import parse_version

from openlineage.airflow.utils import (
    url_to_https,
    get_location,
    get_connection_uri,
    get_normalized_postgres_connection_uri,
    get_connection,
    to_json_encodable,
    DagUtils,
    SafeStrDict,
    _is_name_redactable,
    redact_with_exclusions,
)
from openlineage.client.utils import RedactMixin
import attr
from json import JSONEncoder


AIRFLOW_CONN_ID = 'test_db'
AIRFLOW_CONN_URI = 'postgres://localhost:5432/testdb'
SNOWFLAKE_CONN_URI = 'snowflake://12345.us-east-1.snowflakecomputing.com/MyTestRole?extra__snowflake__account=12345&extra__snowflake__database=TEST_DB&extra__snowflake__insecure_mode=false&extra__snowflake__region=us-east-1&extra__snowflake__role=MyTestRole&extra__snowflake__warehouse=TEST_WH&extra__snowflake__aws_access_key_id=123456&extra__snowflake__aws_secret_access_key=abcdefg'  # NOQA


def test_get_connection_uri():
    conn = Connection(
        conn_id=AIRFLOW_CONN_ID,
        uri=AIRFLOW_CONN_URI
    )
    assert get_connection_uri(conn) == AIRFLOW_CONN_URI


def test_get_connection_from_uri():
    conn = Connection()
    conn.parse_from_uri(uri=AIRFLOW_CONN_URI)
    assert get_normalized_postgres_connection_uri(conn) == AIRFLOW_CONN_URI


def test_get_normalized_postgres_connection_uri():
    conn = Connection()
    conn.parse_from_uri(uri="postgresql://localhost:5432/testdb")
    assert get_normalized_postgres_connection_uri(conn) == AIRFLOW_CONN_URI


def test_get_connection():
    os.environ['AIRFLOW_CONN_DEFAULT'] = AIRFLOW_CONN_URI

    conn = get_connection('default')
    assert conn.host == 'localhost'
    assert conn.port == 5432
    assert conn.conn_type == 'postgres'
    assert conn


def test_get_connection_filter_qs_params():
    conn = Connection(conn_id='snowflake', uri=SNOWFLAKE_CONN_URI)
    uri = get_connection_uri(conn)
    parsed = urlparse(uri)
    qs_dict = parse_qs(parsed.query)
    assert not any(k in qs_dict.keys()
                   for k in ['extra__snowflake__insecure_mode',
                             'extra__snowflake__region',
                             'extra__snowflake__role',
                             'extra__snowflake__aws_access_key_id',
                             'extra__snowflake__aws_secret_access_key'])
    assert all(k in qs_dict.keys()
               for k in ['extra__snowflake__account',
                         'extra__snowflake__database',
                         'extra__snowflake__warehouse'])


def test_get_connection_filter_qs_params_with_boolean_in_conn():
    conn = Connection(
        conn_type="redshift",
        extra={
            "iam": True,
            "cluster_identifier": "redshift-cluster-name",
            "region": "region",
            "aws_secret_access_key": "AKIAIOSFODNN7EXAMPLE",
            "aws_access_key_id": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        }
    )
    uri = get_connection_uri(conn)
    parsed = urlparse(uri)
    qs_dict = parse_qs(parsed.query)
    assert not any(k in qs_dict.keys()
                   for k in ['aws_secret_access_key',
                             'aws_access_key_id'])
    assert all(k in qs_dict.keys()
               for k in ['iam',
                         'cluster_identifier',
                         'region'])


def test_get_location_no_file_path():
    assert get_location(None) is None
    assert get_location("") is None


def test_url_to_https_no_url():
    assert url_to_https(None) is None
    assert url_to_https("") is None


def test_datetime_to_iso_8601():
    dt = datetime.datetime.utcfromtimestamp(1500100900)
    assert "2017-07-15T06:41:40.000000Z" == DagUtils.to_iso_8601(dt)

    dt = datetime.datetime(2021, 8, 6, 2, 5, 1)
    assert "2021-08-06T02:05:01.000000Z" == DagUtils.to_iso_8601(dt)


def test_pendulum_to_iso_8601():
    dt = pendulum.from_timestamp(1500100900)
    assert "2017-07-15T06:41:40.000000Z" == DagUtils.to_iso_8601(dt)

    dt = pendulum.datetime(2021, 8, 6, 2, 5, 1)
    assert "2021-08-06T02:05:01.000000Z" == DagUtils.to_iso_8601(dt)

    tz = pendulum.timezone("America/Los_Angeles")
    assert "2021-08-05T19:05:01.000000Z" == DagUtils.to_iso_8601(tz.convert(dt))


def test_parse_version():
    assert parse_version("2.3.0") >= parse_version("2.3.0.dev0")
    assert parse_version("2.3.0.dev0") >= parse_version("2.3.0.dev0")
    assert parse_version("2.3.0.beta1") >= parse_version("2.3.0.dev0")
    assert parse_version("2.3.1") >= parse_version("2.3.0.dev0")
    assert parse_version("2.4.0") >= parse_version("2.3.0.dev0")
    assert parse_version("3.0.0") >= parse_version("2.3.0.dev0")
    assert parse_version("2.2.0") < parse_version("2.3.0.dev0")
    assert parse_version("2.1.3") < parse_version("2.3.0.dev0")
    assert parse_version("2.2.4") < parse_version("2.3.0.dev0")
    assert parse_version("1.10.15") < parse_version("2.3.0.dev0")
    assert parse_version("2.2.4.dev0") < parse_version("2.3.0.dev0")


def test_to_json_encodable():
    dag = AIRFLOW_DAG(dag_id='test_dag',
                      schedule_interval='*/2 * * * *',
                      start_date=datetime.datetime.now(),
                      catchup=False)
    task = DummyOperator(task_id='test_task', dag=dag)

    encodable = to_json_encodable(task)
    encoded = json.dumps(encodable)
    decoded = json.loads(encoded)
    assert decoded == encodable


def test_safe_dict():
    assert str(SafeStrDict({'a': 1})) == str({'a': 1})

    class NotImplemented():
        def __str__(self):
            raise NotImplementedError
    assert str(SafeStrDict({'a': NotImplemented()})) == str({})


def test_is_name_redactable():
    class NotMixin:
        def __init__(self):
            self.password = "passwd"

    class Mixined(RedactMixin):
        _skip_redact = ["password"]

        def __init__(self):
            self.password = "passwd"
            self.transparent = "123"

    assert _is_name_redactable("password", NotMixin())
    assert not _is_name_redactable("password", Mixined())
    assert _is_name_redactable("transparent", Mixined())


def test_redact_with_exclusions(monkeypatch):
    class NotMixin:
        def __init__(self):
            self.password = "passwd"

    def default(self, o):
        if isinstance(o, NotMixin):
            return o.__dict__
        raise TypeError

    assert redact_with_exclusions(NotMixin()).password == "passwd"
    monkeypatch.setattr(JSONEncoder, "default", default)
    assert redact_with_exclusions(NotMixin()).password == "***"

    class Mixined(RedactMixin):
        _skip_redact = ["password"]

        def __init__(self):
            self.password = "passwd"
            self.transparent = "123"

    @attr.s
    class NestedMixined(RedactMixin):
        _skip_redact = ["nested_field"]
        password: str = attr.ib()
        nested_field = attr.ib()

    assert redact_with_exclusions(Mixined()).password == "passwd"
    assert redact_with_exclusions(Mixined()).transparent == "123"
    assert redact_with_exclusions({"password": "passwd"}) == {"password": "***"}
    redacted_nested = redact_with_exclusions(
        NestedMixined("passwd", NestedMixined("passwd", None))
    )
    assert redacted_nested == NestedMixined("***", NestedMixined("passwd", None))
