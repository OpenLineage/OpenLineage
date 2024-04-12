# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import datetime
import json
import os
import uuid
from json import JSONEncoder

import attr
import pendulum
import pytest
from openlineage.airflow.utils import (
    DagUtils,
    InfoJsonEncodable,
    SafeStrDict,
    _is_name_redactable,
    get_connection,
    get_dagrun_start_end,
    get_location,
    redact_with_exclusions,
    to_json_encodable,
    url_to_https,
)
from openlineage.client.utils import RedactMixin
from packaging.version import Version

from airflow.models import DAG as AIRFLOW_DAG
from airflow.models import DagModel
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.version import version as AIRFLOW_VERSION

AIRFLOW_CONN_ID = "test_db"
AIRFLOW_CONN_URI = "postgres://localhost:5432/testdb"
SNOWFLAKE_CONN_URI = "snowflake://12345.us-east-1.snowflakecomputing.com/MyTestRole?extra__snowflake__account=12345&extra__snowflake__database=TEST_DB&extra__snowflake__insecure_mode=false&extra__snowflake__region=us-east-1&extra__snowflake__role=MyTestRole&extra__snowflake__warehouse=TEST_WH&extra__snowflake__aws_access_key_id=123456&extra__snowflake__aws_secret_access_key=abcdefg"


def test_get_connection():
    os.environ["AIRFLOW_CONN_DEFAULT"] = AIRFLOW_CONN_URI

    conn = get_connection("default")
    assert conn.host == "localhost"
    assert conn.port == 5432
    assert conn.conn_type == "postgres"
    assert conn


def test_get_location_no_file_path():
    assert get_location(None) is None
    assert get_location("") is None


def test_url_to_https_no_url():
    assert url_to_https(None) is None
    assert url_to_https("") is None


def test_datetime_to_iso_8601():
    dt = datetime.datetime.utcfromtimestamp(1500100900)
    assert DagUtils.to_iso_8601(dt) == "2017-07-15T06:41:40.000000Z"

    dt = datetime.datetime(2021, 8, 6, 2, 5, 1)
    assert DagUtils.to_iso_8601(dt) == "2021-08-06T02:05:01.000000Z"


def test_pendulum_to_iso_8601():
    dt = pendulum.from_timestamp(1500100900)
    assert DagUtils.to_iso_8601(dt) == "2017-07-15T06:41:40.000000Z"

    dt = pendulum.datetime(2021, 8, 6, 2, 5, 1)
    assert DagUtils.to_iso_8601(dt) == "2021-08-06T02:05:01.000000Z"

    tz = pendulum.timezone("America/Los_Angeles")
    assert DagUtils.to_iso_8601(tz.convert(dt)) == "2021-08-05T19:05:01.000000Z"


@pytest.mark.skipif(Version(AIRFLOW_VERSION) < Version("2.4.0"), reason="Airflow < 2.4.0")
def test_get_dagrun_start_end():
    dag = AIRFLOW_DAG("test", start_date=days_ago(1), schedule_interval="@once")
    AIRFLOW_DAG.bulk_write_to_db([dag])
    dag_model = DagModel.get_dagmodel(dag.dag_id)
    run_id = str(uuid.uuid1())
    dagrun = dag.create_dagrun(
        state=State.NONE,
        run_id=run_id,
        data_interval=dag.get_next_data_interval(dag_model),
    )
    assert dagrun.data_interval_start is not None
    assert get_dagrun_start_end(dagrun, dag) == (days_ago(1), days_ago(1))


def test_Version():
    assert Version("2.3.0") >= Version("2.3.0.dev0")
    assert Version("2.3.0.dev0") >= Version("2.3.0.dev0")
    assert Version("2.3.0.beta1") >= Version("2.3.0.dev0")
    assert Version("2.3.1") >= Version("2.3.0.dev0")
    assert Version("2.4.0") >= Version("2.3.0.dev0")
    assert Version("3.0.0") >= Version("2.3.0.dev0")
    assert Version("2.2.0") < Version("2.3.0.dev0")
    assert Version("2.1.3") < Version("2.3.0.dev0")
    assert Version("2.2.4") < Version("2.3.0.dev0")
    assert Version("1.10.15") < Version("2.3.0.dev0")
    assert Version("2.2.4.dev0") < Version("2.3.0.dev0")


def test_to_json_encodable():
    dag = AIRFLOW_DAG(
        dag_id="test_dag",
        schedule_interval="*/2 * * * *",
        start_date=datetime.datetime.now(),
        catchup=False,
    )
    task = DummyOperator(task_id="test_task", dag=dag)

    encodable = to_json_encodable(task)
    encoded = json.dumps(encodable)
    decoded = json.loads(encoded)
    assert decoded == encodable


def test_safe_dict():
    assert str(SafeStrDict({"a": 1})) == str({"a": 1})

    class NotImplemented:
        def __str__(self):
            raise NotImplementedError

    assert str(SafeStrDict({"a": NotImplemented()})) == str({})


def test_info_json_encodable():
    class TestInfo(InfoJsonEncodable):
        excludes = ["exclude_1", "exclude_2", "imastring"]
        casts = {"iwanttobeint": lambda x: int(x.imastring)}
        renames = {"_faulty_name": "goody_name"}

    @attr.s
    class Test:
        exclude_1: str = attr.ib()
        imastring: str = attr.ib()
        _faulty_name: str = attr.ib()
        donotcare: str = attr.ib()

    obj = Test("val", "123", "not_funny", "abc")

    assert json.loads(json.dumps(TestInfo(obj))) == {
        "iwanttobeint": 123,
        "goody_name": "not_funny",
        "donotcare": "abc",
    }


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
    redacted_nested = redact_with_exclusions(NestedMixined("passwd", NestedMixined("passwd", None)))
    assert redacted_nested == NestedMixined("***", NestedMixined("passwd", None))
