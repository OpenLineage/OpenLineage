# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""String-typed settings must keep their text when the value looks like JSON (issue #5042).

`OPENLINEAGE__*` values are JSON-decoded and YAML scalars are typed, so values such as `true`,
`1.0`, `4521` or `null` arrive as bool/float/int/None. String settings (tag values, API keys,
file paths, ...) must still end up with the text that was written, while non-string settings
(booleans, numbers, JSON objects) must keep being decoded.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import pickle
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from openlineage.client import event_v2
from openlineage.client.client import OpenLineageClient
from openlineage.client.serde import Serde
from openlineage.client.tags import TagsConfig
from openlineage.client.transport.composite import CompositeTransport
from openlineage.client.transport.datadog import DatadogConfig
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.transport.http import (
    ApiKeyTokenProvider,
    HttpConfig,
    HttpTransport,
    JwtTokenProvider,
    OAuth2ClientCredentialsTokenProvider,
)
from openlineage.client.transport.kafka import KafkaConfig
from openlineage.client.utils import EnvFloat, EnvInt, config_value_to_str, config_values_to_str
from urllib3.util import Retry

if TYPE_CHECKING:
    from pathlib import Path


def _emitted_user_tags(client: OpenLineageClient) -> dict[str, dict[str, Any]]:
    """Return {"job": {key: value}, "run": {key: value}} of USER tags as serialized in the event."""
    event = event_v2.RunEvent(
        eventType=event_v2.RunState.COMPLETE,
        eventTime="2026-10-05T00:00:00+00:00",
        run=event_v2.Run(runId="0199b2c6-7b4b-7d6e-9a5e-1f2c3d4e5f60"),
        job=event_v2.Job(namespace="ns", name="job"),
    )
    payload = json.loads(Serde.to_json(client.update_event_tags_facets(event)))
    result: dict[str, dict[str, Any]] = {}
    for scope in ("job", "run"):
        tags = payload[scope].get("facets", {}).get("tags", {}).get("tags", [])
        # `value` is looked up with .get to detect tags emitted without a value
        result[scope] = {t["key"]: t.get("value", "<missing>") for t in tags if t["source"] == "USER"}
    return result


def _client_from_env(env: dict[str, str]) -> OpenLineageClient:
    with patch.dict(os.environ, {"OPENLINEAGE__TRANSPORT__TYPE": "console", **env}, clear=True):
        client = OpenLineageClient()
        _ = client.config  # load config while the patched environment is active
        return client


class TestTagValuesFromEnv:
    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            # rows of the issue's table
            ("true", "true"),
            ("1.0", "1.0"),
            ("4521", "4521"),
            ("PRODUCTION", "PRODUCTION"),
            # other values that are valid JSON scalars
            ("false", "false"),
            ("1.10", "1.10"),
            ("1e5", "1e5"),
            ("-0", "-0"),
            ("007", "007"),
            ("12345678901234567890", "12345678901234567890"),
            ('"quoted"', "quoted"),
            ('"null"', "null"),
        ],
    )
    def test_tag_value_keeps_env_text(self, env_value: str, expected: str) -> None:
        client = _client_from_env(
            {"OPENLINEAGE__TAGS__JOB__VALUE": env_value, "OPENLINEAGE__TAGS__RUN__VALUE": env_value}
        )
        tags = _emitted_user_tags(client)
        assert tags["job"] == {"value": expected}
        assert tags["run"] == {"value": expected}

    def test_issue_example(self) -> None:
        client = _client_from_env(
            {
                "OPENLINEAGE__TAGS__JOB__ENVIRONMENT": "PRODUCTION",
                "OPENLINEAGE__TAGS__JOB__PIPELINE": "sales_monthly",
                "OPENLINEAGE__TAGS__RUN__adhoc": "true",
                "OPENLINEAGE__TAGS__JOB__VERSION": "1.0",
                "OPENLINEAGE__TAGS__JOB__TICKET": "4521",
            }
        )
        assert _emitted_user_tags(client) == {
            "job": {
                "environment": "PRODUCTION",
                "pipeline": "sales_monthly",
                "version": "1.0",
                "ticket": "4521",
            },
            "run": {"adhoc": "true"},
        }

    def test_null_tag_value_is_skipped_with_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="openlineage.client.tags"):
            client = _client_from_env(
                {"OPENLINEAGE__TAGS__RUN__OWNER": "null", "OPENLINEAGE__TAGS__RUN__TEAM": "data"}
            )
        assert _emitted_user_tags(client)["run"] == {"team": "data"}
        assert "Skipping OpenLineage tag `owner`: its value is null" in caplog.text

    def test_empty_tag_value_is_skipped_with_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="openlineage.client.tags"):
            client = _client_from_env(
                {"OPENLINEAGE__TAGS__RUN__OWNER": "", "OPENLINEAGE__TAGS__RUN__TEAM": "data"}
            )
        assert _emitted_user_tags(client)["run"] == {"team": "data"}
        assert "Skipping OpenLineage tag `owner`: its value is empty" in caplog.text

    def test_two_level_json_form(self) -> None:
        client = _client_from_env(
            {"OPENLINEAGE__TAGS__JOB": '{"adhoc": true, "build": 42, "version": 1.0, "owner": null}'}
        )
        assert client.config.tags.job == {"adhoc": "true", "build": "42", "version": "1.0"}

    def test_whole_tags_json_form(self) -> None:
        client = _client_from_env({"OPENLINEAGE__TAGS": '{"job": {"adhoc": true}, "run": {"build": 42}}'})
        assert client.config.tags == TagsConfig(job={"adhoc": "true"}, run={"build": "42"})

    def test_object_tag_value_is_skipped(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="openlineage.client.tags"):
            client = _client_from_env({"OPENLINEAGE__TAGS__JOB__NESTED": '{"a": "b"}'})
        assert client.config.tags.job == {}
        assert "Skipping OpenLineage tag `nested`" in caplog.text


class TestTagValuesFromYamlAndPython:
    def test_unquoted_yaml_values(self, tmp_path: Path) -> None:
        config_path = tmp_path / "openlineage.yml"
        config_path.write_text(
            "transport:\n"
            "  type: console\n"
            "tags:\n"
            "  job:\n"
            "    adhoc: true\n"
            "    build: 42\n"
            "    version: 1.0\n"
            '    quoted: "1.10"\n'
            "  run:\n"
            "    enabled: false\n"
        )
        with patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(config_path)}, clear=True):
            client = OpenLineageClient()
            assert client.config.tags == TagsConfig(
                job={"adhoc": "true", "build": "42", "version": "1.0", "quoted": "1.10"},
                run={"enabled": "false"},
            )

    def test_python_config(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            client = OpenLineageClient(
                config={
                    "transport": {"type": "console"},
                    "tags": {"job": {"adhoc": True, "build": 42, "version": 1.0}, "run": {"x": "y"}},
                }
            )
            assert _emitted_user_tags(client) == {
                "job": {"adhoc": "true", "build": "42", "version": "1.0"},
                "run": {"x": "y"},
            }

    def test_env_yaml_and_python_produce_the_same_tag(self, tmp_path: Path) -> None:
        from_env = _client_from_env({"OPENLINEAGE__TAGS__RUN__adhoc": "true"}).config.tags.run

        config_path = tmp_path / "openlineage.yml"
        config_path.write_text("transport:\n  type: console\ntags:\n  run:\n    adhoc: true\n")
        with patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(config_path)}, clear=True):
            from_yaml = OpenLineageClient().config.tags.run

        with patch.dict(os.environ, {}, clear=True):
            from_python_bool = OpenLineageClient(
                config={"transport": {"type": "console"}, "tags": {"run": {"adhoc": True}}}
            ).config.tags.run
            from_python_str = OpenLineageClient(
                config={"transport": {"type": "console"}, "tags": {"run": {"adhoc": "true"}}}
            ).config.tags.run

        assert from_env == from_yaml == from_python_bool == from_python_str == {"adhoc": "true"}

    def test_tags_config_converter(self) -> None:
        tags = TagsConfig(job={"a": True, "b": 0, "c": 2.5, "d": "text", "e": None, 7: "seven"}, run=None)  # type: ignore[arg-type,dict-item]
        assert tags.job == {"a": "true", "b": "0", "c": "2.5", "d": "text", "7": "seven"}
        assert tags.run == {}


class TestOtherStringSettingsFromEnv:
    def test_file_log_file_path(self, tmp_path: Path) -> None:
        with patch.dict(
            os.environ,
            {"OPENLINEAGE__TRANSPORT__TYPE": "file", "OPENLINEAGE__TRANSPORT__LOG_FILE_PATH": "20261005"},
            clear=True,
        ):
            client = OpenLineageClient()
        assert isinstance(client.transport, FileTransport)
        assert client.transport.config.log_file_path == "20261005"

    @pytest.mark.parametrize("api_key", ["1234e5678", "12345678", "1.10", "1.0"])
    def test_http_api_key(self, api_key: str) -> None:
        with patch.dict(
            os.environ,
            {
                "OPENLINEAGE__TRANSPORT__TYPE": "http",
                "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5000",
                "OPENLINEAGE__TRANSPORT__AUTH__TYPE": "api_key",
                "OPENLINEAGE__TRANSPORT__AUTH__APIKEY": api_key,
            },
            clear=True,
        ):
            client = OpenLineageClient()
        assert isinstance(client.transport, HttpTransport)
        assert client.transport.config.auth.get_bearer() == f"Bearer {api_key}"

    def test_http_custom_headers(self) -> None:
        with patch.dict(
            os.environ,
            {
                "OPENLINEAGE__TRANSPORT__TYPE": "http",
                "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5000",
                "OPENLINEAGE__TRANSPORT__CUSTOM_HEADERS__X_TENANT": "42",
                "OPENLINEAGE__TRANSPORT__CUSTOM_HEADERS__X_DEBUG": "true",
            },
            clear=True,
        ):
            client = OpenLineageClient()
        assert isinstance(client.transport, HttpTransport)
        assert client.transport.config.custom_headers == {"x_tenant": "42", "x_debug": "true"}

    def test_token_providers(self) -> None:
        assert ApiKeyTokenProvider({"apiKey": 12345678}).api_key == "12345678"
        jwt = JwtTokenProvider({"apiKey": 12345678, "tokenEndpoint": "https://auth.example.com/token"})
        assert jwt.api_key == "12345678"
        oauth2 = OAuth2ClientCredentialsTokenProvider(
            {"clientId": 4521, "clientSecret": True, "tokenEndpoint": "https://auth.example.com/token"}
        )
        assert (oauth2.client_id, oauth2.client_secret) == ("4521", "true")

    def test_datadog_api_key(self) -> None:
        with patch.dict(
            os.environ,
            {"OPENLINEAGE__TRANSPORT__TYPE": "datadog", "OPENLINEAGE__TRANSPORT__APIKEY": "1234e5678"},
            clear=True,
        ):
            transport = OpenLineageClient._load_config_from_env_variables()["transport"]  # noqa: SLF001
        assert DatadogConfig.from_dict(transport).apiKey == "1234e5678"

    def test_kafka_topic_and_message_key(self) -> None:
        with patch.dict(
            os.environ,
            {
                "OPENLINEAGE__TRANSPORT__TYPE": "kafka",
                "OPENLINEAGE__TRANSPORT__TOPIC": "4521",
                "OPENLINEAGE__TRANSPORT__MESSAGE_KEY": "1.0",
                "OPENLINEAGE__TRANSPORT__CONFIG": '{"bootstrap.servers": "localhost:9092"}',
            },
            clear=True,
        ):
            transport = OpenLineageClient._load_config_from_env_variables()["transport"]  # noqa: SLF001
        config = KafkaConfig.from_dict(transport)
        assert (config.topic, config.messageKey) == ("4521", "1.0")

    @pytest.mark.parametrize(("env_value", "expected"), [("0", "0"), ("false", "false")])
    def test_kafka_falsy_message_key(self, env_value: str, expected: str) -> None:
        with patch.dict(
            os.environ,
            {
                "OPENLINEAGE__TRANSPORT__TYPE": "kafka",
                "OPENLINEAGE__TRANSPORT__TOPIC": "events",
                "OPENLINEAGE__TRANSPORT__MESSAGE_KEY": env_value,
                "OPENLINEAGE__TRANSPORT__CONFIG": '{"bootstrap.servers": "localhost:9092"}',
            },
            clear=True,
        ):
            transport = OpenLineageClient._load_config_from_env_variables()["transport"]  # noqa: SLF001
        assert KafkaConfig.from_dict(transport).messageKey == expected

    def test_falsy_credentials(self) -> None:
        assert ApiKeyTokenProvider({"apiKey": 0}).api_key == "0"
        jwt = JwtTokenProvider({"apiKey": False, "tokenEndpoint": "https://auth.example.com/token"})
        assert jwt.api_key == "false"
        oauth2 = OAuth2ClientCredentialsTokenProvider(
            {"clientId": 0, "clientSecret": False, "tokenEndpoint": "https://auth.example.com/token"}
        )
        assert (oauth2.client_id, oauth2.client_secret) == ("0", "false")
        with patch.dict(os.environ, {"DD_API_KEY": "from-env"}, clear=True):
            assert DatadogConfig.from_dict({"apiKey": 0}).apiKey == "0"
            assert DatadogConfig.from_dict({"apiKey": ""}).apiKey == "from-env"
        with pytest.raises(KeyError):
            ApiKeyTokenProvider({"apiKey": ""})


class TestNonStringSettingsFromEnvStillDecoded:
    def test_http_bool_and_numbers(self) -> None:
        with patch.dict(
            os.environ,
            {
                "OPENLINEAGE__TRANSPORT__TYPE": "http",
                "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5000",
                "OPENLINEAGE__TRANSPORT__VERIFY": "false",
                "OPENLINEAGE__TRANSPORT__TIMEOUT": "2.5",
                "OPENLINEAGE__TRANSPORT__RETRY__TOTAL": "7",
                "OPENLINEAGE__TRANSPORT__RETRY__STATUS_FORCELIST": "[500, 503]",
            },
            clear=True,
        ):
            client = OpenLineageClient()
        assert isinstance(client.transport, HttpTransport)
        config: HttpConfig = client.transport.config
        assert config.verify is False
        assert config.timeout == 2.5  # noqa: PLR2004
        assert config.retry["total"] == 7  # noqa: PLR2004
        assert config.retry["status_forcelist"] == [500, 503]

    def test_file_append_bool(self, tmp_path: Path) -> None:
        with patch.dict(
            os.environ,
            {
                "OPENLINEAGE__TRANSPORT__TYPE": "file",
                "OPENLINEAGE__TRANSPORT__LOG_FILE_PATH": str(tmp_path / "events"),
                "OPENLINEAGE__TRANSPORT__APPEND": "true",
            },
            clear=True,
        ):
            client = OpenLineageClient()
        assert isinstance(client.transport, FileTransport)
        assert client.transport.config.append is True

    def test_json_object_properties(self) -> None:
        with patch.dict(
            os.environ,
            {
                "OPENLINEAGE__TRANSPORT__TYPE": "kafka",
                "OPENLINEAGE__TRANSPORT__CONFIG": '{"bootstrap.servers": "localhost:9092", "acks": 1}',
            },
            clear=True,
        ):
            config = OpenLineageClient._load_config_from_env_variables()  # noqa: SLF001
        assert config["transport"]["config"] == {"bootstrap.servers": "localhost:9092", "acks": 1}

    @pytest.mark.parametrize(
        "env",
        [
            {
                "OPENLINEAGE__TRANSPORT__TYPE": "composite",
                "OPENLINEAGE__TRANSPORT__CONTINUE_ON_FAILURE": "false",
                "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE": "console",
                "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__TYPE": "http",
                "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__URL": "http://localhost:5000",
                "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__VERIFY": "false",
            },
            {
                "OPENLINEAGE__TRANSPORT__TYPE": "composite",
                "OPENLINEAGE__TRANSPORT__CONTINUE_ON_FAILURE": "false",
                "OPENLINEAGE__TRANSPORT__TRANSPORTS": json.dumps(
                    {
                        "first": {"type": "console"},
                        "second": {"type": "http", "url": "http://localhost:5000", "verify": False},
                    }
                ),
            },
        ],
        ids=["nested-env-vars", "json-object"],
    )
    def test_composite_transports(self, env: dict[str, str]) -> None:
        with patch.dict(os.environ, env, clear=True):
            client = OpenLineageClient()
        assert isinstance(client.transport, CompositeTransport)
        assert client.transport.config.continue_on_failure is False
        http = next(t for t in client.transport.transports if isinstance(t, HttpTransport))
        assert http.config.verify is False

    @pytest.mark.parametrize(("env_value", "expected"), [("0.50", 0.5), ("1e1", 10.0), ("2.50", 2.5)])
    def test_http_numbers_that_do_not_round_trip(self, env_value: str, expected: float) -> None:
        with patch.dict(
            os.environ,
            {
                "OPENLINEAGE__TRANSPORT__TYPE": "http",
                "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5000",
                "OPENLINEAGE__TRANSPORT__TIMEOUT": env_value,
                "OPENLINEAGE__TRANSPORT__RETRY__BACKOFF_FACTOR": env_value,
            },
            clear=True,
        ):
            client = OpenLineageClient()
        assert isinstance(client.transport, HttpTransport)
        config: HttpConfig = client.transport.config
        assert config.timeout == expected
        assert isinstance(config.timeout, float)
        assert config.retry["backoff_factor"] == expected
        # urllib3 computes backoff with the value, which failed when it was a string
        retry = Retry(total=3, backoff_factor=config.retry["backoff_factor"])
        assert retry.increment().increment().get_backoff_time() == expected * 2

    def test_dataset_reducing_enabled(self) -> None:
        client = _client_from_env({"OPENLINEAGE__DATASET__REDUCING_ENABLED": "true"})
        assert client.config.dataset.reducing_enabled is True


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [
        ("true", True),
        ("false", False),
        ("null", None),
        ("5", 5),
        ("-3", -3),
        ("0.2", 0.2),
        ("1.0", 1.0),
        ('"text"', "text"),
        ('{"a": 1.10}', {"a": 1.1}),
        ("[1, 2]", [1, 2]),
        # numbers whose JSON text does not round-trip keep the text as written
        ("1.10", EnvFloat(1.1, "1.10")),
        ("1e5", EnvFloat(100000.0, "1e5")),
        ("1234e5678", EnvFloat(float("inf"), "1234e5678")),
        ("-0", EnvInt(0, "-0")),
        # not JSON
        ("PRODUCTION", "PRODUCTION"),
        ("007", "007"),
        ("s3://bucket/path", "s3://bucket/path"),
    ],
)
def test_parse_env_value(env_value: str, expected: Any) -> None:
    parsed = OpenLineageClient._parse_env_value("OPENLINEAGE__X", env_value)  # noqa: SLF001
    assert parsed == expected
    assert type(parsed) is type(expected)
    if isinstance(expected, (EnvFloat, EnvInt)):
        assert parsed.text == env_value


@pytest.mark.parametrize(
    "number", [EnvFloat(float("inf"), "1234e5678"), EnvFloat(1e5, "1e5"), EnvInt(0, "-0")]
)
def test_env_number_str_is_text_as_written(number: EnvFloat | EnvInt) -> None:
    # settings without a converter, such as Kafka producer config values, only call str()
    assert str(number) == number.text
    assert f"{number}" == number.text
    assert json.dumps(number) == json.dumps(float(number) if isinstance(number, float) else int(number))


def test_parse_env_value_int_too_long_to_convert() -> None:
    # Python 3.11+ (and 3.10.7+) raises a plain ValueError, not JSONDecodeError, for ints over
    # the int max str digits limit; patched so the test does not depend on the runtime's limit
    digits = "1" * 5000
    error = ValueError("Exceeds the limit (4300 digits) for integer string conversion")
    with patch("openlineage.client.client.json.loads", side_effect=error):
        assert OpenLineageClient._parse_env_value("OPENLINEAGE__X", digits) == digits  # noqa: SLF001


@pytest.mark.parametrize("number", [EnvFloat(0.5, "0.50"), EnvInt(0, "-0")])
def test_env_number_survives_copy_and_pickle(number: EnvFloat | EnvInt) -> None:
    for copied in (copy.deepcopy(number), pickle.loads(pickle.dumps(number))):  # noqa: S301
        assert type(copied) is type(number)
        assert copied == number
        assert copied.text == number.text


def test_config_value_to_str() -> None:
    assert config_value_to_str("text") == "text"
    assert config_value_to_str(True) == "true"
    assert config_value_to_str(False) == "false"
    assert config_value_to_str(4521) == "4521"
    assert config_value_to_str(1.0) == "1.0"
    assert config_value_to_str(EnvFloat(1.1, "1.10")) == "1.10"
    assert config_value_to_str(EnvInt(0, "-0")) == "-0"
    for invalid in (None, {"a": "b"}, ["a"]):
        with pytest.raises(TypeError):
            config_value_to_str(invalid)
    assert config_values_to_str({"a": 1, "b": True, "c": "x", "d": None, "e": EnvFloat(1e5, "1e5")}) == {
        "a": "1",
        "b": "true",
        "c": "x",
        "d": None,
        "e": "1e5",
    }
    assert config_values_to_str({123: "x", True: "y"}) == {"123": "x", "true": "y"}
    assert config_values_to_str(None) == {}


def test_file_config_from_dict_converts_path() -> None:
    assert FileConfig.from_dict({"log_file_path": 20261005}).log_file_path == "20261005"
