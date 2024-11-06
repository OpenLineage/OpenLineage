# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
import re
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions, OpenLineageConfig
from openlineage.client.facets import FacetsConfig
from openlineage.client.generated.environment_variables_run import (
    EnvironmentVariable,
    EnvironmentVariablesRunFacet,
)
from openlineage.client.run import (
    SCHEMA_URL,
    Dataset,
    DatasetEvent,
    Job,
    JobEvent,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.transport.composite import CompositeTransport
from openlineage.client.transport.console import ConsoleTransport
from openlineage.client.transport.http import ApiKeyTokenProvider, HttpTransport, TokenProvider
from openlineage.client.transport.noop import NoopTransport
from openlineage.client.uuid import generate_new_uuid

if TYPE_CHECKING:
    from pathlib import Path

    from openlineage.client.transport.kafka import KafkaTransport
    from pytest_mock import MockerFixture


def test_client_fails_with_wrong_event_type() -> None:
    client = OpenLineageClient(url="http://example.com", session=MagicMock())

    with pytest.raises(
        ValueError,
        match="`emit` only accepts RunEvent, DatasetEvent, JobEvent class",
    ):
        client.emit("event")


@pytest.mark.parametrize(
    "url",
    [
        "notanurl",
        "http://",
        "example.com",
        "http:example.com",
        "http:/example.com",
        "196.168.0.1",
    ],
)
def test_client_fails_to_create_with_wrong_url(url: str) -> None:
    with pytest.raises(ValueError, match=re.escape(url)):
        OpenLineageClient(url=url, session=MagicMock())


@pytest.mark.parametrize(
    ("url", "res"),
    [
        ("http://196.168.0.1", "http://196.168.0.1"),
        ("http://196.168.0.1 ", "http://196.168.0.1"),
        ("http://example.com  ", "http://example.com"),
        (" http://example.com", "http://example.com"),
        ("  http://marquez:5000  ", "http://marquez:5000"),
        ("  https://marquez  ", "https://marquez"),
    ],
)
def test_client_passes_to_create_with_valid_url(url: str, res: str) -> None:
    assert OpenLineageClient(url=url, session=MagicMock()).transport.url == res


def test_client_sends_proper_json_with_minimal_run_event() -> None:
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    client.emit(
        RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job("openlineage", "job"),
            "producer",
        ),
    )

    body = (
        '{"eventTime": "2021-11-03T10:53:52.427343", "eventType": "START", "inputs": [], "job": '
        '{"facets": {}, "name": "job", "namespace": "openlineage"}, "outputs": [], '
        '"producer": "producer", "run": {"facets": {}, "runId": '
        f'"69f4acab-b87d-4fc0-b27b-8ea950370ff3"}}, "schemaURL": "{SCHEMA_URL}"}}'
    )
    session.post.assert_called_with(
        url="http://example.com/api/v1/lineage",
        data=body,
        headers={},
        timeout=5.0,
        verify=True,
    )


def test_client_sends_proper_json_with_minimal_dataset_event() -> None:
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    client.emit(
        DatasetEvent(
            eventTime="2021-11-03T10:53:52.427343",
            producer="producer",
            schemaURL="datasetSchemaUrl",
            dataset=Dataset(namespace="my-namespace", name="my-ds"),
        ),
    )

    body = (
        '{"dataset": {"facets": {}, "name": "my-ds", '
        '"namespace": "my-namespace"}, "eventTime": '
        '"2021-11-03T10:53:52.427343", "producer": "producer", '
        '"schemaURL": "datasetSchemaUrl"}'
    )
    session.post.assert_called_with(
        url="http://example.com/api/v1/lineage",
        data=body,
        headers={},
        timeout=5.0,
        verify=True,
    )


def test_client_sends_proper_json_with_minimal_job_event() -> None:
    session = MagicMock()
    client = OpenLineageClient(url="http://example.com", session=session)

    client.emit(
        JobEvent(
            eventTime="2021-11-03T10:53:52.427343",
            schemaURL="jobSchemaURL",
            job=Job("openlineage", "job"),
            producer="producer",
        ),
    )

    body = (
        '{"eventTime": "2021-11-03T10:53:52.427343", '
        '"inputs": [], "job": {"facets": {}, "name": "job", "namespace": '
        '"openlineage"}, "outputs": [], "producer": "producer", '
        '"schemaURL": "jobSchemaURL"}'
    )

    session.post.assert_called_with(
        url="http://example.com/api/v1/lineage",
        data=body,
        headers={},
        timeout=5.0,
        verify=True,
    )


def test_client_uses_passed_transport() -> None:
    transport = MagicMock()
    client = OpenLineageClient(transport=transport)
    assert client.transport == transport

    client.emit(
        event=RunEvent(
            RunState.START,
            "2021-11-03T10:53:52.427343",
            Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
            Job("openlineage", "job"),
            "producer",
            "schemaURL",
        ),
    )
    client.transport.emit.assert_called_once()


@pytest.mark.parametrize(
    ("name", "config_path", "should_emit"),
    [
        ("job", "exact_filter.yml", False),
        ("wrong", "exact_filter.yml", False),
        ("job1", "exact_filter.yml", True),
        ("1wrong", "exact_filter.yml", True),
        ("asdf", "exact_filter.yml", True),
        ("", "exact_filter.yml", True),
        ("whatever", "regex_filter.yml", False),
        ("something_whatever_asdf", "regex_filter.yml", False),
        ("$$$.whatever", "regex_filter.yml", False),
        ("asdf", "regex_filter.yml", True),
        ("", "regex_filter.yml", True),
    ],
)
def test_client_filters_exact_job_name_events(
    name: str,
    config_path: str,
    root: Path,
    *,
    should_emit: bool,
) -> None:
    with patch.dict(
        os.environ,
        {"OPENLINEAGE_CONFIG": str(root / "config" / config_path)},
    ):
        factory = MagicMock()
        transport = MagicMock()
        factory.create.return_value = transport
        client = OpenLineageClient(factory=factory)

        run = Run(runId=str(generate_new_uuid()))
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2021-11-03T10:53:52.427343",
            run=run,
            job=Job(name=name, namespace=""),
            producer="",
            schemaURL="",
        )

        client.emit(event)
        assert transport.emit.called == should_emit


def test_setting_ol_client_log_level() -> None:
    default_log_level = logging.WARNING
    # without environment variable
    OpenLineageClient()
    parent_logger = logging.getLogger("openlineage.client")
    logger = logging.getLogger("openlineage.client.client")
    assert parent_logger.getEffectiveLevel() == default_log_level
    assert logger.getEffectiveLevel() == default_log_level
    with patch.dict(os.environ, {"OPENLINEAGE_CLIENT_LOGGING": "CRITICAL"}):
        assert parent_logger.getEffectiveLevel() == default_log_level
        assert logger.getEffectiveLevel() == default_log_level
        OpenLineageClient()
        assert parent_logger.getEffectiveLevel() == logging.CRITICAL
        assert logger.getEffectiveLevel() == logging.CRITICAL


@patch.dict("os.environ", {})
@patch("warnings.warn")
def test_client_with_url_and_options(mock_warn) -> None:
    timeout = 10
    options = OpenLineageClientOptions(timeout=timeout, verify=True, api_key="xxx")
    client = OpenLineageClient(url="http://example.com", options=options)
    mock_warn.assert_any_call(
        message="Initializing OpenLineageClient with url, options and session is deprecated.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    assert client.transport.kind == HttpTransport.kind
    assert client.transport.url == "http://example.com"
    assert client.transport.timeout == timeout
    assert client.transport.verify is True
    assert client.transport.config.auth.api_key == "xxx"


@patch.dict(
    "os.environ",
    {"OPENLINEAGE_URL": "http://example.com", "OPENLINEAGE_ENDPOINT": "v7", "OPENLINEAGE_API_KEY": "xxx"},
)
def test_init_with_openlineage_url_env_var_warning() -> None:
    client = OpenLineageClient()
    assert client.transport.kind == HttpTransport.kind
    assert client.transport.url == "http://example.com"
    assert client.transport.endpoint == "v7"
    assert client.transport.config.auth.api_key == "xxx"


@patch("warnings.warn")
def test_from_environment_deprecation_warning(mock_warn) -> None:
    OpenLineageClient.from_environment()
    mock_warn.assert_called_once_with(
        message="`OpenLineageClient.from_environment()` is deprecated. Use `OpenLineageClient()`.",
        category=DeprecationWarning,
        stacklevel=2,
    )


@patch.dict("os.environ", {"OPENLINEAGE_DISABLED": "true"})
def test_client_disabled() -> None:
    client = OpenLineageClient()
    assert isinstance(client.transport, NoopTransport)


def test_client_with_yaml_config(mocker: MockerFixture, root: Path) -> None:
    mocker.patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(root / "config" / "http.yml")})
    client = OpenLineageClient()
    assert client.transport.kind == HttpTransport.kind
    assert client.transport.url == "http://localhost:5050"
    assert client.transport.endpoint == "api/v1/lineage"
    assert client.transport.config.auth.api_key == "random_token"


def test_get_config_file_content(root: Path) -> None:
    result = OpenLineageClient._get_config_file_content(str(root / "config" / "http.yml"))  # noqa: SLF001
    assert result == {
        "transport": {
            "auth": {"apiKey": "random_token", "type": "api_key"},
            "compression": "gzip",
            "endpoint": "api/v1/lineage",
            "type": "http",
            "url": "http://localhost:5050",
        }
    }


@patch("yaml.safe_load", return_value=None)
def test_get_config_file_content_empty(mock_yaml) -> None:  # noqa: ARG001
    result = OpenLineageClient._get_config_file_content("empty.yml")  # noqa: SLF001
    assert result == {}


@patch("os.path.isfile", return_value=False)
def test_find_yaml_config_path_no_config_passed_and_no_files_found(mock_is_file) -> None:
    result = OpenLineageClient._find_yaml_config_path()  # noqa: SLF001
    mock_is_file.assert_any_call(os.path.join(os.getcwd(), "openlineage.yml"))
    mock_is_file.assert_any_call(os.path.join(os.path.expanduser("~/.openlineage"), "openlineage.yml"))
    assert result is None


@patch("os.path.isfile", return_value=True)
@patch("os.access", return_value=True)
def test_find_yaml_config_path_cwd_found(mock_access, mock_is_file) -> None:  # noqa: ARG001
    result = OpenLineageClient._find_yaml_config_path()  # noqa: SLF001
    path = os.path.join(os.getcwd(), "openlineage.yml")
    mock_is_file.assert_any_call(path)
    assert result == path


@patch("os.path.isfile", side_effect=[False, True])
@patch("os.access", return_value=True)
def test_find_yaml_config_path_user_home_found(mock_access, mock_is_file) -> None:  # noqa: ARG001
    result = OpenLineageClient._find_yaml_config_path()  # noqa: SLF001
    path = os.path.join(os.path.expanduser("~/.openlineage"), "openlineage.yml")
    mock_is_file.assert_any_call(path)
    assert result == path


@patch("os.path.isfile", return_value=False)
def test_find_yaml_config_path_checks_all_paths(mock_is_file, mocker: MockerFixture, root: Path) -> None:
    path = str(root / "config" / "openlineage.yml")
    mocker.patch.dict(os.environ, {"OPENLINEAGE_CONFIG": path})
    result = OpenLineageClient._find_yaml_config_path()  # noqa: SLF001
    mock_is_file.assert_any_call(path)
    mock_is_file.assert_any_call(os.path.join(os.getcwd(), "openlineage.yml"))
    mock_is_file.assert_any_call(os.path.join(os.path.expanduser("~/.openlineage"), "openlineage.yml"))
    assert result is None


def test_ol_config_from_dict():
    # Test with complete config
    config_dict = {
        "transport": {"url": "http://localhost:5050"},
        "facets": {"environment_variables": ["VAR1", "VAR2"]},
        "filters": [{"type": "exact", "match": "job_name"}],
    }
    config = OpenLineageConfig.from_dict(config_dict)
    assert config.transport["url"] == "http://localhost:5050"
    assert config.facets.environment_variables == ["VAR1", "VAR2"]
    assert config.filters[0].type == "exact"
    assert config.filters[0].match == "job_name"

    # Test with missing keys
    config_dict = {}
    config = OpenLineageConfig.from_dict(config_dict)
    assert config.transport == {}
    assert config.facets == FacetsConfig()
    assert config.filters == []

    # Test with invalid data type
    with pytest.raises(TypeError):
        OpenLineageConfig.from_dict({"facets": "invalid_data"})


@patch("yaml.safe_load", return_value=None)
def test_config_file_content_empty_file(mock_yaml) -> None:  # noqa: ARG001
    assert OpenLineageClient().config == OpenLineageConfig()


def test_config(mocker: MockerFixture, root: Path) -> None:
    mocker.patch.dict(os.environ, {"OPENLINEAGE_CONFIG": str(root / "config" / "http.yml")})
    assert OpenLineageClient().config == OpenLineageConfig.from_dict(
        {
            "transport": {
                "auth": {"apiKey": "random_token", "type": "api_key"},
                "compression": "gzip",
                "endpoint": "api/v1/lineage",
                "type": "http",
                "url": "http://localhost:5050",
            }
        }
    )


def test_openlineage_client_from_dict() -> None:
    transport_dict = {"type": "http", "url": "http://localhost:5050"}
    client = OpenLineageClient.from_dict(transport_dict)
    assert client.transport.url == "http://localhost:5050"


def test_openlineage_client_from_empty_dict() -> None:
    client = OpenLineageClient.from_dict({})
    assert isinstance(client.transport, ConsoleTransport)


def test_openlineage_config_from_dict() -> None:
    config_dict = {
        "transport": {
            "type": "http",
            "url": "http://localhost:5050",
            "auth": {"api_key": "random_token"},
        },
        "facets": {
            "environment_variables": ["VAR1", "VAR2"],
        },
        "filters": [
            {"type": "regex", "match": ".*"},
        ],
    }
    config = OpenLineageConfig.from_dict(config_dict)

    assert config.transport == config_dict["transport"]
    assert config.facets.environment_variables == config_dict["facets"]["environment_variables"]
    assert len(config.filters) == 1
    assert config.filters[0].type == "regex"
    assert config.filters[0].match == ".*"


def test_openlineage_config_default_values() -> None:
    config = OpenLineageConfig()

    assert config.transport == {}
    assert isinstance(config.facets, FacetsConfig)
    assert config.filters == []


@patch.dict(os.environ, {"ENV_VAR_1": "value1", "ENV_VAR_2": "value2"})
def test_collect_environment_variables():
    client = OpenLineageClient()
    client._config = OpenLineageConfig(  # noqa: SLF001
        facets=FacetsConfig(environment_variables=["ENV_VAR_1", "ENV_VAR_2", "MISSING_VAR"])
    )
    env_vars = client._collect_environment_variables()  # noqa: SLF001
    assert env_vars == {"ENV_VAR_1": "value1", "ENV_VAR_2": "value2"}


@patch.dict(os.environ, {"ENV_VAR_1": "value1", "SENSITIVE_VAR": "PII"})
def test_add_environment_facets():
    client = OpenLineageClient()
    client._config = OpenLineageConfig(  # noqa: SLF001
        facets=FacetsConfig(environment_variables=["ENV_VAR_1"])
    )
    run = Run(runId=str(generate_new_uuid()))
    event = RunEvent(
        eventType=RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=run,
        job=Job(name="name", namespace=""),
        producer="",
        schemaURL="",
    )
    event.run.facets = {}

    modified_event = client.add_environment_facets(event)

    assert "environmentVariables" in modified_event.run.facets
    assert modified_event.run.facets["environmentVariables"] == EnvironmentVariablesRunFacet(
        [EnvironmentVariable(name="ENV_VAR_1", value="value1")]
    )


@patch("openlineage.client.client.OpenLineageClient._find_yaml_config_path")
@patch("openlineage.client.client.OpenLineageClient._get_config_file_content")
def test_config_property_loads_yaml(mock_get_config_content, mock_find_yaml):
    mock_find_yaml.return_value = "config.yml"
    mock_get_config_content.return_value = {"transport": {"type": "http", "url": "http://localhost:5050"}}

    config = OpenLineageClient().config
    assert config.transport["type"] == "http"
    assert config.transport["url"] == "http://localhost:5050"


@patch.dict(
    "os.environ",
    {"OPENLINEAGE_URL": "http://example.com", "OPENLINEAGE_ENDPOINT": "v7", "OPENLINEAGE_API_KEY": "xxx"},
)
def test_http_transport_from_env_variables() -> None:
    transport = OpenLineageClient._http_transport_from_env_variables()  # noqa: SLF001
    assert transport.kind == HttpTransport.kind
    assert transport.url == "http://example.com"
    assert transport.endpoint == "v7"
    assert transport.config.auth.api_key == "xxx"


def test_http_transport_from_url_no_options() -> None:
    timeout = 10
    options = OpenLineageClientOptions(timeout=timeout, verify=True, api_key="xxx")
    transport = OpenLineageClient._http_transport_from_url(  # noqa: SLF001
        url="http://example.com", options=options, session=None
    )
    assert transport.kind == HttpTransport.kind
    assert transport.url == "http://example.com"
    assert transport.timeout == timeout
    assert transport.verify is True
    assert transport.config.auth.api_key == "xxx"


@patch.dict(
    os.environ, {"OPENLINEAGE_URL": "http://example.com", "OPENLINEAGE__TRANSPORT__TYPE": "composite"}
)
def test_composite_transport_with_aliased_url() -> None:
    transport: CompositeTransport = OpenLineageClient().transport
    assert transport.kind == CompositeTransport.kind
    assert len(transport.transports) == 1
    assert transport.transports[0].kind == HttpTransport.kind
    assert isinstance(transport.transports[0].config.auth, TokenProvider)


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE_API_KEY": "random_key",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
    },
)
def test_composite_transport_with_aliased_url_and_api_key() -> None:
    transport: CompositeTransport = OpenLineageClient().transport
    assert transport.kind == CompositeTransport.kind
    assert len(transport.transports) == 1
    assert transport.transports[0].kind == HttpTransport.kind
    assert isinstance(transport.transports[0].config.auth, ApiKeyTokenProvider)
    assert transport.transports[0].config.auth.api_key == "random_key"


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__ANOTHER__TYPE": "console",
    },
)
def test_composite_transport_with_aliased_url_and_second_transport() -> None:
    transport: CompositeTransport = OpenLineageClient().transport
    assert transport.kind == CompositeTransport.kind
    assert len(transport.transports) == 2  # noqa: PLR2004
    assert transport.transports[0].kind == HttpTransport.kind


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP": "{}",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__ANOTHER__TYPE": "console",
    },
)
def test_composite_transport_with_aliased_url_and_overriden_alias() -> None:
    transport: CompositeTransport = OpenLineageClient().transport
    assert transport.kind == CompositeTransport.kind
    assert len(transport.transports) == 1
    assert transport.transports[0].kind == ConsoleTransport.kind


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__ANOTHER__TYPE": "console",
    },
)
def test_openlineage_url_does_not_alias_when_transport_exists() -> None:
    transport: CompositeTransport = OpenLineageClient().transport
    assert transport.kind == CompositeTransport.kind
    assert len(transport.transports) == 2  # noqa: PLR2004
    assert transport.transports[0].kind == transport.transports[1].kind == ConsoleTransport.kind


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE__TRANSPORT__TYPE": "kafka",
        "OPENLINEAGE__TRANSPORT__TOPIC": "my_topic",
        "OPENLINEAGE__TRANSPORT__CONFIG": '{"bootstrap.servers": "localhost:9092,another.host:9092", "acks": "all", "retries": 3}',  # noqa: E501
        "OPENLINEAGE__TRANSPORT__FLUSH": "true",
        "OPENLINEAGE__TRANSPORT__MESSAGE_KEY": "some-value",
    },
)
def test_kafka_transport_configured_with_aliased_message_key() -> None:
    transport: KafkaTransport = OpenLineageClient().transport
    assert transport.message_key == "some-value"
    assert transport.flush is True
    assert transport.kafka_config.config == {
        "bootstrap.servers": "localhost:9092,another.host:9092",
        "acks": "all",
        "retries": 3,
    }


@patch.dict(
    os.environ,
    {
        "CUSTOM_ENV_VAR": "custom_value",
        "OPENLINEAGE__TRANSPORT__TYPE": "console",
        "OPENLINEAGE__FACETS__ENVIRONMENT_VARIABLES": '["CUSTOM_ENV_VAR"]',
    },
)
@patch("openlineage.client.client.OpenLineageClient._resolve_transport")
def test_add_environment_facets_with_custom_env_var(mock_resolve_transport) -> None:
    mock_resolve_transport.return_value = mock_transport = MagicMock()
    client = OpenLineageClient()
    run = Run(runId=str(generate_new_uuid()))
    event = RunEvent(
        eventType=RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=run,
        job=Job(name="name", namespace=""),
        producer="",
        schemaURL="",
    )

    client.emit(event)
    assert mock_transport.emit.call_args[0][0].run.facets[
        "environmentVariables"
    ] == EnvironmentVariablesRunFacet([EnvironmentVariable(name="CUSTOM_ENV_VAR", value="custom_value")])


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE__TRANSPORT__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5050",
        "OPENLINEAGE__TRANSPORT__AUTH__API_KEY": "random_token",
    },
)
@patch("openlineage.client.client.OpenLineageClient._find_yaml_config_path")
def test_config_property_loads_env_vars(mock_find_yaml) -> None:
    mock_find_yaml.return_value = None
    client = OpenLineageClient()
    config = client.config
    assert config.transport["type"] == "http"
    assert config.transport["url"] == "http://localhost:5050"
    assert config.transport["auth"]["api_key"] == "random_token"


def test_config_property_loads_user_defined_config() -> None:
    user_defined_config = {
        "transport": {
            "type": "http",
            "url": "http://localhost:5050",
            "auth": {"api_key": "random_token"},
        }
    }
    client = OpenLineageClient(config=user_defined_config)
    config = client.config
    assert config.transport["type"] == "http"
    assert config.transport["url"] == "http://localhost:5050"
    assert config.transport["auth"]["api_key"] == "random_token"


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE__TRANSPORT__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5050",
    },
)
def test_client_from_empty_dict_with_dynamic_env_vars() -> None:
    client = OpenLineageClient.from_dict({})
    assert client.transport.kind == ConsoleTransport.kind


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
    },
)
def test_client_from_empty_dict_with_url_env_var() -> None:
    client = OpenLineageClient.from_dict({})
    assert client.transport.kind == ConsoleTransport.kind


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE__TRANSPORT__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5050",
    },
)
def test_client_raises_from_wrong_dict() -> None:
    config_without_transport = {
        "facets": {
            "environment_variables": ["VAR1", "VAR2"],
        }
    }
    with pytest.raises(KeyError):
        OpenLineageClient.from_dict(config_without_transport)


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE__FACETS__ENVIRONMENT_VARIABLES": '["VAR1", "VAR2"]',
    },
)
def test_client_from_facets_config_in_env_vars_and_transport_in_config() -> None:
    transport_config = {
        "type": "http",
        "url": "http://localhost:5050",
    }
    client = OpenLineageClient.from_dict(transport_config)
    assert client.config.facets.environment_variables == ["VAR1", "VAR2"]
    assert client.transport.url == "http://localhost:5050"


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE__TRANSPORT__AUTH__API_KEY": "random_token",
    },
)
@patch("openlineage.client.client.OpenLineageClient._find_yaml_config_path")
@patch("openlineage.client.client.OpenLineageClient._get_config_file_content")
def test_config_merge_precedence(mock_get_config_content, mock_find_yaml) -> None:
    transport_config = {
        "type": "http",
        "url": "http://localhost:5050",
    }
    mock_find_yaml.return_value = "config.yml"
    mock_get_config_content.return_value = {
        "transport": {
            "url": "http://another.host:5050",
            "auth": {"api_key": "another_token"},
        }
    }
    client = OpenLineageClient.from_dict(transport_config)
    config = client.config
    assert config.transport["type"] == "http"
    assert config.transport["url"] == "http://localhost:5050"
    assert config.transport["auth"]["api_key"] == "another_token"


class TestOpenLineageConfigLoader:
    @pytest.mark.parametrize(
        ("env_vars", "expected_config"),
        [
            (
                {
                    "OPENLINEAGE__TRANSPORT__TYPE": "http",
                    "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5050",
                    "OPENLINEAGE__TRANSPORT__AUTH__API_KEY": "random_token",
                },
                {
                    "transport": {
                        "type": "http",
                        "url": "http://localhost:5050",
                        "auth": {"api_key": "random_token"},
                    }
                },
            ),
            (
                {
                    "OPENLINEAGE__TRANSPORT__TYPE": "composite",
                    "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE": "http",
                    "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__URL": "http://localhost:5050",
                    "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__TYPE": "console",
                },
                {
                    "transport": {
                        "type": "composite",
                        "transports": {
                            "first": {"type": "http", "url": "http://localhost:5050"},
                            "second": {"type": "console"},
                        },
                    }
                },
            ),
            (
                {"OPENLINEAGE__TRANSPORT": '{"type": "console"}', "OPENLINEAGE__TRANSPORT__TYPE": "http"},
                {"transport": {"type": "console"}},
            ),
            (
                {
                    "OPENLINEAGE__TRANSPORT__TYPE": "kafka",
                    "OPENLINEAGE__TRANSPORT__PROPERTIES": '{"key.serializer": "org.apache.kafka.common.serialization.StringSerializer"}',  # noqa: E501
                },
                {
                    "transport": {
                        "type": "kafka",
                        "properties": {
                            "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
                        },
                    }
                },
            ),
            (
                {
                    "OPENLINEAGE__TRANSPORT__TYPE": "test",
                    "OPENLINEAGE__TRANSPORT__MY_NAME__LIST": '["first", "SeCond"]',
                },
                {"transport": {"my_name": {"list": ["first", "SeCond"]}, "type": "test"}},
            ),
            (
                {
                    "OPENLINEAGE__TRANSPORT__TYPE": "http",
                    "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5050",
                    "OPENLINEAGE__TRANSPORT__AUTH__API_KEY": "random_token",
                    "OPENLINEAGE__TRANSPORT__AUTH__COMPRESSION": "gzip",
                },
                {
                    "transport": {
                        "type": "http",
                        "url": "http://localhost:5050",
                        "auth": {"api_key": "random_token", "compression": "gzip"},
                    }
                },
            ),
            (
                {
                    "OPENLINEAGE__TRANSPORT": '{"type": "console"}',
                    "OPENLINEAGE__TRANSPORT__TYPE": "http",
                    "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5050",
                    "OPENLINEAGE__TRANSPORT__AUTH__API_KEY": "random_token",
                    "OPENLINEAGE__TRANSPORT__AUTH__COMPRESSION": "gzip",
                },
                {"transport": {"type": "console"}},
            ),
        ],
    )
    @patch.dict(os.environ, {})
    def test_config_loader(self, env_vars, expected_config):
        with patch.dict(os.environ, env_vars):
            config = OpenLineageClient._load_config_from_env_variables()  # noqa: SLF001
            assert config == expected_config
