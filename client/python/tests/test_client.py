# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
import re
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client import event_v2
from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions, OpenLineageConfig
from openlineage.client.facets import FacetsConfig
from openlineage.client.generated.environment_variables_run import (
    EnvironmentVariable,
    EnvironmentVariablesRunFacet,
)
from openlineage.client.generated.tags_job import TagsJobFacet, TagsJobFacetFields
from openlineage.client.generated.tags_run import TagsRunFacet, TagsRunFacetFields
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
from openlineage.client.transport.http import (
    ApiKeyTokenProvider,
    HttpCompression,
    HttpTransport,
    TokenProvider,
)
from openlineage.client.transport.noop import NoopTransport
from openlineage.client.uuid import generate_new_uuid

from tests.test_async_http import closing_immediately

if TYPE_CHECKING:
    from pathlib import Path

    from openlineage.client.transport.kafka import KafkaTransport
    from pytest_mock import MockerFixture


def test_client_fails_with_wrong_event_type() -> None:
    client = OpenLineageClient(url="http://example.com")

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
        OpenLineageClient(url=url)


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
    assert OpenLineageClient(url=url).transport.url == res


def test_client_sends_proper_json_with_minimal_run_event(mock_http_session_class) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")
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

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args

    assert call_args.kwargs["url"] == "http://example.com/api/v1/lineage"
    assert call_args.kwargs["headers"]["Content-Type"] == "application/json"

    # Verify the content is the expected JSON
    actual_content = call_args.kwargs["data"]
    assert actual_content == body


def test_client_sends_proper_json_with_minimal_dataset_event(mock_http_session_class) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")

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

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args

    assert call_args.kwargs["url"] == "http://example.com/api/v1/lineage"
    assert call_args.kwargs["headers"]["Content-Type"] == "application/json"

    # Verify the content is the expected JSON
    actual_content = call_args.kwargs["data"]
    assert actual_content == body


def test_client_sends_proper_json_with_minimal_job_event(mock_http_session_class) -> None:
    mock_session_class, mock_client, mock_response = mock_http_session_class

    client = OpenLineageClient(url="http://example.com")

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

    # Verify the post was called with correct parameters
    mock_client.post.assert_called_once()
    call_args = mock_client.post.call_args

    assert call_args.kwargs["url"] == "http://example.com/api/v1/lineage"
    assert call_args.kwargs["headers"]["Content-Type"] == "application/json"

    # Verify the content is the expected JSON
    actual_content = call_args.kwargs["data"]
    assert actual_content == body


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

        event = RunEvent(
            eventType=RunState.START,
            eventTime="2021-11-03T10:53:52.427343",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(name=name, namespace=""),
            producer="",
            schemaURL="",
        )

        client.emit(event)
        assert transport.emit.called == should_emit

        transport.emit.reset_mock()
        assert transport.emit.called is False

        event2 = event_v2.RunEvent(
            eventType=event_v2.RunState.START,
            eventTime="2021-11-03T10:53:52.427343",
            run=event_v2.Run(runId=str(generate_new_uuid())),
            job=event_v2.Job(name=name, namespace=""),
            producer="",
        )

        client.emit(event2)
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

    event2 = event_v2.RunEvent(
        eventType=event_v2.RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=run,
        job=event_v2.Job(name="name", namespace=""),
        producer="",
    )
    event2.run.facets = {}

    modified_event2 = client.add_environment_facets(event2)

    assert "environmentVariables" in modified_event2.run.facets
    assert modified_event2.run.facets["environmentVariables"] == EnvironmentVariablesRunFacet(
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
    client = OpenLineageClient()
    transport = client._http_transport_from_env_variables()  # noqa: SLF001
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
        "OPENLINEAGE__TRANSPORT": '{"type": "async_http", "url": "https://data-obs-intake.datadoghq.com", '
        '"auth": {"type": "apiKey", "apiKey": "YOUR_API_KEY"}}',
    },
)
def test_configures_async_transport() -> None:
    from openlineage.client.transport.async_http import AsyncHttpTransport

    client = OpenLineageClient()
    transport: AsyncHttpTransport = client.transport
    with closing_immediately(transport) as transport:
        assert transport.kind == "async_http"


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE_ENDPOINT": "api/v2/lineage",
        "OPENLINEAGE_API_KEY": "YOUR_API_KEY",
        "OPENLINEAGE__TRANSPORT__TYPE": "async_http",
    },
)
def test_async_transport_with_overwritten_transport_type() -> None:
    from openlineage.client.transport.async_http import AsyncHttpTransport

    client = OpenLineageClient()
    transport: AsyncHttpTransport = client.transport
    assert transport.kind == "async_http"
    assert transport.url == "http://example.com"
    assert transport.endpoint == "api/v2/lineage"
    assert transport.config.auth.api_key == "YOUR_API_KEY"


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TYPE": "async_http",
        "OPENLINEAGE__TRANSPORT__URL": "http://this.should.have.priority.com",
    },
)
def test_async_transport_with_priority_overwrite() -> None:
    from openlineage.client.transport.async_http import AsyncHttpTransport

    client = OpenLineageClient()
    transport: AsyncHttpTransport = client.transport
    assert transport.kind == "async_http"
    assert transport.url == "http://this.should.have.priority.com"


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__ANOTHER__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__ANOTHER__PRIORITY": "4",
    },
)
def test_composite_transport_with_sorted_by_priority() -> None:
    transport: CompositeTransport = OpenLineageClient().transport
    expected_priority = 4
    assert transport.kind == CompositeTransport.kind
    assert transport.config.sort_transports is True
    assert len(transport.transports) == 2  # noqa: PLR2004
    assert transport.transports[0].kind == ConsoleTransport.kind
    assert transport.transports[0].priority == expected_priority
    assert transport.transports[0].name == "another"
    assert transport.transports[1].kind == HttpTransport.kind
    assert transport.transports[1].name == "default_http"
    assert transport.transports[1].priority == 0


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

    mock_transport.emit.reset_mock()
    assert mock_transport.emit.call_args is None

    event2 = event_v2.RunEvent(
        eventType=event_v2.RunState.START,
        eventTime="2021-11-03T10:53:52.427343",
        run=run,
        job=event_v2.Job(name="name", namespace=""),
        producer="",
    )
    client.emit(event2)
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
            (
                {
                    "OPENLINEAGE__TAGS__JOB__ENVIRONMENT": "PRODUCTION",
                    "OPENLINEAGE__TAGS__JOB__pipeline": "finance",
                    "OPENLINEAGE__TAGS__RUN__environment": "PRODUCTION",
                    "OPENLINEAGE__TAGS__RUN__pipeline": "finance",
                },
                {
                    "tags": {
                        "job": {"environment": "PRODUCTION", "pipeline": "finance"},
                        "run": {"environment": "PRODUCTION", "pipeline": "finance"},
                    }
                },
            ),
            (
                {
                    "OPENLINEAGE__TAGS": '{"job": {"a": "b", "c": "d"}, "run": {"a": "b", "c": "d"}}',
                },
                {
                    "tags": {
                        "job": {"a": "b", "c": "d"},
                        "run": {"a": "b", "c": "d"},
                    }
                },
            ),
            (
                {
                    "OPENLINEAGE__TRANSPORT__TYPE": "http",
                    "OPENLINEAGE__TRANSPORT__URL": "http://localhost:5050",
                    "OPENLINEAGE__TRANSPORT__RETRY__TOTAL": "5",
                    "OPENLINEAGE__TRANSPORT__RETRY__BACKOFF_FACTOR": "0.2",
                    "OPENLINEAGE__TRANSPORT__RETRY__ALLOWED_METHODS": '["GET", "POST"]',
                    "OPENLINEAGE__TRANSPORT__RETRY__STATUS_FORCELIST": "[500, 502, 503, 504]",
                },
                {
                    "transport": {
                        "type": "http",
                        "url": "http://localhost:5050",
                        "retry": {
                            "total": 5,
                            "backoff_factor": 0.2,
                            "allowed_methods": ["GET", "POST"],
                            "status_forcelist": [500, 502, 503, 504],
                        },
                    }
                },
            ),
        ],
    )
    @patch.dict(os.environ, {})
    def test_config_loader(self, env_vars, expected_config):
        with patch.dict(os.environ, env_vars):
            config = OpenLineageClient._load_config_from_env_variables()  # noqa: SLF001
            assert config == expected_config


@pytest.fixture(
    scope="module", params=[("V1", RunEvent, Job, Run), ("V2", event_v2.RunEvent, event_v2.Job, event_v2.Run)]
)
def run_event_multi(request):
    """
    Parameterized run events that allow us to test both versions for run events
    """
    event_version = request.param[0]
    event_type = request.param[1]
    job_type = request.param[2]
    run_type = request.param[3]

    job = job_type(name="name", namespace="namespace")
    run = run_type(runId="69f4acab-b87d-4fc0-b27b-8ea950370ff3")
    event_args = {
        "eventTime": "2021-11-03T10:53:52.427343",
        "eventType": RunState.START,
        "producer": "producer",
        "schemaURL": "http:foo.com/schema",
        "job": job,
        "run": run,
    }
    if event_version == "V2":
        del event_args["eventType"]
        del event_args["schemaURL"]

    return event_type(**event_args)


@pytest.fixture(scope="module", params=[("V1", JobEvent, Job), ("V2", event_v2.JobEvent, event_v2.Job)])
def job_event_multi(request):
    """
    Parameterized job events that allow us to test both versions for job events
    """
    event_version = request.param[0]
    event_type = request.param[1]
    job_type = request.param[2]

    job_args = {"name": "name", "namespace": "namespace"}
    job = job_type(**job_args)
    event_args = {
        "eventTime": "2021-11-03T10:53:52.427343",
        "producer": "producer",
        "schemaURL": "http:foo.com/schema",
        "job": job,
    }
    if event_version == "V2":
        del event_args["schemaURL"]
    return event_type(**event_args)


def test_client_creates_new_job_tag_facet(transport, run_event_multi):
    tag_environment_variables = {
        "OPENLINEAGE__TAGS__JOB__ENVIRONMENT": "PRODUCTION",
        "OPENLINEAGE__TAGS__JOB__pipeline": "SALES",
    }

    tags = [
        TagsJobFacetFields("environment", "PRODUCTION", "USER"),
        TagsJobFacetFields("pipeline", "SALES", "USER"),
    ]

    with patch.dict(os.environ, tag_environment_variables):
        client = OpenLineageClient(transport=transport)
        client.emit(run_event_multi)
        assert transport.event.job.facets.get("tags")
        event_tags = sorted(transport.event.job.facets["tags"].tags, key=lambda x: x.key)
        expected_tags = sorted(tags, key=lambda x: x.key)
        assert event_tags == expected_tags


def test_client_updates_existing_job_tags_facet(transport, run_event_multi):
    tag_environment_variables = {
        "OPENLINEAGE__TAGS__JOB__ENVIRONMENT": "PRODUCTION",
        "OPENLINEAGE__TAGS__JOB__pipeline": "SALES",
    }

    existing_tags = [
        TagsJobFacetFields("environment", "STAGING", "USER"),
        TagsJobFacetFields("foo", "bar", "USER"),
    ]
    run_event_multi.job.facets["tags"] = TagsJobFacet(tags=existing_tags)

    tags = [
        TagsJobFacetFields("foo", "bar", "USER"),
        TagsJobFacetFields("environment", "PRODUCTION", "USER"),
        TagsJobFacetFields("pipeline", "SALES", "USER"),
    ]

    with patch.dict(os.environ, tag_environment_variables):
        client = OpenLineageClient(transport=transport)
        client.emit(run_event_multi)
        assert transport.event.job.facets.get("tags")
        event_tags = sorted(transport.event.job.facets["tags"].tags, key=lambda x: x.key)
        expected_tags = sorted(tags, key=lambda x: x.key)
        assert event_tags == expected_tags


def test_client_creates_new_run_tags_facet(transport, run_event_multi):
    tag_environment_variables = {
        "OPENLINEAGE__TAGS__RUN__ENVIRONMENT": "PRODUCTION",
        "OPENLINEAGE__TAGS__RUN__pipeline": "SALES",
    }

    tags = [
        TagsRunFacetFields("environment", "PRODUCTION", "USER"),
        TagsRunFacetFields("pipeline", "SALES", "USER"),
    ]

    with patch.dict(os.environ, tag_environment_variables):
        client = OpenLineageClient(transport=transport)
        client.emit(run_event_multi)
        assert transport.event.run.facets.get("tags")
        event_tags = sorted(transport.event.run.facets["tags"].tags, key=lambda x: x.key)
        expected_tags = sorted(tags, key=lambda x: x.key)
        assert event_tags == expected_tags


def test_client_updates_existing_run_tags_facet(transport, run_event_multi):
    tag_environment_variables = {
        "OPENLINEAGE__TAGS__RUN__ENVIRONMENT": "PRODUCTION",
        "OPENLINEAGE__TAGS__RUN__pipeline": "SALES",
    }

    existing_tags = [
        TagsRunFacetFields("ENVIRONMENT", "STAGING", "USER"),
        TagsRunFacetFields("foo", "bar", "USER"),
    ]
    run_event_multi.run.facets["tags"] = TagsRunFacet(tags=existing_tags)

    # One existing tag (not updated), one existing tag (updated), one new tag from the user
    tags = [
        TagsRunFacetFields("foo", "bar", "USER"),
        TagsRunFacetFields("ENVIRONMENT", "PRODUCTION", "USER"),
        TagsRunFacetFields("pipeline", "SALES", "USER"),
    ]

    with patch.dict(os.environ, tag_environment_variables):
        client = OpenLineageClient(transport=transport)
        client.emit(run_event_multi)
        assert transport.event.run.facets.get("tags")
        event_tags = sorted(transport.event.run.facets["tags"].tags, key=lambda x: x.key)
        expected_tags = sorted(tags, key=lambda x: x.key)
        assert event_tags == expected_tags


def test_client_keeps_key_case_for_existing_tags(transport, run_event_multi):
    tag_environment_variables = {
        "OPENLINEAGE__TAGS__RUN__ENVIRONMENT": "PRODUCTION",
        "OPENLINEAGE__TAGS__RUN__pipeline": "SALES",
    }

    tags = [
        TagsRunFacetFields("environment", "STAGING", "USER"),
        TagsRunFacetFields("PIPELINE", "FINANCE", "USER"),
    ]

    run_event_multi.run.facets["tags"] = TagsRunFacet(tags=tags)

    tags = [
        TagsRunFacetFields("environment", "PRODUCTION", "USER"),
        TagsRunFacetFields("PIPELINE", "SALES", "USER"),
    ]

    with patch.dict(os.environ, tag_environment_variables):
        client = OpenLineageClient(transport=transport)
        client.emit(run_event_multi)
        assert transport.event.run.facets.get("tags")
        event_tags = sorted(transport.event.run.facets["tags"].tags, key=lambda x: x.key)
        expected_tags = sorted(tags, key=lambda x: x.key)
        assert event_tags == expected_tags


def test_client_creates_tag_facets_for_job_events(transport, job_event_multi):
    """
    Same code is used for run and job events to update facets. This just verifies
    it works for job events.
    """
    tag_environment_variables = {
        "OPENLINEAGE__TAGS__JOB__environment": "production",
        "OPENLINEAGE__TAGS__JOB__pipeline": "sales",
    }

    tags = [
        TagsJobFacetFields("environment", "production", "USER"),
        TagsJobFacetFields("pipeline", "sales", "USER"),
    ]

    with patch.dict(os.environ, tag_environment_variables):
        client = OpenLineageClient(transport=transport)
        client.emit(job_event_multi)
        assert transport.event.job.facets.get("tags")
        event_tags = sorted(transport.event.job.facets["tags"].tags, key=lambda x: x.key)
        expected_tags = sorted(tags, key=lambda x: x.key)
        assert event_tags == expected_tags


def test_client_does_not_update_run_tags_for_job_events(transport, job_event_multi):
    """
    Verify we do not try to update run tags in a job event. It will throw and exception
    if we do.
    """
    tag_environment_variables = {
        "OPENLINEAGE__TAGS__JOB__environment": "production",
        "OPENLINEAGE__TAGS__JOB__pipeline": "sales",
        "OPENLINEAGE__TAGS__RUN__pipeline": "sales",
    }

    tags = [
        TagsJobFacetFields("environment", "production", "USER"),
        TagsJobFacetFields("pipeline", "sales", "USER"),
    ]

    with patch.dict(os.environ, tag_environment_variables):
        client = OpenLineageClient(transport=transport)
        client.emit(job_event_multi)
        assert transport.event.job.facets.get("tags")
        event_tags = sorted(transport.event.job.facets["tags"].tags, key=lambda x: x.key)
        expected_tags = sorted(tags, key=lambda x: x.key)
        assert event_tags == expected_tags


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP": "{}",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE": "console",
    },
)
def test_client_transport_from_env_var_precedence_over_url_without_aliasing():
    user_config = {}
    transport = OpenLineageClient(config=user_config).transport
    assert transport.kind == "composite"
    assert len(transport.transports) == 1
    assert transport.transports[0].kind == "console"


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE": "console",
    },
)
def test_client_transport_from_env_var_precedence_over_url_with_aliasing():
    user_config = {}
    transport = OpenLineageClient(config=user_config).transport
    assert transport.kind == "composite"
    assert len(transport.transports) == 2
    assert transport.transports[0].kind == "console"
    assert transport.transports[1].kind == "http"
    assert transport.transports[1].url == "http://example.com"


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP": "{}",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__NAME": "some_name",
        "OPENLINEAGE__TRANSPORT__CONTINUE_ON_FAILURE": "false",
        "OPENLINEAGE__TRANSPORT__CONTINUE_ON_SUCCESS": "false",
        "OPENLINEAGE__TRANSPORT__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__CONTINUE_ON_FAILURE": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__CONTINUE_ON_SUCCESS": "false",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__PRIORITY": "-1",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_FIRST__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_FIRST__PRIORITY": "2",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__URL": "http://inner.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__COMPRESSION": "gzip",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__PRIORITY": "1",
    },
)
def test_client_transport_from_env_var_precedence_composite_with_priority():
    user_config = {}
    transport = OpenLineageClient(config=user_config).transport
    assert transport.kind == "composite"
    assert transport.name == "some_name"
    assert transport.config.continue_on_failure is False
    assert transport.config.continue_on_success is False
    assert transport.config.sort_transports is True
    assert len(transport.transports) == 2
    assert transport.transports[0].kind == "console"
    assert transport.transports[0].name == "second"
    assert transport.transports[0].priority == 1

    composite_transport = transport.transports[1]
    assert composite_transport.kind == "composite"
    assert composite_transport.name == "first"
    assert composite_transport.priority == -1
    assert composite_transport.config.continue_on_failure is True
    assert composite_transport.config.continue_on_success is False
    assert composite_transport.config.sort_transports is True
    assert len(composite_transport.transports) == 2
    assert composite_transport.transports[0].kind == "console"
    assert composite_transport.transports[0].name == "inner_first"
    assert composite_transport.transports[0].priority == 2
    assert composite_transport.transports[1].kind == "http"
    assert composite_transport.transports[1].name == "inner_backup"
    assert composite_transport.transports[1].priority == 0
    assert composite_transport.transports[1].url == "http://inner.com"
    assert composite_transport.transports[1].compression == HttpCompression.GZIP


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP": "{}",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__NAME": "some_name",
        "OPENLINEAGE__TRANSPORT__CONTINUE_ON_FAILURE": "false",
        "OPENLINEAGE__TRANSPORT__CONTINUE_ON_SUCCESS": "false",
        "OPENLINEAGE__TRANSPORT__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__CONTINUE_ON_FAILURE": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__CONTINUE_ON_SUCCESS": "false",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__PRIORITY": "-1",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_FIRST__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_FIRST__PRIORITY": "2",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__URL": "http://inner.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__COMPRESSION": "gzip",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__PRIORITY": "1",
    },
)
def test_client_transport_from_env_var_precedence_composite_with_priority_and_user_config():
    user_config = {"transport": {"type": "console", "name": "user_transport"}}
    transport = OpenLineageClient(config=user_config).transport
    assert transport.kind == "console"
    assert transport.name == "user_transport"


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP": "{}",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__NAME": "some_name",
        "OPENLINEAGE__TRANSPORT__CONTINUE_ON_FAILURE": "false",
        "OPENLINEAGE__TRANSPORT__CONTINUE_ON_SUCCESS": "false",
        "OPENLINEAGE__TRANSPORT__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__CONTINUE_ON_FAILURE": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__CONTINUE_ON_SUCCESS": "false",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__PRIORITY": "-1",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_FIRST__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_FIRST__PRIORITY": "2",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__URL": "http://inner.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__COMPRESSION": "gzip",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__PRIORITY": "1",
    },
)
def test_client_transport_from_env_var_composite_with_user_config_merging():
    user_config = {"transport": {"type": "composite", "name": "user_transport"}}
    transport = OpenLineageClient(config=user_config).transport
    assert transport.kind == "composite"
    assert transport.name == "user_transport"
    assert transport.config.continue_on_failure is False
    assert transport.config.continue_on_success is False
    assert transport.config.sort_transports is True
    assert len(transport.transports) == 2
    assert transport.transports[0].kind == "console"
    assert transport.transports[0].name == "second"
    assert transport.transports[0].priority == 1

    composite_transport = transport.transports[1]
    assert composite_transport.kind == "composite"
    assert composite_transport.name == "first"
    assert composite_transport.priority == -1
    assert composite_transport.config.continue_on_failure is True
    assert composite_transport.config.continue_on_success is False
    assert composite_transport.config.sort_transports is True
    assert len(composite_transport.transports) == 2
    assert composite_transport.transports[0].kind == "console"
    assert composite_transport.transports[0].name == "inner_first"
    assert composite_transport.transports[0].priority == 2
    assert composite_transport.transports[1].kind == "http"
    assert composite_transport.transports[1].name == "inner_backup"
    assert composite_transport.transports[1].priority == 0
    assert composite_transport.transports[1].url == "http://inner.com"
    assert composite_transport.transports[1].compression == HttpCompression.GZIP


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP": "{}",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__NAME": "some_name",
        "OPENLINEAGE__TRANSPORT__CONTINUE_ON_FAILURE": "false",
        "OPENLINEAGE__TRANSPORT__CONTINUE_ON_SUCCESS": "false",
        "OPENLINEAGE__TRANSPORT__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__CONTINUE_ON_FAILURE": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__CONTINUE_ON_SUCCESS": "false",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__PRIORITY": "-1",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_FIRST__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_FIRST__PRIORITY": "2",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__URL": "http://inner.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__COMPRESSION": "gzip",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__PRIORITY": "1",
    },
)
def test_client_transport_from_env_var_composite_with_user_config_overwrite_transports():
    user_config = {
        "transport": {
            "type": "composite",
            "name": "user_transport",
            "transports": [
                {"type": "console", "name": "should_be_second", "priority": "1"},
                {"type": "console", "name": "should_be_first", "priority": "2"},
            ],
        }
    }
    transport = OpenLineageClient(config=user_config).transport
    assert transport.kind == "composite"
    assert transport.name == "user_transport"
    assert transport.config.continue_on_failure is False  # This is still coming from env vars
    assert transport.config.continue_on_success is False  # This is still coming from env vars
    assert transport.config.sort_transports is True  # This is still coming from env vars
    assert len(transport.transports) == 2
    assert transport.transports[0].kind == "console"
    assert transport.transports[0].name == "should_be_first"
    assert transport.transports[0].priority == 2
    assert transport.transports[1].kind == "console"
    assert transport.transports[1].name == "should_be_second"
    assert transport.transports[1].priority == 1


@patch.dict(
    os.environ,
    {
        "OPENLINEAGE_URL": "http://example.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP": "{}",
        "OPENLINEAGE__TRANSPORT__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__NAME": "some_name",
        "OPENLINEAGE__TRANSPORT__CONTINUE_ON_FAILURE": "false",
        "OPENLINEAGE__TRANSPORT__CONTINUE_ON_SUCCESS": "false",
        "OPENLINEAGE__TRANSPORT__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE": "composite",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__CONTINUE_ON_FAILURE": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__CONTINUE_ON_SUCCESS": "false",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__SORT_TRANSPORTS": "true",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__PRIORITY": "-1",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_FIRST__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_FIRST__PRIORITY": "2",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__TYPE": "http",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__URL": "http://inner.com",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TRANSPORTS__INNER_BACKUP__COMPRESSION": "gzip",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__TYPE": "console",
        "OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__PRIORITY": "1",
    },
)
def test_client_transport_from_env_var_composite_with_user_config_non_composite():
    user_config = {
        "transport": {
            "type": "http",
            "name": "user_transport",
            "url": "http://user.com",
            "compression": "gzip",
        }
    }
    transport = OpenLineageClient(config=user_config).transport
    assert transport.kind == "http"
    assert transport.name == "user_transport"
    assert transport.priority == 0
    assert transport.url == "http://user.com"
    assert transport.endpoint == "api/v1/lineage"
    assert transport.compression == HttpCompression.GZIP
