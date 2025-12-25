# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.run import (
    Dataset,
    DatasetEvent,
    Job,
    JobEvent,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.serde import Serde
from openlineage.client.transport.amazon_datazone import AmazonDataZoneConfig, AmazonDataZoneTransport

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.fixture
def domain_id() -> str:
    return "dzd_a1b2c3d4e5f6g7"


@pytest.fixture
def run_event() -> RunEvent:
    return RunEvent(
        eventType=RunState.START,
        eventTime="2024-08-20T11:08:01.123456",
        run=Run(runId="f8b61441-a9fb-4f65-a156-be466eb29832"),
        job=Job(namespace="test-namespace", name="test-job"),
        producer="prod",
        schemaURL="schema",
    )


@pytest.fixture
def dataset_event() -> DatasetEvent:
    return DatasetEvent(
        eventTime="2024-08-20T11:08:01.123456",
        dataset=Dataset(namespace="test-namespace", name="test-dataset"),
        producer="prod",
        schemaURL="schema",
    )


@pytest.fixture
def job_event() -> JobEvent:
    return JobEvent(
        eventTime="2024-08-20T11:08:01.123456",
        job=Job(namespace="test-namespace", name="test-job"),
        producer="prod",
        schemaURL="schema",
    )


def test_datazone_config_raises_when_missing_domain_id() -> None:
    with pytest.raises(TypeError):
        AmazonDataZoneConfig()


def test_client_with_amazon_datazone_transport_emits_run_event(
    run_event: RunEvent, domain_id: str, mocker: MockerFixture
) -> None:
    mocker.patch("boto3.client")
    config = AmazonDataZoneConfig(domain_id=domain_id)
    transport = AmazonDataZoneTransport(config)
    client = OpenLineageClient(transport=transport)

    client.emit(run_event)

    transport.datazone.post_lineage_event.assert_called_once_with(
        domainIdentifier=domain_id,
        event=Serde.to_json(run_event).encode("utf-8"),
    )


def test_client_with_amazon_datazone_transport_skips_job_event(
    job_event: JobEvent, mocker: MockerFixture
) -> None:
    mocker.patch("boto3.client")
    config = AmazonDataZoneConfig(domain_id=domain_id)
    transport = AmazonDataZoneTransport(config)
    client = OpenLineageClient(transport=transport)

    client.emit(job_event)

    transport.datazone.post_lineage_event.assert_not_called()


def test_client_with_amazon_datazone_transport_skips_dataset_event(
    dataset_event: DatasetEvent, mocker: MockerFixture
) -> None:
    mocker.patch("boto3.client")
    config = AmazonDataZoneConfig(domain_id=domain_id)
    transport = AmazonDataZoneTransport(config)
    client = OpenLineageClient(transport=transport)

    client.emit(dataset_event)

    transport.datazone.post_lineage_event.assert_not_called()


def test_client_with_amazon_datazone_transport_close(
    dataset_event: DatasetEvent, mocker: MockerFixture
) -> None:
    mocker.patch("boto3.client")
    config = AmazonDataZoneConfig(domain_id=domain_id)

    transport = AmazonDataZoneTransport(config)
    transport.close()

    transport.datazone.close.assert_called_once()


def test_setup_datazone_raises_when_boto3_missing(mocker: MockerFixture) -> None:
    mocker.patch("boto3.client", side_effect=ModuleNotFoundError("No module named 'boto3'"))
    config = AmazonDataZoneConfig(domain_id=domain_id)

    with pytest.raises(ModuleNotFoundError):
        AmazonDataZoneTransport(config)


def test_setup_datazone_raises_when_boto3_corrupted(mocker: MockerFixture) -> None:
    mocker.patch("boto3.client", side_effect=ImportError("Cannot import 'boto3'"))
    config = AmazonDataZoneConfig(domain_id=domain_id)

    with pytest.raises(ImportError):
        AmazonDataZoneTransport(config)


def test_use_region_if_provided(domain_id: str, mocker: MockerFixture) -> None:
    """Test that region is used when provided"""
    mock_client = mocker.patch("boto3.client")
    config = AmazonDataZoneConfig(domain_id=domain_id, region="us-west-2")

    AmazonDataZoneTransport(config)

    mock_client.assert_called_once_with("datazone", region_name="us-west-2")


def test_no_region_when_not_provided(domain_id: str, mocker: MockerFixture) -> None:
    """Test that no region parameter is passed when region is not provided"""
    mock_client = mocker.patch("boto3.client")
    config = AmazonDataZoneConfig(domain_id=domain_id)

    AmazonDataZoneTransport(config)

    mock_client.assert_called_once_with("datazone")


def test_endpoint_override_with_region(domain_id: str, mocker: MockerFixture) -> None:
    """Test that endpointOverride takes precedence over region"""
    mock_client = mocker.patch("boto3.client")
    config = AmazonDataZoneConfig(
        domain_id=domain_id, region="us-east-1", endpoint_override="https://datazone.us-east-1.api.aws"
    )

    AmazonDataZoneTransport(config)

    # endpointOverride should take precedence, region should be ignored
    mock_client.assert_called_once_with("datazone", endpoint_url="https://datazone.us-east-1.api.aws")


def test_endpoint_override_without_region(domain_id: str, mocker: MockerFixture) -> None:
    """Test that endpointOverride is used without region"""
    mock_client = mocker.patch("boto3.client")
    config = AmazonDataZoneConfig(domain_id=domain_id, endpoint_override="https://datazone.us-east-1.api.aws")

    AmazonDataZoneTransport(config)

    mock_client.assert_called_once_with("datazone", endpoint_url="https://datazone.us-east-1.api.aws")


def test_config_from_dict_with_region() -> None:
    """Test that AmazonDataZoneConfig.from_dict correctly parses region"""
    config_dict = {"domainId": "dzd-test-domain", "region": "eu-west-1"}

    config = AmazonDataZoneConfig.from_dict(config_dict)

    assert config.domain_id == "dzd-test-domain"
    assert config.region == "eu-west-1"
    assert config.endpoint_override is None


def test_config_from_dict_with_all_parameters() -> None:
    """Test that AmazonDataZoneConfig.from_dict correctly parses all parameters"""
    config_dict = {
        "domainId": "dzd-test-domain",
        "region": "ap-southeast-2",
        "endpointOverride": "https://datazone.us-east-1.api.aws",
    }

    config = AmazonDataZoneConfig.from_dict(config_dict)

    assert config.domain_id == "dzd-test-domain"
    assert config.region == "ap-southeast-2"
    assert config.endpoint_override == "https://datazone.us-east-1.api.aws"


def test_config_from_dict_without_region() -> None:
    """Test that AmazonDataZoneConfig.from_dict works without region"""
    config_dict = {"domainId": "dzd-test-domain", "endpointOverride": "https://datazone.us-east-1.api.aws"}

    config = AmazonDataZoneConfig.from_dict(config_dict)

    assert config.domain_id == "dzd-test-domain"
    assert config.region is None
    assert config.endpoint_override == "https://datazone.us-east-1.api.aws"
