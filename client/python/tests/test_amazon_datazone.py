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
