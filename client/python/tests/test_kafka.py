# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import ANY, call

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.facet import ParentRunFacet
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
from openlineage.client.transport.kafka import KafkaConfig, KafkaTransport

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.fixture()
def run_event() -> RunEvent:
    return RunEvent(
        eventType=RunState.START,
        eventTime="2024-04-10T15:08:01.333999",
        run=Run(runId="ea445b5c-22eb-457a-8007-01c7c52b6e54"),
        job=Job(namespace="test-namespace", name="test-job"),
        producer="prod",
        schemaURL="schema",
    )


@pytest.fixture()
def run_event_with_parent() -> RunEvent:
    parent_run_facet = ParentRunFacet.create(
        runId="d9cb8e0b-a410-435e-a619-da5e87ba8508",
        namespace="parent-namespace",
        name="parent-job",
    )

    return RunEvent(
        eventType=RunState.START,
        eventTime="2024-04-10T15:08:01.333999",
        run=Run(runId="ea445b5c-22eb-457a-8007-01c7c52b6e54", facets={"parent": parent_run_facet}),
        job=Job(namespace="test-namespace", name="test-job"),
        producer="prod",
        schemaURL="schema",
    )


@pytest.fixture()
def dataset_event() -> DatasetEvent:
    return DatasetEvent(
        eventTime="2024-04-10T15:08:01.333999",
        dataset=Dataset(namespace="test-namespace", name="test-dataset"),
        producer="prod",
        schemaURL="schema",
    )


@pytest.fixture()
def job_event() -> JobEvent:
    return JobEvent(
        eventTime="2024-04-10T15:08:01.333999",
        job=Job(namespace="test-namespace", name="test-job"),
        producer="prod",
        schemaURL="schema",
    )


def test_kafka_loads_full_config() -> None:
    config = KafkaConfig.from_dict(
        {
            "type": "kafka",
            "config": {"bootstrap.servers": "localhost:9092"},
            "topic": "random-topic",
            "messageKey": "key",
            "flush": False,
        },
    )

    assert config.config["bootstrap.servers"] == "localhost:9092"
    assert config.topic == "random-topic"
    assert config.messageKey == "key"
    assert config.flush is False


def test_kafka_loads_partial_config_with_defaults() -> None:
    config = KafkaConfig.from_dict(
        {
            "type": "kafka",
            "config": {"bootstrap.servers": "localhost:9092"},
            "topic": "random-topic",
        },
    )

    assert config.config["bootstrap.servers"] == "localhost:9092"
    assert config.topic == "random-topic"
    assert config.messageKey is None
    assert config.flush is True


def test_kafka_config_accepts_aliased_message_key() -> None:
    config = KafkaConfig.from_dict(
        {
            "type": "kafka",
            "topic": "my_topic",
            "message_key": "some-value",
            "flush": True,
            "config": {"bootstrap.servers": "localhost:9092,another.host:9092", "acks": "all", "retries": 3},
        }
    )

    assert config.messageKey == "some-value"
    assert config.topic == "my_topic"
    assert config.flush is True


def test_kafka_load_config_fails_on_no_config() -> None:
    with pytest.raises(TypeError):
        KafkaConfig.from_dict(
            {
                "type": "kafka",
                "config": {"bootstrap.servers": "localhost:9092"},
            },
        )


def test_client_with_kafka_transport_emits_run_event(run_event: RunEvent, mocker: MockerFixture) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        flush=True,
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)

    client.emit(run_event)
    transport.producer.produce.assert_called_once_with(
        topic="random-topic",
        key="run:test-namespace/test-job",
        value=Serde.to_json(run_event).encode("utf-8"),
        on_delivery=ANY,
    )
    transport.producer.flush.assert_called_once()


def test_client_with_kafka_transport_emits_run_event_with_parent(
    run_event_with_parent: RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        flush=True,
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)

    client.emit(run_event_with_parent)
    transport.producer.produce.assert_called_once_with(
        topic="random-topic",
        key="run:parent-namespace/parent-job",
        value=Serde.to_json(run_event_with_parent).encode("utf-8"),
        on_delivery=ANY,
    )
    transport.producer.flush.assert_called_once()


def test_client_with_kafka_transport_emits_run_event_with_explicit_message_key(
    run_event: RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        messageKey="explicit-key",
        flush=True,
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)

    client.emit(run_event)
    transport.producer.produce.assert_called_once_with(
        topic="random-topic",
        key="explicit-key",
        value=Serde.to_json(run_event).encode("utf-8"),
        on_delivery=ANY,
    )
    transport.producer.flush.assert_called_once()


def test_client_with_kafka_transport_emits_job_event(job_event: DatasetEvent, mocker: MockerFixture) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        flush=True,
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)

    client.emit(job_event)
    transport.producer.produce.assert_called_once_with(
        topic="random-topic",
        key="job:test-namespace/test-job",
        value=Serde.to_json(job_event).encode("utf-8"),
        on_delivery=ANY,
    )
    transport.producer.flush.assert_called_once()


def test_client_with_kafka_transport_emits_job_event_with_explicit_message_key(
    job_event: DatasetEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        messageKey="explicit-key",
        flush=True,
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)

    client.emit(job_event)
    transport.producer.produce.assert_called_once_with(
        topic="random-topic",
        key="explicit-key",
        value=Serde.to_json(job_event).encode("utf-8"),
        on_delivery=ANY,
    )
    transport.producer.flush.assert_called_once()


def test_client_with_kafka_transport_emits_dataset_event(
    dataset_event: DatasetEvent, mocker: MockerFixture
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        flush=True,
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)

    client.emit(dataset_event)
    transport.producer.produce.assert_called_once_with(
        topic="random-topic",
        key="dataset:test-namespace/test-dataset",
        value=Serde.to_json(dataset_event).encode("utf-8"),
        on_delivery=ANY,
    )
    transport.producer.flush.assert_called_once()


def test_client_with_kafka_transport_emits_dataset_event_explicit_message_key(
    dataset_event: DatasetEvent, mocker: MockerFixture
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        messageKey="explicit-key",
        flush=True,
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)

    client.emit(dataset_event)
    transport.producer.produce.assert_called_once_with(
        topic="random-topic",
        key="explicit-key",
        value=Serde.to_json(dataset_event).encode("utf-8"),
        on_delivery=ANY,
    )
    transport.producer.flush.assert_called_once()


def test_airflow_sqlalchemy_constructs_producer_each_call(
    run_event: RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch(
        "openlineage.client.transport.kafka._check_if_airflow_sqlalchemy_context",
        return_value=True,
    )
    mock = mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        flush=True,
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)

    client.emit(run_event)
    client.emit(run_event)
    mock.assert_has_calls([call(config.config), call(config.config)], any_order=True)


def test_airflow_direct_constructs_producer_once(run_event: RunEvent, mocker: MockerFixture) -> None:
    mocker.patch(
        "openlineage.client.transport.kafka._check_if_airflow_sqlalchemy_context",
        return_value=False,
    )
    mock = mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        flush=True,
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)

    client.emit(run_event)
    client.emit(run_event)
    mock.assert_called_once_with(config.config)
