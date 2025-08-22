# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import ANY, call

import pytest
from openlineage.client import OpenLineageClient, event_v2
from openlineage.client.facet import ParentRunFacet
from openlineage.client.facet_v2 import parent_run
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
    from pytest_kafka_transport_mock import MockerFixture


@pytest.fixture
def run_event() -> RunEvent:
    return RunEvent(
        eventType=RunState.START,
        eventTime="2024-04-10T15:08:01.333999",
        run=Run(runId="ea445b5c-22eb-457a-8007-01c7c52b6e54"),
        job=Job(namespace="test-namespace", name="test-job"),
        producer="prod",
        schemaURL="schema",
    )


@pytest.fixture
def run_event_v2() -> RunEvent:
    return event_v2.RunEvent(
        eventType=event_v2.RunState.START,
        eventTime="2024-04-10T15:08:01.333999",
        run=event_v2.Run(runId="ea445b5c-22eb-457a-8007-01c7c52b6e54"),
        job=event_v2.Job(namespace="test-namespace", name="test-job"),
        producer="prod",
    )


@pytest.fixture
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


@pytest.fixture
def run_event_with_parent_v2() -> RunEvent:
    parent_run_facet = parent_run.ParentRunFacet(
        job=parent_run.Job(namespace="parent-namespace", name="parent-job"),
        run=parent_run.Run(runId="d9cb8e0b-a410-435e-a619-da5e87ba8508"),
    )

    return event_v2.RunEvent(
        eventType=event_v2.RunState.START,
        eventTime="2024-04-10T15:08:01.333999",
        run=event_v2.Run(runId="ea445b5c-22eb-457a-8007-01c7c52b6e54", facets={"parent": parent_run_facet}),
        job=event_v2.Job(namespace="test-namespace", name="test-job"),
        producer="prod",
    )


@pytest.fixture
def run_event_with_root_parent_v2() -> RunEvent:
    root_run = parent_run.Root(
        job=parent_run.RootJob(namespace="root-namespace", name="root-job"),
        run=parent_run.RootRun(runId="ad0995e1-49f1-432d-92b6-2e2739034c13"),
    )

    parent_run_facet = parent_run.ParentRunFacet(
        job=parent_run.Job(namespace="parent-namespace", name="parent-job"),
        run=parent_run.Run(runId="d9cb8e0b-a410-435e-a619-da5e87ba8508"),
        root=root_run,
    )

    return event_v2.RunEvent(
        eventType=event_v2.RunState.START,
        eventTime="2024-04-10T15:08:01.333999",
        run=event_v2.Run(runId="ea445b5c-22eb-457a-8007-01c7c52b6e54", facets={"parent": parent_run_facet}),
        job=event_v2.Job(namespace="test-namespace", name="test-job"),
        producer="prod",
    )


@pytest.fixture
def dataset_event() -> DatasetEvent:
    return DatasetEvent(
        eventTime="2024-04-10T15:08:01.333999",
        dataset=Dataset(namespace="test-namespace", name="test-dataset"),
        producer="prod",
        schemaURL="schema",
    )


@pytest.fixture
def dataset_event_v2() -> DatasetEvent:
    return event_v2.DatasetEvent(
        eventTime="2024-04-10T15:08:01.333999",
        dataset=event_v2.Dataset(namespace="test-namespace", name="test-dataset"),
        producer="prod",
    )


@pytest.fixture
def job_event() -> JobEvent:
    return JobEvent(
        eventTime="2024-04-10T15:08:01.333999",
        job=Job(namespace="test-namespace", name="test-job"),
        producer="prod",
        schemaURL="schema",
    )


@pytest.fixture
def job_event_v2() -> JobEvent:
    return event_v2.JobEvent(
        eventTime="2024-04-10T15:08:01.333999",
        job=event_v2.Job(namespace="test-namespace", name="test-job"),
        producer="prod",
    )


def test_kafka_transport_loads_full_config() -> None:
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


def test_kafka_transport_loads_partial_config_with_defaults() -> None:
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


def test_kafka_transport_config_accepts_aliased_message_key() -> None:
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


def test_kafka_transport_load_config_fails_on_no_config() -> None:
    with pytest.raises(TypeError):
        KafkaConfig.from_dict(
            {
                "type": "kafka",
                "config": {"bootstrap.servers": "localhost:9092"},
            },
        )


def test_kafka_transport_emits_run_event(
    run_event: RunEvent,
    run_event_v2: event_v2.RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
    )

    for event in [run_event, run_event_v2]:
        transport = KafkaTransport(config)

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        transport.producer.produce.assert_called_once_with(
            topic="random-topic",
            key="run:test-namespace/test-job",
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=ANY,
        )
        transport.producer.reset_mock()


def test_kafka_transport_emits_run_event_with_parent(
    run_event_with_parent: RunEvent,
    run_event_with_parent_v2: event_v2.RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
    )

    for event in [run_event_with_parent, run_event_with_parent_v2]:
        transport = KafkaTransport(config)

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        transport.producer.produce.assert_called_once_with(
            topic="random-topic",
            key="run:parent-namespace/parent-job",
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=ANY,
        )
        transport.producer.reset_mock()


def test_kafka_transport_emits_run_event_with_root_parent(
    run_event_with_root_parent_v2: event_v2.RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)

    client.emit(run_event_with_root_parent_v2)
    transport.producer.produce.assert_called_once_with(
        topic="random-topic",
        key="run:root-namespace/root-job",
        value=Serde.to_json(run_event_with_root_parent_v2).encode("utf-8"),
        on_delivery=ANY,
    )
    transport.producer.flush.assert_called_once()


def test_kafka_transport_emits_run_event_with_explicit_message_key(
    run_event: RunEvent,
    run_event_v2: event_v2.RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        messageKey="explicit-key",
    )

    for event in [run_event, run_event_v2]:
        transport = KafkaTransport(config)

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        transport.producer.produce.assert_called_once_with(
            topic="random-topic",
            key="explicit-key",
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=ANY,
        )
        transport.producer.reset_mock()


def test_kafka_transport_emits_job_event(
    job_event: JobEvent,
    job_event_v2: event_v2.JobEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
    )

    for event in [job_event, job_event_v2]:
        transport = KafkaTransport(config)

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        transport.producer.produce.assert_called_once_with(
            topic="random-topic",
            key="job:test-namespace/test-job",
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=ANY,
        )
        transport.producer.reset_mock()


def test_kafka_transport_emits_job_event_with_explicit_message_key(
    job_event: JobEvent,
    job_event_v2: event_v2.JobEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        messageKey="explicit-key",
    )
    for event in [job_event, job_event_v2]:
        transport = KafkaTransport(config)

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        transport.producer.produce.assert_called_once_with(
            topic="random-topic",
            key="explicit-key",
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=ANY,
        )
        transport.producer.reset_mock()


def test_kafka_transport_emits_dataset_event(
    dataset_event: DatasetEvent,
    dataset_event_v2: event_v2.DatasetEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
    )

    for event in [dataset_event, dataset_event_v2]:
        transport = KafkaTransport(config)

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        transport.producer.produce.assert_called_once_with(
            topic="random-topic",
            key="dataset:test-namespace/test-dataset",
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=ANY,
        )
        transport.producer.reset_mock()


def test_kafka_transport_emits_dataset_event_explicit_message_key(
    dataset_event: DatasetEvent,
    dataset_event_v2: event_v2.DatasetEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        messageKey="explicit-key",
    )

    for event in [dataset_event, dataset_event_v2]:
        transport = KafkaTransport(config)

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        transport.producer.produce.assert_called_once_with(
            topic="random-topic",
            key="explicit-key",
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=ANY,
        )
        transport.producer.reset_mock()


def test_kafka_transport_emits_run_event_with_flush(
    run_event: RunEvent,
    run_event_v2: event_v2.RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        flush=True,
    )

    for event in [run_event, run_event_v2]:
        transport = KafkaTransport(config)

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        transport.producer.produce.assert_called_once_with(
            topic="random-topic",
            key="run:test-namespace/test-job",
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=ANY,
        )
        transport.producer.flush.assert_called_once()
        transport.producer.reset_mock()


def test_kafka_transport_emits_run_event_without_flush(
    run_event: RunEvent,
    run_event_v2: event_v2.RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch("confluent_kafka.Producer")
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        flush=False,
    )

    for event in [run_event, run_event_v2]:
        transport = KafkaTransport(config)

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        transport.producer.produce.assert_called_once_with(
            topic="random-topic",
            key="run:test-namespace/test-job",
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=ANY,
        )
        transport.producer.flush.assert_not_called()
        transport.producer.reset_mock()


def test_kafka_transport_airflow_sqlalchemy_constructs_producer_each_call(
    run_event: RunEvent,
    run_event_v2: event_v2.RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch(
        "openlineage.client.transport.kafka._check_if_airflow_sqlalchemy_context",
        return_value=True,
    )
    mock = mocker.patch("confluent_kafka.Producer")
    mock_producer = mock.return_value
    mock_producer.flush.return_value = 0

    for event in [run_event, run_event_v2]:
        config = KafkaConfig(
            config={"bootstrap.servers": "localhost:9092"},
            topic="random-topic",
        )
        transport = KafkaTransport(config)
        # producer wasn't created in constructor
        assert transport.producer is None

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        client.emit(event)

        # producer was removed in .close(), and then created again
        mock.assert_has_calls([call(config.config), call(config.config)], any_order=True)
        mock_producer.flush.assert_has_calls([call(timeout=-1), call(timeout=-1)], any_order=True)
        assert transport.producer is None

        mock.reset_mock()


def test_kafka_transport_airflow_direct_constructs_producer_once(
    run_event: RunEvent,
    run_event_v2: event_v2.RunEvent,
    mocker: MockerFixture,
) -> None:
    mocker.patch(
        "openlineage.client.transport.kafka._check_if_airflow_sqlalchemy_context",
        return_value=False,
    )
    mock = mocker.patch("confluent_kafka.Producer")
    mock.return_value.flush.return_value = 0

    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
    )

    for event in [run_event, run_event_v2]:
        transport = KafkaTransport(config)
        producer1 = transport.producer
        # producer is just created
        assert producer1 is not None

        client = OpenLineageClient(transport=transport)

        client.emit(event)
        client.emit(event)

        # producer is reused
        mock.assert_called_once_with(config.config)
        mock.reset_mock()

        producer2 = transport.producer
        assert producer2 is not None
        assert producer1 is producer2


def test_kafka_transport_close(mocker: MockerFixture) -> None:
    mock = mocker.patch("confluent_kafka.Producer")
    mock_producer = mock.return_value
    mock_producer.flush.return_value = 0

    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
    )

    transport = KafkaTransport(config)
    transport.close()

    mock_producer.flush.assert_called_once_with(timeout=-1)
    assert transport.producer is None

    mock_producer.reset_mock()

    transport = KafkaTransport(config)
    transport.close(10)

    mock_producer.flush.assert_called_once_with(timeout=10)
    assert transport.producer is None
