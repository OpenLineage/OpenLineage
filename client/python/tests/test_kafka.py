# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import datetime
import uuid
from unittest.mock import patch

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde
from openlineage.client.transport.kafka import KafkaConfig, KafkaTransport


def test_kafka_loads_full_config():
    config = KafkaConfig.from_dict({
        "type": "kafka",
        "config": {"bootstrap.servers": "localhost:9092"},
        "topic": "random-topic",
        "flush": False
    })

    assert config.config['bootstrap.servers'] == "localhost:9092"
    assert config.topic == "random-topic"
    assert config.flush is False


def test_kafka_loads_partial_config_with_defaults():
    config = KafkaConfig.from_dict({
        "type": "kafka",
        "config": {"bootstrap.servers": "localhost:9092"},
        "topic": "random-topic"
    })

    assert config.config['bootstrap.servers'] == "localhost:9092"
    assert config.topic == "random-topic"
    assert config.flush is True


def test_kafka_load_config_fails_on_no_config():
    with pytest.raises(TypeError):
        KafkaConfig.from_dict({
            "type": "kafka",
            "config": {"bootstrap.servers": "localhost:9092"}
        })


@patch("confluent_kafka.Producer")
def test_client_with_kafka_transport_emits(kafka):
    config = KafkaConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        flush=True
    )
    transport = KafkaTransport(config)

    client = OpenLineageClient(transport=transport)
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(uuid.uuid4())),
        job=Job(namespace="kafka", name="test"),
        producer="prod",
    )

    client.emit(event)
    transport.producer.produce.assert_called_once_with(
        topic='random-topic',
        value=Serde.to_json(event).encode("utf-8")
    )
    transport.producer.flush.assert_called_once()
