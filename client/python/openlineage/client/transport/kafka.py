# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar, Union

import attr

from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields

if TYPE_CHECKING:
    from openlineage.client.run import DatasetEvent, JobEvent, RunEvent
log = logging.getLogger(__name__)

_T = TypeVar("_T", bound="KafkaConfig")


@attr.s
class KafkaConfig(Config):
    # Kafka producer config
    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
    config: dict[str, str] = attr.ib()

    # Topic on which we should send messages
    topic: str = attr.ib()

    # Set to true if Kafka should flush after each event. The process that emits can be killed in
    # some cases - for example in Airflow integration, so flushing is desirable there.
    flush: bool = attr.ib(default=True)

    @classmethod
    def from_dict(cls: type[_T], params: dict[str, str]) -> _T:
        if "config" not in params:
            msg = "kafka `config` not passed to KafkaConfig"
            raise RuntimeError(msg)
        if not isinstance(params["config"], dict):
            msg = "`config` passed to KafkaConfig must be dict"
            raise RuntimeError(msg)  # noqa: TRY004
        return cls(**get_only_specified_fields(cls, params))


# Very basic transport impl
class KafkaTransport(Transport):
    kind = "kafka"
    config = KafkaConfig

    def __init__(self, config: KafkaConfig) -> None:
        try:
            import confluent_kafka as kafka

            self.producer = kafka.Producer(config.config)
            self.topic = config.topic
            self.flush = config.flush
        except ModuleNotFoundError:
            log.exception(
                "OpenLineage client did not found confluent-kafka module. "
                "Installing it is required for KafkaTransport to work. "
                "You can also get it via `pip install openlineage-python[kafka]`",
            )
            raise
        log.debug("Constructing openlineage client to send events to topic %s", config.topic)

    def emit(self, event: Union[RunEvent, DatasetEvent, JobEvent]) -> None:  # noqa: UP007
        self.producer.produce(topic=self.topic, value=Serde.to_json(event).encode("utf-8"))
        if self.flush:
            self.producer.flush(timeout=5)
