# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar, Union

import attr
from pkg_resources import parse_version

from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields

if TYPE_CHECKING:
    from confluent_kafka import KafkaError, Message

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


def on_delivery(err: KafkaError, msg: Message) -> None:
    # Used as callback for Kafka producer delivery confirmation
    if err:
        log.exception(err)
    log.debug("Send message %s", msg)


class KafkaTransport(Transport):
    kind = "kafka"
    config_class = KafkaConfig

    def __init__(self, config: KafkaConfig) -> None:
        self.topic = config.topic
        self.flush = config.flush
        self.kafka_config = config
        self._is_airflow_sqlalchemy = _check_if_airflow_sqlalchemy_context()
        self.producer = None
        if not self._is_airflow_sqlalchemy:
            self._setup_producer(self.kafka_config.config)
        log.debug("Constructing openlineage client to send events to topic %s", config.topic)

    def emit(self, event: Union[RunEvent, DatasetEvent, JobEvent]) -> None:  # noqa: UP007
        if self._is_airflow_sqlalchemy:
            self._setup_producer(self.kafka_config.config)
        self.producer.produce(  # type: ignore[attr-defined]
            topic=self.topic,
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=on_delivery,
        )
        if self.flush:
            rest = self.producer.flush(timeout=10)  # type: ignore[attr-defined]
            log.debug("Amount of messages left in Kafka buffers after flush %d", rest)
        if self._is_airflow_sqlalchemy:
            self.producer = None

    def _setup_producer(self, config: dict) -> None:  # type: ignore[type-arg]
        try:
            import confluent_kafka as kafka

            added_config = {}
            if log.isEnabledFor(logging.DEBUG):
                added_config = {
                    "logger": log,
                    "debug": "all",
                    "log_level": 7,
                }
            self.producer = kafka.Producer({**added_config, **config})
        except ModuleNotFoundError:
            log.exception(
                "OpenLineage client did not found confluent-kafka module. "
                "Installing it is required for KafkaTransport to work. "
                "You can also get it via `pip install openlineage-python[kafka]`",
            )
            raise


def _check_if_airflow_sqlalchemy_context() -> bool:
    try:
        from airflow.version import version  # type: ignore[import]

        parsed_version = parse_version(version)
        if parse_version("2.3.0") <= parsed_version < parse_version("2.6.0"):
            return True
    except ImportError:
        pass  # we want to leave it to false if airflow import fails
    return False
