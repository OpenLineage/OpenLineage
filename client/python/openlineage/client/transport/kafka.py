# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypeVar

import attr
from openlineage.client.facet import ParentRunFacet
from openlineage.client.run import DatasetEvent, JobEvent, RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields
from packaging.version import Version

if TYPE_CHECKING:
    from confluent_kafka import KafkaError, Message
    from openlineage.client.client import Event
log = logging.getLogger(__name__)

_T = TypeVar("_T", bound="KafkaConfig")


@attr.s
class KafkaConfig(Config):
    # Kafka producer config
    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
    config: dict[str, str] = attr.ib()

    # Topic on which we should send messages
    topic: str = attr.ib()

    # Explicit key for Kafka producer
    messageKey: str | None = attr.ib(default=None)  # noqa: N815

    # Set to true if Kafka should flush after each event. The process that emits can be killed in
    # some cases - for example in Airflow integration, so flushing is desirable there.
    flush: bool = attr.ib(default=True)

    @classmethod
    def from_dict(cls: type[_T], params: dict[str, Any]) -> _T:
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
        self.message_key = config.messageKey
        self.kafka_config = config
        self._is_airflow_sqlalchemy = _check_if_airflow_sqlalchemy_context()
        self.producer = None
        if not self._is_airflow_sqlalchemy:
            self._setup_producer(self.kafka_config.config)
        log.debug("Constructing openlineage client to send events to topic %s", config.topic)

    def _get_message_key(self, event: RunEvent | DatasetEvent | JobEvent) -> str:
        if isinstance(event, DatasetEvent):
            return f"dataset:{event.dataset.namespace}/{event.dataset.name}"

        if isinstance(event, JobEvent):
            return f"job:{event.job.namespace}/{event.job.name}"

        parent_run_facet: ParentRunFacet = event.run.facets.get("parent") or ParentRunFacet({}, {})
        parent_job_namespace: str | None = parent_run_facet.job.get("namespace")
        parent_job_name: str | None = parent_run_facet.job.get("name")
        parent_run_id: str | None = parent_run_facet.run.get("runId")
        if parent_job_namespace and parent_job_name and parent_run_id:
            return f"run:{parent_job_namespace}/{parent_job_name}/{parent_run_id}"

        return f"run:{event.job.namespace}/{event.job.name}/{event.run.runId}"

    def emit(self, event: Event) -> None:
        if self._is_airflow_sqlalchemy:
            self._setup_producer(self.kafka_config.config)

        key = self.message_key or self._get_message_key(event)

        self.producer.produce(  # type: ignore[attr-defined]
            topic=self.topic,
            key=key,
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

        parsed_version = Version(version)
        if Version("2.3.0") <= parsed_version < Version("2.6.0"):
            return True
    except ImportError:
        pass  # we want to leave it to false if airflow import fails
    return False
