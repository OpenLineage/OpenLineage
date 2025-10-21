# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypeVar

import attr
from openlineage.client import event_v2
from openlineage.client.facet_v2 import parent_run
from openlineage.client.run import DatasetEvent, JobEvent, RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields
from packaging.version import Version

if TYPE_CHECKING:
    from confluent_kafka import KafkaError, Message, Producer
    from openlineage.client.client import Event
    from openlineage.client.facet import ParentRunFacet

log = logging.getLogger(__name__)

_T = TypeVar("_T", bound="KafkaConfig")


@attr.define
class KafkaConfig(Config):
    # Kafka producer config
    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
    config: dict[str, str]

    # Topic on which we should send messages
    topic: str

    # Explicit key for Kafka producer
    messageKey: str | None = None  # noqa: N815

    # Set to true if Kafka should flush after each event. The process that emits can be killed in
    # some cases - for example in Airflow integration, so flushing is desirable there.
    flush: bool = True

    @classmethod
    def from_dict(cls: type[_T], params: dict[str, Any]) -> _T:
        # alias message_key to messageKey
        if message_key := params.pop("message_key", None):
            params["messageKey"] = params.get("messageKey") or message_key
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
        self.producer: Producer | None = None
        if not self._is_airflow_sqlalchemy:
            self._setup_producer(self.kafka_config.config)
        log.debug("Constructing OpenLineage transport that will send events to kafka topic `%s`", self.topic)

    def _get_message_key(self, event: Event) -> str | None:
        if isinstance(event, (DatasetEvent, event_v2.DatasetEvent)):
            return f"dataset:{event.dataset.namespace}/{event.dataset.name}"

        if isinstance(event, (JobEvent, event_v2.JobEvent)):
            return f"job:{event.job.namespace}/{event.job.name}"

        if isinstance(event, (RunEvent, event_v2.RunEvent)):
            return self._get_run_message_key(event)

        return None

    def _get_run_message_key(self, event: RunEvent | event_v2.RunEvent) -> str:
        """
        To keep order of events in Kafka topic, we need to send them to the same partition.
        This is the case for:
            1. different runs of the same job.
            2. runs in the chain parent -> child -> grandchild.

        For (1) Kafka message_key has format "run:<namespace>/<name>".
        For (2) source for `<namespace>` and `<name>` is selected using this order:
        - run.facets.parent.root.job
        - run.facets.parent.job
        - run.job
        """

        run_default_result = f"run:{event.job.namespace}/{event.job.name}"

        run_facets: dict[str, Any] = event.run.facets or {}
        parent_run_facet: ParentRunFacet | parent_run.ParentRunFacet | None = run_facets.get("parent")
        if not parent_run_facet:
            return run_default_result

        parent_job_namespace: str | None = None
        parent_job_name: str | None = None
        if isinstance(parent_run_facet, parent_run.ParentRunFacet):
            (
                parent_job_namespace,
                parent_job_name,
            ) = self._get_run_message_key_args_from_parent_run_facet_v2(parent_run_facet)
        else:
            (
                parent_job_namespace,
                parent_job_name,
            ) = self._get_run_message_key_args_from_parent_run_facet(parent_run_facet)

        if not parent_job_namespace or not parent_job_name:
            return run_default_result

        return f"run:{parent_job_namespace}/{parent_job_name}"

    def _get_run_message_key_args_from_parent_run_facet(
        self,
        parent_run_facet: ParentRunFacet,
    ) -> tuple[str | None, str | None]:
        parent_job_namespace = parent_run_facet.job.get("namespace")
        parent_job_name = parent_run_facet.job.get("name")
        return parent_job_namespace, parent_job_name

    def _get_run_message_key_args_from_parent_run_facet_v2(
        self,
        parent_run_facet: parent_run.ParentRunFacet,
    ) -> tuple[str, str]:
        if parent_run_facet.root:
            root_job_namespace: str = parent_run_facet.root.job.namespace
            root_job_name: str = parent_run_facet.root.job.name
            return root_job_namespace, root_job_name

        parent_job_namespace: str = parent_run_facet.job.namespace
        parent_job_name: str = parent_run_facet.job.name
        return parent_job_namespace, parent_job_name

    def emit(self, event: Event) -> None:
        if self.producer is None:
            self._setup_producer(self.kafka_config.config)

        key = self.message_key or self._get_message_key(event)

        self.producer.produce(  # type: ignore[union-attr]
            topic=self.topic,
            key=key,
            value=Serde.to_json(event).encode("utf-8"),
            on_delivery=on_delivery,
        )
        if self.flush:
            self.wait_for_completion()
        if self._is_airflow_sqlalchemy:
            self.close()

    def wait_for_completion(self, timeout: float = -1) -> bool:
        """
        Block until all events are processed or timeout is reached.

        Params:
          timeout: Timeout in seconds. `-1` means to block until last event is processed, 0 means no timeout.

        Returns:
            bool: True if all events were processed, False if some events were not processed.
        """
        if self.producer is None:
            return True
        messages_left: int = self.producer.flush(timeout=timeout)
        log.debug("Amount of messages left in Kafka buffers after flush: %d", messages_left)
        return not messages_left

    def close(self, timeout: float = -1) -> bool:
        all_processed = self.wait_for_completion(timeout)
        self.producer = None
        return all_processed

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
        from airflow.version import version  # type: ignore[import-not-found]

        parsed_version = Version(version)
        if Version("2.3.0") <= parsed_version < Version("2.6.0"):
            return True
    except ImportError:
        pass  # we want to leave it to false if airflow import fails
    return False
