# SPDX-License-Identifier: Apache-2.0.
import logging
from typing import Dict

import attr

from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Transport, Config
from openlineage.client.utils import get_only_specified_fields

log = logging.getLogger(__name__)


@attr.s
class KafkaConfig(Config):
    # Kafka producer config
    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
    config: Dict[str, str] = attr.ib()

    # Topic on which we should send messages
    topic: str = attr.ib()

    # Set to true if Kafka should flush after each event. The process that emits can be killed in
    # some cases - for example in Airflow integration, so flushing is desirable there.
    flush: bool = attr.ib(default=True)

    @classmethod
    def from_dict(cls, params: dict):
        if 'config' not in params:
            raise RuntimeError("kafka `config` not passed to KafkaConfig")
        if not isinstance(params['config'], dict):
            raise RuntimeError("`config` passed to KafkaConfig must be dict")
        return cls(**get_only_specified_fields(cls, params))


# Very basic transport impl
class KafkaTransport(Transport):
    kind = "kafka"
    config = KafkaConfig

    def __init__(self, config: KafkaConfig):
        import confluent_kafka as kafka
        self.producer = kafka.Producer(config.config)
        self.topic = config.topic
        self.flush = config.flush

        log.debug(f"Constructing openlineage client to send events to topic {config.topic}")

    def emit(self, event: RunEvent):
        self.producer.produce(topic=self.topic, value=Serde.to_json(event).encode('utf-8'))
        if self.flush:
            self.producer.flush(timeout=2)
