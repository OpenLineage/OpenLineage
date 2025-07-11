# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import copy
import inspect
import logging
from typing import TYPE_CHECKING, Any

import attr
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields, import_from_string

if TYPE_CHECKING:
    from openlineage.client.client import Event

log = logging.getLogger(__name__)


class EventTransformer:
    """Base class for transformers of OpenLineage events."""

    def __init__(self, properties: dict[str, Any]) -> None:
        """Initialize the transformer with a dictionary of properties.

        These properties can be used by subclasses to configure the transformation behavior.
        """
        self.properties = properties

    def transform(self, event: Event) -> Event | None:
        """Transform an OpenLineage event.

        Subclasses must override this method to apply specific transformations
        to the given event.

        Args:
            event: The event to be transformed.

        Returns:
            The transformed event, or None to not emit the event.
        """
        raise NotImplementedError

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}({self.properties})>"


@attr.define
class TransformConfig(Config):
    """Configuration class for transform transport.

    Holds configuration details including inner transport settings, the name of the
    transformer class to use, and optional transformer-specific properties.
    """

    transport: dict[str, Any]
    transformer_class: str
    transformer_properties: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> TransformConfig:
        """Create a TransformConfig instance from a dictionary of parameters.

        Validates that required fields (`transport` and `transformer_class`) are present
        and filters out unspecified fields based on the class definition.

        Args:
            params: Dictionary containing configuration parameters.

        Returns:
            An instance of TransformConfig populated with the provided data.

        Raises:
            RuntimeError: If required keys (`transport` or `transformer_class`) are missing.
        """
        if "transport" not in params:
            msg = "'transport' not passed to TransformConfig"
            raise RuntimeError(msg)
        if "transformer_class" not in params:
            msg = "'transformer_class' not passed to TransformConfig"
            raise RuntimeError(msg)
        return cls(**get_only_specified_fields(cls, params))


class TransformTransport(Transport):
    """Transport that transforms events before emitting them.

    This transport wraps another transport (defined in the configuration) and intercepts
    events for transformation using a user-defined EventTransformer class. Events can be
    modified or discarded prior to being sent.
    """

    kind = "transform"
    config_class = TransformConfig

    def __init__(self, config: TransformConfig) -> None:
        """Initialize the TransformTransport with the given configuration.

        Dynamically loads the transformer class specified in the config, validates it,
        and initializes both the transformer and the inner transport.

        Args:
            config: Configuration object containing transport and transformer settings.

        Raises:
            TypeError: If the transformer is not a valid subclass of EventTransformer.
        """
        # Import within method to avoid circular import error
        from openlineage.client.transport import get_default_factory

        transformer_class = import_from_string(config.transformer_class)
        if not inspect.isclass(transformer_class) or not issubclass(transformer_class, EventTransformer):
            msg = f"Transformer `{transformer_class}` has to be class, and subclass of EventTransformer"
            raise TypeError(msg)
        self.transformer = transformer_class(properties=config.transformer_properties or {})
        self.transport = get_default_factory().create(config.transport)

    def emit(self, event: Event) -> Any:
        """Transform and emit an OpenLineage event.

        Applies the configured EventTransformer to the event. If the result is not `None`,
        the transformed event is passed to the wrapped transport's `emit` method.

        Args:
            event: The OpenLineage event to be transformed and emitted.

        Returns:
            The result of the inner transport's emit call, or None if the event is dropped.
        """
        log.debug("Event before copy: %s", Serde.to_json(event))
        try:
            event = copy.deepcopy(event)  # Copy to make sure the transformer does not modify original event
            log.debug("Event after copy, before transformation: %s", Serde.to_json(event))
        except AttributeError as e:
            msg = "Error serializing copy of event."
            raise ValueError(msg) from e
        log.debug("Transforming OpenLineage event with %s", self.transformer)
        transformed = self.transformer.transform(event)
        if transformed is None:
            log.info("Transformed OpenLineage event is None. Skipping emission.")
            return None
        log.debug("Event after transformation: %s", Serde.to_json(transformed))
        return self.transport.emit(transformed)

    def close(self, timeout: float = -1) -> bool:
        return self.transport.close(timeout)
