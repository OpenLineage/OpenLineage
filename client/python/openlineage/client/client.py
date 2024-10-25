# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import logging
import os
import warnings
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

import attr
import yaml
from openlineage.client.filter import Filter, FilterConfig, create_filter
from openlineage.client.serde import Serde
from openlineage.client.utils import deep_merge_dicts

if TYPE_CHECKING:
    from requests import Session
    from requests.adapters import HTTPAdapter

import contextlib

from openlineage.client import event_v2
from openlineage.client.facets import FacetsConfig
from openlineage.client.generated.environment_variables_run import (
    EnvironmentVariable,
    EnvironmentVariablesRunFacet,
)
from openlineage.client.run import DatasetEvent, JobEvent, RunEvent
from openlineage.client.transport import (
    Transport,
    TransportFactory,
    get_default_factory,
)
from openlineage.client.transport.http import HttpConfig, HttpTransport, create_token_provider
from openlineage.client.transport.noop import NoopConfig, NoopTransport

Event_v1 = Union[RunEvent, DatasetEvent, JobEvent]
Event_v2 = Union[event_v2.RunEvent, event_v2.DatasetEvent, event_v2.JobEvent]
Event = Union[Event_v1, Event_v2]


@attr.s
class OpenLineageClientOptions:
    timeout: float = attr.ib(default=5.0)
    verify: bool = attr.ib(default=True)
    api_key: str = attr.ib(default=None)
    adapter: HTTPAdapter = attr.ib(default=None)


@attr.s
class OpenLineageConfig:
    transport: dict[str, Any] | None = attr.ib(factory=dict)
    facets: FacetsConfig = attr.ib(factory=FacetsConfig)
    filters: list[FilterConfig] = attr.ib(factory=list)

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> OpenLineageConfig:
        config = cls()
        if "transport" in params:
            config.transport = params["transport"]
        if "facets" in params:
            config.facets = FacetsConfig(**params["facets"])
        if "filters" in params:
            config.filters = [FilterConfig(**filter_config) for filter_config in params["filters"]]
        return config


log = logging.getLogger(__name__)
_T = TypeVar("_T", bound="OpenLineageClient")


class OpenLineageClient:
    DYNAMIC_ENV_VARS_PREFIX = "OPENLINEAGE__"
    DEFAULT_URL_TRANSPORT_NAME = "default_http"

    def __init__(  # noqa: PLR0913
        self,
        url: str | None = None,
        options: OpenLineageClientOptions | None = None,
        session: Session | None = None,
        transport: Transport | None = None,
        factory: TransportFactory | None = None,
        *,
        config: dict[str, str] | None = None,
    ) -> None:
        # Set parent's logging level if environment variable is present
        custom_logging_level = os.getenv("OPENLINEAGE_CLIENT_LOGGING", None)
        if custom_logging_level:
            logging.getLogger(__name__.rpartition(".")[0]).setLevel(custom_logging_level)

        if url:
            warnings.warn(
                message="Initializing OpenLineageClient with url, options and session is deprecated.",
                category=DeprecationWarning,
                stacklevel=2,
            )

        # Make config ellipsis - as a guard value to not try to
        # reload yaml each time config is referred to.
        self._config: OpenLineageConfig | None = None

        self.user_defined_config: dict[str, str] | None = config

        self._alias_env_vars()

        self.transport = self._resolve_transport(
            url=url, options=options, session=session, transport=transport, factory=factory
        )
        log.info("OpenLineageClient will use `%s` transport", self.transport.kind)

        self._filters: list[Filter] = []
        for conf in self.config.filters:
            _filter = create_filter(conf)
            if _filter:
                self._filters.append(_filter)

    @classmethod
    def from_environment(cls: type[_T]) -> _T:
        warnings.warn(
            message="`OpenLineageClient.from_environment()` is deprecated. Use `OpenLineageClient()`.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return cls()

    @classmethod
    def from_dict(cls: type[_T], config: dict[str, str]) -> _T:
        return cls(config=config)

    def filter_event(
        self,
        event: Event,
    ) -> Event | None:
        """Filters jobs according to config-defined events"""
        for _filter in self._filters:
            if isinstance(event, RunEvent) and _filter.filter_event(event) is None:
                return None
        return event

    def emit(self, event: Event) -> None:
        from typing import get_args

        if type(event) not in get_args(Event):
            msg = "`emit` only accepts RunEvent, DatasetEvent, JobEvent classes"
            raise ValueError(msg)
        if not self.transport:
            log.error("Tried to emit OpenLineage event, but transport is not configured.")
            return
        if self.transport.kind == NoopTransport.kind:
            log.debug("OpenLineage is disabled. No events will be emitted.")
            return
        if self._filters and self.filter_event(event) is None:
            log.debug("OpenLineage event has been filtered out and will not be emitted.")
            return

        event = self.add_environment_facets(event)

        if log.isEnabledFor(logging.DEBUG):
            val = Serde.to_json(event).encode("utf-8")
            log.debug("OpenLineageClient will emit event %s", val)
        self.transport.emit(event)
        log.debug("OpenLineage event successfully emitted.")

    @property
    def config(self) -> OpenLineageConfig:
        """
        Retrieves the OpenLineage configuration.

        This property method returns the content of the OpenLineage YAML config file.
        The configuration is determined by merging sources in the following order of precedence:
        1. User-defined configuration passed to the client constructor.
        2. YAML config file located in one of the following paths:
        - Path specified by the `OPENLINEAGE_CONFIG` environment variable.
        - Current working directory.
        - `$HOME/.openlineage`.
        3. Environment variables with the `OPENLINEAGE__` prefix.
        If the configuration is not already loaded, it will be constructed by merging the above sources.
        In case of a TypeError during the parsing of the configuration, a ValueError will be raised indicating
        that the structure of the config does not match the expected format.
        """
        if self._config is None:
            config_dict: dict[str, Any] = {}
            if self.user_defined_config:
                config_dict = self.user_defined_config
            if config_path := self._find_yaml_config_path():
                config_dict = deep_merge_dicts(self._get_config_file_content(config_path), config_dict)
            if config_from_env_vars := self._load_config_from_env_variables():
                config_dict = deep_merge_dicts(config_from_env_vars, config_dict)
            try:
                self._config = OpenLineageConfig.from_dict(config_dict)
            except TypeError as e:
                # raise exception that structure of the config does not match
                msg = "Failed to parse OpenLineage config."
                raise ValueError(msg) from e
        return self._config

    def _resolve_transport(self, **kwargs: Any) -> Transport:
        """
        Resolves the transport mechanism based on the provided arguments or environment settings.

        This method determines the appropriate transport by executing a sequence of checks:
        1. Verifies if OpenLineage is disabled through environment variable.
        2. Looks for a transport object provided in the arguments.
        3. Attempts to configure the transport from a YAML config file.
        4. Tries to initialize HTTP transport with an url argument (deprecated).
        5. Tries to set up HTTP transport using environment variables.
        6. If no configuration is found, defaults to a console transport and logs a warning message.

        Returns:
            The transport object that will be used to send lineage events.
        """
        # 1. Check if OpenLineage is disabled
        if os.getenv("OPENLINEAGE_DISABLED", "").lower().strip() == "true":
            log.info("OpenLineage is disabled. No events will be emitted.")
            return NoopTransport(NoopConfig())

        # 2. Check if transport is provided explicitly
        if kwargs.get("transport"):
            return cast(Transport, kwargs["transport"])

        # 3. Check if transport configuration is provided in YAML config file
        if self.config.transport and self.config.transport.get("type"):
            factory = kwargs.get("factory") or get_default_factory()
            return factory.create(self.config.transport)

        # 4. Check legacy HTTP transport initialization with url and options
        if kwargs.get("url"):
            return self._http_transport_from_url(
                url=kwargs["url"], options=kwargs.get("options"), session=kwargs.get("session")
            )

        # 5. Check HTTP transport initialization with env variables
        if os.environ.get("OPENLINEAGE_URL"):
            return self._http_transport_from_env_variables()

        # 6. If all else fails, print events to console
        from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport

        log.warning("Couldn't find any OpenLineage transport configuration; will print events to console.")
        return ConsoleTransport(ConsoleConfig())

    @staticmethod
    def _get_config_file_content(config_path: str) -> dict[str, Any]:
        try:
            with open(config_path) as f:
                config: dict[str, Any] | None = yaml.safe_load(f)
                if not config:
                    log.error("Empty OpenLineage config file: `%s`", config_path)
                    return {}
                log.debug("Using content of OpenLineage config file: `%s`", config_path)
                return config
        except Exception:
            log.exception("Couldn't read content of the OpenLineage config file `%s`: ", config_path)
        return {}

    @staticmethod
    def _find_yaml_config_path() -> str | None:
        paths_to_check = (
            # Check OPENLINEAGE_CONFIG env variable
            (os.getenv("OPENLINEAGE_CONFIG", None), True),
            # Check current working directory for `openlineage.yml` file
            (os.path.join(os.getcwd(), "openlineage.yml"), False),
            # Check $HOME/.openlineage dir for `openlineage.yml` file
            (os.path.join(os.path.expanduser("~/.openlineage"), "openlineage.yml"), False),
        )

        for path, verbose in paths_to_check:
            try:
                if path and os.path.isfile(path) and os.access(path, os.R_OK):
                    return path
                if path and verbose:
                    log.debug("OpenLineage config file is missing or not readable: `%s`.", path)
            except Exception:  # noqa: BLE001
                # We can get different errors depending on system
                if verbose:
                    log.exception("Couldn't check if OpenLineage config file is readable: `%s`", path)
        return None

    @staticmethod
    def _http_transport_from_env_variables() -> HttpTransport:
        config = HttpConfig(
            url=os.environ["OPENLINEAGE_URL"],
            auth=create_token_provider(
                {
                    "type": "api_key",
                    "apiKey": os.environ.get("OPENLINEAGE_API_KEY", ""),
                },
            ),
        )
        endpoint = os.environ.get("OPENLINEAGE_ENDPOINT", None)
        if endpoint is not None:
            config.endpoint = endpoint

        return HttpTransport(config)

    @staticmethod
    def _http_transport_from_url(
        url: str,
        options: OpenLineageClientOptions | None,
        session: Session | None,
    ) -> HttpTransport:
        if not options:
            options = OpenLineageClientOptions()
        return HttpTransport(
            HttpConfig.from_options(
                url=url,
                options=options,
                session=session,
            ),
        )

    def _alias_env_vars(self) -> None:
        default_transport_name = self.DEFAULT_URL_TRANSPORT_NAME.upper()
        if url := os.environ.get("OPENLINEAGE_URL"):
            if any(
                k.startswith(f"OPENLINEAGE__TRANSPORT__TRANSPORTS__{default_transport_name}")
                for k in os.environ
            ):
                log.warning(
                    "%s already found in environment variables, skipping aliasing OPENLINEAGE_URL",
                    default_transport_name,
                )
                return
            os.environ[f"OPENLINEAGE__TRANSPORT__TRANSPORTS__{default_transport_name}__TYPE"] = "http"
            os.environ[f"OPENLINEAGE__TRANSPORT__TRANSPORTS__{default_transport_name}__URL"] = url
            if api_key := os.environ.get("OPENLINEAGE_API_KEY"):
                os.environ[
                    f"OPENLINEAGE__TRANSPORT__TRANSPORTS__{default_transport_name}__AUTH"
                ] = json.dumps(
                    {
                        "type": "api_key",
                        "apiKey": api_key,
                    }
                )
            if endpoint := os.environ.get("OPENLINEAGE_ENDPOINT"):
                os.environ[
                    f"OPENLINEAGE__TRANSPORT__TRANSPORTS__{default_transport_name}__ENDPOINT"
                ] = endpoint

    @classmethod
    def _load_config_from_env_variables(cls) -> dict[str, Any] | None:
        config: dict[str, Any] = {}

        # get os.environ.items only starting with OPENLINEAGE_ prefix and reverse sort
        # to make sure that top-level keys have precedence
        env_vars = sorted(
            filter(lambda k: k[0].startswith(cls.DYNAMIC_ENV_VARS_PREFIX), os.environ.items()), reverse=True
        )

        for env_key, env_value in env_vars:
            keys = env_key[len(cls.DYNAMIC_ENV_VARS_PREFIX) :].split("__")

            # Parse value (try to parse as JSON, otherwise lowercase the value)
            with contextlib.suppress(json.JSONDecodeError):
                env_value = json.loads(env_value)  # noqa: PLW2901

            cls._insert_into_config(config, keys, env_value)

        return config

    @staticmethod
    def _insert_into_config(config: dict[str, Any], key_path: list[str], value: str) -> None:
        keys = [key.lower() for key in key_path]

        current = config
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        # Overwrite if key already exists
        current[keys[-1]] = value

    def add_environment_facets(self, event: Event) -> Event:
        """
        Adds environment variables as facets to the event object.
        """
        if isinstance(event, RunEvent) and (env_vars := self._collect_environment_variables()):
            event.run.facets["environmentVariables"] = EnvironmentVariablesRunFacet(
                environmentVariables=[
                    EnvironmentVariable(name=name, value=value) for name, value in env_vars.items()
                ]
            )
        return event

    def _collect_environment_variables(self) -> dict[str, str]:
        """
        Collects and returns a dictionary of relevant environment variables.
        """
        filtered_vars = {k: v for k, v in os.environ.items() if k in self.config.facets.environment_variables}
        missing_vars = set(self.config.facets.environment_variables) - set(filtered_vars)
        if missing_vars:
            log.warning(
                "The following environment variables are missing: %s when adding to OpenLineage event",
                missing_vars,
            )
        return filtered_vars
