# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from functools import cached_property
from importlib import import_module
from typing import TYPE_CHECKING, Any, Literal, TypeVar

import attr
from openlineage.client import event_v2
from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields

if TYPE_CHECKING:
    from google.cloud.datacatalog_lineage_v1 import LineageAsyncClient, LineageClient
    from openlineage.client.client import Event

log = logging.getLogger(__name__)

T = TypeVar("T")
DEFAULT_LOCATION = "us-central1"
GCPLineageMode = Literal["sync", "async"]


@attr.define
class GCPLineageConfig(Config):
    """Configuration for GCP Data Catalog Lineage transport.

    Args:
        project_id: GCP project ID where the lineage data will be stored
        location: GCP location (region) for the lineage service (default: "us-central1")
        credentials_path: Path to service account JSON credentials file (optional)
        endpoint: Data Lineage API endpoint override (optional)
        mode: Force all events through the sync or async client. If omitted, the
            existing async_transport_rules routing is used.
        timeout: Per-request timeout in seconds (optional)
        retry: Arguments passed to google.api_core.retry.Retry/AsyncRetry (optional)
        async_transport_rules: Controls async vs sync transport based on integration and jobType.
            Format: {"integration": {"jobType": True/False}}. Use "*" wildcards. True=async, False=sync.
            Default: {"dbt": {"*": True}}

    The transport will use default credentials if credentials_path is not provided.
    """

    project_id: str = attr.field()
    location: str = attr.field(default=DEFAULT_LOCATION)
    credentials_path: str | None = attr.field(default=None)
    endpoint: str | None = attr.field(default=None)
    mode: GCPLineageMode | None = attr.field(default=None)
    timeout: float | None = attr.field(default=None)
    retry: dict[str, Any] | None = attr.field(default=None)
    async_transport_rules: dict[str, dict[str, bool]] = attr.field(factory=lambda: {"dbt": {"*": True}})

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> GCPLineageConfig:
        if "project_id" not in params:
            msg = "project_id is required for GCPLineageTransport. Provide it in config"
            raise ValueError(msg)

        specified_dict = get_only_specified_fields(cls, params)
        mode = specified_dict.get("mode")
        if mode is not None:
            mode = str(mode).lower()
            if mode not in ("sync", "async"):
                msg = "mode must be either 'sync' or 'async' for GCPLineageTransport"
                raise ValueError(msg)
            specified_dict["mode"] = mode
        return cls(**specified_dict)


class GCPLineageTransport(Transport):
    kind = "gcplineage"
    config_class = GCPLineageConfig

    def __init__(self, config: GCPLineageConfig) -> None:
        self.config = config
        self.parent = f"projects/{self.config.project_id}/locations/{self.config.location}"

    @cached_property
    def client(self) -> LineageClient:
        """Lazy initialization of sync LineageClient."""
        try:
            from google.cloud.datacatalog_lineage_v1 import LineageClient
            from google.oauth2 import service_account

            kwargs: dict[str, Any] = {}
            if self.config.credentials_path:
                credentials_factory: Any = service_account.Credentials.from_service_account_file
                credentials = credentials_factory(self.config.credentials_path)
                kwargs["credentials"] = credentials
            if self.config.endpoint:
                kwargs["client_options"] = {"api_endpoint": self.config.endpoint}
            return LineageClient(**kwargs)

        except ModuleNotFoundError:
            log.exception(
                "OpenLineage client did not find google-cloud-datacatalog-lineage module. "
                "Installing it is required for GCPLineage to work. "
                "You can also get it via `pip install google-cloud-datacatalog-lineage`",
            )
            raise

    @cached_property
    def async_client(self) -> LineageAsyncClient:
        """Lazy initialization of async LineageAsyncClient."""
        try:
            from google.cloud.datacatalog_lineage_v1 import LineageAsyncClient
            from google.oauth2 import service_account

            kwargs: dict[str, Any] = {}
            if self.config.credentials_path:
                credentials_factory: Any = service_account.Credentials.from_service_account_file
                credentials = credentials_factory(self.config.credentials_path)
                kwargs["credentials"] = credentials
            if self.config.endpoint:
                kwargs["client_options"] = {"api_endpoint": self.config.endpoint}
            return LineageAsyncClient(**kwargs)

        except ModuleNotFoundError:
            log.exception(
                "OpenLineage client did not find google-cloud-datacatalog-lineage module. "
                "Installing it is required for GCPLineage to work. "
                "You can also get it via `pip install google-cloud-datacatalog-lineage`",
            )
            raise

    def _run_async_safely(self, coro: Coroutine[Any, Any, T]) -> T:
        """Safely run async coroutine in sync context with robust event loop handling."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Event loop is running, use thread pool
                import concurrent.futures

                def _run_coro() -> T:
                    return asyncio.run(coro)

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    return executor.submit(_run_coro).result()
            else:
                # Event loop exists but not running
                return loop.run_until_complete(coro)
        except RuntimeError:
            # No event loop exists, create one
            return asyncio.run(coro)

    def emit(self, event: Event) -> Any:
        if not isinstance(event, (RunEvent, event_v2.RunEvent)):
            log.warning("GCP Lineage only supports RunEvent")
            return

        if self.config.mode is not None:
            should_use_async = self.config.mode == "async"
        else:
            should_use_async = self._should_use_async_transport(event)

        if should_use_async:
            return self._run_async_safely(self._emit_async(event))
        else:
            return self._emit_sync(event)

    def _emit_sync(self, event: Event) -> Any:
        try:
            event_dict = Serde.to_dict(event)
            self.client.process_open_lineage_run_event(
                parent=self.parent,
                open_lineage=event_dict,
                **self._request_options(async_client=False),
            )

        except Exception as e:
            log.error("Failed to send lineage event to GCP (sync): %s", e)
            raise

    async def _emit_async(self, event: Event) -> Any:
        try:
            event_dict = Serde.to_dict(event)

            await self.async_client.process_open_lineage_run_event(
                parent=self.parent,
                open_lineage=event_dict,
                **self._request_options(async_client=True),
            )

        except Exception as e:
            log.error("Failed to send lineage event to GCP (async): %s", e)
            raise

    def _request_options(self, *, async_client: bool) -> dict[str, Any]:
        options: dict[str, Any] = {}
        if self.config.timeout is not None:
            options["timeout"] = self.config.timeout
        if self.config.retry is not None:
            retry_module = import_module("google.api_core.retry")
            retry_class = retry_module.AsyncRetry if async_client else retry_module.Retry
            options["retry"] = retry_class(**self.config.retry)
        return options

    def _should_use_async_transport(self, event: Event) -> bool:
        """
        Determine if event should use async transport based on async_transport_rules.
        Returns True if the event matches the configured rules for async transport.
        """

        wildcard_rules = self.config.async_transport_rules.get("*", {})
        if wildcard_rules.get("*"):
            return True

        if not hasattr(event, "job") or not hasattr(event.job, "facets"):
            return False

        job_facets = event.job.facets
        if not job_facets:
            return False

        job_type_facet = job_facets.get("jobType")
        if not job_type_facet:
            return False

        integration = getattr(job_type_facet, "integration", "").lower()
        job_type = getattr(job_type_facet, "jobType", "").lower()

        if not integration:
            return False

        for rule_integration, job_type_rules in self.config.async_transport_rules.items():
            if rule_integration == "*" or rule_integration.lower() == integration:
                for rule_job_type, use_async in job_type_rules.items():
                    if rule_job_type == "*" or rule_job_type.lower() == job_type:
                        return use_async
        return False

    def close(self, timeout: float = -1) -> bool:
        sync_result = True
        async_result = True

        sync_client = self.__dict__.get("client")
        if sync_client is not None:
            try:
                sync_client.transport.close()
            except Exception as e:
                log.warning("Error closing GCP lineage client (sync): %s", e)
                sync_result = False
        # Close async client
        async_client = self.__dict__.get("async_client")
        if async_client is not None:

            async def close_async() -> None:
                await async_client.transport.close()

            try:
                self._run_async_safely(close_async())
            except Exception as e:
                log.warning("Error closing GCP lineage client (async): %s", e)
                async_result = False

        return sync_result and async_result
