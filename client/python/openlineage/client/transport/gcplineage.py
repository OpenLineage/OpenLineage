# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Optional, Union

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


@attr.define
class GCPLineageConfig(Config):
    """Configuration for GCP Data Catalog Lineage transport.

    Args:
        project_id: GCP project ID where the lineage data will be stored
        location: GCP location (region) for the lineage service (default: "us-central1")
        credentials_path: Path to service account JSON credentials file (optional)
        async_transport_rules: Controls async vs sync transport based on integration and jobType.
            Format: {"integration": {"jobType": True/False}}. Use "*" wildcards. True=async, False=sync.
            Default: {"dbt": {"*": True}}

    The transport will use default credentials if credentials_path is not provided.
    """

    project_id: str = attr.field()
    location: str = attr.field(default="us-central1")
    credentials_path: Union[str, None] = attr.field(default=None)
    async_transport_rules: dict[str, dict[str, bool]] = attr.field(default={"dbt": {"*": True}})

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> GCPLineageConfig:
        if "project_id" not in params:
            msg = "project_id is required for GCPLineageTransport. Provide it in config"
            raise ValueError(msg)

        specified_dict = get_only_specified_fields(cls, params)
        return cls(**specified_dict)


class GCPLineageTransport(Transport):
    kind = "gcplineage"
    config_class = GCPLineageConfig

    def __init__(self, config: GCPLineageConfig) -> None:
        self.config = config
        self.client: Optional[LineageClient] = None
        self.async_client: Optional[LineageAsyncClient] = None
        self.parent = f"projects/{self.config.project_id}/locations/{self.config.location}"

        self._setup_client()

    def _setup_client(self) -> None:
        try:
            from google.cloud.datacatalog_lineage_v1 import LineageAsyncClient, LineageClient
            from google.oauth2 import service_account

            if self.config.credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    self.config.credentials_path
                )
                self.client = LineageClient(credentials=credentials)
                self.async_client = LineageAsyncClient(credentials=credentials)
            else:
                self.client = LineageClient()
                self.async_client = LineageAsyncClient()

        except ModuleNotFoundError:
            log.exception(
                "OpenLineage client did not find google-cloud-datacatalog-lineage module. "
                "Installing it is required for GCPLineage to work. "
                "You can also get it via `pip install google-cloud-datacatalog-lineage`",
            )
            raise

    def emit(self, event: Event) -> Any:
        if not isinstance(event, (RunEvent, event_v2.RunEvent)):
            log.warning("GCP Lineage only supports RunEvent")
            return

        # Check if event should use async transport based on rules
        should_use_async = self._should_use_async_transport(event)

        if should_use_async:
            # Run the async function in the current thread's event loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # If loop is already running, create a task
                    import concurrent.futures

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(asyncio.run, self._emit_async(event))
                        return future.result()
                else:
                    return loop.run_until_complete(self._emit_async(event))
            except RuntimeError:
                # No event loop exists, create one
                return asyncio.run(self._emit_async(event))
        else:
            return self._emit_sync(event)

    def _emit_sync(self, event: Event) -> Any:
        try:
            event_dict = Serde.to_dict(event)

            if self.client is None:
                raise RuntimeError("GCP Lineage client not initialized")
            self.client.process_open_lineage_run_event(parent=self.parent, open_lineage=event_dict)

        except Exception as e:
            log.error("Failed to send lineage event to GCP (sync): %s", e)
            raise

    async def _emit_async(self, event: Event) -> Any:
        try:
            import json

            event_dict = json.loads(Serde.to_json(event))

            if self.async_client is None:
                raise RuntimeError("GCP Lineage async client not initialized")
            await self.async_client.process_open_lineage_run_event(
                parent=self.parent, open_lineage=event_dict
            )

        except Exception as e:
            log.error("Failed to send lineage event to GCP (async): %s", e)
            raise

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

        if self.client:
            try:
                self.client.transport.close()
            except Exception as e:
                log.warning("Error closing GCP lineage client (sync): %s", e)
                sync_result = False
            finally:
                self.client = None

        if self.async_client:
            try:
                # Async client cleanup - run async close properly
                async def close_async() -> None:
                    if self.async_client is not None:
                        close_result = self.async_client.transport.close()
                        if hasattr(close_result, "__await__"):
                            await close_result
                        else:
                            # Fallback - just close the client
                            await self.async_client.close()

                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        import concurrent.futures

                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future = executor.submit(asyncio.run, close_async())
                            future.result()
                    else:
                        loop.run_until_complete(close_async())
                except RuntimeError:
                    asyncio.run(close_async())
            except Exception as e:
                log.warning("Error closing GCP lineage client (async): %s", e)
                async_result = False
            finally:
                self.async_client = None

        return sync_result and async_result
