# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Warning: this integration is experimental and in active development.

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
import datetime
import logging
import os
from typing import Optional

from adapter import TemporalOpenLineageAdapter
from openlineage.client.uuid import generate_static_uuid
from openlineage.client.run import RunState
from temporalio.worker import ActivityInboundInterceptor, ExecuteActivityInput, Interceptor, WorkerConfig
from temporalio import activity
from temporalio.plugin import SimplePlugin


JOB_NAMESPACE: str = os.environ.get("OPENLINEAGE_NAMESPACE", "default")

logger: logging.Logger = logging.getLogger(__name__)

class OpenLineagePlugin(SimplePlugin):
    def __init__(self, config: Optional[dict] = None) -> None:
        self._config = config
        super().__init__(
            name="OpenLineage-plugin",
            interceptors=[OpenLineageWorkerInterceptor()] 
        )
    
    @property
    def config(self) -> dict:
        return self._config


class OpenLineageWorkerInterceptor(Interceptor):
    def __init__(
        self, run_context: Callable[[], AbstractAsyncContextManager[None]] | None = None
    ):
        self.run_context = run_context
        self.config = self.configure_worker(WorkerConfig())

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        config["plugins"] = [self]
        return config
    
    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return TemporalOpenLineageInboundListener(next)

    
class TemporalOpenLineageInboundListener(ActivityInboundInterceptor):
    def __init__(self, next: ActivityInboundInterceptor):
        super().__init__(
            next=next
        )
        self._ol_adapter=TemporalOpenLineageAdapter()

    def build_run_id(
        self, execution_time: datetime, run_name: str, namespace: str
    ) -> str:
        """
        Build a deterministic UUID for the OpenLineage run based on the execution
        time, run name, and namespace.
        """

        return str(
            generate_static_uuid(
                instant=execution_time,
                data=f"{namespace}.{run_name}".encode(),
            )
        )
    
    async def execute_activity(self, input: ExecuteActivityInput) -> any:
        try:
            info = activity.info()
        except Exception as e:
            logger.error(f"Failed to retrieve activity info: {e}")
            raise

        task_name = info.activity_type
        event_time = info.started_time
        event_run_id = self.build_run_id(event_time, task_name, JOB_NAMESPACE)

        # start_ns = datetime.datetime.now() # option instead of info.started_time
        try:
            result = await self.next.execute_activity(input)
        except Exception as e:
            self._ol_adapter.create_and_emit_task_event(
                event_run_id, RunState.FAILED, event_time, task_name
            )
            raise

        self._ol_adapter.create_and_emit_task_event(
            event_run_id, RunState.START, event_time, task_name
        )

        duration_ms = (datetime.datetime.now().astimezone() - event_time).total_seconds() * 1000
        event_time = event_time + datetime.timedelta(milliseconds=duration_ms)
        self._ol_adapter.create_and_emit_task_event(
            event_run_id, RunState.COMPLETE, event_time, task_name
        )
        return result
