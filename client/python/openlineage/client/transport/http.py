# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import gzip
import hashlib
import inspect
import logging
import queue
import threading
import time
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import attr
import httpx

if TYPE_CHECKING:
    from openlineage.client.client import Event, OpenLineageClientOptions

import http.client as http_client

from openlineage.client.event_v2 import RunEvent as RunEventV2
from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields, try_import_from_string

log = logging.getLogger(__name__)


class TokenProvider:
    def __init__(self, config: dict[str, str]) -> None:
        ...

    def get_bearer(self) -> str | None:
        return None


class HttpCompression(Enum):
    GZIP = "gzip"

    def __str__(self) -> str:
        return self.value


@attr.s
class AsyncConfig:
    enabled: bool = attr.ib(default=True)
    max_queue_size: int = attr.ib(default=1000000)
    max_concurrent_requests: int = attr.ib(default=100)


class ApiKeyTokenProvider(TokenProvider):
    def __init__(self, config: dict[str, str]) -> None:
        super().__init__(config)
        try:
            self.api_key = config["api_key"]
            msg = "'api_key' option is deprecated, please use 'apiKey'"
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        except KeyError:
            self.api_key = config["apiKey"]

    def get_bearer(self) -> str | None:
        if self.api_key is None or self.api_key == "":
            return None
        return f"Bearer {self.api_key}"


def create_token_provider(auth: dict[str, str]) -> TokenProvider:
    if "type" in auth:
        if auth["type"] == "api_key" or auth["type"] == "apiKey":
            return ApiKeyTokenProvider(auth)

        of_type: str = auth["type"]
        subclass = try_import_from_string(of_type)
        if inspect.isclass(subclass) and issubclass(subclass, TokenProvider):
            return subclass(auth)

    return TokenProvider({})


"""
HTTP Transport for OpenLineage Events

This module provides both synchronous and asynchronous HTTP transport mechanisms for sending
OpenLineage events to remote endpoints. The key component is the AsyncEmitter which implements
a sophisticated event ordering and delivery system.

## AsyncEmitter Architecture

The AsyncEmitter is designed to handle high-throughput event emission with the following features:

### 1. Event Ordering Guarantee
- **START-before-completion ordering**: COMPLETE, FAIL, and ABORT events are held until their
  corresponding START event has been successfully sent
- **Pending event queue**: Completion events are stored in memory until their START event succeeds
- **Automatic release**: When a START event succeeds, all pending completion events for that run
  are immediately queued for processing

### 2. Concurrent Processing
- **Bounded parallelism**: Configurable maximum concurrent HTTP requests (default: 50)
- **Semaphore-controlled**: Uses asyncio.Semaphore to limit concurrent operations
- **Task-based architecture**: Each event is processed as an independent asyncio task
- **Non-blocking queue**: Events are queued without blocking the calling thread

### 3. Event Tracking and Statistics
- **Comprehensive tracking**: Every event is tracked through its lifecycle (pending → success/failed)
- **Run-based organization**: Tracks which runs have successful START events
- **Real-time statistics**: Provides counts of pending, successful, and failed events
- **Memory management**: Periodic cleanup of completed run tracking data

### 4. Reliability Features
- **Bounded queue**: Prevents memory exhaustion with configurable queue size (default: 1000)
- **Retry logic**: Configurable retry attempts with exponential backoff
- **Error categorization**: Distinguishes between retryable and permanent failures
- **Graceful degradation**: Continues processing other events even if some fail

### 5. Shutdown Behavior
- **Graceful shutdown**: Processes remaining events before terminating
- **Timeout enforcement**: Respects shutdown timeouts to prevent hanging
- **Event accounting**: Ensures no events are silently dropped during shutdown
- **Resource cleanup**: Properly closes asyncio loops and HTTP connections

## Event Flow

1. **Event Submission**: Events are submitted via `emit(event)` method
2. **Event Classification**: RunEvents are classified as START or completion events
3. **Ordering Check**: Completion events check if their START event has succeeded
4. **Queue Management**:
   - START events → immediate queue for processing
   - Completion events with successful START → immediate queue
   - Completion events without successful START → pending queue
5. **Concurrent Processing**: Worker thread processes events using asyncio task pool
6. **Event Release**: Successful START events trigger release of pending completion events
7. **Status Tracking**: All events tracked through completion with statistics available

## Configuration

Key configuration parameters:
- `max_queue_size`: Maximum events in processing queue (default: 1000)
- `max_concurrent_requests`: Maximum parallel HTTP requests (default: 50)
- `timeout`: HTTP request timeout
- `retry`: Retry configuration (attempts, backoff, status codes)

## Error Handling

- **HTTP errors**: Retried based on status code and retry configuration
- **Network errors**: Retried with exponential backoff
- **Queue overflow**: Events marked as failed if queue is full
- **Shutdown timeout**: Remaining events marked as failed during forced shutdown

## Thread Safety

- **Lock-protected state**: Event tracking uses threading.Lock for thread safety
- **Queue thread safety**: Uses thread-safe queue.Queue for event passing
- **Worker isolation**: Async processing isolated in dedicated worker thread

Example usage:
```python
config = HttpConfig(
    url="https://example.com",
    async_config=AsyncConfig(enabled=True, max_concurrent_requests=20)
)
transport = HttpTransport(config)

# Events are processed asynchronously with ordering guarantees
transport.emit(start_event)  # Sent immediately
transport.emit(complete_event)  # Waits for START success, then sent

# Wait for all events to complete
transport.wait_for_completion(timeout=30)

# Get processing statistics
stats = transport.get_stats()

# Graceful shutdown
transport.shutdown(wait=True, timeout=30)
```
"""


class EventStatus:
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


class EventTracker:
    def __init__(self):
        self.events: dict[str, str] = {}
        self.lock = threading.Lock()
        self.event_emitted = threading.Event()
        # Track which runs have successful START events
        self.successful_start_events: set[str] = set()
        # Queue for completion events waiting for their START event
        self.pending_completion_events: dict[str, list] = {}  # run_id -> list of (event_id, body, headers)

    def add_event(self, event_id: str):
        with self.lock:
            self.events[event_id] = EventStatus.PENDING

    def mark_success(self, event_id: str, run_id: str | None = None, event_type: str | None = None):
        with self.lock:
            self.events[event_id] = EventStatus.SUCCESS
            self.event_emitted.set()

            # If this was a START event, mark it as successful and release any pending completion events
            if run_id and event_type == "START":
                self.successful_start_events.add(run_id)
                # Return any pending completion events for this run
                return self.pending_completion_events.pop(run_id, [])
        return []

    def mark_failed(self, event_id: str):
        with self.lock:
            self.events[event_id] = EventStatus.FAILED
            self.event_emitted.set()

    def add_pending_completion_event(
        self, run_id: str, event_id: str, body: bytes | str, headers: dict[str, str]
    ):
        with self.lock:
            if run_id not in self.pending_completion_events:
                self.pending_completion_events[run_id] = []
            self.pending_completion_events[run_id].append((event_id, body, headers))

    def is_start_successful(self, run_id: str) -> bool:
        with self.lock:
            return run_id in self.successful_start_events

    def all_processed(self) -> bool:
        with self.lock:
            return all(status != EventStatus.PENDING for status in self.events.values())

    def get_stats(self) -> dict[str, int]:
        with self.lock:
            stats = {
                "pending": 0,
                "success": 0,
                "failed": 0,
                "pending_completion_events": sum(
                    len(events) for events in self.pending_completion_events.values()
                ),
                "successful_starts": len(self.successful_start_events),
            }
            for status in self.events.values():
                stats[status] += 1
            return stats


class Emitter:
    def emit(self, _: Event) -> None:
        msg = "Subclasses must implement this method"
        raise NotImplementedError(msg)

    def wait_for_completion(self, _: float = 30.0) -> bool:
        return True

    def get_stats(self) -> dict[str, int]:
        return {}

    def shutdown(self, _: bool = True, timeout: float = 30.0) -> bool:
        return self.wait_for_completion(timeout)

    def _prepare_request(self, event: str) -> tuple[bytes | str, dict[str, str]]:
        headers = {
            "Content-Type": "application/json",
            **self._auth_headers(self.config.auth),
            **self.config.custom_headers,
        }
        if self.config.compression == HttpCompression.GZIP:
            headers["Content-Encoding"] = "gzip"
            return gzip.compress(event.encode("utf-8")), headers

        return event, headers

    def _auth_headers(self, token_provider: TokenProvider) -> dict:  # type: ignore[type-arg]
        bearer = token_provider.get_bearer()
        if bearer:
            return {"Authorization": bearer}
        return {}


class AsyncEmitter(Emitter):
    def __init__(self, config: HttpConfig) -> None:
        self.config = config
        # Create a bounded queue with a buffer (default 1000 events)
        self.max_queue_size = config.async_config.max_queue_size
        self.event_queue = queue.Queue(maxsize=self.max_queue_size)
        self.event_tracker = EventTracker()
        self.should_exit = threading.Event()
        # Maximum number of concurrent requests
        self.max_concurrent = config.async_config.max_concurrent_requests
        # Create a stopping event that will be set when we're shutting down
        self.stopping = False
        self.shutdown_event = threading.Event()
        self.worker_thread = threading.Thread(target=self._setup_loop, daemon=True)
        self.worker_thread.start()
        log.debug(
            "AsyncEmitter for %s started with max queue size: %s, max concurrent: %s",
            self.config.url,
            self.max_queue_size,
            self.max_concurrent,
        )

    def _setup_loop(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._event_loop_worker())
        except Exception:
            log.exception("Exception in async event loop")
        finally:
            self.loop.close()
            log.debug("AsyncEmitter event loop closed")

    async def _event_loop_worker(self) -> None:
        # Create a semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent)
        # Keep track of active tasks
        active_tasks: set[asyncio.Task] = set()

        # Httpx client for the worker
        limits = httpx.Limits(
            max_connections=self.max_concurrent, max_keepalive_connections=self.max_concurrent // 2
        )

        transport = httpx.AsyncHTTPTransport(limits=limits, retries=self.config.retry.get("total", 5))

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout=self.config.timeout),
            verify=self.config.verify,
            follow_redirects=True,
            transport=transport,
        ) as client:
            while not self.should_exit.is_set():
                processed_items = False
                while not self.event_queue.empty() and len(active_tasks) < self.max_concurrent:
                    try:
                        event_id, body, headers, run_id, event_type = self.event_queue.get_nowait()
                        task = asyncio.create_task(
                            self._process_event(
                                client,
                                semaphore,
                                event_id,
                                body,
                                headers,
                                run_id,
                                event_type,
                                from_main_queue=True,
                            ),
                            name=event_id,
                        )
                        active_tasks.add(task)
                        task.add_done_callback(lambda t: active_tasks.remove(t))
                        processed_items = True
                    except queue.Empty:
                        break

                if active_tasks:
                    done, _ = await asyncio.wait(
                        active_tasks, timeout=0.1, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in done:
                        try:
                            # Get any pending completion events that can now be processed
                            pending_events = await task
                            if pending_events:
                                # Add the released completion events back to the queue - we can
                                # process them now
                                for event_id, body, headers in pending_events:
                                    if len(active_tasks) < self.max_concurrent:
                                        release_task = asyncio.create_task(
                                            self._process_event(
                                                client,
                                                semaphore,
                                                event_id,
                                                body,
                                                headers,
                                                None,
                                                None,
                                                from_main_queue=False,
                                            ),
                                            name=event_id,
                                        )
                                        active_tasks.add(release_task)
                                        release_task.add_done_callback(lambda t: active_tasks.remove(t))
                                    else:
                                        # Put back in queue if we're at capacity - these will be processed as
                                        # main queue items
                                        self.event_queue.put((event_id, body, headers, None, None))
                        except Exception:
                            log.exception("Error in event processing task")
                # No tasks are active, didn't process any new items - sleep
                elif not processed_items:
                    await asyncio.sleep(0.01)

            log.debug("Shutdown requested, processing remaining events...")
            while not self.event_queue.empty() or active_tasks:
                # Process any remaining queue items
                while not self.event_queue.empty() and len(active_tasks) < self.max_concurrent:
                    try:
                        event_id, body, headers, run_id, event_type = self.event_queue.get_nowait()
                        task = asyncio.create_task(
                            self._process_event(
                                client,
                                semaphore,
                                event_id,
                                body,
                                headers,
                                run_id,
                                event_type,
                                from_main_queue=True,
                            )
                        )
                        active_tasks.add(task)
                        task.add_done_callback(lambda t: active_tasks.remove(t))
                    except queue.Empty:
                        break

                # Wait for active tasks to complete
                if active_tasks:
                    done, active_tasks = await asyncio.wait(active_tasks, timeout=0.5)
                    for task in done:
                        try:
                            await task
                        except Exception:
                            log.exception("Error in shutdown event processing")
                else:
                    break

            log.debug("AsyncEmitter event loop worker completed")

    async def _process_event(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        event_id: str,
        body: bytes | str,
        headers: dict[str, str],
        run_id: str | None = None,
        event_type: str | None = None,
        from_main_queue: bool = True,
    ) -> list:
        """Process a single event and return any pending completion events if this was a successful START"""
        pending_events = []

        async with semaphore:  # Limit concurrent requests
            for attempt in range(self.config.retry.get("total", 5) + 1):
                try:
                    resp = await client.post(
                        url=urljoin(self.config.url, self.config.endpoint),
                        content=body,
                        headers=headers,
                    )

                    if resp.status_code >= 200 and resp.status_code < 300:
                        pending_events = self.event_tracker.mark_success(event_id, run_id, event_type)
                        log.debug(
                            "Successfully emitted event (event_id) %s to %s. Released %s "
                            "pending completion events.",
                            event_id,
                            self.config.url,
                            len(pending_events),
                        )
                        break

                    if resp.status_code not in self.config.retry.get(
                        "status_forcelist", [500, 502, 503, 504]
                    ):
                        self.event_tracker.mark_failed(event_id)
                        log.error(
                            "Failed to emit event (event_id) %s to %s, status code: %s",
                            event_id,
                            self.config.url,
                            resp.status_code,
                        )
                        log.debug("Response body: %s", resp.text)
                        break

                    await asyncio.sleep(self.config.retry.get("backoff_factor", 0.3) * (2**attempt))

                except Exception as e:
                    if attempt == self.config.retry.get("total", 5):
                        self.event_tracker.mark_failed(event_id)
                        log.exception(
                            "Failed to emit event (event_id) %s to %s after %s attempts: %s",
                            event_id,
                            self.config.url,
                            attempt + 1,
                            e,
                        )
                        break

                    log.warning(
                        "Attempt %s failed for event (event_id) %s to %s: %s",
                        attempt + 1,
                        event_id,
                        self.config.url,
                        e,
                    )
                    await asyncio.sleep(self.config.retry.get("backoff_factor", 0.3) * (2**attempt))

            # Only mark task as done in the queue if this event came from the main queue
            if from_main_queue:
                self.event_queue.task_done()

        return pending_events

    def emit(self, event: Event) -> None:
        event_str = Serde.to_json(event)

        run_id = None
        event_type = None

        if isinstance(event, (RunEvent, RunEventV2)):
            event_id = event.run.runId + "-" + str(event.eventType.value)
            run_id = event.run.runId
            event_type = str(event.eventType.value) if hasattr(event, "eventType") else None

            # We don't want to emit terminal events until we have a successful START event
            # This does not 100% prevents race conditions
            if event_type in ["COMPLETE", "FAIL", "ABORT"]:
                if not self.event_tracker.is_start_successful(run_id):
                    body, headers = self._prepare_request(event_str)
                    self.event_tracker.add_event(event_id)
                    self.event_tracker.add_pending_completion_event(run_id, event_id, body, headers)

                    log.debug(
                        "Queued completion event %s for run %s, waiting for START event",
                        event_type,
                        run_id,
                    )
                    return
        else:
            event_id = hashlib.md5(event_str.encode("utf-8")).hexdigest()

        body, headers = self._prepare_request(event_str)

        self.event_tracker.add_event(event_id)

        # Add to queue - include run_id and event_type for START event tracking
        self.event_queue.put((event_id, body, headers, run_id, event_type))
        log.debug(f"Queued event with event_id {event_id} for async emission to {self.config.url}")

    def wait_for_completion(self, timeout: float = 30.0) -> bool:
        # Block until all events are processed or timeout is reached.
        log.debug("Waiting for events to be sent in wait_for_completion.")
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.event_tracker.all_processed():
                log.debug("All events were processed.")
                return True

            # Wait for notification or timeout
            self.event_tracker.event_emitted.wait(timeout=0.5)
            self.event_tracker.event_emitted.clear()

        return self.event_tracker.all_processed()

    def get_stats(self) -> dict[str, int]:
        return self.event_tracker.get_stats()

    def shutdown(self, wait: bool = True, timeout: float = 30.0) -> bool:
        self.should_exit.set()

        if wait:
            result = self.wait_for_completion(timeout)
            if self.worker_thread.is_alive():
                self.worker_thread.join(timeout=1.0)
            return result

        return True


class SyncEmitter(Emitter):
    def __init__(self, config: HttpConfig) -> None:
        self.config = config
        self.url = config.url
        self.endpoint = config.endpoint
        self.timeout = config.timeout
        self.verify = config.verify
        self.compression = config.compression

    def emit(self, event: Event) -> httpx.Response:
        # If anyone overrides debuglevel manually, we can potentially leak secrets to logs.
        # Override this setting to make sure it does not happen.
        prev_debuglevel = http_client.HTTPConnection.debuglevel
        http_client.HTTPConnection.debuglevel = 0

        event_str = Serde.to_json(event)

        body, headers = self._prepare_request(event_str)

        transport = httpx.HTTPTransport(retries=self.config.retry.get("total", 5))

        with httpx.Client(
            timeout=self.config.timeout, verify=self.config.verify, follow_redirects=True, transport=transport
        ) as client:
            resp = client.post(
                url=urljoin(self.url, self.endpoint),
                content=body,
                headers=headers,
            )

        http_client.HTTPConnection.debuglevel = prev_debuglevel
        resp.raise_for_status()


@attr.s
class HttpConfig(Config):
    url: str = attr.ib()
    endpoint: str = attr.ib(default="api/v1/lineage")
    timeout: float = attr.ib(default=5.0)
    # check TLS certificates
    verify: bool = attr.ib(default=True)
    auth: TokenProvider = attr.ib(factory=lambda: TokenProvider({}))
    compression: HttpCompression | None = attr.ib(default=None)
    custom_headers: dict[str, str] = attr.ib(factory=dict)
    async_config: AsyncConfig = attr.ib(factory=lambda: AsyncConfig())
    retry: dict[str, Any] = attr.ib(
        default={
            "total": 5,
            "read": 5,
            "connect": 5,
            "backoff_factor": 0.3,
            "status_forcelist": [500, 502, 503, 504],
            "allowed_methods": ["HEAD", "POST"],
        }
    )

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> HttpConfig:
        if "url" not in params:
            msg = "`url` key not passed to HttpConfig"
            raise RuntimeError(msg)
        specified_dict = get_only_specified_fields(cls, params)
        specified_dict["auth"] = create_token_provider(specified_dict.get("auth", {}))
        compression = specified_dict.get("compression")
        if compression:
            specified_dict["compression"] = HttpCompression(compression)
        async_config = specified_dict.get("async_config", {})
        if async_config.get("enabled", False):
            specified_dict["async_config"] = AsyncConfig(**async_config)
        else:
            specified_dict["async_config"] = AsyncConfig()
        return cls(**specified_dict)

    @classmethod
    def from_options(
        cls,
        url: str,
        options: OpenLineageClientOptions,
        session: Any,  # for backwards compatibility
    ) -> HttpConfig:
        return cls(
            url=url,
            timeout=options.timeout,
            verify=options.verify,
            auth=ApiKeyTokenProvider({"api_key": options.api_key}) if options.api_key else TokenProvider({}),
        )


class HttpTransport(Transport):
    kind = "http"
    config_class = HttpConfig

    def __init__(self, config: HttpConfig) -> None:
        url = config.url.strip()
        self.config = config

        log.debug(
            "Constructing OpenLineage transport that will send events "
            "to HTTP endpoint `%s` using the following config: %s",
            urljoin(url, config.endpoint),
            config,
        )
        try:
            parsed = httpx.URL(url)
            if not (parsed.scheme and parsed.host):
                msg = f"Need valid url for OpenLineageClient, passed {url}"
                raise ValueError(msg)
        except Exception as e:
            msg = f"Need valid url for OpenLineageClient, passed {url}. Exception: {e}"
            raise ValueError(msg) from None

        self.url = url
        self.endpoint = config.endpoint
        self.timeout = config.timeout
        self.verify = config.verify
        self.compression = config.compression

        self.async_emitter = AsyncEmitter(config)
        log.debug("Using async emitter for HTTP transport for %s", self.config.url)
        self.sync_emitter = SyncEmitter(config)
        log.debug("Using sync emitter for HTTP transport for %s", self.config.url)

    def emit(self, event: Event) -> httpx.Response | None:
        try:
            if jobtype := event.job.facets.get("jobType"):
                if jobtype.integration == "AIRFLOW":
                    return self.sync_emitter.emit(event)
        except Exception as e:
            log.debug("Failed to emit event synchronous:", e)

        return self.async_emitter.emit(event)

    def wait_for_completion(self, timeout: float = 10.0) -> bool:
        return self.async_emitter.wait_for_completion(timeout)

    def get_stats(self) -> dict[str, int]:
        return self.async_emitter.get_stats()

    def shutdown(self, wait: bool = True, timeout: float = 30.0) -> bool:
        self.async_emitter.shutdown(wait, timeout)
