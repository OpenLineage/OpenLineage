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
    enabled: bool = attr.ib(default=False)
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
OpenLineage events to remote endpoints using httpx. The AsyncEmitter implements a sophisticated
event ordering and delivery system with race-condition-free task management.

## Transport Modes

### SyncEmitter
- **Simple synchronous transport**: Blocks until each event is sent
- **Direct httpx.Client usage**: Uses httpx with built-in retry support
- **Immediate error handling**: Raises exceptions on failure via raise_for_status()
- **Suitable for**: Low-throughput scenarios and simple integrations

### AsyncEmitter
- **High-throughput asynchronous transport**: Non-blocking event emission
- **Event ordering guarantees**: Ensures START events are sent before terminal events
- **Concurrent processing**: Configurable parallel HTTP requests
- **Advanced error handling**: Retry logic with exponential backoff

## AsyncEmitter Architecture

### 1. Event Ordering Guarantee
- **Conditional START-before-completion ordering**: COMPLETE, FAIL, and ABORT events are held
  only if their corresponding START event is currently in progress (pending submission)
- **No START event**: If no START event was ever submitted, completion events are processed immediately
- **Pending event queue**: Completion events are stored in memory only while their START event is pending
- **Automatic release**: When a START event succeeds, all pending completion events for that run
  are immediately queued for processing

### 2. Concurrent Processing
- **Bounded parallelism**: Configurable maximum concurrent HTTP requests (default: 100)
- **Semaphore-controlled**: Uses asyncio.Semaphore to limit concurrent operations
- **Task-based architecture**: Each event is processed as an independent asyncio task
- **Race-condition-free**: Immediate task removal prevents double-processing of completed tasks
- **Reserved queue capacity**: 2x configured queue size to accommodate released completion events

### 3. Event Tracking and Statistics
- **Simplified tracking**: Uses direct counters for pending, success, and failed events
- **Real-time statistics**: Thread-safe access to event processing status
- **Memory efficient**: Tracks only essential state without complex run metadata

### 4. Reliability Features
- **Bounded queue**: Prevents memory exhaustion with configurable queue size (default: 1,000,000)
- **Retry logic**: Configurable retry attempts (default: 5) with exponential backoff
- **Error categorization**: Distinguishes between retryable (5xx) and permanent failures (4xx)
- **Graceful degradation**: Continues processing other events even if some fail

### 5. Task Management (Race-Condition-Free)
- **Immediate task removal**: `active_tasks -= done` after `asyncio.wait()`
- **No async callbacks**: Removed `task.add_done_callback()` to prevent race conditions
- **Clean shutdown**: Simplified shutdown without complex task cleanup

## Event Flow

1. **Event Submission**: Events are submitted via `emit(event)` method
2. **Event Classification**: RunEvents are classified as START or completion events
3. **Ordering Check**: Completion events check if their START event is currently in progress
4. **Queue Management**:
   - START events → immediate queue for processing
   - Completion events with no START ever submitted → immediate queue
   - Completion events with START in progress → pending queue until START succeeds
   - Completion events with START already completed → immediate queue
5. **Concurrent Processing**: Worker thread processes events using asyncio task pool
6. **Event Release**: Successful START events trigger release of pending completion events
7. **Status Tracking**: Events tracked with simple counters (pending/success/failed)

## Configuration

Key configuration parameters:
- `max_queue_size`: Maximum events in processing queue (default: 1,000,000)
- `max_concurrent_requests`: Maximum parallel HTTP requests (default: 100)
- `timeout`: HTTP request timeout (default: 5.0 seconds)
- `retry`: Retry configuration with total attempts, backoff factor, and retryable status codes

## Error Handling

- **Structured logging**: Consistent error messages with event context
- **HTTP errors**: Retried based on status code (5xx retried, 4xx not retried)
- **Network errors**: Retried with exponential backoff
- **Response body logging**: Safe response text reading with fallback
- **Queue capacity**: Events wait for space rather than being dropped

## Thread Safety

- **Lock-protected state**: Event tracking uses threading.Lock for thread safety
- **Queue thread safety**: Uses thread-safe queue.Queue for event passing
- **Worker isolation**: Async processing isolated in dedicated worker thread

## Backward Compatibility

- **Deprecated parameters**: `session` and `adapter` parameters maintained with deprecation warnings
- **Migration from requests**: Moved from requests library to httpx for better async support
- **Configuration compatibility**: Existing HttpConfig remains compatible

Example usage:
```python
# Async transport (recommended for high throughput)
config = HttpConfig(
    url="https://example.com",
    async_config=AsyncConfig(enabled=True, max_concurrent_requests=50)
)
transport = HttpTransport(config)

# Events are processed asynchronously with ordering guarantees
transport.emit(start_event)      # Sent immediately
transport.emit(complete_event)   # Waits for START success, then sent

# Wait for all events to complete
transport.wait_for_completion(timeout=30)

# Get processing statistics
stats = transport.get_stats()    # {"pending": 0, "success": 10, "failed": 0}

# Graceful shutdown
transport.shutdown(wait=True, timeout=30)

# Sync transport (for simple use cases)
config = HttpConfig(url="https://example.com")  # async disabled by default
transport = HttpTransport(config)
response = transport.emit(event)  # Blocks until sent, returns httpx.Response
```
"""


class EventStatus:
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


@attr.s
class Request:
    event_id: str = attr.ib()  # run_id + event type for RunEvent, md5 of body for others
    body: bytes | str = attr.ib()
    headers: dict[str, str] = attr.ib()
    run_id: str | None = attr.ib(default=None)
    event_type: str | None = attr.ib(default=None)


class EventTracker:
    def __init__(self):
        self.events: dict[str, str] = {}
        self.lock = threading.Lock()
        # Queue for completion events waiting for their START event
        self.pending_completion_events: dict[str, list[Request]] = {}  # run_id -> list of Request
        self.stats = {
            "pending": 0,
            "success": 0,
            "failed": 0,
        }

    def add_event(self, request: Request):
        with self.lock:
            self.stats["pending"] += 1
            self.events[request.event_id] = EventStatus.PENDING

    def mark_success(self, request: Request) -> list[Request]:
        with self.lock:
            self.stats["pending"] -= 1
            self.stats["success"] += 1
            if self.events.get(request.event_id) == EventStatus.PENDING:
                del self.events[request.event_id]

            # If this was a START event, mark it as successful and release any pending completion events
            if request.run_id and request.event_type == "START":
                # Return any pending completion events for this run
                return self.pending_completion_events.pop(request.run_id, [])
        return []

    def mark_failed(self, request: Request):
        with self.lock:
            log.debug("Marking event %s as failed", request.event_id)
            self.stats["pending"] -= 1
            self.stats["failed"] += 1
            if self.events.get(request.event_id) == EventStatus.PENDING:
                del self.events[request.event_id]

    def add_pending_completion_event(self, request: Request):
        if not request.run_id:
            return
        with self.lock:
            if request.run_id not in self.pending_completion_events:
                self.pending_completion_events[request.run_id] = []
            self.pending_completion_events[request.run_id].append(request)

    def is_start_in_progress(self, run_id: str) -> bool:
        with self.lock:
            start_event_id = f"{run_id}-START"
            return start_event_id in self.events

    def all_processed(self) -> bool:
        with self.lock:
            return len(self.events) == 0

    def get_stats(self) -> dict[str, int]:
        return self.stats


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
        # Create a queue with 2x the configured size to reserve space for released completion events
        self.configured_queue_size = config.async_config.max_queue_size
        self.event_queue = queue.Queue(maxsize=self.configured_queue_size * 2)
        self.event_tracker = EventTracker()
        self.should_exit = threading.Event()
        # Maximum number of concurrent requests
        self.max_concurrent = config.async_config.max_concurrent_requests
        # Create a stopping event that will be set when we're shutting down
        self.stopping = False
        self.worker_thread = threading.Thread(target=self._setup_loop, daemon=True)
        self.worker_thread.start()
        log.debug(
            "AsyncEmitter for %s started with configured queue size: %s (actual: %s), max concurrent: %s",
            self.config.url,
            self.configured_queue_size,
            self.configured_queue_size * 2,
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
                        request = self.event_queue.get_nowait()
                        task = asyncio.create_task(
                            self._process_event(
                                client,
                                semaphore,
                                request,
                                from_main_queue=True,
                            ),
                            name=request.event_id,
                        )
                        active_tasks.add(task)
                        processed_items = True
                    except queue.Empty:
                        break

                if active_tasks:
                    done, _ = await asyncio.wait(
                        active_tasks, timeout=0.1, return_when=asyncio.FIRST_COMPLETED
                    )
                    # Remove completed tasks immediately to prevent double-processing
                    active_tasks -= done
                    for task in done:
                        try:
                            # Get any pending requests that can now be processed
                            pending_requests = await task
                            if pending_requests:
                                # Add the released requests back to the queue - we can
                                # process them now
                                for request in pending_requests:
                                    if len(active_tasks) < self.max_concurrent:
                                        release_task = asyncio.create_task(
                                            self._process_event(
                                                client,
                                                semaphore,
                                                request,
                                                from_main_queue=False,
                                            ),
                                            name=request.event_id,
                                        )
                                        active_tasks.add(release_task)
                                    else:
                                        # Put released completion events directly into the reserved queue
                                        # space (we have 2x capacity, so these should always fit)
                                        self.event_queue.put(request)
                        except Exception:
                            log.exception("Error in event processing task")
                # No tasks are active, didn't process any new items - sleep
                elif not processed_items:
                    await asyncio.sleep(0.01)

            log.debug("AsyncEmitter event loop worker completed")

    async def _process_event(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        request: Request,
        from_main_queue: bool = True,
    ) -> list:
        """Process a single event and return any pending completion events if this was a successful START"""
        pending_events = []

        log.debug("Processing event %s", request.event_id)

        def handle_failure(response=None, exception=None):
            self.event_tracker.mark_failed(request)

            if response is not None:
                log.error(
                    "Failed to emit event (event_id) %s to %s, status code: %s",
                    request.event_id,
                    self.config.url,
                    response.status_code,
                )
                try:
                    log.debug("Response body: %s", response.text)
                except (AttributeError, ValueError, UnicodeDecodeError):
                    log.debug("Could not read response body")
            elif exception:
                log.exception(
                    "Failed to emit event (event_id) %s to %s due to exception",
                    request.event_id,
                    self.config.url,
                )
            else:
                log.error(
                    "Failed to emit event (event_id) %s to %s",
                    request.event_id,
                    self.config.url,
                )

        async with semaphore:  # Limit concurrent requests
            for attempt in range(self.config.retry.get("total", 5) + 1):
                if self.should_exit.is_set():
                    break
                try:
                    resp = await client.post(
                        url=urljoin(self.config.url, self.config.endpoint),
                        content=request.body,
                        headers=request.headers,
                    )

                    if 200 <= resp.status_code < 300:
                        pending_events = self.event_tracker.mark_success(request)
                        log.debug(
                            "Successfully emitted event (event_id) %s to %s. Released %s "
                            "pending completion events.",
                            request.event_id,
                            self.config.url,
                            len(pending_events),
                        )
                        break

                    if resp.status_code not in self.config.retry.get(
                        "status_forcelist", [500, 502, 503, 504]
                    ):
                        handle_failure(response=resp)
                        break
                    elif attempt == self.config.retry["total"]:
                        handle_failure(response=resp)
                        break

                    await asyncio.sleep(self.config.retry["backoff_factor"] * (2**attempt))

                except Exception as e:
                    if attempt == self.config.retry["total"]:
                        handle_failure(response=None, exception=e)
                        break

                    log.debug(
                        "Attempt %s failed for event (event_id) %s to %s: %s",
                        attempt,
                        request.event_id,
                        self.config.url,
                        e,
                    )
                    await asyncio.sleep(self.config.retry["backoff_factor"] * (2**attempt))

            # Only mark task as done in the queue if this event came from the main queue
            if from_main_queue:
                self.event_queue.task_done()

        return pending_events

    def emit(self, event: Event) -> None:
        event_str = Serde.to_json(event)

        run_id = None
        event_type = None

        if isinstance(event, (RunEvent, RunEventV2)):
            event_type = str(event.eventType.value)
            event_id = event.run.runId + "-" + event_type
            run_id = event.run.runId

            # We don't want to emit terminal events until we have a successful START event
            # Only queue as pending if START has been scheduled but not yet successful
            # If START was never scheduled, process COMPLETE normally (fall through)
            if event_type in ["COMPLETE", "FAIL", "ABORT"]:
                if self.event_tracker.is_start_in_progress(run_id):
                    body, headers = self._prepare_request(event_str)
                    request = Request(
                        event_id=event_id, run_id=run_id, event_type=event_type, body=body, headers=headers
                    )
                    self.event_tracker.add_event(request)
                    self.event_tracker.add_pending_completion_event(request)

                    log.debug(
                        "Queued completion event %s for run %s, waiting for START event",
                        event_type,
                        run_id,
                    )
                    return
        else:
            event_id = hashlib.md5(event_str.encode("utf-8")).hexdigest()

        body, headers = self._prepare_request(event_str)
        request = Request(event_id=event_id, run_id=run_id, event_type=event_type, body=body, headers=headers)
        self.event_tracker.add_event(request)

        # Wait for space in the regular capacity portion of the queue
        while self.event_queue.qsize() >= self.configured_queue_size:
            time.sleep(0.01)

        # Add to queue - include run_id and event_type for START event tracking
        self.event_queue.put(request)
        log.debug(f"Queued event with event_id {event_id} for async emission to {self.config.url}")

    def wait_for_completion(self, timeout: float = 30.0) -> bool:
        # Block until all events are processed or timeout is reached.
        log.debug("Waiting for events to be sent in wait_for_completion.")
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.event_tracker.all_processed():
                log.debug("All events were processed.")
                return True

            # Brief sleep before checking again
            time.sleep(0.01)

        return self.event_tracker.all_processed()

    def get_stats(self) -> dict[str, int]:
        return self.event_tracker.get_stats()

    def shutdown(self, wait: bool = True, timeout: float = 30.0) -> bool:
        if wait:
            result = self.wait_for_completion(timeout)
            self.should_exit.set()
            if self.worker_thread.is_alive():
                self.worker_thread.join(timeout=1.0)
            return result
        self.should_exit.set()
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
        try:
            http_client.HTTPConnection.debuglevel = 0

            event_str = Serde.to_json(event)
            body, headers = self._prepare_request(event_str)
            transport = httpx.HTTPTransport(retries=self.config.retry.get("total", 5))

            with httpx.Client(
                timeout=self.config.timeout,
                verify=self.config.verify,
                follow_redirects=True,
                transport=transport,
            ) as client:
                resp = client.post(
                    url=urljoin(self.url, self.endpoint),
                    content=body,
                    headers=headers,
                )
        finally:
            http_client.HTTPConnection.debuglevel = prev_debuglevel
        resp.raise_for_status()
        return resp


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
        if session is not None:
            warnings.warn(
                message="The 'session' parameter is deprecated and no longer used with httpx transport.",
                category=DeprecationWarning,
                stacklevel=3,
            )
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

        # Set up emitter based on config
        if config.async_config.enabled:
            self.emitter = AsyncEmitter(config)
            log.debug("Using async emitter for HTTP transport for %s", self.config.url)
        else:
            self.emitter = SyncEmitter(config)
            log.debug("Using sync emitter for HTTP transport for %s", self.config.url)

    def emit(self, event: Event) -> httpx.Response | None:
        return self.emitter.emit(event)

    def wait_for_completion(self, timeout: float = 10.0) -> bool:
        return self.emitter.wait_for_completion(timeout)

    def get_stats(self) -> dict[str, int]:
        return self.emitter.get_stats()

    def shutdown(self, wait: bool = True, timeout: float = 30.0) -> bool:
        return self.emitter.shutdown(wait, timeout)
