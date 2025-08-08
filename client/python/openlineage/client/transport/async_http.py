# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import gzip
import hashlib
import logging
import queue
import threading
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import attr
import httpx
from openlineage.client.event_v2 import RunEvent as RunEventV2
from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.http import HttpCompression, TokenProvider, create_token_provider
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields

if TYPE_CHECKING:
    from openlineage.client.client import Event


"""
Async HTTP Transport for OpenLineage Events

This module provides asynchronous HTTP transport mechanisms for sending
OpenLineage events to remote endpoints using httpx. The AsyncHttpTransport implements a sophisticated
event ordering and delivery system with race-condition-free task management.

## Transport Architecture

The AsyncHttpTransport provides:
- **High-throughput asynchronous transport**: Non-blocking event emission
- **Event ordering guarantees**: Ensures START events are sent before terminal events
- **Concurrent processing**: Configurable parallel HTTP requests
- **Advanced error handling**: Retry logic with exponential backoff

## AsyncHttpTransport Architecture

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

Example usage:
```python
# Async transport (recommended for high throughput)
config = AsyncHttpConfig(
    url="https://example.com",
    max_concurrent_requests=50,
    max_queue_size=500000
)
transport = AsyncHttpTransport(config)

# Events are processed asynchronously with ordering guarantees
transport.emit(start_event)      # Sent immediately
transport.emit(complete_event)   # Waits for START success, then sent

# Wait for all events to complete
transport.wait_for_completion(timeout=30)

# Get processing statistics
stats = transport.get_stats()    # {"pending": 0, "success": 10, "failed": 0}

# Graceful shutdown
transport.close(timeout=30)
```
"""


log = logging.getLogger(__name__)


@attr.define
class Request:
    event_id: str = attr.field()  # run_id + event type for RunEvent, md5 of body for others
    body: bytes | str = attr.field()
    headers: dict[str, str] = attr.field()
    run_id: str | None = attr.field(default=None)
    event_type: str | None = attr.field(default=None)


@attr.define
class AsyncHttpConfig(Config):
    url: str = attr.field()
    endpoint: str = attr.field(default="api/v1/lineage")
    timeout: float = attr.field(default=5.0)
    # check TLS certificates
    verify: bool = attr.field(default=True)
    auth: TokenProvider = attr.field(factory=lambda: TokenProvider({}))
    compression: HttpCompression | None = attr.field(default=None)
    custom_headers: dict[str, str] = attr.field(factory=dict)
    # Async configuration fields (flattened from AsyncConfig)
    max_queue_size: int = attr.field(default=10000)
    max_concurrent_requests: int = attr.field(default=100)
    retry: dict[str, Any] = attr.field(
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
    def from_dict(cls, params: dict[str, Any]) -> AsyncHttpConfig:
        if "url" not in params:
            msg = "`url` key not passed to AsyncHttpConfig"
            raise RuntimeError(msg)
        specified_dict = get_only_specified_fields(cls, params)
        specified_dict["auth"] = create_token_provider(specified_dict.get("auth", {}))
        compression = specified_dict.get("compression")
        if compression:
            specified_dict["compression"] = HttpCompression(compression)
        return cls(**specified_dict)


class AsyncHttpTransport(Transport):
    kind = "async_http"
    config_class = AsyncHttpConfig

    def __init__(self, config: AsyncHttpConfig) -> None:
        url = config.url.strip()
        self.config = config

        log.debug(
            "Constructing OpenLineage async transport that will send events "
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

        # Initialize async emitter components directly
        # Create a queue with 2x the configured size to reserve space for released completion events
        self.configured_queue_size = config.max_queue_size
        self.event_queue: queue.Queue[Request] = queue.Queue(maxsize=self.configured_queue_size * 2)

        # Event tracking
        self.events: dict[str, str] = {}
        self.event_lock = threading.Lock()
        # Queue for completion events waiting for their START event
        self.pending_completion_events: dict[str, list[Request]] = {}  # run_id -> list of Request
        self.event_stats = {
            "pending": 0,
            "success": 0,
            "failed": 0,
        }

        # "Hard" exit. Does not wait for completion for events in flight.
        self.should_exit = threading.Event()
        # "Soft" exit. Does not exit when there are events in the queue and active tasks
        self.may_exit = threading.Event()

        # Maximum number of concurrent requests
        self.max_concurrent = config.max_concurrent_requests
        self.worker_thread = threading.Thread(target=self._setup_loop, daemon=True)
        self.worker_thread.start()
        log.debug(
            "AsyncHttpTransport for %s started with configured queue"
            " size: %s (actual: %s), max concurrent: %s",
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
            log.debug("AsyncHttpTransport event loop closed")

    async def _event_loop_worker(self) -> None:
        # Create a semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent)
        # Keep track of active tasks
        active_tasks: set[asyncio.Task[list[Request]]] = set()

        # Httpx client for the worker
        limits = httpx.Limits(
            max_connections=self.max_concurrent, max_keepalive_connections=self.max_concurrent // 2
        )

        transport = httpx.AsyncHTTPTransport(limits=limits, retries=self.config.retry["total"])

        def _should_exit() -> bool:
            return self.should_exit.is_set() or (
                self.event_queue.empty() and not len(active_tasks) and self.may_exit.is_set()
            )

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout=self.config.timeout),
            verify=self.config.verify,
            follow_redirects=True,
            transport=transport,
        ) as client:
            last_processed_time = time.time()
            while not _should_exit():
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

                # Update last processed time if we processed items
                if processed_items:
                    last_processed_time = time.time()

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
                    current_time = time.time()
                    if current_time - last_processed_time >= 10.0:
                        log.warning(
                            "No new events processed for %.1fs. Queue empty: %s, "
                            "active tasks: %d, max concurrent: %d, "
                            "query stats: pending %d success %d failed %s",
                            current_time - last_processed_time,
                            self.event_queue.empty(),
                            len(active_tasks),
                            self.max_concurrent,
                            self.event_stats["pending"],
                            self.event_stats["success"],
                            self.event_stats["failed"],
                        )
                        last_processed_time = current_time
                    await asyncio.sleep(0.01)

            log.debug(
                "AsyncHttpTransport event loop worker completed. "
                "Stats: %d pending, %d success, %d failed events",
                self.event_stats["pending"],
                self.event_stats["success"],
                self.event_stats["failed"],
            )

    async def _process_event(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        request: Request,
        from_main_queue: bool = True,
    ) -> list[Request]:
        """Process a single event and return any pending completion events if this was a successful START"""
        pending_events = []

        log.debug("Processing event %s", request.event_id)

        def handle_failure(
            response: httpx.Response | None = None, exception: Exception | None = None
        ) -> None:
            self._mark_failed(request)

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
            for attempt in range(self.config.retry["total"] + 1):
                if self.should_exit.is_set():
                    break
                try:
                    resp = await client.post(
                        url=urljoin(self.config.url, self.config.endpoint),
                        content=request.body,
                        headers=request.headers,
                    )

                    if 200 <= resp.status_code < 300:
                        pending_events = self._mark_success(request)
                        log.debug(
                            "Successfully emitted event (event_id) %s to %s. Released %s "
                            "pending completion events.",
                            request.event_id,
                            self.config.url,
                            len(pending_events),
                        )
                        break

                    if resp.status_code not in self.config.retry["status_forcelist"]:
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

    # Event tracking methods (inlined from EventTracker)
    def _add_event(self, request: Request) -> None:
        with self.event_lock:
            self.event_stats["pending"] += 1
            self.events[request.event_id] = "pending"

    def _mark_success(self, request: Request) -> list[Request]:
        with self.event_lock:
            self.event_stats["pending"] -= 1
            self.event_stats["success"] += 1
            if self.events.get(request.event_id) == "pending":
                del self.events[request.event_id]

            # If this was a START event, mark it as successful and release any pending completion events
            if request.run_id and request.event_type == "START":
                # Return any pending completion events for this run
                return self.pending_completion_events.pop(request.run_id, [])
        return []

    def _mark_failed(self, request: Request) -> None:
        with self.event_lock:
            log.debug("Marking event %s as failed", request.event_id)
            self.event_stats["pending"] -= 1
            self.event_stats["failed"] += 1
            if self.events.get(request.event_id) == "pending":
                del self.events[request.event_id]

    def _add_pending_completion_event(self, request: Request) -> None:
        if not request.run_id:
            return
        with self.event_lock:
            if request.run_id not in self.pending_completion_events:
                self.pending_completion_events[request.run_id] = []
            self.pending_completion_events[request.run_id].append(request)

    def _is_start_in_progress(self, run_id: str) -> bool:
        with self.event_lock:
            start_event_id = f"{run_id}-START"
            return start_event_id in self.events

    def _all_processed(self) -> bool:
        with self.event_lock:
            return len(self.events) == 0

    def emit(self, event: Event) -> None:
        event_str = Serde.to_json(event)

        run_id = None
        event_type = None

        if isinstance(event, (RunEvent, RunEventV2)):
            event_type = str(event.eventType.value)  # type: ignore[union-attr]
            event_id = event.run.runId + "-" + event_type
            run_id = event.run.runId

            # We don't want to emit terminal events until we have a successful START event
            # Only queue as pending if START has been scheduled but not yet successful
            # If START was never scheduled, process COMPLETE normally (fall through)
            if event_type != "START":
                if self._is_start_in_progress(run_id):
                    body, headers = self._prepare_request(event_str)
                    request = Request(
                        event_id=event_id, run_id=run_id, event_type=event_type, body=body, headers=headers
                    )
                    self._add_event(request)
                    self._add_pending_completion_event(request)

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
        self._add_event(request)

        # Wait for space in the regular capacity portion of the queue
        wait_start_time = time.time()
        last_log_time = wait_start_time
        while (qsize := self.event_queue.qsize()) >= self.configured_queue_size:
            current_time = time.time()
            if current_time - last_log_time >= 15.0:
                # Check if worker thread is still alive
                if not self.worker_thread.is_alive():
                    log.error(
                        "Worker thread died while waiting for queue space. Queue size: %d, "
                        "configured size: %d, waiting time: %.1fs",
                        qsize,
                        self.configured_queue_size,
                        current_time - wait_start_time,
                    )
                    raise RuntimeError("AsyncHttpTransport worker thread died")

                log.warning(
                    "Queue capacity exceeded, waiting for space. Queue size: %d, configured size: %d, "
                    "waiting time: %.1fs, worker thread alive: %s",
                    qsize,
                    self.configured_queue_size,
                    current_time - wait_start_time,
                    self.worker_thread.is_alive(),
                )
                last_log_time = current_time
            time.sleep(0.01)

        # Add to queue - include run_id and event_type for START event tracking
        self.event_queue.put(request)
        log.debug("Queued event with event_id %s for async emission to %s", event_id, self.config.url)

    def wait_for_completion(self, timeout: float = -1) -> bool:
        """
        Block until all events are processed or timeout is reached.

        Params:
          timeout: Timeout in seconds. `-1` means to block until last event is processed, 0 means no timeout.

        Returns:
            bool: True if all events were processed, False if some events were not processed.
        """
        log.debug("Waiting for events completion for %.3f seconds", timeout)
        self.may_exit.set()
        start_time = time.time()
        while timeout < 0 or time.time() - start_time < timeout:
            if self._all_processed():
                log.debug("All events were processed.")
                return True

            # Brief sleep before checking again
            time.sleep(0.01)

        return self._all_processed()

    def close(self, timeout: float = -1.0) -> bool:
        log.debug("Closing Async HTTP transport with timeout %.3f seconds", timeout)
        result = self.wait_for_completion(timeout)
        self.should_exit.set()
        self.events = {}
        if self.worker_thread.is_alive():
            self.worker_thread.join()
        return result

    def get_stats(self) -> dict[str, int]:
        return self.event_stats

    def _prepare_request(self, event: str) -> tuple[bytes | str, dict[str, str]]:
        headers = {
            "Content-Type": "application/json",
            **self._auth_headers(self.config.auth),
            **self.config.custom_headers,
        }
        if self.config.compression == HttpCompression.GZIP:
            headers["Content-Encoding"] = "gzip"
            # levels higher than 3 are twice as slow:
            # https://github.com/python/cpython/issues/91349#issuecomment-2737161048
            return gzip.compress(event.encode("utf-8"), compresslevel=3), headers

        return event, headers

    def _auth_headers(self, token_provider: TokenProvider) -> dict:  # type: ignore[type-arg]
        bearer = token_provider.get_bearer()
        if bearer:
            return {"Authorization": bearer}
        return {}
