# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import threading
import time
import weakref
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property
from typing import TYPE_CHECKING, Any, Coroutine, Optional, TypeVar
from contextlib import contextmanager

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
    location: str = attr.field(default=DEFAULT_LOCATION)
    credentials_path: Optional[str] = attr.field(default=None)
    async_transport_rules: dict[str, dict[str, bool]] = attr.field(factory=lambda: {"dbt": {"*": True}})

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> GCPLineageConfig:
        if "project_id" not in params:
            msg = "project_id is required for GCPLineageTransport. Provide it in config"
            raise ValueError(msg)

        # Validate async_transport_rules structure if provided
        if "async_transport_rules" in params:
            cls._validate_async_transport_rules(params["async_transport_rules"])

        specified_dict = get_only_specified_fields(cls, params)
        return cls(**specified_dict)
    
    @staticmethod
    def _validate_async_transport_rules(rules: Any) -> None:
        """Validate the structure of async_transport_rules configuration."""
        if not isinstance(rules, dict):
            raise ValueError(
                "async_transport_rules must be a dictionary. "
                "Format: {'integration': {'jobType': True/False}}"
            )
        
        for integration, job_type_rules in rules.items():
            if not isinstance(integration, str):
                raise ValueError(
                    f"Integration key must be a string, got {type(integration).__name__}: {integration}"
                )
            
            if not isinstance(job_type_rules, dict):
                raise ValueError(
                    f"Job type rules for integration '{integration}' must be a dictionary, "
                    f"got {type(job_type_rules).__name__}: {job_type_rules}"
                )
            
            for job_type, use_async in job_type_rules.items():
                if not isinstance(job_type, str):
                    raise ValueError(
                        f"Job type key must be a string, got {type(job_type).__name__}: {job_type} "
                        f"in integration '{integration}'"
                    )
                
                if not isinstance(use_async, bool):
                    raise ValueError(
                        f"Async flag for integration '{integration}', job type '{job_type}' "
                        f"must be a boolean, got {type(use_async).__name__}: {use_async}"
                    )


class GCPLineageTransport(Transport):
    kind = "gcplineage"
    config_class = GCPLineageConfig

    def __init__(self, config: GCPLineageConfig) -> None:
        # Validate configuration at initialization
        self._validate_config(config)
        
        self.config = config
        self.parent = f"projects/{self.config.project_id}/locations/{self.config.location}"
        
        # Enhanced task tracking with memory leak prevention
        self._pending_operations: weakref.WeakSet[asyncio.Task[Any]] = weakref.WeakSet()
        self._operations_lock = threading.RLock()  # Use RLock for nested acquisition safety
        self._shutdown_event = threading.Event()
        self._closed = False  # Track if transport has been properly closed
        
        # Add periodic cleanup to prevent WeakSet from growing indefinitely
        self._last_cleanup_time = time.time()
        self._cleanup_interval = 60.0  # Cleanup every 60 seconds
        
        # Thread pool for running async operations when event loop is busy  
        self._thread_pool: Optional[ThreadPoolExecutor] = None
        
        log.info(
            "Initialized GCP Lineage transport for project %s in location %s with %d async transport rules",
            config.project_id, config.location, len(config.async_transport_rules)
        )
    
    def _validate_config(self, config: GCPLineageConfig) -> None:
        """Validate configuration parameters."""
        if not config.project_id or not isinstance(config.project_id, str):
            raise ValueError("project_id must be a non-empty string")
        
        if not config.location or not isinstance(config.location, str):
            raise ValueError("location must be a non-empty string")
            
        if config.credentials_path is not None and not isinstance(config.credentials_path, str):
            raise ValueError("credentials_path must be a string or None")
            
        # Re-validate async_transport_rules structure
        GCPLineageConfig._validate_async_transport_rules(config.async_transport_rules)
    
    def __enter__(self) -> 'GCPLineageTransport':
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - ensures proper cleanup."""
        self.close()
    
    def __del__(self) -> None:
        """Destructor - ensures resource cleanup even if close() wasn't called."""
        if not self._closed:
            try:
                log.warning(
                    "GCPLineageTransport was not properly closed. "
                    "Use close() method or context manager for proper cleanup."
                )
                # Proper emergency cleanup with resource leak prevention
                self._shutdown_event.set()
                
                # Cancel all pending operations to prevent hanging
                try:
                    with self._operations_lock:
                        for task in self._pending_operations:
                            if not task.done():
                                task.cancel()
                        self._pending_operations.clear()
                except (RuntimeError, AttributeError):
                    # Transport may be in invalid state during destruction
                    pass
                
                # Proper thread pool shutdown with timeout
                if self._thread_pool is not None:
                    try:
                        # Try graceful shutdown first
                        self._thread_pool.shutdown(wait=False)
                        # Force cleanup after brief wait
                        import time
                        time.sleep(0.1)  # Brief grace period for cleanup
                        
                        # Ensure all resources are released
                        if hasattr(self._thread_pool, '_threads'):
                            # Force join any remaining threads
                            for thread in list(self._thread_pool._threads):
                                if thread.is_alive():
                                    thread.join(timeout=0.5)
                                    
                        self._thread_pool = None
                    except Exception as thread_cleanup_error:
                        log.error("Error during thread pool cleanup in __del__: %s", thread_cleanup_error)
                        # Force None assignment to prevent further issues
                        self._thread_pool = None
                
                # Close any cached clients to prevent resource leaks
                try:
                    if hasattr(self, '_client') and self._client is not None:
                        if hasattr(self._client, 'transport') and hasattr(self._client.transport, 'close'):
                            self._client.transport.close()
                except Exception as client_cleanup_error:
                    log.error("Error closing sync client in __del__: %s", client_cleanup_error)
                
                try:
                    if hasattr(self, '_async_client') and self._async_client is not None:
                        if hasattr(self._async_client, 'transport') and hasattr(self._async_client.transport, 'close'):
                            # Cannot safely await in __del__, just close synchronously if possible
                            if hasattr(self._async_client.transport, '_channel'):
                                self._async_client.transport._channel.close()
                except Exception as async_client_cleanup_error:
                    log.error("Error closing async client in __del__: %s", async_client_cleanup_error)
                    
            except Exception as e:
                log.error("Error during emergency cleanup in __del__: %s", e)
            finally:
                # Always mark as closed to prevent further cleanup attempts
                self._closed = True

    @cached_property
    def client(self) -> "LineageClient":
        """Lazy initialization of sync LineageClient with comprehensive error handling."""
        try:
            from google.cloud.datacatalog_lineage_v1 import LineageClient
            from google.oauth2 import service_account
            from google.auth.exceptions import GoogleAuthError
            import json
            import os

            if self.config.credentials_path:
                # Comprehensive credential file validation
                if not os.path.exists(self.config.credentials_path):
                    raise FileNotFoundError(
                        f"Credentials file not found: {self.config.credentials_path}"
                    )
                    
                if not os.access(self.config.credentials_path, os.R_OK):
                    raise PermissionError(
                        f"Cannot read credentials file: {self.config.credentials_path}"
                    )
                
                try:
                    # Validate JSON format before passing to Google Auth
                    with open(self.config.credentials_path, 'r') as f:
                        cred_data = json.load(f)
                        
                    # Basic structure validation
                    required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email']
                    missing_fields = [field for field in required_fields if field not in cred_data]
                    if missing_fields:
                        raise ValueError(
                            f"Invalid credentials file structure. Missing fields: {missing_fields}"
                        )
                        
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Credentials file is not valid JSON: {self.config.credentials_path}. Error: {e}"
                    )
                except IOError as e:
                    raise IOError(
                        f"Error reading credentials file {self.config.credentials_path}: {e}"
                    )
                
                try:
                    credentials = service_account.Credentials.from_service_account_file(
                        self.config.credentials_path
                    )
                    return LineageClient(credentials=credentials)
                except GoogleAuthError as e:
                    raise ValueError(
                        f"Invalid Google Cloud credentials in {self.config.credentials_path}: {e}"
                    )
            else:
                try:
                    return LineageClient()
                except GoogleAuthError as e:
                    raise ValueError(
                        f"Failed to initialize LineageClient with default credentials. "
                        f"Ensure you have valid default credentials configured (gcloud auth application-default login) "
                        f"or provide credentials_path. Error: {e}"
                    )

        except ModuleNotFoundError as e:
            log.exception(
                "OpenLineage client did not find google-cloud-datacatalog-lineage module. "
                "Installing it is required for GCPLineage to work. "
                "You can also get it via `pip install google-cloud-datacatalog-lineage`",
            )
            raise RuntimeError(
                "Missing required dependency: google-cloud-datacatalog-lineage. "
                "Install it with: pip install google-cloud-datacatalog-lineage"
            ) from e
        except Exception as e:
            log.error("Unexpected error initializing sync LineageClient: %s", e)
            raise

    @cached_property
    def async_client(self) -> "LineageAsyncClient":
        """Lazy initialization of async LineageAsyncClient with comprehensive error handling."""
        try:
            from google.cloud.datacatalog_lineage_v1 import LineageAsyncClient
            from google.oauth2 import service_account
            from google.auth.exceptions import GoogleAuthError
            import json
            import os

            if self.config.credentials_path:
                # Reuse validation logic from sync client
                if not os.path.exists(self.config.credentials_path):
                    raise FileNotFoundError(
                        f"Credentials file not found: {self.config.credentials_path}"
                    )
                    
                if not os.access(self.config.credentials_path, os.R_OK):
                    raise PermissionError(
                        f"Cannot read credentials file: {self.config.credentials_path}"
                    )
                
                try:
                    # Validate JSON format
                    with open(self.config.credentials_path, 'r') as f:
                        cred_data = json.load(f)
                        
                    # Basic structure validation
                    required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email']
                    missing_fields = [field for field in required_fields if field not in cred_data]
                    if missing_fields:
                        raise ValueError(
                            f"Invalid credentials file structure. Missing fields: {missing_fields}"
                        )
                        
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Credentials file is not valid JSON: {self.config.credentials_path}. Error: {e}"
                    )
                except IOError as e:
                    raise IOError(
                        f"Error reading credentials file {self.config.credentials_path}: {e}"
                    )
                
                try:
                    credentials = service_account.Credentials.from_service_account_file(
                        self.config.credentials_path
                    )
                    return LineageAsyncClient(credentials=credentials)
                except GoogleAuthError as e:
                    raise ValueError(
                        f"Invalid Google Cloud credentials in {self.config.credentials_path}: {e}"
                    )
            else:
                try:
                    return LineageAsyncClient()
                except GoogleAuthError as e:
                    raise ValueError(
                        f"Failed to initialize LineageAsyncClient with default credentials. "
                        f"Ensure you have valid default credentials configured (gcloud auth application-default login) "
                        f"or provide credentials_path. Error: {e}"
                    )

        except ModuleNotFoundError as e:
            log.exception(
                "OpenLineage client did not find google-cloud-datacatalog-lineage module. "
                "Installing it is required for GCPLineage to work. "
                "You can also get it via `pip install google-cloud-datacatalog-lineage`",
            )
            raise RuntimeError(
                "Missing required dependency: google-cloud-datacatalog-lineage. "
                "Install it with: pip install google-cloud-datacatalog-lineage"
            ) from e
        except Exception as e:
            log.error("Unexpected error initializing async LineageAsyncClient: %s", e)
            raise

    @property
    def _get_thread_pool(self) -> ThreadPoolExecutor:
        """Get thread pool for async operations (lazy initialization)."""
        if self._thread_pool is None:
            self._thread_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="gcplineage-async")
        return self._thread_pool
    
    def _cleanup_completed_tasks(self) -> None:
        """
        Periodic cleanup of completed tasks to prevent memory leaks.
        
        WeakSet should automatically clean up, but explicitly removing completed
        tasks helps prevent unbounded growth under high load.
        """
        current_time = time.time()
        if current_time - self._last_cleanup_time < self._cleanup_interval:
            return  # Not time for cleanup yet
            
        try:
            with self._operations_lock:
                # Get snapshot of current tasks
                current_tasks = list(self._pending_operations)
                completed_count = 0
                
                for task in current_tasks:
                    if task.done():
                        # Explicitly remove completed tasks
                        self._pending_operations.discard(task)
                        completed_count += 1
                
                self._last_cleanup_time = current_time
                
                if completed_count > 0:
                    remaining_tasks = len([t for t in self._pending_operations if not t.done()])
                    log.debug(
                        "Cleaned up %d completed tasks, %d active tasks remaining",
                        completed_count, remaining_tasks
                    )
                    
        except Exception as e:
            log.warning("Error during periodic task cleanup: %s", e)

    def _run_async_safely(self, coro: Coroutine[Any, Any, T]) -> T:
        """
        Safely run async coroutine in sync context with robust event loop handling.
        
        Eliminates nested event loop deadlock by always using thread pool
        when an event loop is detected in the current thread.
        """
        try:
            # Always check for running loop first to prevent deadlock
            try:
                loop = asyncio.get_running_loop()
                # If we get here, there's a running loop - ALWAYS use thread pool to avoid deadlock
                def _run_coro_in_new_thread() -> T:
                    # Create completely isolated event loop in thread pool
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(coro)
                    finally:
                        new_loop.close()
                        asyncio.set_event_loop(None)  # Clean thread-local state

                return self._get_thread_pool.submit(_run_coro_in_new_thread).result(timeout=30.0)
                
            except RuntimeError:
                # No running event loop detected, safe to run directly
                try:
                    # Try to get existing event loop
                    loop = asyncio.get_event_loop()
                    if not loop.is_running():
                        # Loop exists but not running, safe to use
                        return loop.run_until_complete(coro)
                    else:
                        # This shouldn't happen after the get_running_loop check above,
                        # but handle it defensively
                        raise RuntimeError("Event loop is running but was not detected")
                except RuntimeError:
                    # No event loop exists, create one
                    return asyncio.run(coro)
                    
        except Exception as e:
            log.error("Error in _run_async_safely: %s", e)
            # Last resort: run in thread pool with new event loop
            def _emergency_run_coro() -> T:
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(coro)
                finally:
                    new_loop.close()
                    asyncio.set_event_loop(None)

            return self._get_thread_pool.submit(_emergency_run_coro).result(timeout=30.0)

    def emit(self, event: Event) -> Any:
        """
        Emit a lineage event to GCP Data Catalog Lineage service.
        
        Args:
            event: The OpenLineage event to emit (must be RunEvent or event_v2.RunEvent)
            
        Returns:
            Result from the emission operation (None for async operations)
            
        Raises:
            ValueError: If event is None or invalid type
            RuntimeError: If transport is closed
        """
        # Comprehensive input validation
        if event is None:
            raise ValueError("Event cannot be None")
            
        if not isinstance(event, (RunEvent, event_v2.RunEvent)):
            raise ValueError(
                f"GCP Lineage only supports RunEvent, received {type(event).__name__}. "
                f"Expected types: {RunEvent.__name__} or {event_v2.RunEvent.__name__}"
            )
        
        # Check if transport is closed
        if self._closed:
            raise RuntimeError("Cannot emit events on closed transport. Create a new transport instance.")
            
        if self._shutdown_event.is_set():
            raise RuntimeError("Transport is shutting down. Cannot emit new events.")

        # Perform periodic cleanup to prevent memory leaks
        self._cleanup_completed_tasks()
        
        # Check if event should use async transport based on rules
        should_use_async = self._should_use_async_transport(event)
        
        # Log queue depth for monitoring
        with self._operations_lock:
            active_operations = len([task for task in self._pending_operations if not task.done()])
            
        log.debug(
            "Emitting event (async=%s, pending_operations=%d, shutdown=%s)",
            should_use_async, active_operations, self._shutdown_event.is_set()
        )
        
        if active_operations > 50:  # Alert on high queue depth
            log.warning(
                "High number of pending async operations: %d. This may indicate performance issues.",
                active_operations
            )

        if should_use_async:
            return self._emit_async_with_tracking(event)
        else:
            return self._emit_sync(event)

    def _emit_sync(self, event: Event) -> Any:
        """
        Emit event synchronously to GCP Data Catalog Lineage service.
        
        Args:
            event: Pre-validated OpenLineage event
            
        Raises:
            RuntimeError: If client initialization fails or transport is closed
            Exception: Various exceptions from GCP client
        """
        # Additional validation and state checks
        if self._closed:
            raise RuntimeError("Cannot emit on closed transport")
            
        if self._shutdown_event.is_set():
            raise RuntimeError("Transport is shutting down")
            
        start_time = time.time()
        try:
            # Validate serialization before sending
            try:
                event_dict = Serde.to_dict(event)
                if not event_dict:
                    raise ValueError("Event serialization resulted in empty dictionary")
            except Exception as e:
                raise ValueError(f"Failed to serialize event: {e}") from e
                
            # Validate client is available
            if not hasattr(self, 'client') or self.client is None:
                raise RuntimeError("Sync LineageClient is not initialized")
                
            self.client.process_open_lineage_run_event(parent=self.parent, open_lineage=event_dict)
            
            duration = time.time() - start_time
            log.debug("Sync event emission completed in %.3fs", duration)
            
            if duration > 5.0:  # Alert on slow operations
                log.warning("Slow sync event emission: %.3fs", duration)

        except Exception as e:
            duration = time.time() - start_time
            log.error("Failed to send lineage event to GCP (sync) after %.3fs: %s", duration, e)
            raise

    def _emit_async_with_tracking(self, event: Event) -> Any:
        """
        Emit event async with proper tracking for graceful shutdown.
        
        This uses a simplified approach that works reliably across all event loop states.
        Race condition eliminated by making shutdown check and task creation atomic.
        """
        # Always use thread pool for true async behavior - this ensures we can track operations
        def _run_async_emit() -> Any:
            # Check shutdown status before creating any resources
            with self._operations_lock:
                if self._shutdown_event.is_set():
                    log.warning("Transport is shutting down, rejecting event emission")
                    return None
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # Create task and add to tracking atomically under lock
                with self._operations_lock:
                    # Double-check shutdown status after acquiring lock
                    if self._shutdown_event.is_set():
                        log.warning("Transport is shutting down, cancelling event emission")
                        return None
                    
                    # Create task and immediately add to tracking - this is atomic
                    task = loop.create_task(self._emit_async(event))
                    self._pending_operations.add(task)
                
                # Enhanced cleanup with comprehensive error handling
                def cleanup_task(completed_task: asyncio.Task[Any]) -> None:
                    try:
                        # Check for task exceptions and log them
                        if completed_task.done() and not completed_task.cancelled():
                            try:
                                exception = completed_task.exception()
                                if exception is not None:
                                    log.warning("Async task completed with exception: %s", exception)
                            except (asyncio.CancelledError, asyncio.InvalidStateError):
                                # Task was cancelled or is in invalid state - this is normal during shutdown
                                pass
                        
                        # Clean up from tracking
                        with self._operations_lock:
                            self._pending_operations.discard(completed_task)
                    except (KeyError, RuntimeError, AttributeError) as e:
                        # Already removed by WeakSet or transport is closing
                        log.debug("Expected error during task cleanup: %s", e)
                    except Exception as e:
                        log.warning("Unexpected error during task cleanup: %s", e)
                        
                task.add_done_callback(cleanup_task)
                
                try:
                    result = loop.run_until_complete(task)
                    return result
                except asyncio.CancelledError:
                    log.info("Async event emission was cancelled")
                    return None
                except Exception as task_error:
                    log.error("Async event emission failed: %s", task_error)
                    raise task_error
            except Exception as e:
                # Ensure task is cleaned up on any failure with comprehensive error handling
                cleanup_errors = []
                try:
                    if 'task' in locals() and task is not None:
                        # Cancel task if it's still running
                        if not task.done():
                            task.cancel()
                        
                        # Remove from tracking
                        with self._operations_lock:
                            self._pending_operations.discard(task)
                except Exception as cleanup_error:
                    cleanup_errors.append(str(cleanup_error))
                
                if cleanup_errors:
                    log.warning("Errors during cleanup: %s", cleanup_errors)
                    
                raise e
            finally:
                try:
                    loop.close()
                except Exception as loop_close_error:
                    log.warning("Error closing event loop: %s", loop_close_error)
        
        # Enhanced error handling for thread pool submission
        try:
            future = self._get_thread_pool.submit(_run_async_emit)
            return future.result(timeout=30.0)  # Add timeout to prevent hanging
        except concurrent.futures.TimeoutError:
            log.error("Async event emission timed out after 30 seconds")
            raise RuntimeError("Async event emission timed out") from None
        except Exception as e:
            log.error("Thread pool execution failed for async event emission: %s", e)
            raise RuntimeError(f"Async event emission failed: {e}") from e

    def flush(self, timeout: float = 30.0) -> bool:
        """
        Flush pending async operations by waiting for them to complete.
        
        Args:
            timeout: Maximum time to wait for operations to complete (seconds)
            
        Returns:
            True if all operations completed successfully, False if timeout occurred
        """
        # Input validation
        if timeout != -1 and (not isinstance(timeout, (int, float)) or timeout < 0):
            raise ValueError("timeout must be a positive number or -1")
            
        flush_start_time = time.time()
        with self._operations_lock:
            pending_tasks = [task for task in self._pending_operations if not task.done()]
            total_operations = len(self._pending_operations)
            completed_operations = total_operations - len(pending_tasks)
            
        if not pending_tasks:
            log.debug("No pending async operations to flush (total tracked: %d, completed: %d)", 
                     total_operations, completed_operations)
            return True
            
        log.info("Flushing %d pending async operations (total: %d, completed: %d, timeout: %.1fs)", 
                len(pending_tasks), total_operations, completed_operations, timeout)
        
        # Eliminate nested event loop deadlock with simplified thread-based approach
        def _wait_for_tasks() -> bool:
            """
            Run the flush operation safely without nested event loops.
            
            This approach completely eliminates deadlock risk by always using
            a fresh event loop in a dedicated thread.
            """
            try:
                async def _await_tasks() -> None:
                    """Wait for all pending tasks to complete."""
                    if pending_tasks:
                        # Use gather with return_exceptions to handle individual task failures
                        results = await asyncio.wait_for(
                            asyncio.gather(*pending_tasks, return_exceptions=True),
                            timeout=timeout
                        )
                        
                        # Log any exceptions from completed tasks
                        for i, result in enumerate(results):
                            if isinstance(result, Exception):
                                log.warning("Task %d completed with exception: %s", i, result)
                
                def _run_flush_in_isolated_loop() -> bool:
                    """Run flush operation in completely isolated event loop."""
                    # Create fresh event loop isolated from any existing ones
                    flush_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(flush_loop)
                    try:
                        flush_loop.run_until_complete(_await_tasks())
                        return True
                    except asyncio.TimeoutError:
                        log.warning("Timeout after %.1fs waiting for %d async operations to complete", 
                                   timeout, len(pending_tasks))
                        return False
                    except Exception as e:
                        log.error("Error while flushing pending async operations: %s", e)
                        return False
                    finally:
                        flush_loop.close()
                        asyncio.set_event_loop(None)  # Clean thread-local state
                
                # Always use thread pool regardless of current event loop state
                # This completely eliminates the risk of nested event loops
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="gcplineage-flush") as executor:
                    future = executor.submit(_run_flush_in_isolated_loop)
                    return future.result(timeout=timeout + 10.0)  # Add buffer for thread scheduling
                    
            except concurrent.futures.TimeoutError:
                log.warning("Flush operation timed out after %.1fs (including thread scheduling overhead)", 
                           timeout + 10.0)
                return False
            except Exception as e:
                log.error("Unexpected error during flush operation: %s", e)
                return False
        
        try:
            result = _wait_for_tasks()
            flush_duration = time.time() - flush_start_time
            log.info("Flush operation completed in %.3fs (success=%s)", flush_duration, result)
            return result
        except Exception as e:
            flush_duration = time.time() - flush_start_time
            log.error("Failed to execute flush operation after %.3fs: %s", flush_duration, e)
            return False

    async def _emit_async(self, event: Event) -> Any:
        """
        Emit event asynchronously to GCP Data Catalog Lineage service.
        
        Args:
            event: Pre-validated OpenLineage event
            
        Raises:
            RuntimeError: If async client initialization fails or transport is closed
            ValueError: If event serialization fails
            Exception: Various exceptions from GCP async client
        """
        # Additional validation and state checks for async path
        if self._closed:
            raise RuntimeError("Cannot emit on closed transport")
            
        if self._shutdown_event.is_set():
            raise RuntimeError("Transport is shutting down")
            
        start_time = time.time()
        try:
            # Validate serialization with proper error handling
            try:
                import json
                event_json = Serde.to_json(event)
                if not event_json or event_json.strip() == "":
                    raise ValueError("Event serialization resulted in empty JSON")
                    
                event_dict = json.loads(event_json)
                if not event_dict:
                    raise ValueError("Event deserialization resulted in empty dictionary")
            except (json.JSONDecodeError, ValueError) as e:
                raise ValueError(f"Failed to serialize/deserialize event: {e}") from e
            except Exception as e:
                raise ValueError(f"Unexpected error during event serialization: {e}") from e

            # Validate async client is available
            if not hasattr(self, 'async_client') or self.async_client is None:
                raise RuntimeError("Async LineageClient is not initialized")

            await self.async_client.process_open_lineage_run_event(
                parent=self.parent, open_lineage=event_dict
            )
            
            duration = time.time() - start_time
            log.debug("Async event emission completed in %.3fs", duration)
            
            if duration > 10.0:  # Alert on slow async operations
                log.warning("Slow async event emission: %.3fs", duration)

        except Exception as e:
            duration = time.time() - start_time
            log.error("Failed to send lineage event to GCP (async) after %.3fs: %s", duration, e)
            raise

    def _should_use_async_transport(self, event: Event) -> bool:
        """
        Determine if event should use async transport based on async_transport_rules.
        Returns True if the event matches the configured rules for async transport.
        """
        try:
            wildcard_rules = self.config.async_transport_rules.get("*", {})
            if wildcard_rules.get("*"):
                log.debug("Using async transport due to wildcard rule")
                return True

            if not hasattr(event, "job") or not hasattr(event.job, "facets"):
                log.debug("Event has no job or job facets, defaulting to sync transport")
                return False

            job_facets = event.job.facets
            if not job_facets:
                log.debug("Event has empty job facets, defaulting to sync transport")
                return False

            job_type_facet = job_facets.get("jobType")
            if not job_type_facet:
                log.debug("Event has no jobType facet, defaulting to sync transport")
                return False

            integration = getattr(job_type_facet, "integration", "").lower()
            job_type = getattr(job_type_facet, "jobType", "").lower()

            if not integration:
                log.debug("Event has no integration in jobType facet, defaulting to sync transport")
                return False

            for rule_integration, job_type_rules in self.config.async_transport_rules.items():
                if rule_integration == "*" or rule_integration.lower() == integration:
                    for rule_job_type, use_async in job_type_rules.items():
                        if rule_job_type == "*" or rule_job_type.lower() == job_type:
                            log.debug(
                                "Matched rule integration=%s, jobType=%s -> async=%s",
                                rule_integration, rule_job_type, use_async
                            )
                            return use_async
            
            log.debug(
                "No matching rules for integration=%s, jobType=%s, defaulting to sync transport",
                integration, job_type
            )
            return False
        except Exception as e:
            log.warning("Error evaluating async transport rules: %s. Defaulting to sync transport", e)
            return False

    def close(self, timeout: float = -1) -> bool:
        """
        Close the transport after ensuring all pending async operations complete.
        
        Args:
            timeout: Maximum time to wait for pending operations (seconds).
                    If -1 (default), uses a 30-second timeout for flushing.
                    
        Returns:
            True if transport was closed successfully, False otherwise.
        """
        # Input validation
        if timeout != -1 and (not isinstance(timeout, (int, float)) or timeout < 0):
            raise ValueError("timeout must be a positive number or -1")
            
        log.info("Closing GCP Lineage transport")
        
        # Signal shutdown to prevent new operations
        self._shutdown_event.set()
        
        # Determine flush timeout
        flush_timeout = 30.0 if timeout == -1 else max(0, timeout)
        
        # Flush pending operations
        flush_success = True
        if flush_timeout > 0:
            try:
                flush_success = self.flush(timeout=flush_timeout)
            except Exception as e:
                log.warning("Error during flush: %s", e)
                flush_success = False
        
        # Close clients
        clients_success = self._close_clients()
        
        # Shutdown thread pool
        thread_pool_success = self._shutdown_thread_pool()
        
        success = flush_success and clients_success and thread_pool_success
        self._closed = True  # Mark as closed
        log.info("GCP Lineage transport closed (success=%s)", success)
        return success
    
    def _close_clients(self) -> bool:
        """Close sync and async clients."""
        success = True
        
        # Close sync client
        try:
            if hasattr(self, '_client_cached_property') or 'client' in self.__dict__:
                if hasattr(self.client, 'transport'):
                    self.client.transport.close()
        except Exception as e:
            log.warning("Error closing sync client: %s", e)
            success = False
            
        # Close async client  
        try:
            if hasattr(self, '_async_client_cached_property') or 'async_client' in self.__dict__:
                async def close_async() -> None:
                    await self.async_client.transport.close()
                    
                self._run_async_safely(close_async())
        except Exception as e:
            log.warning("Error closing async client: %s", e)
            success = False
            
        return success
        
    def _shutdown_thread_pool(self) -> bool:
        """Shutdown the thread pool if it exists."""
        try:
            if self._thread_pool is not None:
                # Handle different Python versions for thread pool shutdown
                try:
                    # Python 3.9+ supports timeout parameter
                    self._thread_pool.shutdown(wait=True, timeout=5.0)
                except TypeError:
                    # Fallback for older Python versions without timeout parameter
                    self._thread_pool.shutdown(wait=True)
                    
                self._thread_pool = None
            return True
        except Exception as e:
            log.warning("Error shutting down thread pool: %s", e)
            return False
