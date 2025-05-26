# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import gzip
import hashlib
import os
import threading
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.serde import Serde
from openlineage.client.transport.http import (
    ApiKeyTokenProvider,
    AsyncConfig,
    AsyncEmitter,
    EventStatus,
    EventTracker,
    HttpCompression,
    HttpConfig,
    HttpTransport,
    Request,
    SyncEmitter,
)
from openlineage.client.uuid import generate_new_uuid


class TestAsyncConfig:
    def test_async_config_defaults(self):
        config = AsyncConfig()
        assert config.enabled is False
        assert config.max_queue_size == 1000000
        assert config.max_concurrent_requests == 100

    def test_async_config_custom_values(self):
        config = AsyncConfig(enabled=True, max_queue_size=5000, max_concurrent_requests=50)
        assert config.enabled is True
        assert config.max_queue_size == 5000
        assert config.max_concurrent_requests == 50


class TestHttpConfig:
    def test_http_loads_full_config(self):
        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "endpoint": "api/v1/lineage",
                "verify": False,
                "auth": {
                    "type": "api_key",
                    "api_key": "1500100900",
                },
                "compression": "gzip",
                "retry": {
                    "total": 7,
                    "connect": 3,
                    "read": 2,
                    "status": 5,
                    "other": 1,
                    "allowed_methods": ["POST"],
                    "status_forcelist": [500, 502, 503, 504],
                    "backoff_factor": 0.5,
                    "raise_on_redirect": False,
                    "raise_on_status": False,
                },
            },
        )

        assert config.url == "http://backend:5000"
        assert config.endpoint == "api/v1/lineage"
        assert config.verify is False
        assert config.auth.api_key == "1500100900"
        assert config.compression is HttpCompression.GZIP
        assert config.retry == {
            "total": 7,
            "connect": 3,
            "read": 2,
            "status": 5,
            "other": 1,
            "allowed_methods": ["POST"],
            "status_forcelist": [500, 502, 503, 504],
            "backoff_factor": 0.5,
            "raise_on_redirect": False,
            "raise_on_status": False,
        }

    def test_http_loads_minimal_config(self):
        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000/api/v1/lineage",
            },
        )
        assert config.url == "http://backend:5000/api/v1/lineage"
        assert config.verify is True
        assert not hasattr(config.auth, "api_key")
        assert config.compression is None

    def test_http_client_loads_retry(self):
        total = 7
        connect = 3
        read = 2
        status = 5
        other = 1
        backoff = 0.5

        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "retry": {
                    "total": total,
                    "connect": connect,
                    "read": read,
                    "status": status,
                    "other": other,
                    "allowed_methods": ["POST"],
                    "status_forcelist": [500, 502, 503, 504, 590],
                    "backoff_factor": backoff,
                    "raise_on_redirect": False,
                    "raise_on_status": False,
                },
            },
        )

        client = OpenLineageClient(transport=HttpTransport(config))
        assert client.transport.kind == "http"
        assert client.transport.url == "http://backend:5000"

        # The new implementation stores retry config directly
        retry = client.transport.config.retry
        assert retry["total"] == total
        assert retry["read"] == read
        assert retry["connect"] == connect
        assert retry["status"] == status
        assert retry["other"] == other
        assert retry["allowed_methods"] == ["POST"]
        assert retry["status_forcelist"] == [500, 502, 503, 504, 590]
        assert retry["backoff_factor"] == backoff
        assert retry["raise_on_redirect"] is False
        assert retry["raise_on_status"] is False

    def test_http_config_async_enabled(self):
        config = HttpConfig.from_dict(
            {
                "url": "http://example.com",
                "async_config": {"enabled": True, "max_queue_size": 2000, "max_concurrent_requests": 25},
            }
        )

        assert config.url == "http://example.com"
        assert config.async_config.enabled is True
        assert config.async_config.max_queue_size == 2000
        assert config.async_config.max_concurrent_requests == 25

    def test_http_config_async_disabled(self):
        config = HttpConfig.from_dict({"url": "http://example.com", "async_config": {"enabled": False}})

        assert config.async_config.enabled is False

    def test_http_config_no_async_config(self):
        config = HttpConfig.from_dict({"url": "http://example.com"})

        assert config.async_config.enabled is False
        assert config.async_config.max_queue_size == 1000000


class TestEventTracker:
    def test_event_tracker_initialization(self):
        tracker = EventTracker()
        assert len(tracker.events) == 0
        assert len(tracker.pending_completion_events) == 0

    def test_add_event(self):
        tracker = EventTracker()
        tracker.add_event(Request("event-1", "", {}))

        assert tracker.events["event-1"] == EventStatus.PENDING

    def test_mark_success_regular_event(self):
        tracker = EventTracker()
        request = Request("event-1", "", {})
        tracker.add_event(request)

        assert tracker.events["event-1"] == EventStatus.PENDING

        pending_events = tracker.mark_success(request)

        assert tracker.events.get("event-1") is None
        assert len(pending_events) == 0
        assert tracker.stats["success"] == 1

    def test_mark_success_start_event(self):
        tracker = EventTracker()
        request = Request("start-event", "", {})
        tracker.add_event(request)

        pending_events = tracker.mark_success(request)

        assert tracker.events.get("start-event") is None
        assert len(pending_events) == 0
        assert tracker.stats["success"] == 1

    def test_mark_success_start_event_releases_pending(self):
        tracker = EventTracker()
        request = Request(
            event_id="run-123-COMPLETE",
            run_id="run-123",
            body=b"data",
            event_type="COMPLETE",
            headers={"header": "value"},
        )
        start_request = Request(
            event_id="run-123-START",
            run_id="run-123",
            body=b"data",
            event_type="START",
            headers={"header": "value"},
        )

        # Add pending completion event first
        tracker.add_pending_completion_event(request)

        # Mark START as successful
        pending_events = tracker.mark_success(start_request)

        assert len(pending_events) == 1
        assert pending_events[0] == request
        assert "run-123" not in tracker.pending_completion_events

    def test_mark_failed(self):
        tracker = EventTracker()
        request = Request("event-1", "", {})
        tracker.add_event(request)

        assert tracker.events["event-1"] == EventStatus.PENDING

        tracker.mark_failed(request)

        assert tracker.events.get(request.event_id) is None

    def test_add_pending_completion_event(self):
        tracker = EventTracker()
        request = Request(
            event_id="run-123-COMPLETE",
            event_type="COMPLETE",
            run_id="run-123",
            body=b"data",
            headers={"header": "value"},
        )

        tracker.add_pending_completion_event(request)

        assert "run-123" in tracker.pending_completion_events.keys()
        assert len(tracker.pending_completion_events["run-123"]) == 1
        assert tracker.pending_completion_events["run-123"][0] == request

    def test_all_processed(self):
        tracker = EventTracker()

        assert tracker.all_processed()  # No events
        request = Request("event-1", "", {})

        tracker.add_event(request)
        assert not tracker.all_processed()  # Pending event

        tracker.mark_success(request)
        assert tracker.all_processed()  # All processed

    def test_get_stats(self):
        tracker = EventTracker()

        stats = tracker.get_stats()
        expected = {
            "pending": 0,
            "success": 0,
            "failed": 0,
        }
        assert stats == expected

        r1 = Request("event-1", "", {}, run_id="run-123", event_type="START")
        r2 = Request("event-2", "", {}, run_id="run-1234", event_type="START")

        tracker.add_event(r1)
        tracker.add_event(r2)
        tracker.mark_success(r1)
        tracker.mark_failed(r2)

        stats = tracker.get_stats()
        assert stats["pending"] == 0
        assert stats["success"] == 1
        assert stats["failed"] == 1

    def test_thread_safety(self):
        """Test that EventTracker operations are reasonably thread-safe"""
        tracker = EventTracker()
        errors = []

        def worker(thread_id):
            try:
                for i in range(100):
                    event_id = f"event-{thread_id}-{i}"
                    request = Request(event_id, "", {}, run_id=f"run-{thread_id}", event_type="COMPLETE")
                    tracker.add_event(request)
                    if i % 2 == 0:
                        tracker.mark_success(request)
                    else:
                        tracker.mark_failed(request)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(tracker.events) == 0
        assert tracker.stats["success"] == 250
        assert tracker.stats["failed"] == 250
        assert tracker.all_processed()


class TestSyncEmitter:
    def test_sync_emitter_initialization(self):
        config = HttpConfig(url="http://example.com")
        emitter = SyncEmitter(config)

        assert emitter.config == config
        assert emitter.url == "http://example.com"
        assert emitter.endpoint == "api/v1/lineage"
        assert emitter.timeout == 5.0
        assert emitter.verify is True

    @patch("httpx.Client")
    def test_sync_emitter_emit_success(self, mock_client):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response

        config = HttpConfig(url="http://example.com")
        emitter = SyncEmitter(config)

        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        emitter.emit(event)

        mock_client.return_value.__enter__.return_value.post.assert_called_once()
        call_args = mock_client.return_value.__enter__.return_value.post.call_args

        assert call_args.kwargs["url"] == "http://example.com/api/v1/lineage"
        assert call_args.kwargs["headers"]["Content-Type"] == "application/json"

    @patch("httpx.Client")
    def test_sync_emitter_with_gzip_compression(self, mock_client):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_client.return_value.__enter__.return_value.post.return_value = mock_response

        config = HttpConfig(url="http://example.com", compression=HttpCompression.GZIP)
        emitter = SyncEmitter(config)

        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        emitter.emit(event)

        call_args = mock_client.return_value.__enter__.return_value.post.call_args

        assert call_args.kwargs["headers"]["Content-Encoding"] == "gzip"
        # Content should be compressed
        content = call_args.kwargs["content"]
        assert isinstance(content, bytes)
        # Should be able to decompress
        decompressed = gzip.decompress(content)
        assert len(decompressed) > 0


class TestAsyncEmitter:
    @pytest.fixture
    def async_config(self):
        return HttpConfig(
            url="http://example.com",
            async_config=AsyncConfig(enabled=True, max_queue_size=100, max_concurrent_requests=5),
        )

    def test_async_emitter_initialization(self, async_config):
        emitter = AsyncEmitter(async_config)

        assert emitter.config == async_config
        assert emitter.configured_queue_size == 100
        assert emitter.max_concurrent == 5
        assert emitter.worker_thread.is_alive()

    def test_async_emitter_event_id_generation_run_event(self, async_config):
        emitter = AsyncEmitter(async_config)

        run_id = str(generate_new_uuid())
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=run_id),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        # Mock the queue and event tracker to capture event details without blocking
        with patch.object(emitter.event_queue, "put") as mock_put, patch.object(
            emitter.event_tracker, "add_event"
        ) as mock_add:
            emitter.emit(event)

            mock_put.assert_called_once()
            mock_add.assert_called_once()
            request = mock_put.call_args[0][0]

            assert request.event_id == f"{run_id}-START"
            assert request.run_id == run_id
            assert request.event_type == "START"

    def test_async_emitter_event_id_generation_non_run_event(self, async_config):
        emitter = AsyncEmitter(async_config)

        # Create a mock event that's not a RunEvent
        mock_event = MagicMock()
        event_json = '{"test": "event"}'

        with patch("openlineage.client.serde.Serde.to_json", return_value=event_json):
            with patch.object(emitter.event_queue, "put") as mock_put:
                emitter.emit(mock_event)

                mock_put.assert_called_once()
                request = mock_put.call_args[0][0]

                # Should use MD5 hash for non-RunEvent
                expected_id = hashlib.md5(event_json.encode("utf-8")).hexdigest()
                assert request.event_id == expected_id
                assert request.run_id is None
                assert request.event_type is None

    def test_async_emitter_completion_event_ordering(self, async_config):
        emitter = AsyncEmitter(async_config)

        # Create START and COMPLETE events
        run_id = str(generate_new_uuid())
        start_event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-01-01T00:00:00Z",
            run=Run(runId=run_id),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        complete_event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime="2024-01-01T00:00:01Z",
            run=Run(runId=run_id),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        # First, emit START event to schedule it
        with patch.object(emitter.event_queue, "put") as mock_put:
            emitter.emit(start_event)
            mock_put.assert_called_once()
            request = mock_put.call_args[0][0]
            assert request.event_id == f"{run_id}-START"

        # Now emit COMPLETE event should be queued as pending since START
        # is scheduled but not successful yet
        with patch.object(emitter.event_queue, "put") as mock_put:
            emitter.emit(complete_event)

            # Should not add to main queue, but to pending
            mock_put.assert_not_called()

            # Check pending events
            assert run_id in emitter.event_tracker.pending_completion_events
            assert len(emitter.event_tracker.pending_completion_events[run_id]) == 1

    def test_async_emitter_completion_without_start_scheduled(self, async_config):
        """Test that COMPLETE events without a scheduled START are processed immediately."""
        emitter = AsyncEmitter(async_config)

        # Create COMPLETE event
        run_id = str(generate_new_uuid())
        complete_event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime="2024-01-01T00:00:01Z",
            run=Run(runId=run_id),
            job=Job(namespace="test", name="job"),
            producer="test",
            schemaURL="test",
        )

        # Emit COMPLETE event without scheduling START first
        with patch.object(emitter.event_queue, "put") as mock_put:
            emitter.emit(complete_event)

            # Should add to main queue since no START was scheduled
            mock_put.assert_called_once()
            request = mock_put.call_args[0][0]
            assert request.event_id == f"{run_id}-COMPLETE"

            # Should not be in pending events
            assert request.run_id not in emitter.event_tracker.pending_completion_events

    def test_async_emitter_wait_for_completion(self, async_config):
        emitter = AsyncEmitter(async_config)

        try:
            # Test with events not completed - mock all_processed to return False
            with patch.object(emitter.event_tracker, "all_processed", return_value=False):
                result = emitter.wait_for_completion(timeout=0.1)
                assert not result  # Should timeout

            # Test with events completed - mock all_processed to return True
            with patch.object(emitter.event_tracker, "all_processed", return_value=True):
                result = emitter.wait_for_completion(timeout=1.0)
                assert result  # Should succeed immediately
        finally:
            emitter.shutdown()

    def test_async_emitter_get_stats(self, async_config):
        emitter = AsyncEmitter(async_config)

        stats = emitter.get_stats()
        assert stats["pending"] == 0
        assert stats["success"] == 0
        assert stats["failed"] == 0

        r1 = Request("event-1", "", {}, run_id="", event_type="START")
        r2 = Request("event-2", "", {}, run_id="", event_type="START")

        # Add some events
        emitter.event_tracker.add_event(r1)
        emitter.event_tracker.add_event(r2)

        assert stats["pending"] == 2
        assert stats["success"] == 0
        assert stats["failed"] == 0

        emitter.event_tracker.mark_success(r1)
        emitter.event_tracker.mark_failed(r2)

        stats = emitter.get_stats()
        assert stats["pending"] == 0
        assert stats["success"] == 1
        assert stats["failed"] == 1

    def test_async_emitter_shutdown(self, async_config):
        emitter = AsyncEmitter(async_config)

        # Emitter should be running
        assert emitter.worker_thread.is_alive()

        # Mock wait_for_completion to return immediately
        with patch.object(emitter, "wait_for_completion", return_value=True) as mock_wait:
            result = emitter.shutdown(wait=True, timeout=1.0)

            assert result
            mock_wait.assert_called_once_with(1.0)

        # Signal shutdown for cleanup without blocking
        emitter.should_exit.set()


class TestHttpTransport:
    def test_http_transport_sync_emitter_selection(self):
        config = HttpConfig(url="http://example.com")  # async disabled by default
        transport = HttpTransport(config)

        assert isinstance(transport.emitter, SyncEmitter)

    def test_http_transport_async_emitter_selection(self):
        config = HttpConfig(url="http://example.com", async_config=AsyncConfig(enabled=True))
        transport = HttpTransport(config)

        assert isinstance(transport.emitter, AsyncEmitter)

        # Cleanup
        transport.shutdown()

    def test_http_transport_invalid_url(self):
        with pytest.raises(ValueError, match="Need valid url"):
            HttpTransport(HttpConfig(url="not-a-url"))

        with pytest.raises(ValueError, match="Need valid url"):
            HttpTransport(HttpConfig(url=""))

    def test_http_transport_emit_delegates_to_emitter(self):
        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)

        mock_event = MagicMock()

        with patch.object(transport.emitter, "emit") as mock_emit:
            transport.emit(mock_event)
            mock_emit.assert_called_once_with(mock_event)

    def test_http_transport_wait_for_completion_delegates(self):
        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)

        with patch.object(transport.emitter, "wait_for_completion", return_value=True) as mock_wait:
            result = transport.wait_for_completion(timeout=5.0)
            mock_wait.assert_called_once_with(5.0)
            assert result is True

    def test_http_transport_get_stats_delegates(self):
        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)

        expected_stats = {"pending": 1, "success": 2, "failed": 0}

        with patch.object(transport.emitter, "get_stats", return_value=expected_stats) as mock_stats:
            result = transport.get_stats()
            mock_stats.assert_called_once()
            assert result == expected_stats

    def test_http_transport_shutdown_delegates(self):
        config = HttpConfig(url="http://example.com", async_config=AsyncConfig(enabled=True))
        transport = HttpTransport(config)

        with patch.object(transport.emitter, "shutdown") as mock_shutdown:
            transport.shutdown(wait=True, timeout=10.0)
            mock_shutdown.assert_called_once_with(True, 10.0)


class TestHttpRequestPreparation:
    def test_prepare_request_basic(self):
        config = HttpConfig(url="http://example.com")
        emitter = SyncEmitter(config)

        event_json = '{"test": "event"}'
        body, headers = emitter._prepare_request(event_json)

        assert body == event_json
        assert headers["Content-Type"] == "application/json"

    def test_prepare_request_with_gzip(self):
        config = HttpConfig(url="http://example.com", compression=HttpCompression.GZIP)
        emitter = SyncEmitter(config)

        event_json = '{"test": "event"}'
        body, headers = emitter._prepare_request(event_json)

        assert isinstance(body, bytes)
        assert headers["Content-Type"] == "application/json"
        assert headers["Content-Encoding"] == "gzip"

        # Verify compression worked
        decompressed = gzip.decompress(body).decode("utf-8")
        assert decompressed == event_json

    def test_prepare_request_with_custom_headers(self):
        custom_headers = {"X-Custom": "value", "X-Another": "test"}
        config = HttpConfig(url="http://example.com", custom_headers=custom_headers)
        emitter = SyncEmitter(config)

        event_json = '{"test": "event"}'
        body, headers = emitter._prepare_request(event_json)

        assert headers["Content-Type"] == "application/json"
        assert headers["X-Custom"] == "value"
        assert headers["X-Another"] == "test"

    def test_prepare_request_with_auth(self):
        from openlineage.client.transport.http import ApiKeyTokenProvider

        auth = ApiKeyTokenProvider({"api_key": "test-key"})
        config = HttpConfig(url="http://example.com", auth=auth)
        emitter = SyncEmitter(config)

        event_json = '{"test": "event"}'
        body, headers = emitter._prepare_request(event_json)

        assert headers["Content-Type"] == "application/json"
        assert headers["Authorization"] == "Bearer test-key"


@pytest.mark.unit
class TestHttpMock:
    """Tests that test the complete HTTP transport flow"""

    @patch("httpx.Client")
    def test_client_with_http_transport_emits(self, mock_client_class):
        # Mock the context manager and post method
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
            },
        )
        transport = HttpTransport(config)

        client = OpenLineageClient(transport=transport)
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.datetime.now().isoformat(),
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="http", name="test"),
            producer="prod",
            schemaURL="schema",
        )

        client.emit(event)

        # Verify the post was called with correct parameters
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args

        assert call_args.kwargs["url"] == "http://backend:5000/api/v1/lineage"
        assert call_args.kwargs["headers"]["Content-Type"] == "application/json"

        # Verify the content is the serialized event
        actual_content = call_args.kwargs["content"]
        expected_content = Serde.to_json(event)
        assert actual_content == expected_content

    @patch("httpx.Client")
    def test_client_with_http_transport_emits_custom_endpoint(self, mock_client_class):
        # Mock the context manager and post method
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "endpoint": "custom/lineage",
            },
        )
        transport = HttpTransport(config)

        client = OpenLineageClient(transport=transport)
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.datetime.now().isoformat(),
            run=Run(runId=str(generate_new_uuid())),
            job=Job(namespace="http", name="test"),
            producer="prod",
            schemaURL="schema",
        )

        client.emit(event)

        # Verify the post was called with correct custom endpoint
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args.kwargs["url"] == "http://backend:5000/custom/lineage"

    @patch("httpx.Client")
    def test_client_with_http_transport_emits_with_gzip_compression(self, mock_client_class):
        # Mock the context manager and post method
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        config = HttpConfig.from_dict(
            {
                "type": "http",
                "url": "http://backend:5000",
                "compression": "gzip",
            },
        )
        transport = HttpTransport(config)

        client = OpenLineageClient(transport=transport)
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2024-04-12T18:04:58.134314",
            run=Run(runId="75782cf3-8be4-49dc-83e5-d2cf6239c168"),
            job=Job(namespace="http", name="test"),
            producer="prod",
            schemaURL="schema",
        )

        client.emit(event)

        # Verify compression was applied
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args

        headers = call_args.kwargs["headers"]
        assert headers["Content-Encoding"] == "gzip"

        content = call_args.kwargs["content"]
        assert isinstance(content, bytes)

        # Verify content can be decompressed and matches expected JSON
        decompressed = gzip.decompress(content)
        expected_json = (
            b'{"eventTime": "2024-04-12T18:04:58.134314", "eventType": "START", '
            b'"inputs": [], "job": {"facets": {}, "name": "test", "namespace": "http"}, "outputs": [], '
            b'"producer": "prod", "run": {"facets": {}, "runId": "75782cf3-8be4-49dc-83e5-d2cf6239c168"}, '
            b'"schemaURL": "schema"}'
        )
        assert decompressed == expected_json

    @patch("httpx.Client")
    def test_http_transport_custom_headers_applied(self, mock_client_class):
        # Mock the context manager and post method
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        custom_headers = {"X-Custom-Header": "CustomValue", "X-Another-Header": "AnotherValue"}
        config = HttpConfig(url="http://example.com", custom_headers=custom_headers)
        transport = HttpTransport(config)
        mock_event = MagicMock()

        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        headers = call_args.kwargs["headers"]

        assert headers["X-Custom-Header"] == "CustomValue"
        assert headers["X-Another-Header"] == "AnotherValue"

    @patch("httpx.Client")
    def test_http_transport_auth_and_custom_headers_applied(self, mock_client_class):
        # Mock the context manager and post method
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        custom_headers = {"X-Custom-Header": "CustomValue"}
        auth_token = "Bearer test_token"  # noqa: S105

        # Set up config with an ApiKeyTokenProvider
        config = HttpConfig(
            url="http://example.com",
            auth=ApiKeyTokenProvider({"api_key": "test_token"}),
            custom_headers=custom_headers,
        )

        transport = HttpTransport(config)
        mock_event = MagicMock()

        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        headers = call_args.kwargs["headers"]

        assert headers["Authorization"] == auth_token
        assert headers["X-Custom-Header"] == "CustomValue"

    @patch("httpx.Client")
    def test_http_transport_no_custom_headers(self, mock_client_class):
        # Mock the context manager and post method
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        config = HttpConfig(url="http://example.com")
        transport = HttpTransport(config)
        mock_event = MagicMock()

        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        headers = call_args.kwargs["headers"]

        # Only the Content-Type header should be set if no custom headers
        assert headers["Content-Type"] == "application/json"
        assert "X-Custom-Header" not in headers

    @patch("httpx.Client")
    def test_http_transport_compression_with_custom_headers(self, mock_client_class):
        # Mock the context manager and post method
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        custom_headers = {"X-Custom-Header": "CustomValue"}

        config = HttpConfig(
            url="http://example.com", compression=HttpCompression.GZIP, custom_headers=custom_headers
        )
        transport = HttpTransport(config)
        mock_event = MagicMock()

        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'), patch(
            "gzip.compress", return_value=b"compressed_data"
        ):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        headers = call_args.kwargs["headers"]
        content = call_args.kwargs["content"]

        assert headers["X-Custom-Header"] == "CustomValue"
        assert headers["Content-Encoding"] == "gzip"
        assert content == b"compressed_data"

    @patch("httpx.Client")
    @patch.dict(
        os.environ,
        {
            "OPENLINEAGE__TRANSPORT__TYPE": "http",
            "OPENLINEAGE__TRANSPORT__URL": "http://example.com",
            "OPENLINEAGE__TRANSPORT__CUSTOM_HEADERS__CUSTOM_HEADER": "FIRST",
            "OPENLINEAGE__TRANSPORT__CUSTOM_HEADERS__ANOTHER_HEADER": "second",
        },
    )
    def test_http_transport_with_custom_headers_from_env_vars(self, mock_client_class):
        # Mock the context manager and post method
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__enter__.return_value = mock_client
        mock_client_class.return_value.__exit__.return_value = None

        transport = OpenLineageClient().transport
        mock_event = MagicMock()

        with patch("openlineage.client.serde.Serde.to_json", return_value='{"mock": "event"}'):
            transport.emit(mock_event)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        headers = call_args.kwargs["headers"]

        assert headers["custom_header"] == "FIRST"
        assert headers["another_header"] == "second"
