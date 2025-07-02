# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing

import pytest
from openlineage.client.transport.async_http import AsyncHttpConfig, AsyncHttpTransport

from tests.fixtures.events import (
    BASIC_RUN_EVENTS,
    FAILED_RUN_EVENTS,
    create_multiple_runs,
    create_run_lifecycle_events,
)


@pytest.mark.integration
class TestBasicEventFlow:
    """Test basic event emission and reception."""

    def test_sync_transport_single_event(self, sync_transport, server_helper):
        """Test synchronous transport emits single event successfully."""
        event = BASIC_RUN_EVENTS["start"]

        # Emit event
        response = sync_transport.emit(event)
        assert response is not None

        # Verify event was received (reduced timeout)
        assert server_helper.wait_for_events(1, timeout=0.5)

        events = server_helper.get_events()
        assert len(events) == 1

        received_event = events[0]
        assert received_event["run_id"] == event.run.runId
        assert received_event["event_type"] == "START"
        assert received_event["job_name"] == event.job.name
        assert received_event["job_namespace"] == event.job.namespace
        assert len(received_event["validation_errors"]) == 0

    def test_sync_transport_error_handling(self, sync_transport, server_helper):
        """Test synchronous transport handles HTTP errors properly."""
        # Configure server to return 500 error
        server_helper.simulate_error_sequence([500], enabled=True)

        event = BASIC_RUN_EVENTS["start"]

        # Sync transport should raise an exception when encountering HTTP errors
        with pytest.raises(Exception):  # Expecting an exception to be raised
            sync_transport.emit(event)

        # Reset error simulation
        server_helper.simulate_error_sequence([], enabled=False)

        # Verify that no events were successfully processed due to server errors
        time.sleep(0.1)  # Brief wait for any potential processing
        events = server_helper.get_events()
        # With persistent 500 errors, we expect 0 successful events
        assert len(events) == 0

    def test_sync_transport_multiple_events_sequential(self, sync_transport, server_helper):
        """Test synchronous transport processes multiple events sequentially."""
        # Create a complete run lifecycle
        start_event = BASIC_RUN_EVENTS["start"]
        complete_event = BASIC_RUN_EVENTS["complete"]

        # Emit events sequentially (sync transport processes immediately)
        response1 = sync_transport.emit(start_event)
        response2 = sync_transport.emit(complete_event)

        assert response1 is not None
        assert response2 is not None

        # Verify both events were received
        assert server_helper.wait_for_events(2, timeout=1.0)

        events = server_helper.get_events()
        assert len(events) == 2

        # Verify event order and content
        run_id = start_event.run.runId
        run_events = [e for e in events if e["run_id"] == run_id]
        assert len(run_events) == 2

        # Check that we have both START and COMPLETE events
        event_types = {event["event_type"] for event in run_events}
        assert event_types == {"START", "COMPLETE"}

        # Verify no validation errors
        for event in run_events:
            assert len(event["validation_errors"]) == 0

    def test_async_transport_single_event(self, async_transport, server_helper):
        """Test asynchronous transport emits single event successfully."""
        event = BASIC_RUN_EVENTS["start"]

        # Emit event
        async_transport.emit(event)

        # Wait for processing (reduced timeout)
        assert async_transport.wait_for_completion(timeout=0.5)

        # Verify event was received (reduced timeout)
        assert server_helper.wait_for_events(1, timeout=0.5)

        events = server_helper.get_events()
        assert len(events) == 1

        received_event = events[0]
        assert received_event["run_id"] == event.run.runId
        assert received_event["event_type"] == "START"
        assert len(received_event["validation_errors"]) == 0

    @pytest.mark.parametrize(
        "event_type,events_key", [("successful", "BASIC_RUN_EVENTS"), ("failed", "FAILED_RUN_EVENTS")]
    )
    def test_run_lifecycle_events(self, async_transport, server_helper, event_type, events_key):
        """Test complete run lifecycle patterns."""
        if events_key == "BASIC_RUN_EVENTS":
            start_event = BASIC_RUN_EVENTS["start"]
            end_event = BASIC_RUN_EVENTS["complete"]
            expected_end_type = "COMPLETE"
        else:
            start_event = FAILED_RUN_EVENTS["start"]
            end_event = FAILED_RUN_EVENTS["fail"]
            expected_end_type = "FAIL"

        # Emit events
        async_transport.emit(start_event)
        async_transport.emit(end_event)

        # Wait for processing (reduced timeout)
        assert async_transport.wait_for_completion(timeout=2.0)

        # Verify both events received (reduced timeout)
        assert server_helper.wait_for_events(2, timeout=2.0)

        # Check run sequence
        sequence = server_helper.get_run_sequence(start_event.run.runId)
        assert sequence["total_events"] == 2
        assert sequence["has_start_event"] is True
        assert len(sequence["ordering_issues"]) == 0

        # Verify event order
        events = sequence["sequence"]
        assert events[0]["event_type"] == "START"
        assert events[1]["event_type"] == expected_end_type


@pytest.mark.integration
class TestAsyncEventOrdering:
    """Test async event ordering guarantees."""

    def test_multiple_completion_events(self, async_transport_custom, server_helper):
        """Test multiple completion events for same run."""
        transport = async_transport_custom(max_concurrent_requests=2)

        with closing(transport):
            run_events = create_run_lifecycle_events()
            start_event = run_events["start"]
            complete_event = run_events["complete"]

            # Create a second complete event with different timestamp
            complete_event2 = create_run_lifecycle_events(
                run_id=start_event.run.runId, job_name=start_event.job.name
            )["complete"]

            # Emit completion events first
            transport.emit(complete_event)
            transport.emit(complete_event2)
            time.sleep(0.1)
            transport.emit(start_event)

            # Wait for processing
            assert transport.wait_for_completion(timeout=5.0)

            # Verify all events received
            assert server_helper.wait_for_events(3, timeout=5.0)

            # Check sequence
            sequence = server_helper.get_run_sequence(start_event.run.runId)
            assert sequence["total_events"] == 3
            assert sequence["has_start_event"] is True

    @pytest.mark.parametrize("num_runs", [2, 3, 5])
    def test_concurrent_runs_ordering(self, async_transport_custom, server_helper, num_runs):
        """Test event ordering across multiple concurrent runs."""
        transport = async_transport_custom(max_concurrent_requests=5)

        with closing(transport):
            multiple_runs = create_multiple_runs(num_runs=num_runs)

            # Emit all events in mixed order
            for run_events in multiple_runs:
                # Emit COMPLETE first, then START for each run
                transport.emit(run_events["complete"])

            time.sleep(0.1)

            for run_events in multiple_runs:
                transport.emit(run_events["start"])

            # Wait for processing
            assert transport.wait_for_completion(timeout=10.0)

            # Verify all events received
            expected_events = num_runs * 2
            assert server_helper.wait_for_events(expected_events, timeout=5.0)

            # Check ordering for each run
            for run_events in multiple_runs:
                run_id = run_events["start"].run.runId
                sequence = server_helper.get_run_sequence(run_id)

                assert sequence["total_events"] == 2
                assert sequence["has_start_event"] is True
                assert len(sequence["ordering_issues"]) == 0

                # Since we emit COMPLETE before START, we expect the order to reflect
                # the actual emission order, not a logical order
                events = sequence["sequence"]
                event_types = [event["event_type"] for event in events]
                assert "START" in event_types
                assert "COMPLETE" in event_types


@pytest.mark.integration
class TestErrorHandling:
    """Test error handling and retry behavior."""

    @pytest.mark.parametrize(
        "sequence,expected_success,expected_retries",
        [
            ([500, 500, 200], True, 2),  # Server errors should be retried, succeed on 3rd attempt
            ([502, 502, 200], True, 2),  # Bad gateway should be retried, succeed on 3rd attempt
            ([503, 503, 200], True, 2),  # Service unavailable should be retried, succeed on 3rd attempt
            ([400], False, 0),  # Client errors should not be retried
            ([401], False, 0),  # Unauthorized should not be retried
            ([404], False, 0),  # Not found should not be retried
            ([500, 500, 500, 500, 500], False, 3),  # Exhaust retries and fail
        ],
    )
    def test_error_retry_behavior(
        self, async_transport, server_helper, sequence, expected_success, expected_retries
    ):
        """Test retry behavior with deterministic error sequences."""
        # Configure server to return specified error sequence
        server_helper.simulate_error_sequence(sequence, enabled=True)

        event = BASIC_RUN_EVENTS["start"]

        # Emit event
        async_transport.emit(event)

        # Wait for processing
        timeout = 10.0 if expected_retries > 0 else 5.0
        completed = async_transport.wait_for_completion(timeout=timeout)

        # For tests that should fail (like exhaust retries), wait_for_completion might timeout
        # but that's expected behavior for persistent failures
        if expected_success:
            assert completed, "Transport should complete successfully"
        # For failed cases, we allow either completion (fast failure) or timeout (retry exhaustion)

        # Check results
        transport_stats = async_transport.get_stats()

        if expected_success:
            assert transport_stats["success"] == 1
            assert transport_stats["failed"] == 0
            assert transport_stats["pending"] == 0
        else:
            # For failure cases, we should have no successful requests
            # Either failed >= 1 (fast failure) or pending >= 1 (retry exhaustion/timeout)
            assert transport_stats["success"] == 0
            assert transport_stats["failed"] >= 1 or transport_stats["pending"] >= 1

        # Reset error simulation
        server_helper.simulate_error_sequence([], enabled=False)

    def test_timeout_handling(self, async_transport_custom, server_helper):
        """Test timeout handling."""
        # Create transport with short timeout
        transport = async_transport_custom(timeout=0.5, backoff_factor=0.1, retry_count=3)

        with closing(transport):
            # Configure server with long delay that will cause timeout
            logging.getLogger("openlineage.client").debug(
                server_helper.simulate_error_sequence([0], enabled=True, delay_ms=2000)
            )  # 0 = timeout

            event = BASIC_RUN_EVENTS["start"]

            transport.emit(event)

            assert transport.wait_for_completion(timeout=5.0)
            assert transport.get_stats()["failed"] > 0


@pytest.mark.integration
class TestPerformanceAndLoad:
    """Test performance and load characteristics."""

    @pytest.mark.parametrize(
        "num_runs,max_concurrent",
        [
            (10, 5),  # Low load
            (20, 10),  # Medium load
            (50, 15),  # High load
        ],
    )
    def test_high_throughput_events(self, async_transport_custom, server_helper, num_runs, max_concurrent):
        """Test high throughput event emission with different loads."""
        transport = async_transport_custom(
            max_queue_size=num_runs * 3,  # Generous queue size
            max_concurrent_requests=max_concurrent,
        )

        with closing(transport):
            # Create many events
            multiple_runs = create_multiple_runs(num_runs=num_runs)

            # Emit all events quickly
            for run_events in multiple_runs:
                transport.emit(run_events["start"])
                transport.emit(run_events["complete"])

            # Wait for processing
            assert transport.wait_for_completion(timeout=5.0)

            # Verify all events received
            expected_events = num_runs * 2
            assert server_helper.wait_for_events(expected_events, timeout=5.0)

            stats = server_helper.get_stats()
            assert stats["total_events"] == expected_events

            transport_stats = transport.get_stats()
            assert transport_stats["success"] == expected_events
            assert transport_stats["failed"] == 0

    @pytest.mark.parametrize("num_clients", [3, 5, 8])
    def test_concurrent_clients(self, async_transport_custom, server_helper, num_clients):
        """Test multiple concurrent clients."""

        def emit_events(client_id: int):
            transport = async_transport_custom(max_concurrent_requests=3)
            with closing(transport):
                run_events = create_run_lifecycle_events(job_name=f"client-{client_id}-job")

                transport.emit(run_events["start"])
                transport.emit(run_events["complete"])

                assert transport.wait_for_completion(timeout=2.0)

                stats = transport.get_stats()
                return stats["success"]

        # Run concurrent clients
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(emit_events, client_id) for client_id in range(num_clients)]

            successful_events = 0
            for future in as_completed(futures):
                successful_events += future.result()

        # Verify all events received
        total_expected = num_clients * 2  # START + COMPLETE per client
        assert server_helper.wait_for_events(total_expected, timeout=3.0)

        stats = server_helper.get_stats()
        assert stats["total_events"] == total_expected
        assert successful_events == total_expected

    @pytest.mark.parametrize(
        "queue_size,concurrent_requests",
        [
            (5, 1),  # Very small queue, slow processing
            (10, 2),  # Small queue, limited concurrency
            (20, 5),  # Medium queue
        ],
    )
    def test_queue_saturation(self, async_transport_custom, server_helper, queue_size, concurrent_requests):
        """Test behavior when queue reaches capacity."""
        transport = async_transport_custom(
            max_queue_size=queue_size,
            max_concurrent_requests=concurrent_requests,
            timeout=0.5,
            retry_count=3,
            backoff_factor=0.1,
        )
        with closing(transport):
            # Emit more events than queue can handle
            num_events = queue_size + 1

            # Configure server with delay to slow processing and return 500 errors
            server_helper.simulate_error_sequence([500], enabled=True, delay_ms=100)
            runs = create_multiple_runs(num_runs=num_events)
            for run_events in runs:
                transport.emit(run_events["start"])

            # Wait for processing
            transport.wait_for_completion(timeout=15.0)

            # Some events may have been dropped due to queue saturation
            transport_stats = transport.get_stats()
            print(
                f"Queue {queue_size}: Success: {transport_stats['success']}, "
                f"Failed: {transport_stats['failed']}"
            )

            # At least some events should have been processed
            assert transport_stats["success"] + transport_stats["failed"] > 0


@pytest.mark.integration
class TestConfiguration:
    """Test different transport configurations."""

    def test_custom_endpoint(self, test_server_url, server_helper):
        """Test transport with custom endpoint."""
        # Note: Our test server only has /api/v1/lineage endpoint
        # This test verifies the URL construction works correctly
        config = AsyncHttpConfig(
            url=test_server_url,
            endpoint="api/v1/lineage",  # Explicit endpoint
        )
        transport = AsyncHttpTransport(config)

        with closing(transport):
            event = BASIC_RUN_EVENTS["start"]
            transport.emit(event)
            assert transport.wait_for_completion(timeout=5.0)

            # Verify event received
            assert server_helper.wait_for_events(1, timeout=5.0)

    @pytest.mark.parametrize(
        "custom_headers",
        [
            {"X-Test-Header": "test-value"},
            {"X-Client": "integration-test", "X-Version": "1.0"},
            {"Authorization": "Bearer fake-token", "X-Custom": "value"},
        ],
    )
    def test_custom_headers(self, test_server_url, server_helper, custom_headers):
        """Test transport with various custom headers."""
        config = AsyncHttpConfig(url=test_server_url, custom_headers=custom_headers)
        transport = AsyncHttpTransport(config)

        with closing(transport):
            event = BASIC_RUN_EVENTS["start"]
            transport.emit(event)
            assert transport.wait_for_completion(timeout=5.0)

            # Verify event received with custom headers
            assert server_helper.wait_for_events(1, timeout=5.0)

            events = server_helper.get_events()
            received_event = events[0]

            # Check that custom headers were included
            headers = received_event["headers"]
            for header_name, header_value in custom_headers.items():
                header_name_lower = header_name.lower()
                assert header_name_lower in headers
                assert headers[header_name_lower] == header_value

    @pytest.mark.parametrize(
        "max_queue_size,max_concurrent_requests",
        [
            (50, 2),
            (200, 8),
            (1000, 20),
        ],
    )
    def test_different_async_configs(
        self, test_server_url, server_helper, max_queue_size, max_concurrent_requests
    ):
        """Test different async configuration parameters."""
        config = AsyncHttpConfig(
            url=test_server_url,
            max_queue_size=max_queue_size,
            max_concurrent_requests=max_concurrent_requests,
        )
        transport = AsyncHttpTransport(config)

        with closing(transport):
            # Emit a test event
            event = create_run_lifecycle_events(
                job_name=f"config-test-{max_queue_size}-{max_concurrent_requests}"
            )["start"]
            transport.emit(event)
            assert transport.wait_for_completion(timeout=5.0)

            # Verify it was processed
            stats = transport.get_stats()
            assert stats["success"] >= 1
