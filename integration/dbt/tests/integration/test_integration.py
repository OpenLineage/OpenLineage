# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from utils.event_validation import (
    filter_events_by_job,
    get_events_by_type,
    validate_event_schema,
    validate_lineage_chain,
)


class TestDbtIntegration:
    """Integration tests for dbt OpenLineage integration."""

    def test_basic_seed_to_staging_pipeline(self, dbt_runner, reset_test_server):
        """Test basic seed to staging model pipeline."""
        # Clear any existing events (done by reset_test_server fixture)

        # Run seed command
        result = dbt_runner.run_dbt_command(["seed"])
        assert result["success"], f"dbt seed failed: {result['output']}"

        # Run staging models
        result = dbt_runner.run_dbt_ol_command(["run", "--select", "staging"])
        assert result["success"], f"dbt run failed: {result['output']}"

        # Get all events
        events = dbt_runner.get_events()
        assert len(events) > 0, "No events received"

        # Validate event schemas
        for event in events:
            assert validate_event_schema(event), f"Invalid event schema: {event}"

        # Check for expected models
        expected_models = [
            "test.main.openlineage_integration_test.stg_customers",
            "test.main.openlineage_integration_test.stg_orders",
            "test.main.openlineage_integration_test.stg_payments",
        ]
        assert validate_lineage_chain(events, expected_models), "Missing expected models in lineage"

    def test_full_pipeline_lineage(self, dbt_runner, reset_test_server):
        """Test full pipeline from seeds to marts."""
        # Clear any existing events (done by reset_test_server fixture)

        # Run seed command
        result = dbt_runner.run_dbt_command(["seed"])
        assert result["success"], f"dbt seed failed: {result['output']}"

        # Run all models
        result = dbt_runner.run_dbt_ol_command(["run"])
        assert result["success"], f"dbt run failed: {result['output']}"

        # Run tests
        result = dbt_runner.run_dbt_ol_command(["test"])
        assert result["success"], f"dbt test failed: {result['output']}"

        # Get all events
        events = dbt_runner.get_events()

        # Validate all expected models are present
        expected_models = [
            "test.main.openlineage_integration_test.stg_customers",
            "test.main.openlineage_integration_test.stg_orders",
            "test.main.openlineage_integration_test.stg_payments",
            "test.main.openlineage_integration_test.orders",
            "test.main.openlineage_integration_test.customers",
        ]
        for event in events:
            assert validate_event_schema(event), f"Invalid event schema: {event}"

        assert validate_lineage_chain(events, expected_models), "Missing expected models in lineage"

        # Check for START and COMPLETE events for each model
        for model in expected_models:
            model_events = filter_events_by_job(events, model)
            start_events = get_events_by_type(model_events, "START")
            complete_events = get_events_by_type(model_events, "COMPLETE")

            assert len(start_events) >= 1, f"Missing START event for {model}"
            assert len(complete_events) >= 1, f"Missing COMPLETE event for {model}"

    def test_empty_model_execution(self, dbt_runner, reset_test_server):
        """Test that empty models do not generate events."""
        # Clear any existing events (done by reset_test_server fixture)

        # Try to run a non-existent model (should not do anything)
        result = dbt_runner.run_dbt_ol_command(["run", "--select", "non_existent_model"])
        assert result["success"], "Expected command to succeed"

        # Get events
        events = dbt_runner.get_events()
        assert len(events) == 2, "Not only JOB events received"

    def test_dbt_tests_lineage(self, dbt_runner, reset_test_server):
        """Test that dbt tests generate lineage events."""
        # Clear any existing events (done by reset_test_server fixture)

        # First run models to ensure they exist
        result = dbt_runner.run_dbt_command(["seed"])
        assert result["success"], f"dbt seed failed: {result['output']}"

        result = dbt_runner.run_dbt_ol_command(["run"])
        assert result["success"], f"dbt run failed: {result['output']}"

        # Run tests
        result = dbt_runner.run_dbt_ol_command(["test"])
        assert result["success"], f"dbt test failed: {result['output']}"

        # Get events
        events = dbt_runner.get_events()

        # Should have events from both run and test commands
        assert len(events) > 0, "No events received"

        # Validate event schemas
        for event in events:
            assert validate_event_schema(event), f"Invalid event schema: {event}"

    def test_local_artifacts_mode(self, dbt_runner, reset_test_server):
        """Test local artifacts mode."""
        # Clear any existing events (done by reset_test_server fixture)

        # Run with local artifacts mode using dbt-ol (without --consume-structured-logs)
        result = dbt_runner.run_dbt_ol_command(["run", "--select", "stg_customers"])
        assert result["success"], f"dbt-ol run with local artifacts failed: {result['output']}"

        # Get events
        events = dbt_runner.get_events()

        # Should have events
        assert len(events) > 0, "No events received in local artifacts mode"

        # Validate event schemas
        for event in events:
            assert validate_event_schema(event), f"Invalid event schema: {event}"

    def test_event_schema_validation(self, dbt_runner, reset_test_server):
        """Test that all events conform to OpenLineage schema."""
        # Clear any existing events (done by reset_test_server fixture)
        result = dbt_runner.run_dbt_command(["seed"])
        assert result["success"], f"dbt seed failed: {result['output']}"

        # Run a simple command
        result = dbt_runner.run_dbt_ol_command(["run", "--select", "stg_customers"])
        assert result["success"], f"dbt run failed: {result['output']}"

        # Get events
        events = dbt_runner.get_events()

        # Validate each event
        for event in events:
            assert validate_event_schema(event), f"Invalid event schema: {event}"

            # Check for required facets
            assert "job" in event, "Missing job field"
            assert "run" in event, "Missing run field"
            assert "eventType" in event, "Missing eventType field"
            assert "eventTime" in event, "Missing eventTime field"

    def test_event_ordering(self, dbt_runner, reset_test_server):
        """Test that events are ordered correctly (START before COMPLETE)."""
        # Clear any existing events (done by reset_test_server fixture)
        result = dbt_runner.run_dbt_command(["seed"])
        assert result["success"], f"dbt seed failed: {result['output']}"

        # Run a single model
        result = dbt_runner.run_dbt_ol_command(["run", "--select", "stg_customers"])
        assert result["success"], f"dbt run failed: {result['output']}"

        # Get events
        events = dbt_runner.get_events()

        # Filter events for our model
        model_events = filter_events_by_job(events, "test.main.openlineage_integration_test.stg_customers")

        # Should have START and COMPLETE events
        start_events = get_events_by_type(model_events, "START")
        complete_events = get_events_by_type(model_events, "COMPLETE")

        assert len(start_events) >= 1, "Missing START event"
        assert len(complete_events) >= 1, "Missing COMPLETE event"

        # START should come before COMPLETE
        if start_events and complete_events:
            start_time = start_events[0]["eventTime"]
            complete_time = complete_events[0]["eventTime"]

            assert start_time < complete_time, "START event should come before COMPLETE event"
