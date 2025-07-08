# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from utils.event_validation import validate_event_schema


class TestStructuredLogs:
    """Test dbt structured logs mode with detailed event validation."""

    def test_structured_logs_event_hierarchy(self, dbt_runner, reset_test_server):
        """Test structured logs mode with detailed event hierarchy validation."""
        # Clear any existing events (done by reset_test_server fixture)
        result = dbt_runner.run_dbt_command(["seed"])
        assert result["success"], f"dbt seed failed: {result['output']}"

        # Run with structured logs mode using dbt-ol for a single model
        result = dbt_runner.run_dbt_ol_command(
            ["--consume-structured-logs", "run", "--select", "stg_customers"]
        )
        assert result["success"], f"dbt-ol run with structured logs failed: {result['output']}"
        print(result["output"])

        # Get events
        events = dbt_runner.get_events()
        print(f"Total events received: {len(events)}")

        # Should have at least 6 events
        assert len(events) >= 6, f"Expected at least 6 events, got {len(events)}"

        # Validate all events have proper schema
        for event in events:
            assert validate_event_schema(event), f"Invalid event schema: {event}"

        # Categorize events by JobTypeJobFacet
        job_events = []
        model_events = []
        sql_events = []

        for event in events:
            job_facets = event.get("job", {}).get("facets", {})
            job_type_facet = job_facets.get("jobType", {})
            job_type = job_type_facet.get("jobType") if job_type_facet else None

            if job_type == "JOB":
                job_events.append(event)
            elif job_type == "MODEL":
                model_events.append(event)
            elif job_type == "SQL":
                sql_events.append(event)
            else:
                print(f"Warning: Event without recognized jobType: {job_type}")

        print(f"Job events: {len(job_events)}")
        print(f"Model events: {len(model_events)}")
        print(f"SQL events: {len(sql_events)}")

        # Should have exactly 2 job-level events (START and COMPLETE)
        assert len(job_events) == 2, f"Expected 2 job events, got {len(job_events)}"
        job_start_events = [e for e in job_events if e["eventType"] == "START"]
        job_complete_events = [e for e in job_events if e["eventType"] == "COMPLETE"]
        assert len(job_start_events) == 1, f"Expected 1 job START event, got {len(job_start_events)}"
        assert len(job_complete_events) == 1, f"Expected 1 job COMPLETE event, got {len(job_complete_events)}"

        # Should have exactly 2 model-level events (START and COMPLETE)
        assert len(model_events) == 2, f"Expected 2 model events, got {len(model_events)}"
        model_start_events = [e for e in model_events if e["eventType"] == "START"]
        model_complete_events = [e for e in model_events if e["eventType"] == "COMPLETE"]
        assert len(model_start_events) == 1, f"Expected 1 model START event, got {len(model_start_events)}"
        assert (
            len(model_complete_events) == 1
        ), f"Expected 1 model COMPLETE event, got {len(model_complete_events)}"

        # Rest should be SQL events (at least 2, likely more for START/COMPLETE pairs)
        assert len(sql_events) >= 2, f"Expected at least 2 SQL events, got {len(sql_events)}"
        sql_start_events = [e for e in sql_events if e["eventType"] == "START"]
        sql_complete_events = [e for e in sql_events if e["eventType"] == "COMPLETE"]
        assert len(sql_start_events) >= 1, f"Expected at least 1 SQL START event, got {len(sql_start_events)}"
        assert (
            len(sql_complete_events) >= 1
        ), f"Expected at least 1 SQL COMPLETE event, got {len(sql_complete_events)}"

        # Get job run ID for parent validation
        job_run_id = job_events[0]["run"]["runId"]

        # Validate model events have ParentRunFacet pointing to job events
        for model_event in model_events:
            run_facets = model_event.get("run", {}).get("facets", {})
            parent_run_facet = run_facets.get("parent", {})

            assert parent_run_facet, f"Model event missing ParentRunFacet: {model_event}"
            parent_run_id = parent_run_facet.get("run", {}).get("runId")
            assert (
                parent_run_id == job_run_id
            ), f"Model event parent run ID {parent_run_id} doesn't match job run ID {job_run_id}"

        # Get model run ID for SQL parent validation
        model_run_id = model_events[0]["run"]["runId"]

        # Validate SQL events have ParentRunFacet pointing to model events
        for sql_event in sql_events:
            run_facets = sql_event.get("run", {}).get("facets", {})
            parent_run_facet = run_facets.get("parent", {})

            assert parent_run_facet, f"SQL event missing ParentRunFacet: {sql_event}"
            parent_run_id = parent_run_facet.get("run", {}).get("runId")
            assert (
                parent_run_id == model_run_id
            ), f"SQL event parent run ID {parent_run_id} doesn't match model run ID {model_run_id}"

        # Validate event ordering (START before COMPLETE for each level)
        for level_events, level_name in [(job_events, "job"), (model_events, "model"), (sql_events, "sql")]:
            start_events = [e for e in level_events if e["eventType"] == "START"]
            complete_events = [e for e in level_events if e["eventType"] == "COMPLETE"]

            if start_events and complete_events:
                start_time = start_events[0]["eventTime"]
                complete_time = complete_events[0]["eventTime"]
                assert (
                    start_time < complete_time
                ), f"{level_name} START event should come before COMPLETE event"

        print("✅ All event hierarchy validations passed!")

        # Print summary for debugging
        print("\n=== Event Summary ===")
        print(
            f"Job events: {len(job_events)} (START: {len(job_start_events)}, "
            f"COMPLETE: {len(job_complete_events)})"
        )
        print(
            f"Model events: {len(model_events)} (START: {len(model_start_events)}, "
            f"COMPLETE: {len(model_complete_events)})"
        )
        print(
            f"SQL events: {len(sql_events)} (START: {len(sql_start_events)}, "
            f"COMPLETE: {len(sql_complete_events)})"
        )
        print(f"Total events: {len(events)}")
