---
sidebar_position: 5
title: New Consumer Scenario
---

# New Consumer Scenario

Consumer scenarios validate that OpenLineage events can be properly ingested and result in expected consumer state changes. Unlike producer scenarios that focus on event generation, consumer scenarios test the ingestion and transformation of existing OpenLineage events into consumer system entities.

The process follows these key steps: consume OpenLineage events → transform to consumer format → validate resulting state.

## Step 1: Create Consumer Scenario Directory
Navigate to the global consumer scenarios directory (shared across all consumers) and create your new scenario:

```bash
mkdir -p ./consumer/scenarios/my_scenario
cd ./consumer/scenarios/my_scenario
```

## Step 2: Create configuration file
Create a simple configuration file specifying the OpenLineage version used to generate the input events:

<details>
<summary><strong>File: `config.json`</strong> (Consumer-Specific Configuration)</summary>

```json
{
  "tests": [
    {
      "name": "processes",
      "path": "validation/processes/processes.json",
      "entity": "process",
      "tags": {
        "facets": ["run_event", "processing_engine"]
      }
    },
    ...
  ]
}
```

</details>

## Step 4: Define Maintainers
Create a maintainers file for the scenario:

<details>
<summary><strong>File: `maintainers.json`</strong></summary>

```json
[
  {
    "type": "author",
    "github-name": "your_github_user",
    "email": "your.email@example.com",
    "link": ""
  }
]
```

</details>

## Step 5: Document the Scenario
Create comprehensive documentation describing what the scenario tests:

<details>
<summary><strong>File: `scenario.md`</strong></summary>

```markdown
# My Scenario

## Description
Brief description of what this consumer scenario validates.

## Input Events
- **spark_bigquery_job.json**: Tests Spark to BigQuery lineage consumption
- **airflow_dag_run.json**: Tests Airflow workflow consumption

## Expected Consumer Behavior
Describe what should happen when consumers ingest these events:
- What entities should be created
- What relationships should be established
- What metadata should be preserved

## Facets Tested
- run_event
- dataSource
- schema
- processing_engine

## Consumer System Requirements
- List any special requirements for consumer systems
- Required permissions or configurations
```

</details>

## Step 6: Create Consumer-Specific Validation Data
Each consumer that supports this scenario must create consumer-specific validation data in their own scenarios directory:

```bash
mkdir -p ./consumer/consumers/example_consumer/scenarios/my_scenario
cd ./consumer/consumers/example_consumer/scenarios/my_scenario
```

## Step 7: Configure Consumer-Specific Tests
Create a consumer-specific configuration that defines what entities and validation files to expect:



## Step 8: Create Expected Validation Files
Create the expected consumer system state after event ingestion:

<details>
<summary><strong>Directory Structure</strong></summary>

```bash
mkdir -p validation/{processes,runs,datasets,lineage_events,links}
mkdir -p api_state
```

</details>

**Example Validation Files:**

<details>
<summary><strong>File: `validation/processes/processes.json`</strong></summary>

```json
[
  {
    "name": "projects/12345/locations/us/processes/abc123def456",
    "display_name": "spark_namespace:spark_job_name",
    "attributes": {
      "processing_engine": {
        "name": "spark",
        "version": "3.4.0",
        "openlineageAdapterVersion": "1.31.0"
      }
    }
  }
]
```

</details>

<details>
<summary><strong>File: `validation/runs/runs.json`</strong></summary>

```json
[
  {
    "name": "projects/12345/locations/us/processes/abc123def456/runs/run789xyz",
    "display_name": "run_12345abcd",
    "attributes": {
      "start_time": "2024-01-15T10:30:00Z",
      "end_time": "2024-01-15T10:45:00Z",
      "state": "COMPLETED"
    }
  }
]
```

</details>

<details>
<summary><strong>File: `validation/lineage_events/lineage_events.json`</strong></summary>

```json
[
  {
    "name": "projects/12345/locations/us/processes/abc123def456/runs/run789xyz/lineageEvents/event001",
    "source": {
      "fully_qualified_name": "bigquery://project.dataset.source_table"
    },
    "target": {
      "fully_qualified_name": "bigquery://project.dataset.target_table"
    },
    "event_time": "2024-01-15T10:30:00Z"
  }
]
```

</details>

## Step 9: Add Consumer Scenario Maintainers
Create maintainers file for the consumer-specific scenario:

<details>
<summary><strong>File: `maintainers.json`</strong></summary>

```json
[
  {
    "type": "maintainer",
    "github-name": "consumer_maintainer",
    "email": "maintainer@example.com",
    "link": ""
  }
]
```

</details>

## Step 10: Document Consumer-Specific Behavior
Create consumer-specific scenario documentation:

<details>
<summary><strong>File: `scenario.md`</strong></summary>

```markdown
# My Scenario - Example Consumer

## Consumer-Specific Behavior
How this consumer system processes the OpenLineage events from the global scenario.

## Entity Mapping
- **OpenLineage Job** → Consumer Process entity
- **OpenLineage Run** → Consumer Run entity  
- **OpenLineage Dataset** → Consumer Dataset entity
- **Input/Output relationships** → Consumer Lineage Events

## Validation Details
- **Processes**: Validates job metadata and processing engine information
- **Runs**: Validates execution state and timing information
- **Lineage Events**: Validates data flow relationships and transformations

## Known Limitations
- List any OpenLineage facets not supported by this consumer
- Any data transformations or losses during ingestion
```

</details>

## Key Differences from Producer Scenarios
- **Dual Configuration**: Both global scenario config (simple) and consumer-specific config (complex)
- **Input vs Output**: Consumes existing events rather than generating new ones
- **Entity Validation**: Validates consumer system entities rather than OpenLineage events
- **State-Based Testing**: Tests resulting system state rather than event structure
- **Cross-Consumer Sharing**: Global scenarios can be tested by multiple consumers

## Validation Process Flow
1. **Global Scenario**: Provides OpenLineage events and basic metadata
2. **Consumer Ingestion**: Consumer system processes the OpenLineage events
3. **Entity Creation**: Consumer creates system-specific entities (processes, runs, etc.)
4. **State Validation**: Compare actual consumer state against expected validation files
5. **Report Generation**: Generate compatibility report for the consumer scenario
