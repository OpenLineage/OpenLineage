---
sidebar_position: 4
title: Reusable actions and common scripts
---
# Reusable actions and common scripts

## Reusable actions

### Run Event Validation

The `run_event_validation` action is a custom GitHub action that handles validation logic for OpenLineage events. Because OpenLineage events have a standardized structure, we provide a generic action that validates events against OpenLineage specifications.

The action 
- retrieves the OpenLineage specification for all releases defined in `release_tags`
- runs syntax validation
- runs validation with a use of [Event Comparison](#event-comparison)
- creates a comprehensive report with use of [Report](#report) 

**Inputs:**

| Name                | Description                                 | Required | Default |
|:--------------------|:--------------------------------------------|:---------|:--------|
| `release_tags`      | List of the spec versions to check against | false    | ""      |
| `ol_release`        | Release to run the validation with          | false    | ""      |
| `component_release` | Release of the component producing events   | false    | ""      |
| `target-path`       | Path to save the report to                  | true     | -       |
| `event-directory`   | Directory containing the events to validate | true     | -       |
| `producer-dir`      | Directory with producer definitions         | true     | -       |
| `component`         | Component name to use                       | true     | -       |

**Outputs:**

| Name          | Description              |
|:--------------|:-------------------------|
| `report_path` | Path to generated report |

#### Structuree

The action requires a specific directory structure for validation to work properly:

**Event Directory Structure:**
- **Root event directory** - Top-level directory containing scenario subdirectories
  - **Scenario subdirectories** - One directory per test scenario
    - **Generated event files** - Actual OpenLineage events produced by the component being tested
    - **File naming** - Events should be named descriptively (e.g., `job_start.json`, `job_complete.json`)
    - **Format** - All files must be valid JSON containing OpenLineage events

**Producer Directory Structure:**
- **Producer root** - Main directory for the producer component
  - **Scenarios directory** - Contains expected event definitions
    - **Scenario subdirectories** - Mirror the structure of event directory
      - **`config.json`** - Configuration file with test specifications and version constraints
      - **`events/`** - Directory containing expected OpenLineage event templates
        - **Expected event files** - Template events using Jinja functions for flexible validation
      - **`maintainers.json`** - File listing scenario maintainers
      - **`scenario.md`** - Documentation describing the test scenario

**Example Directory Layout:**
```
event-directory/
├── scenario1/
│   ├── job_start.json          # Generated events
│   └── job_complete.json
└── scenario2/
    ├── spark_read.json
    └── spark_write.json

producer-dir/
├── scenarios/
│   ├── scenario1/
│   │   ├── config.json         # Test configuration
│   │   ├── events/
│   │   │   ├── job_start.json  # Expected event template
│   │   │   └── job_complete.json
│   │   ├── maintainers.json
│   │   └── scenario.md
│   └── scenario2/
│       ├── config.json
│       ├── events/
│       │   ├── spark_read.json
│       │   └── spark_write.json
│       ├── maintainers.json
│       └── scenario.md
```

**Validation Process:**
- **Discovery** - Action scans event directory for scenario subdirectories
- **Matching** - For each scenario, finds corresponding producer scenario definition
- **Configuration Loading** - Reads scenario config.json for version constraints and test specifications
- **Event Pairing** - Matches generated events with expected event templates by filename
- **Validation Execution** - Runs comparison between generated and expected events
- **Report Generation** - Compiles results into comprehensive compatibility report










### Get OpenLineage Artifacts

Action that downloads OpenLineage artifacts from either the latest OpenLineage builds or Maven repository. If `get-latest-snapshots` is true, the action attempts to get each non-skipped artifact from the latest build. If that fails, it falls back to getting the artifact from Maven Central using the version specified in `version`.

**Inputs:**

| Name                   | Description                                                                                                                   | Required | Default |
|:-----------------------|:------------------------------------------------------------------------------------------------------------------------------|:---------|:--------|
| `get-latest-snapshots` | First try to download artifacts from OpenLineage builds, rather than Maven repository                                        | false    | false   |
| `version`              | OpenLineage artifact version to use if `get-latest-snapshots` is false or artifact is unavailable in latest build artifacts | true     |         |
| `skip-spark`           | Skip Spark integration download                                                                                               | false    | false   |
| `skip-java`            | Skip Java client download                                                                                                     | false    | false   |
| `skip-flink`           | Skip Flink integration download                                                                                               | false    | false   |
| `skip-sql`             | Skip SQL interface download                                                                                                   | false    | false   |
| `skip-extensions`      | Skip extensions download                                                                                                      | false    | false   |
| `skip-gcp-lineage`     | Skip GCP-lineage transport download                                                                                           | false    | false   |
| `skip-gcs`             | Skip GCS transport download                                                                                                   | false    | false   |
| `skip-s3`              | Skip S3 transport download                                                                                                    | false    | false   |

**Outputs:**

| Name          | Description                                            |
|:--------------|:-------------------------------------------------------|
| `spark`       | File path of the downloaded openlineage-spark jar     |
| `java`        | File path of the downloaded openlineage-java jar      |
| `flink`       | File path of the downloaded openlineage-flink jar     |
| `sql`         | File path of the downloaded openlineage-sql-java jar  |
| `extensions`  | File path of the downloaded openlineage-extensions jar|
| `gcp-lineage` | File path of the downloaded transports-gcp-lineage jar|
| `gcs`         | File path of the downloaded transports-gcs jar        |
| `s3`          | File path of the downloaded transports-s3 jar         |


## Common scripts

### Event Comparison

Events are compared using the `compare_events.py` script, which iterates through the expected JSON and for each defined field checks if there is a corresponding one in the result file. Helper Jinja functions are defined to improve test coverage.

Value functions are used in example events to substitute exact values:

- `any` - If the key has any value defined
- `is_datetime` - Field value is a parsable datetime
- `is_uuid` - Field value is a UUID
- `contains` - Field value contains the exact string
- `match` - Field value matches the given regex
- `not_match` - Field value doesn't match the given regex
- `one_of` - Field value is one of the given values

key functions

- `key_not_defined` - key is not defined
- `unordered_list` - for every element of expected array it checks if any of the elements in result array matches
  instead of comparing elements on the same indexes

#### Event structure

Example structure of expected json

```json
{
  "eventTime": "{{ is_datetime(result) }}",
  "eventType": "{{ one_of(result, 'RUNNING', 'COMPLETE') }}",
  "run": {
    "runId": "{{ is_uuid(result) }}",
    "facets": {
      "{{ key_not_defined(result, 'parent') }}": {}
    }
  },
  "job": {
    "namespace": "Example Namespace",
    "name": "Example Name"
  },
  "outputs": [
    {
      "namespace": "hdfs://dataproc-producer-test-m",
      "name": "/user/hive/warehouse/t2",
      "facets": {
        "columnLineage": {
          "fields": {
            "a": {
              "inputFields": [
                {
                  "namespace": "hdfs://dataproc-producer-test-m",
                  "name": "/user/hive/warehouse/t1",
                  "field": "a",
                  "{{ unordered_list(result, transformations) }} ": [
                    {
                      "type": "DIRECT",
                      "subtype": "TRANSFORMATION"
                    },
                    {
                      "type": "INDIRECT",
                      "subtype": "CONDITIONAL"
                    }
                  ]
                },
                {
                  "namespace": "hdfs://dataproc-producer-test-m",
                  "name": "/user/hive/warehouse/t1",
                  "field": "a"
                }
              ]
            }
          }
        }
      }
    }
  ]
}
```

### Report

The `scripts/report.py` provides a structured representation of test results using Python classes