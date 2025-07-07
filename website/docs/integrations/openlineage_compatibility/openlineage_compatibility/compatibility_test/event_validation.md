---
sidebar_position: 4
title: Event validation
---

# Event validation

json event validation is the base of our test suite
we can differentiate 3 general types of validation

1. Producer validation
2. Consumer Validation
3. Consumer Input Events validation

## Scenarios validation

Because in case of Producer and Consumer Input events we are validating OL events we were able to make it reusable as
github action.

### Reusable Actions

#### Run event validation

The `run_event_validation` action is a custom GitHub action that handles validation logic for OpenLineage events. 
Because the OpenLineage events have a standardized structure, we were able to provide generic action that takes

The action gets the OpenLineage spec for all releases defined in `release_tags`, runs validation and creates report.

inputs:

| name                | description                                 | required | default |
|:--------------------|:--------------------------------------------|:---------|:--------|
| `release_tags`      | list of the spec versions to check against  | false    | ""      |
| `ol_release`        | release to run the validation with          | false    | ""      |
| `component_release` | release of the component producing events   | false    | ""      |
| `target-path`       | Path to save the report to                  | true     | -       |
| `event-directory`   | Directory containing the events to validate | true     | -       |
| `producer-dir`      | directory with producer definitions         | true     | -       |
| `component`         | Component name to use                       | true     | -       |

outputs

| name          | description              |
|:--------------|:-------------------------|
| `report_path` | Path to generated report |

#### Get OpenLineage artifacts

Action that downloads the OpenLineage artifacts, it can either try to get latest OpenLineage build or from maven repository.
If `get-latest-snapshots` is true, the action will try to get each not-skipped artifact from latest build, if that fails it will fallback to getting artifact from maven central in version given in `version`

inputs:

| name                   | description                                                                                                                 | required | default |
|:-----------------------|:----------------------------------------------------------------------------------------------------------------------------|:---------|:--------|
| `get-latest-snapshots` | first try to download artifacts from OpenLineage builds, rather than maven repository                                       | false    | false   |
| `version`              | OpenLineage artifact version to use if `get-latest-snapshots` is false or artifact is unavailable in latest build artifacts | true     |         |
| `skip-spark`           | Skip spark integration download                                                                                             | false    | false   |
| `skip-java`            | Skip java client download                                                                                                   | false    | false   |
| `skip-flink`           | Skip spark integration download                                                                                             | false    | false   |
| `skip-sql`             | Skip sql interface download                                                                                                 | false    | false   |
| `skip-extensions`      | Skip extensions download                                                                                                    | false    | false   |
| `skip-gcp-lineage`     | Skip gcp-lineage transport download                                                                                         | false    | false   |
| `skip-gcs`             | Skip gcs transport download                                                                                                 | false    | false   |
| `skip-s3`              | Skip s3 transport download                                                                                                  | false    | false   |

outputs

| name          | description                                            |
|:--------------|:-------------------------------------------------------|
| `spark`       | File path of the downloaded openlineage-java jar       |
| `java`        | File path of the downloaded openlineage-spark jar      |
| `flink`       | File path of the downloaded openlineage-flink jar      |
| `sql`         | File path of the downloaded openlineage-sql-java jar   |
| `extensions`  | File path of the downloaded openlineage-extensions jar |
| `gcp-lineage` | File path of the downloaded transports-gcp-lineage jar |
| `gcs`         | File path of the downloaded transports-gcs jar         |
| `s3`          | File path of the downloaded transports-s3 jar          |

### Structure

***TODO: FINISH THE STRUCTURE DESCRIPTION***
each component tests should result in OpenLineage events which should be stored in directories with corresponding names

## Event Comparison

events are compared using compare_events.py script, which goes through expected json and for each defined filed checks
if there is corresponding one in result file
There are helper jinja functions defined to improve coverage

value functions are used in example events to substitute exact value

- `any` - if the key has any value defined
- `is_datetime` - field value is parsable datetime
- `is_uuid` - field value is UUID
- `contains` - field value contains the exact string
- `match` - field value matches given regex
- `not_match` - field value doesn't match given regex
- `one_of` - field value is one of given values

key functions

- `key_not_defined` - key is not defined
- `unordered_list` - for every element of expected array it checks if any of the elements in result array matches
  instead of comparing elements on the same indexes

### Event structure

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


