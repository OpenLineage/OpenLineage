---
sidebar_position: 2
title: Structure
---

# Structure

## Producer

Contains files and directories related to a specific producer. Each producer should contain:
- `runner` directory containing files necessary to run tests
- `scenarios` directory containing scenario directories
- `maintainers.json` file with the list of people to notify in case of component failures
- `versions.json` file with supported OpenLineage and component versions


<details>
<summary>producer catalog structure</summary>
```text
producer
└── example_producer
    ├── maintainers.json
    ├── versions.json
    ├── runner
    │   └── ...
    └── scenarios
        ├── ...
        └── example_scenario
            ├── config.json
            ├── events
            │   ├── ...
            │   └── expected_event_structure_1.json
            ├── maintainers.json
            ├── scenario.md
            └── test
                └── scenario_test_script
```
</details>


### Runner

Contains any scripts or resources necessary to run the producer tests.

### Scenarios

The scenarios directory contains one or more subdirectories, each containing files related to a particular test scenario:
- `config.json` file with the scenario configuration
- `scenario.md` file with description of the scenario  
- `maintainers.json` file with the list of people responsible for the scenario


#### Config

Each config file contains metadata for the tests in the scenario. There are three types of metadata:

1. **Scenario scope config**
   - **Scenario version filters**: We may want to test many versions of the producer against many versions of OpenLineage, but not every test scenario needs to run for every version. These filters allow us to define minimum and maximum versions of OpenLineage or producer for which we want to run the scenario.

2. **Test scope configs**
   - **name**: Name of the test
   - **path**: Path to expected event this test will use
   - **test version filters**: Define minimum and maximum versions of OpenLineage or producer. Semantic tests for filtered out tests will be skipped.

3. **Test tags**: They will be present in the report and reflected in compatibility tables
   - **facets**: List of facets that the test checks
   - **lineage level**: Indicates dataset lineage level
     - `dataset` → No column level lineage available
     - `column` → Column level lineage available  
     - `transformation` → Transformation info available

<details>
<summary>Example config</summary>

```json
{
  "component_versions": {
    "min": "0.0.1",
    "max": "9.99.9"
  },
  "openlineage_versions": {
    "min": "0.0.1",
    "max": "9.99.9"
  },
  "tests": [
    {
      "name": "name",
      "path": "path/to/file.json",
      "component_versions": {
        "min": "0.0.1",
        "max": "9.99.9"
      },
      "openlineage_versions": {
        "min": "0.0.1",
        "max": "9.99.9"
      },
      "tags": {
        "facets": [
          "list",
          "of",
          "supported",
          "facets"
        ],
        "lineage_level": {
          "bigquery": [
            "dataset",
            "column",
            "transformation"
          ]
        }
      }
    }
  ]
}

```
</details>

#### Events

Directory contains expected events in the form of JSON files. More information on setting up the events for validation can be found in [Event validation](reusable_actions_and_common_scripts.md#event-comparison).

## Consumer

Consumer directory contains two subdirectories for:

- `consumers` - with list of consumers and their test scenarios
- `scenarios` - scenario input events that are used in test, the directory is in separate location from the consumer definitions so the events can be used by multiple consumers for testing

Each directory in `scenarios` has following content:
- `events` - directory containing openlineage events to use in consumer tests
- `maintainers.json` - file with the list of people responsible for the scenario events
- `scenario.md` - human-readable description of the scenario events (producer type, inputs, outputs, facets, executed operations)

Each directory represents a consumer and contains:

- `validator` - directory with the validation logic (unlike producers where produced Openlineage events can be validated
  by generic component)
- `mapping.json` - file with the mapping between Openlineage events and consumer API entities
- `maintainers.json` - file with the list of people responsible for the component
- `scenarios` - directory containing scenario directories with following structure:
    - `config.json`-  file with the scenario configuration
    - `scenario.md` - human-readable description of the scenario (expected change in consumer state)
    - `maintainers.json` - file with the list of people responsible for the scenario
    - `validation` - directory with expected state of consumer API to validate against

<details>
<summary>consumer catalog structure</summary>
```text
consumer
├── consumers
│   └── <consumer name>
│       ├── README.md
│       ├── maintainers.json
│       ├── mapping.json
│       ├── run_dataplex_tests.sh
│       ├── scenarios
│       │   ├── ...
│       │   └── <scenario name>
│       │       └── api_state
│       │           ├── config.json
│       │           ├── maintainers.json
│       │           ├── scenario.md
│       │           └── validation
│       │               ├── ...
│       │               └── validation_file
│       └── validator
│           └── validator.py
└── scenarios
    ├── ...
    └── <scenario name>
        ├── config.json
        ├── events
        │   ├── ...
        │   └── openlineage_event.json
        ├── maintainers.json
        └── scenario.md
```
</details>


### Validator

Contains any scripts or resources necessary to run the consumer tests. 

### Scenarios
The scenarios directory contains input events defined for use by any consumer to run tests. Each of the scenarios contains:

- directory with event files
- `maintainers.json` file with the list of people responsible for the scenario
- `scenario.md` file with the scenario description containing information about the events that would be useful for the consumer scenario creators to know (e.g., which producer created them, what they represent, etc.)

#### Config

<details>
<summary><strong>Example consumer scenario config</strong></summary>

```json
{
  "tests": [
    {
      "name": "name",
      "path": "path/to/file.json",
      "entity": "entity",
      "tags": {
        "facets": [
          "list",
          "of",
          "supported",
          "facets"
        ],
        "producer": "producer"
      }
    }
  ]
}
```
</details>


Each config file contains metadata of the tests for the scenario, unlike producer scenarios, we can decide which scenario do we want to run on the level of defining said scenario for existing input events. So all configurations are on the scope of test.
1. Configs
   1. name - name of the test
   2. path - path to expected event this test will use
   3. entity - hint which entities this test covers
2. Test tags - they will be present in the report and will be reflected in compatibility tables
   1. facets - list of facets that the test checks
   2. producer - name of the producer of the events

#### Validation

Directory contains json files representing the expected consumer state after sending OpenLineage events. 
The events can be either exact expected state or use methods defined in [Event Comparison](reusable_actions_and_common_scripts.md#event-comparison).


#### Mapping

Mapping file contains the mapping between OpenLineage events and consumer API entities. It has two functions, first is
documentation, for anyone to know how much information is extracted form OpenLineage events by this consumer. Second is
defining basic expectations for tests i.e. if the tests claim support of particular facet then we can check which entities we should expect in this test.

If possible, the file should contain the list of mapped entities as well as list of facets that are not mapped.

<details>
<summary><strong>Example mapping structure</strong></summary>

```json 
{
  "mapped": {
    "core": {
      "eventTime": "Consumer entity representing event time",
      "run.id": "Consumer entity ID",
      "job.name": "part of consumer entity name",
      "job.namespace": "part of consumer entity name",
      ...
    },
    "ExampleFacet": {
      "field1": "Consumer entity field",
      "field2": "Consumer entity field"  
    },
    ...
  },
  "knownUnmapped": {
    "ExampleUnmappedFacet": ["*"],
    ...
  }
}
```
</details>

## Helper Scripts

Directory contains scripts used by the workflow, internal scripts used by actions and common classes used by producer and consumer tests. 

## Generated files

Contains files that are automatically generated or updated by the workflows.

### Report

`report.json` contains all the test results. It's main uses are:
1. checking for new failures - we want to send notifications about failures, but if the same failure happens on multiple runs, we don't want to repeat those notification. 
So each time the failures in tests are compared with failures that are already in the report. If failure is already in the report, we don't notify about it.
2. input for compatibility tables - the report file is used to generate compatibility tables as the most complete source of truth we have.

```json
{
  "name": "component name",
  "component_type": "[producer|consumer]",
  "component_version": "1.2.3",
  "openlineage_version": "1.2.3",
  "scenarios": [
    {
      "name": "hive",
      "status": "[SUCCESS|FAILURE]",
      "tests": [
        {
          "name": "test_name",
          "status": "[SUCCESS|FAILURE]",
          "validation_type": "[syntax|semantics]",
          "entity_type": "[openlineage|consumer_entity_type]",
          "details": [],
          "tags": {}
        }
      ]
    }
  ]
}
```


### Releases and Spec versions

To check for changes in spec or new releases we need to store information about latest versions we already checked.
The `releases.json` stores information about which release of OpenLineage or Components we last checked for.

<details>
<summary><strong>Example release entries</strong></summary>

```json
[
  {
    "name": "openlineage",
    "latest_version": "1.2.3" // latest checked release
  },
  {
    "name": "versioned component",
    "latest_version": "1.2.3" // latest checked release
  },
  {
    "name": "non-versioned component",
    "latest_version": "" // no release meaning we check on each run of the workflow
  }
]
```

</details>


The `spec_versions.json` stores information about which are the latest checked versions of spec and facets.

<details>
<summary><strong>Example spec version entries</strong></summary>

```json
{
  "OpenLineage": {
    "major": "1",
    "minor": "2",
    "patch": "3"
  },
  "ExampleFacet": {
    "major": "1",
    "minor": "2",
    "patch": "3"
  }
}
```

</details>













