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
Create configuration file with test metadata. 

<details>
<summary><strong>File: `config.json`</strong> (Consumer-Specific Configuration)</summary>

```json
{
  "tests": [
    {
      "name": "name",
      "path": "path/to/example_entity.json",
      "entity": "entity_type",
      "tags": {
        "facets": ["example_facet"],
        "producer": "example_producer"
      }
    },
    ...
  ]
}
```

</details>

## Step 3: Define Maintainers
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

## Expected Consumer Behavior
Expected entities:
- ExpectedEntity

## Facets Tested
- ExampleFacet

```

</details>


## Step 8: Create Expected Validation Files
Create the expected consumer system state after event ingestion:

<details>
<summary><strong>Directory Structure</strong></summary>

```bash
mkdir -p validation/{processes,runs,datasets,lineage_events,links}
mkdir -p api_state
```

</details>

