---
sidebar_position: 4
title: New Producer Scenario
---

# New Producer Scenario

Producer scenarios test specific use cases and validate OpenLineage event generation. The process is analogical to re, but with a more complex configuration structure that includes component version constraints and test specifications.

Each producer scenario should include:

- **`config.json`** - Test configuration with version constraints and test definitions
- **`events/`** - Expected OpenLineage event JSON files  
- **`scenario.md`** - Documentation describing the test scenario
- **`maintainers.json`** - Responsible maintainers

## Step 1: Create Producer Scenario Directory
Navigate to your producer's scenarios directory and create your new scenario:

```bash
mkdir -p ./producer/example_producer/scenarios/my_scenario
cd ./producer/example_producer/scenarios/my_scenario
```

## Step 2: Configure Test Specifications
Create a detailed configuration file specifying component versions, OpenLineage versions, and individual test definitions. Producer scenarios use a more complex config structure than consumer tests:

<details>
<summary><strong>File: `config.json`</strong> (Producer Scenario Configuration)</summary>

```json
{
  "component_versions": {
    "min": "3.1.0",
    "max": "3.5.1"
  },
  "openlineage_versions": {
    "min": "1.0.0",
    "max": "5.0.0"
  },
  "tests": [
    {
      "name": "basic_lineage_test",
      "path": "events/basic_lineage_test.json",
      "component_versions": {
        "min": "3.1.0",
        "max": "3.3.2"
      },
      "openlineage_versions": {
        "min": "1.22.0",
        "max": "1.30.0"
      },
      "tags": {
        "facets": [
          "run_event",
          "parent",
          "dataSource",
          "schema"
        ],
        "lineage_level": {
          "bigquery": ["dataset", "column"]
        }
      }
    },
    ...
  ]
}
```

</details>

**Configuration Structure:**
- **`component_versions`** - Global version constraints for the producer component (e.g., Spark, Flink)
- **`openlineage_versions`** - Global OpenLineage version compatibility range
- **`tests[]`** - Array of individual test definitions, each with:
  - **`name`** - Unique identifier for the test case
  - **`path`** - Relative path to the expected event JSON file
  - **`component_versions`** - Test-specific component version overrides (optional)
  - **`openlineage_versions`** - Test-specific OpenLineage version overrides (optional)
  - **`tags`** - Test metadata including:
    - **`facets`** - List of OpenLineage facets being tested
    - **`lineage_level`** - Expected lineage granularity by data source type


## Step 3: Define Maintainers
Create a maintainers file listing yourself as the author:

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

## Step 4: Add Expected Events
Create an events directory and add your expected OpenLineage events. Each event file should correspond to a test defined in `config.json`:

```bash
mkdir -p events
# Add your expected event files
cp /path/to/your/expected/events/basic_lineage_test.json events/
cp /path/to/your/expected/events/advanced_facets_test.json events/
```

The expected events shouldn't be real OpenLineage events. They should be done according to [Event comparison](../reusable_actions_and_common_scripts.md#event-comparison)

## Step 5: Document the Scenario
Create comprehensive documentation for your scenario:

<details>
<summary><strong>File: `scenario.md`</strong></summary>

```markdown
# Description
Scenario contains a job that executes queries:


# Entities

input entities are
- input1
- input2

output entities are
- output1

# Facets
Facets present in the events:
- ExampleFacet

```

</details>

