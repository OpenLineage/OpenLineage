---
sidebar_position: 5
title: Contributing
---

# Contributing

How to contribute a new component or scenario to the OpenLineage Compatibility Tests.

To make a contribution to Compatibility Tests, submit a pull request to the [Compatibility Tests](https://github.com/OpenLineage/compatibility-tests/) repository. Depending on the scope of your contribution, you can use one of the following playbooks below.

## New Input Events for Consumer Tests

The easiest contribution to make. Follow these steps to add new input events:

#### Step 1: Create Scenario Directory
Navigate to the consumer scenarios directory and create your new scenario:

```bash
mkdir -p ./consumer/scenarios/my_scenario
cd ./consumer/scenarios/my_scenario
```

#### Step 2: Configure OpenLineage Version
Create a configuration file specifying the OpenLineage version used to generate the events:

**File: `config.json`**
```json
{
    "openlineage_version": "1.2.0"
}
```

#### Step 3: Define Maintainers
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

#### Step 4: Add Events
Create an events directory and copy your OpenLineage events:

```bash
mkdir -p events
cp /path/to/your/openlineage/events/* events/
```

## New Producer

Adding a new producer to the test suite requires more setup as some processes aren't completely standardized yet. The following steps include notes where generic approaches are still being developed.

In this example, we'll walk through adding a producer called `example_producer`.

### Step 1: Create Producer Directory Structure

```bash
mkdir -p ./producer/example_producer
cd ./producer/example_producer
```

### Step 2: Set Up Runner Environment
Create a `runner` directory with all resources and scripts necessary to run the producer:

```bash
mkdir -p runner
```

> **Note**: This step is not yet standardized. Include whatever scripts, configurations, and resources are needed to execute your producer. This might include:
> - Docker configurations
> - Environment setup scripts  
> - Dependency installation scripts
> - Producer execution scripts

### Step 3: Create Scenarios Structure
```bash
mkdir -p scenarios
```

The process of adding scenarios is described in [New Producer Scenario](#new-producer-scenario)

### Step 4: Define Maintainers

<details>
<summary><strong>File: `maintainers.json`</strong></summary>

```json
[
  {
    "type": "maintainer", 
    "github-name": "your_github_user",
    "email": "your.email@example.com",
    "link": ""
  }
]
```
</details>

### Step 5: Prepare Events Directory
```bash
mkdir -p events
```

> **Note**: Events will be generated during test execution, so this directory will be populated automatically.

### Step 6: Configure Supported Versions

<details>
<summary><strong>File: `versions.json`</strong></summary>

```json
{
  "openlineage_versions": ["1.2.0"],
  "component_version": ["2.1.0"]
}
```
</details>

### Step 7: Create GitHub Workflow

<details>
<summary><strong>File: `.github/workflows/producer_example_producer.yml`</strong></summary>

```yaml
name: Example Producer

on:
  workflow_call:
    secrets:
      secret1:
        required: true
    inputs:
      producer_release:
        description: "release of producer to use"
        type: string
      ol_release:
        description: "release tag of OpenLineage to use"
        type: string
      get-latest-snapshots:
        description: "Should the artifact be downloaded from maven repo or circleci"
        type: string

jobs:
  run-spark-tests:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: initialize tests
        id: init
        run: |
          # Run through all scenarios and check if the produce_release and ol_release are within bounds, if not skip
          # If all scenarios are skipped skip all further steps
          scenarios=$(./scripts/get_valid_test_scenarios.sh "producer/spark_dataproc/scenarios/" ${{ inputs.producer_release }} ${{ inputs.ol_release }} )
          [[ "$scenarios" == "" ]] || echo "scenarios=$scenarios" >> $GITHUB_OUTPUT

      - name: Get OL artifacts
        id: get-ol-artifacts
        if: ${{ steps.init.outputs.scenarios }} 
        uses: ./.github/actions/get_openlineage_artifacts
        with:
          version: ${{ inputs.ol_release }}

      - name: Start producer
        id: start-producer
        if: ${{ steps.init.outputs.scenarios }}
        run: # code for starting the producer instance


      - name: Run producer jobs and create OL events
        if: ${{ steps.init.outputs.scenarios }}
        id: run-producer
        continue-on-error: true
        run: |
          # code for running the test scenarios, events should be stored in <test_output_dir>/scenario_name/
           
          echo "event_dir=<test_output_dir>" >> $GITHUB_OUTPUT
          
      - name: Terminate producer
        if: ${{ always() && steps.init.outputs.scenarios }}
        run: # code for terminating the producer

      - name: Fail if jobs failed
        if: ${{ steps.init.outputs.scenarios && steps.run-producer.outcome  == 'failure' }}
        run: |
          echo "step 'Run producer jobs and create OL events' has ended with failure but to terminate the cluster we needed to continue on error but fail the job after cluster termination."
          echo "This will become redundant if starting the cluster is extracted to custom action with post actions defined."
          echo "For now composite actions do not support post actions."
          exit 1

      - name: Validation
        if: ${{ steps.init.outputs.scenarios }}
        uses: ./.github/actions/run_event_validation
        with:
          component: '<producer name>'
          release_tags: ${{ inputs.get-latest-snapshots == 'true' && 'main' || inputs.ol_release }}
          ol_release: ${{ inputs.ol_release }}
          component_release: ${{ inputs.producer_release }}
          event-directory: ${{ steps.run-producer.outputs.event_dir }}
          target-path: 'spark-dataproc-${{inputs.producer_release}}-${{inputs.ol_release}}-report.json'

      - uses: actions/upload-artifact@v4
        if: ${{ steps.init.outputs.scenarios }}
        with:
          name: spark-dataproc-${{inputs.spark_release}}-${{inputs.ol_release}}-report
          path: spark-dataproc-${{inputs.spark_release}}-${{inputs.ol_release}}-report.json
          retention-days: 1
```
</details>

**Update the main workflow files:**

<details>
<summary><strong>PR Workflow Updates</strong></summary>

```yaml
  initialize_workflow:
    outputs:
    # ...  
      example_producer_matrix: ${{ steps.set-matrix-values.outputs.example_producer_matrix }}
    steps:
       - name: get changed files
         id: get-changed
         run: |
          # ...
          example_producer=$(check_path "producer/example_producer/" "producer_name_changed")
          
          if [[ $scenarios || $dataplex || $spark_dataproc || $example_producer ]]; then
              echo "any_changed=true" >> $GITHUB_OUTPUT
          fi
   
       - name: set-matrix-values
         id: set-matrix-values
         run: |
           # ...
     
           echo "example_producer_matrix=$(get_matrix example_producer)" >> $GITHUB_OUTPUT

  example_producer:
    needs: initialize_workflow
    if: ${{ needs.initialize_workflow.outputs.run_example_producer == 'true' }}
    uses: ./.github/workflows/producer_example_producer.yml
    strategy:
      matrix: ${{ fromJson(needs.initialize_workflow.outputs.example_producer_matrix) }}
    secrets:
      secret1: ${{ secrets.SECRET1 }}
    with:
      ol_release: ${{ matrix.openlineage_versions }}
      producer_release: ${{ matrix.component_version }}
      get-latest-snapshots: 'false'

  collect-and-compare-reports:
    needs:
      # ... other producers
      - example_producer
    if: ${{ !failure() && needs.initialize_workflow.outputs.any_run  == 'true'}}
    uses: ./.github/workflows/collect_and_compare_reports.yml
    with:
      fail-for-new-failures: true
```
</details>

<details>
<summary><strong>Release Workflow Updates</strong></summary>

```yaml
on:
  workflow_dispatch:
    inputs:
       #...
      example_producer_matrix:
        description: 'Overwrite matrix for example_producer tests'
        required: false
jobs:
  initialize_workflow:
    outputs:
      # ...
      run_example_producer: ${{ github.event.inputs.run_example_producer || 'true' }}
      example_producer_matrix: ${{ github.event.inputs.example_producer_matrix || steps.set-matrix-values.outputs.example_producer_matrix }}
    steps:
      - name: set-matrix-values
        id: set-matrix-values
        run: |
          # ...
          echo "example_producer_matrix=$(get_matrix example_producer)" >> $GITHUB_OUTPUT
           
  spark-dataproc:
    needs: initialize_workflow
    if: ${{ needs.initialize_workflow.outputs.run_<produce_name> == 'true' }}
    uses: ./.github/workflows/producer_<produce_name>.yml
    strategy:
      matrix: ${{ fromJson(needs.initialize_workflow.outputs.<produce_name>_matrix) }}
    secrets:
      secret1: ${{ secrets.SECRET1 }}
    with:
      ol_release: ${{ matrix.openlineage_versions }}
      spark_release: ${{ matrix.component_version }}
      get-latest-snapshots: 'false'
      
  collect-and-compare-reports:
    needs:
      # ... other producers
      - <produce_name>
```
</details>

<details>
<summary><strong>Spec Change Workflow Updates</strong></summary>

```yaml
on:
  workflow_dispatch:
    inputs:
       #...
      example_producer_matrix:
        description: 'Overwrite matrix for example_producer tests'
        required: false
jobs:
  initialize_workflow:
    outputs:
      # ...
      run_example_producer: ${{ github.event.inputs.run_example_producer || 'true' }}
      example_producer_matrix: ${{ github.event.inputs.example_producer_matrix || steps.set-matrix-values.outputs.example_producer_matrix }}
    steps:
      - name: set-matrix-values
        id: set-matrix-values
        run: |
          # ...
          echo "example_producer_matrix=$(get_matrix example_producer)" >> $GITHUB_OUTPUT
           
  example_producer:
    needs: initialize_workflow
    if: ${{ needs.initialize_workflow.outputs.run_example_producer == 'true' }}
    uses: ./.github/workflows/producer_example_producer.yml
    strategy:
      matrix: ${{ fromJson(needs.initialize_workflow.outputs.example_producer_matrix) }}
    secrets:
      secret1: ${{ secrets.SECRET1 }}
    with:
      ol_release: ${{ matrix.openlineage_versions }}
      producer_release: ${{ matrix.component_version }}
      get-latest-snapshots: 'true'

  collect-and-compare-reports:
    needs:
      # ... other producers
      - example_producer
```
</details>

### Step 7: Create GitHub Workflow

Create a workflow file and update the main workflow files to include your producer in the test matrix.

**Create producer workflow file:**
Create `.github/workflows/producer_example_producer.yml` with your producer-specific test logic.

**Update main workflows:**
Add your producer to the existing workflow files (New Release, Spec Update, Test Suite PR) by including it in the job dependencies and matrix configurations.

### Step 8: Update Release Configuration

Add your producer to the releases tracking by updating `generated-files/releases.json`.

### Available Reusable Actions

The test suite provides these custom GitHub actions for reuse:
- **`run_event_validation`** - Generic OpenLineage event validation
- **`get_openlineage_artifacts`** - Download OpenLineage artifacts

#### `run_event_validation` Action

This action validates OpenLineage events against expected results and performs compatibility testing.

**Location:** `.github/actions/run_event_validation/action.yml`

**Inputs:**
- `working-directory` (required) - Directory containing the test scenario files
- `test-name` (required) - Name identifier for the test scenario

**Function:**
1. Sets up Python environment and installs dependencies from `scripts/requirements.txt`
2. Validates scenario configuration using `scripts/check_configs.sh`
3. Compares generated events against expected events using `scripts/compare_events.py`
4. Uploads validation results and artifacts to Google Cloud Storage
5. Generates compatibility reports using `scripts/report.py`

**Usage Example:**
```yaml
- name: Validate OpenLineage Events
  uses: ./.github/actions/run_event_validation
  with:
    working-directory: producer/spark_dataproc/scenarios/example_scenario
    test-name: spark-dataproc-example-scenario
```

**Outputs:**
- Validation results uploaded to GCS bucket
- Test reports generated in `generated-files/`
- Compatibility data updated in project tracking files

#### `get_openlineage_artifacts` Action

This action downloads OpenLineage JAR artifacts from specified sources for testing.

**Location:** `.github/actions/get_openlineage_artifacts/action.yml`

**Inputs:**
- `ol-version` (required) - OpenLineage version to download (e.g., "1.31.0")
- `component` (required) - Component to download (e.g., "spark", "flink", "java")

**Function:**
1. Downloads OpenLineage artifacts from Maven Central or GitHub releases
2. Caches artifacts to avoid repeated downloads
3. Makes artifacts available for subsequent workflow steps
4. Handles version-specific artifact naming and locations

**Usage Example:**
```yaml
- name: Get OpenLineage Spark JAR
  uses: ./.github/actions/get_openlineage_artifacts
  with:
    ol-version: "1.31.0"
    component: "spark"
```

**Outputs:**
- Downloaded JAR files placed in workspace
- Artifact paths available to subsequent steps
- Cached artifacts for workflow efficiency

**Supported Components:**
- `spark` - OpenLineage Spark integration JAR
- `flink` - OpenLineage Flink integration JAR  
- `java` - OpenLineage Java client JAR
- Additional components as specified in the action implementation

#### Creating Custom Workflows with Actions

When creating new producer or consumer workflows, you can combine these actions for comprehensive testing:

<details>
<summary>Example Producer Workflow Template</summary>

```yaml
name: Test Example Producer
on:
  pull_request:
    paths: ['producer/example_producer/**']
  workflow_dispatch:

jobs:
  test-scenarios:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        scenario: [scenario1, scenario2, scenario3]
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Get OpenLineage Artifacts
      uses: ./.github/actions/get_openlineage_artifacts
      with:
        ol-version: "1.31.0"
        component: "spark"
    
    # Your producer-specific setup steps here
    - name: Setup Producer Environment
      run: |
        # Install dependencies, configure environment
        echo "Setting up example producer..."
    
    - name: Run Producer Test
      run: |
        # Execute your producer test scenario
        echo "Running producer test for ${{ matrix.scenario }}"
    
    - name: Validate Events
      uses: ./.github/actions/run_event_validation
      with:
        working-directory: producer/example_producer/scenarios/${{ matrix.scenario }}
        test-name: example-producer-${{ matrix.scenario }}
```

</details>

#### Best Practices for Action Usage

**For `get_openlineage_artifacts`:**
- Always specify the exact OpenLineage version you need for testing
- Cache artifacts when possible to reduce download times in workflows
- Use matrix strategies to test against multiple OpenLineage versions
- Verify artifact availability before referencing in your test scripts

**For `run_event_validation`:**
- Ensure your `working-directory` contains all required files (`config.json`, `events/`, etc.)
- Use descriptive `test-name` values that include producer/consumer and scenario information
- Place expected event files in the `events/` subdirectory with `.json` extension
- Include proper error handling in your producer setup before event validation

**Common Troubleshooting:**
- **Missing artifacts**: Verify the OpenLineage version exists and component name is correct
- **Validation failures**: Check that expected events match the exact structure of generated events
- **Configuration errors**: Ensure `config.json` follows the required schema (see structure documentation)
- **Path issues**: Use relative paths from repository root in `working-directory` parameter

## New Producer Scenario

Producer scenarios test specific use cases and validate OpenLineage event generation. The process is analogical to [New Input Events for Consumer Tests](#new-input-events-for-consumer-tests), but with a more complex configuration structure that includes component version constraints and test specifications.

Each producer scenario should include:

- **`config.json`** - Test configuration with version constraints and test definitions
- **`events/`** - Expected OpenLineage event JSON files  
- **`scenario.md`** - Documentation describing the test scenario
- **`maintainers.json`** - Responsible maintainers

### Step 1: Create Producer Scenario Directory
Navigate to your producer's scenarios directory and create your new scenario:

```bash
mkdir -p ./producer/example_producer/scenarios/my_scenario
cd ./producer/example_producer/scenarios/my_scenario
```

### Step 2: Configure Test Specifications
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
    {
      "name": "advanced_facets_test",
      "path": "events/advanced_facets_test.json",
      "component_versions": {
        "max": "3.4.0"
      },
      "tags": {
        "facets": [
          "spark_properties",
          "processing_engine",
          "columnLineage"
        ]
      }
    }
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
    - **`max_version`/`min_version`** - Additional version constraints (deprecated, use objects instead)

### Step 3: Define Maintainers
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

### Step 4: Add Expected Events
Create an events directory and add your expected OpenLineage events. Each event file should correspond to a test defined in `config.json`:

```bash
mkdir -p events
# Add your expected event files
cp /path/to/your/expected/events/basic_lineage_test.json events/
cp /path/to/your/expected/events/advanced_facets_test.json events/
```

### Step 5: Document the Scenario
Create comprehensive documentation for your scenario:

<details>
<summary><strong>File: `scenario.md`</strong></summary>

```markdown
# My Scenario

## Description
Brief description of what this scenario tests.

## Test Cases
- **basic_lineage_test**: Tests basic lineage tracking functionality
- **advanced_facets_test**: Validates advanced facet generation

## Setup Requirements
- List any special setup requirements
- Environment variables needed
- External dependencies

## Expected Behavior
Describe what the producer should generate and why.
```

</details>

### Key Differences from Consumer Tests
- **Version Constraints**: Producer scenarios include detailed component and OpenLineage version compatibility matrices
- **Multiple Tests**: Single scenario can contain multiple test cases with different version constraints
- **Facet Tagging**: Tests are tagged with specific OpenLineage facets for targeted validation
- **Lineage Levels**: Specification of expected lineage granularity by data source type

## New Consumer

Adding a consumer requires similar setup to producers but focuses on event ingestion validation instead of event generation. Consumers validate that OpenLineage events can be properly ingested and result in expected state changes in the target system.

In this example, we'll walk through adding a consumer called `example_consumer`.

### Step 1: Create Consumer Directory Structure

```bash
mkdir -p ./consumer/consumers/example_consumer
cd ./consumer/consumers/example_consumer
```

### Step 2: Set Up Consumer Infrastructure
Create the necessary files and directories for consumer validation:

**Required Structure:**
- **`validator/`** - Scripts and dependencies to validate consumer state changes
- **`mapping.json`** - Mapping between OpenLineage fields and consumer system fields
- **`maintainers.json`** - Maintainer information
- **`scenarios/`** - Consumer-specific test scenario validation data
- **`README.md`** - Consumer documentation

<details>
<summary><strong>File: `validator/requirements.txt`</strong></summary>

```plaintext
# Add your consumer-specific dependencies
requests==2.32.3
google-cloud-datacatalog-lineage==0.3.8
# ... other dependencies needed for your consumer validation
```

</details>

<details>
<summary><strong>File: `validator/validator.py`</strong></summary>

```python
import argparse
import json
from pathlib import Path
from compare_events import diff
from report import Report, Component, Scenario, Test

class ConsumerValidator:
    def __init__(self, consumer_dir=None, scenario_dir=None, release=None):
        self.consumer_dir = Path(consumer_dir)
        self.scenario_dir = Path(scenario_dir)
        self.release = release
    
    def validate_scenario(self, scenario_name):
        """Validate that events were properly consumed"""
        # Load OpenLineage events from scenario
        events = self.load_events(scenario_name)
        
        # Send events to consumer system
        self.send_events(events)
        
        # Validate consumer state matches expectations
        return self.validate_consumer_state(scenario_name)
    
    def load_events(self, scenario):
        """Load OpenLineage events from scenario directory"""
        scenario_path = self.scenario_dir / scenario / "events"
        return [json.loads(f.read_text()) for f in scenario_path.glob("*.json")]
    
    def send_events(self, events):
        """Send events to consumer system for ingestion"""
        # Implement consumer-specific event sending logic
        pass
    
    def validate_consumer_state(self, scenario):
        """Validate consumer system state against expected results"""
        # Implement consumer-specific validation logic
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumer_dir", required=True)
    parser.add_argument("--scenario_dir", required=True)
    parser.add_argument("--release", required=True)
    args = parser.parse_args()
    
    validator = ConsumerValidator(
        consumer_dir=args.consumer_dir,
        scenario_dir=args.scenario_dir,
        release=args.release
    )
    # Run validation logic
```

</details>

### Step 3: Create Field Mapping Configuration
Define how OpenLineage fields map to your consumer system's data model:

<details>
<summary><strong>File: `mapping.json`</strong></summary>

```json
{
  "mapped": {
    "core": {
      "eventTime": "consumer_field_for_event_time",
      "eventType": "consumer_field_for_event_type",
      "run.id": "consumer_field_for_run_id",
      "job.name": "consumer_field_for_job_name",
      "job.namespace": "consumer_field_for_job_namespace",
      "input.name": "consumer_field_for_input_dataset",
      "input.namespace": "consumer_field_for_input_namespace",
      "output.name": "consumer_field_for_output_dataset",
      "output.namespace": "consumer_field_for_output_namespace"
    },
    "ProcessingEngineRunFacet": {
      "name": "consumer_field_for_engine_name",
      "version": "consumer_field_for_engine_version",
      "openlineageAdapterVersion": "consumer_field_for_adapter_version"
    },
    "EnvironmentFacet": {
      "spark.app.id": "consumer_field_for_spark_app_id",
      "spark.app.name": "consumer_field_for_spark_app_name"
    }
  },
  "knownUnmapped": {
    "DatasourceDatasetFacet": ["*"],
    "SchemaDatasetFacet": ["*"],
    "SymlinksDatasetFacet": ["*"],
    "JobTypeJobFacet": ["*"],
    "ParentRunFacet": ["*"]
  }
}
```

</details>

**Mapping Structure:**
- **`mapped`** - Fields that are successfully mapped to consumer system
- **`knownUnmapped`** - Fields that are intentionally not mapped (with justification)

### Step 4: Define Maintainers
Create a maintainers file:

<details>
<summary><strong>File: `maintainers.json`</strong></summary>

```json
[
  {
    "type": "maintainer",
    "github-name": "your_github_user",
    "email": "your.email@example.com",
    "link": ""
  }
]
```

</details>

### Step 5: Create Consumer Workflow
Create a GitHub Actions workflow for your consumer:

<details>
<summary><strong>File: `.github/workflows/consumer_example_consumer.yml`</strong></summary>

```yaml
name: Example Consumer

on:
  workflow_call:
    secrets:
      # Add any required secrets for your consumer
      consumerCredentials:
        required: true
    inputs:
      release:
        description: "release tag of OpenLineage to use"
        type: string

permissions:
  contents: read

jobs:
  run-example-consumer-tests:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python 3.11
      uses: actions/setup-python@v5
      with:
        python-version: "3.11"
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r consumer/consumers/example_consumer/validator/requirements.txt
        pip install -r scripts/requirements.txt
    
    - name: Run consumer validation
      run: |
        export PYTHONPATH=scripts
        python consumer/consumers/example_consumer/validator/validator.py \
          --consumer_dir consumer/consumers/example_consumer \
          --scenario_dir consumer/scenarios/ \
          --release ${{ inputs.release }} \
          --target example-consumer-report.json
    
    - uses: actions/upload-artifact@v4
      with:
        name: example-consumer-report
        path: example-consumer-report.json
        retention-days: 1
```

</details>

### Step 6: Create Consumer Scenarios
Unlike producers that generate events, consumers create scenario-specific validation data that represents the expected state after event ingestion:

```bash
mkdir -p ./consumer/consumers/example_consumer/scenarios/my_scenario
cd ./consumer/consumers/example_consumer/scenarios/my_scenario
```

**Consumer Scenario Structure:**
- **`config.json`** - Test configuration with entity types and validation paths
- **`validation/`** - Expected consumer system state after event ingestion
- **`api_state/`** - Expected API responses from consumer system
- **`maintainers.json`** - Scenario maintainers
- **`scenario.md`** - Scenario documentation

<details>
<summary><strong>File: `config.json`</strong> (Consumer Scenario Configuration)</summary>

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
    {
      "name": "runs",
      "path": "validation/runs/runs.json", 
      "entity": "run",
      "tags": {
        "facets": ["run_event"]
      }
    },
    {
      "name": "lineage_events",
      "path": "validation/lineage_events/lineage_events.json",
      "entity": "lineage_event", 
      "tags": {
        "facets": ["dataSource", "schema"]
      }
    },
    {
      "name": "links",
      "path": "validation/links/links.json",
      "entity": "link",
      "tags": {
        "facets": ["columnLineage"]
      }
    }
  ]
}
```

</details>

**Consumer Config Structure:**
- **`tests[]`** - Array of validation tests, each with:
  - **`name`** - Unique identifier for the validation test
  - **`path`** - Relative path to expected consumer state JSON file
  - **`entity`** - Consumer system entity type being validated
  - **`tags.facets`** - OpenLineage facets that should affect this entity

### Key Differences from Producer Setup
- **Validation Focus**: Consumers validate state changes rather than event generation
- **Mapping Requirements**: Must define field mappings between OpenLineage and consumer system
- **Entity-Based Testing**: Tests are organized by consumer system entity types (processes, runs, links, etc.)
- **State Validation**: Expected validation files contain consumer system state, not OpenLineage events
- **System Integration**: Requires actual integration with target consumer system for testing

## New Consumer Scenario

Consumer scenarios validate that OpenLineage events can be properly ingested and result in expected consumer state changes. Unlike producer scenarios that focus on event generation, consumer scenarios test the ingestion and transformation of existing OpenLineage events into consumer system entities.

The process follows these key steps: consume OpenLineage events → transform to consumer format → validate resulting state.

### Step 1: Create Consumer Scenario Directory
Navigate to the global consumer scenarios directory (shared across all consumers) and create your new scenario:

```bash
mkdir -p ./consumer/scenarios/my_scenario
cd ./consumer/scenarios/my_scenario
```

### Step 2: Configure OpenLineage Version
Create a simple configuration file specifying the OpenLineage version used to generate the input events:

<details>
<summary><strong>File: `config.json`</strong> (Global Consumer Scenario)</summary>

```json
{
  "openlineage_version": "1.31.0"
}
```

</details>

### Step 3: Add Input Events
Create an events directory and add the OpenLineage events that will be consumed:

```bash
mkdir -p events
cp /path/to/your/openlineage/events/* events/
```

**Event Requirements:**
- Events should be valid OpenLineage JSON files
- File naming should be descriptive (e.g., `spark_bigquery_job.json`, `airflow_dag_run.json`)
- Events should represent realistic producer scenarios

### Step 4: Define Maintainers
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

### Step 5: Document the Scenario
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

### Step 6: Create Consumer-Specific Validation Data
Each consumer that supports this scenario must create consumer-specific validation data in their own scenarios directory:

```bash
mkdir -p ./consumer/consumers/example_consumer/scenarios/my_scenario
cd ./consumer/consumers/example_consumer/scenarios/my_scenario
```

### Step 7: Configure Consumer-Specific Tests
Create a consumer-specific configuration that defines what entities and validation files to expect:

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
    {
      "name": "runs",
      "path": "validation/runs/runs.json",
      "entity": "run",
      "tags": {
        "facets": ["run_event", "parent"]
      }
    },
    {
      "name": "datasets",
      "path": "validation/datasets/datasets.json",
      "entity": "dataset",
      "tags": {
        "facets": ["dataSource", "schema"]
      }
    },
    {
      "name": "lineage_events",
      "path": "validation/lineage_events/lineage_events.json",
      "entity": "lineage_event",
      "tags": {
        "facets": ["dataSource", "schema", "columnLineage"]
      }
    }
  ]
}
```

</details>

### Step 8: Create Expected Validation Files
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

### Step 9: Add Consumer Scenario Maintainers
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

### Step 10: Document Consumer-Specific Behavior
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

### Key Differences from Producer Scenarios
- **Dual Configuration**: Both global scenario config (simple) and consumer-specific config (complex)
- **Input vs Output**: Consumes existing events rather than generating new ones
- **Entity Validation**: Validates consumer system entities rather than OpenLineage events
- **State-Based Testing**: Tests resulting system state rather than event structure
- **Cross-Consumer Sharing**: Global scenarios can be tested by multiple consumers

### Validation Process Flow
1. **Global Scenario**: Provides OpenLineage events and basic metadata
2. **Consumer Ingestion**: Consumer system processes the OpenLineage events
3. **Entity Creation**: Consumer creates system-specific entities (processes, runs, etc.)
4. **State Validation**: Compare actual consumer state against expected validation files
5. **Report Generation**: Generate compatibility report for the consumer scenario