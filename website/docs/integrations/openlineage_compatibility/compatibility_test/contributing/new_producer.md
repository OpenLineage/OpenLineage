---
sidebar_position: 2
title: New Producer
---


# New Producer

Adding a new producer to the test suite requires more setup as some processes aren't completely standardized yet. The following steps include notes where generic approaches are still being developed.

In this example, we'll walk through adding a producer called `example_producer`.

## Step 1: Create Producer Directory Structure

```bash
mkdir -p ./producer/example_producer
cd ./producer/example_producer
```

## Step 2: Set Up Runner Environment
Create a `runner` directory with all resources and scripts necessary to run the producer:

```bash
mkdir -p runner
```

> **Note**: This step is not yet standardized. Include whatever scripts, configurations, and resources are needed to execute your producer. This might include:
> - Docker configurations
> - Environment setup scripts  
> - Dependency installation scripts
> - Producer execution scripts

## Step 3: Create Scenarios Structure
```bash
mkdir -p scenarios
```

The process of adding scenarios is described in [New Producer Scenario](new_producer_scenario.md)

## Step 4: Define Maintainers

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

## Step 5: Configure Supported Versions

<details>
<summary><strong>File: `versions.json`</strong></summary>

```json
{
  "openlineage_versions": ["1.2.0"],
  "component_version": ["2.1.0"]
}
```
</details>

## Step 6: Create GitHub Workflow

Beside adding your producer files, you need to add workflow that will handle automated running of the producer tests. 
First is step of that is adding your workflow file. Below we provided template/reference to use.

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

### Available Reusable Actions

For more information about custom actions used here go to:
- [run_event_validation](../reusable_actions_and_common_scripts.md#run-event-validation)
- [get_openlineage_artifacts](../reusable_actions_and_common_scripts.md#get-openlineage-artifacts)

## Step 7: Update main workflow files
Next step is adding your workflow to main workflows. Producers take part in each one of three main workflows.
Below we provided reference for updating the workflows.
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
    if: ${{ needs.initialize_workflow.outputs.run_example_producer == 'true' }}
    uses: ./.github/workflows/producer_example_producer.yml
    strategy:
      matrix: ${{ fromJson(needs.initialize_workflow.outputs.example_producer_matrix) }}
    secrets:
      secret1: ${{ secrets.SECRET1 }}
    with:
      ol_release: ${{ matrix.openlineage_versions }}
      spark_release: ${{ matrix.component_version }}
      get-latest-snapshots: 'false'
      
  collect-and-compare-reports:
    needs:
      # ... other producers
      - example_producer
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

## Step 8: Update Release Configuration

Add your producers entry to the releases tracking by updating `generated-files/releases.json`.

