---
sidebar_position: 3
title: New Consumer
---

# New Consumer

Adding a consumer requires similar setup to producers but focuses on event ingestion validation instead of event generation. Consumers validate that OpenLineage events can be properly ingested and result in expected state changes in the target system.

In this example, we'll walk through adding a consumer called `example_consumer`.

## Step 1: Create Consumer Directory Structure

```bash
mkdir -p ./consumer/consumers/example_consumer
cd ./consumer/consumers/example_consumer
```

## Step 2: Set Up Consumer Infrastructure
Create the necessary files and directories for consumer validation:

**Required Structure:**
- **`validator/`** - Scripts and dependencies to validate consumer state changes
- **`mapping.json`** - Mapping between OpenLineage fields and consumer system fields
- **`maintainers.json`** - Maintainer information
- **`scenarios/`** - Consumer-specific test scenario validation data


## Step 3: Create Field Mapping Configuration
Create `mapping.json` file with mapping between OpenLineage and consumer entities. Use [mapping](../structure.md#mapping) as a reference.

## Step 4: Define Maintainers
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

## Step 5: Create Consumer Workflow
Create a GitHub Actions workflow for your consumer:

<details>
<summary><strong>File: `.github/workflows/consumer_example_consumer.yml`</strong></summary>

```yaml
name: Example Consumer

on:
  workflow_call:
    secrets:
      # Add any required secrets for your consumer
      secret1:
        required: true
    inputs:
      input1:
        description: "example input"
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
    
    
    - name: Start Consumer Instance 
      run: |
        # Start consumer instance, as many consumers are services that do not need starting up, this one is optional
    
    - name: Run consumer validation
      run: |
        # We don't have standardized step to validate the Consumer API as they have different structures
        # This step should produce report file.
    
    - name: Terminate consumer instance
      run: |
        # Terminate consumer instance, as many consumers are services that do not need starting up, this one is optional
    
    - uses: actions/upload-artifact@v4
      with:
        name: example-consumer-report
        path: example-consumer-report.json
        retention-days: 1
```

</details>


## Step 6: Update main workflow files

<details>
<summary><strong>PR Workflow Updates</strong></summary>

```yaml
  initialize_workflow:
    outputs:
    # ...  
      run_consumer: ${{ steps.get-changed.outputs.example_consumer_changed }}
    steps:
       - name: get changed files
         id: get-changed
         run: |
          # ...
          example_consumer=$(check_path "consumer/example_consumer/" "example_consumer_changed")
          
          if [[ $scenarios || $dataplex || $spark_dataproc || $example_consumer ]]; then
              echo "any_changed=true" >> $GITHUB_OUTPUT
          fi
   

  example_consumer:
    needs: 
      - initialize_workflow
      - scenarios
    if: ${{ needs.initialize_workflow.outputs.run_example_consumer == 'true' }}
    uses: ./.github/workflows/consumer_example_consumer.yml
    secrets:
      secret1: ${{ secrets.SECRET1 }}
    with:
      input1: ""

  collect-and-compare-reports:
    needs:
      # ... other consumers
      - example_consumer

```
</details>

<details>
<summary><strong>Release Workflow Updates</strong></summary>

```yaml
on:
  workflow_dispatch:
    inputs:
       #...
      example_consumer_matrix:
        description: 'Overwrite matrix for example_consumer tests'
        required: false
jobs:
  initialize_workflow:
    outputs:
      # ...
      run_example_consumer: ${{ github.event.inputs.run_example_consumer || 'true' }}
    steps:
      - name: set-matrix-values
        id: set-matrix-values
        run: |
          # ...
          echo "example_consumer_matrix=$(get_matrix example_consumer)" >> $GITHUB_OUTPUT
           
  spark-dataproc:
    needs: initialize_workflow
    if: ${{ needs.initialize_workflow.outputs.run_example_consumer == 'true' }}
    uses: ./.github/workflows/consumer_example_consumer.yml
    strategy:
      matrix: ${{ fromJson(needs.initialize_workflow.outputs.<produce_name>_matrix) }}
    secrets:
      secret1: ${{ secrets.SECRET1 }}
    with:
      input1: ""

      
  collect-and-compare-reports:
    needs:
      # ... other consumers
      - <produce_name>
```
</details>





## Step 6: Create Consumer Scenarios
Unlike producers that generate events, consumers create scenario-specific validation data that represents the expected state after event ingestion:

```bash
mkdir -p ./consumer/consumers/example_consumer/scenarios/my_scenario
cd ./consumer/consumers/example_consumer/scenarios/my_scenario
```




