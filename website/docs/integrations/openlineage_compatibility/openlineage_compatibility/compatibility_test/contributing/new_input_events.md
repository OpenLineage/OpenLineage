---
sidebar_position: 1
title: New Input Events for Consumer Tests
---

# New Input Events for Consumer Tests

The easiest contribution to make. Follow these steps to add new input events:

## Step 1: Create Scenario Directory
Navigate to the consumer scenarios directory and create your new scenario:

```bash
mkdir -p ./consumer/scenarios/my_scenario
cd ./consumer/scenarios/my_scenario
```

## Step 2: Configure OpenLineage Version
Create a configuration file specifying the OpenLineage version used to generate the events:

**File: `config.json`**
```json
{
    "openlineage_version": "1.2.3"
}
```

## Step 3: Define Maintainers
Create a maintainers file listing yourself as the author:

***File: `maintainers.json`***

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


## Step 4: Add Events
Create an events directory and copy your OpenLineage events:

```bash
mkdir -p events
cp /path/to/your/openlineage/events/* events/
```
