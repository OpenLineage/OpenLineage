---
sidebar_position: 1
title: Contributing
---

# Contributing

How to contribute a new component or scenario to the OpenLineage Compatibility Tests.

To make a contribution to Compatibility Tests, submit a pull request to the [Compatibility Tests](https://github.com/OpenLineage/compatibility-tests/) repository. Depending on the scope of your contribution, you can use one of the following guides:

## Quick Navigation

### Adding Test Data
- **[New Input Events for Consumer Tests](new_input_events.md)** - The easiest contribution to make. Add new OpenLineage events for consumer testing.

### Adding Components
- **[New Producer](new_producer.md)** - Add a new OpenLineage producer (e.g., Spark, Flink, Airflow) to the test suite.
- **[New Consumer](new_consumer.md)** - Add a new OpenLineage consumer (e.g., Dataplex, Marquez) to the test suite.

### Adding Scenarios  
- **[New Producer Scenario](new_producer_scenario.md)** - Add test scenarios for existing producers.
- **[New Consumer Scenario](new_consumer_scenario.md)** - Add test scenarios for existing consumers.

## Overview

The OpenLineage Compatibility Tests framework supports different types of contributions:

### 1. Input Events (Easiest)
Add new OpenLineage event files to test consumer ingestion capabilities. This requires minimal setup and is the quickest way to contribute.

### 2. Producers (Complex)
Add support for new OpenLineage producers that generate events. This requires setting up test infrastructure, workflows, and validation logic.

### 3. Consumers (Complex)  
Add support for new OpenLineage consumers that ingest events. This requires implementing validation logic and mapping configurations.

### 4. Scenarios (Moderate)
Add new test scenarios for existing producers or consumers. This involves creating configuration files and expected results.

## Getting Started

1. **Choose your contribution type** from the navigation above
2. **Follow the step-by-step guide** for your chosen contribution
3. **Test locally** before submitting your pull request
4. **Update documentation** as needed for your changes

## Support

If you need help with your contribution:
- Check the existing examples in the repository
- Review the [structure documentation](../structure.md) for file organization
- Look at [test workflows](../test_workflows.md) for understanding the automation
- Consult [event validation](../reusable_actions_and_custom_scripts.md#) for testing details
