---
sidebar_position: 1
title: Contributing
---

# Contributing

How to contribute a new component or scenario to the OpenLineage Compatibility Tests.

:::info Key Terms
- **Producer**: A system that generates OpenLineage events (e.g., Apache Spark, Apache Airflow, dbt)
- **Consumer**: A system that receives and processes OpenLineage events (e.g., Apache Atlas, DataHub, Marquez)
- **Scenario**: A specific test case that validates how a component handles OpenLineage events
:::

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
