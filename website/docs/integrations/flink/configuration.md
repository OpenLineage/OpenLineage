---
sidebar_position: 2
title: Configuration
---

:::info
Flink 1.x and 2.x integrations use common OpenLineage java client methods to extract configuration from. 
:::

## Configuring OpenLineage connector

Flink OpenLineage connector utilizes standard [Java client for Openlineage](../../client/java/configuration.md)
and allows all the configuration features present there to be used. The configuration can be passed with:
* `openlineage.yml` file with a environment property `OPENLINEAGE_CONFIG` being set and pointing to configuration file. 
* Standard Flink configuration with the parameters defined below.

Please refer to [Java client for Openlineage](../../client/java/configuration.md) for more details on configuration options.

## Flink specific configuration

| Parameter                             | Definition                                                                                                                                                                                                           | Example                 |
|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| openlineage.resolveTopicPattern       | This option is used to control whether topic pattern resolution should be used for Kafka topics to extract lineage information, as this may require an extra Kafka client call. The option works only for Flink 2.x. | True (default) or False |
| openlineage.trackingIntervalInSeconds | Defines polling interval for a tracking thread to refresh lineage metadata from jobs API and emit it in a form of `RUNNING` OpenLineage events.                                                                      | 60 (default)            |

