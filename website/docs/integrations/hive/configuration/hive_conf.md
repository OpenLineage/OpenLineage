---
sidebar_position: 2
title: Configuration parameters
---
:::info
This list doesn't include information transport configuration parameters, see [Transport](transport.md)

Additionally, any properties from OpenLineage client can be defined using `hive.openlineage` instead of `openlineage`  
:::
## Configuration

The following parameters can be specified:

| Parameter                       | Definition                                                        | Example     |
|---------------------------------|-------------------------------------------------------------------|-------------|
| hive.openlineage.transport.type | The transport type used for event emit, default type is `console` | http        |
| hive.openlineage.namespace      | The default namespace to be applied for any jobs                  | mynamespace |
| hive.openlineage.job.name       | The default name to be applied for any jobs                       | myname      |
