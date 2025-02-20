---
sidebar_position: 2
title: Configuration
---

We recommend configuring the client with an `openlineage.yml` file that contains all the
details of how to connect to your OpenLineage backend.

See [example configurations.](#transports)

You can make this file available to the client in three ways (the list also presents precedence of the configuration):

1. Set an `OPENLINEAGE_CONFIG` environment variable to a file path: `OPENLINEAGE_CONFIG=path/to/openlineage.yml`.
2. Place an `openlineage.yml` in the user's current working directory.
3. Place an `openlineage.yml` under `.openlineage/` in the user's home directory (`~/.openlineage/openlineage.yml`).

## Environment Variables

The following environment variables are available:

| Name                 | Description                                                                 | Since |
|----------------------|-----------------------------------------------------------------------------|-------|
| OPENLINEAGE_CONFIG   | The path to the YAML configuration file. Example: `path/to/openlineage.yml` |       |
| OPENLINEAGE_DISABLED | When `true`, OpenLineage will not emit events.                              | 0.9.0 |

You can also configure the client with dynamic environment variables.

import DynamicEnvVars from './partials/java_dynamic_env_vars.md';

<DynamicEnvVars/>

## Facets Configuration

In YAML configuration file you can also disable facets to filter them out from the OpenLineage event.

*YAML Configuration*

```yaml
transport:
  type: console
facets:
  spark_unknown:
    disabled: true
  spark:
    logicalPlan:
      disabled: true
```

### Deprecated syntax

The following syntax is deprecated and soon will be removed:

```yaml
transport:
  type: console
facets:
  disabled:
    - spark_unknown
    - spark.logicalPlan
```

The rationale behind deprecation is that some of the facets were disabled by default in some integrations. When we added
something extra but didn't include the defaults, they were unintentionally enabled.

## Transports

import Transports from './partials/java_transport.md';

<Transports/>

### Error Handling via Transport

```java
// Connect to http://localhost:5000
OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    HttpTransport.builder()
      .uri("http://localhost:5000")
      .apiKey("f38d2189-c603-4b46-bdea-e573a3b5a7d5")
      .build())
  .registerErrorHandler(new EmitErrorHandler() {
    @Override
    public void handleError(Throwable throwable) {
      // Handle emit error here
    }
  }).build();
```

### Defining Your Own Transport

```java
OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    new MyTransport() {
      @Override
      public void emit(OpenLineage.RunEvent runEvent) {
        // Add emit logic here
      }
    }).build();
```

## Circuit Breakers

import CircuitBreakers from './partials/java_circuit_breaker.md';

<CircuitBreakers/>

## Metrics

import Metrics from './partials/java_metrics.md';

<Metrics/>

## Dataset Namespace Resolver

import DatasetNamespaceResolver from './partials/java_namespace_resolver.md';

<DatasetNamespaceResolver/>


## Tags

You can specify tags for Runs and Jobs using the following syntax:
Tags added by run will be added as TagsRunFacet, while tags added by jobs will be added as TagsJobFacet.

```yaml
run:
    tags:
      - "key:value"
      - "label"
      - "key:value:source"
jobs:
    tags: [
      "key:value",
      "label",
      "key:value:source"
    ] 
```
