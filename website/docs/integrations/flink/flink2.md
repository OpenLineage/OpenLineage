---
sidebar_position: 3
title: Flink 2.x
---

## Overview

With the release of Apache Flink 2.0, the OpenLineage integration has been updated to utilize the native API for lineage extraction,
which was initially proposed in [FLIP-314](https://cwiki.apache.org/confluence/display/FLINK/FLIP-314%3A+Support+Customized+Job+Lineage+Listener).
This new API allows for a more efficient and streamlined approach to lineage extraction, eliminating the need for modifications to the job code.
The other advantage of this implementation is that it supports Flink SQL, which was not possible with the previous version.

At the same time, it is the Flink's connectors which contain implementation of sources and sinks, which 
are responsible for providing methods to extract lineage information. This poses a challenge for the OpenLineage integration,
as it requires the connectors to implement the lineage interfaces. Currently, only the Kafka connector supports this functionality.

## Usage

To enable OpenLineage integration in Flink 2.x, a job status change listener has to be configured 
as described in [Flink docs](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/advanced/job_status_listener/#configuration).

This can be achieved by including `openlineage-flink` package on the classpath and providing extra config:
```properties
execution.job-status-changed-listeners = io.openlineage.flink.listener.OpenLineageJobStatusChangedListenerFactory
```

Please refer to [configuration section](./configuration.md) for more details about the configuration options.

## Implementation

OpenLineage implements `io.openlineage.flink.listener.OpenLineageJobStatusChangedListener` which is a subclass
of `org.apache.flink.core.execution.JobStatusChangedListener`. One of its subclasses is 
`org.apache.flink.streaming.runtime.execution.JobCreatedEvent` which contains a method
that returns `LineageGraph` object. This object contains all the lineage information about the job.

Additionally, after a job is triggered, OpenLineage integration starts job tracker thread that periodically polls
lineage metadata updates from Flink jobs API. Currently, it is used to collect information about the checkpoints processed.

## Column Level Lineage

Unfortunately, lineage interfaces in Flink 2.x do not provide column level lineage information.
In general, this may be difficult to extract for the transformations defined through the programming language.
However, it is possible to extract column level lineage information for Flink SQL jobs.

Following [PR](https://github.com/apache/flink/pull/26089#issuecomment-2626542070)
contains a potential extension to Flink to make it available. Please refer to 
[this document for more information about the implementation](https://docs.google.com/document/d/1XmbHy6XqBrMoH9rkSyOG0wbwQZgf0epz-07lr_NfikI/edit?tab=t.0#heading=h.gw6ivvgpdre0).

