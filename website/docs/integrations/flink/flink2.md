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

Additionally, after a job is triggered, OpenLineage integration can start a job tracker thread that periodically polls
lineage metadata updates from Flink jobs API. Currently, it is used to collect information about the checkpoints processed.
If your runtime REST endpoint differs from the configured `rest.address`/`rest.port`, you can override it with
`openlineage.restApiBaseUrl`. This is especially useful in Application mode deployments.
Set `openlineage.flink.disableCheckpointTracking` to `true` if you want to disable checkpoint polling and checkpoint-driven
`RUNNING` events.

## Column Level Lineage

Unfortunately, lineage interfaces in Flink 2.x do not provide column level lineage information.
In general, this may be difficult to extract for the transformations defined through the programming language.
However, it is possible to extract column level lineage information for Flink SQL jobs.

Following [PR](https://github.com/apache/flink/pull/26089#issuecomment-2626542070)
contains a potential extension to Flink to make it available. Please refer to 
[this document for more information about the implementation](https://docs.google.com/document/d/1XmbHy6XqBrMoH9rkSyOG0wbwQZgf0epz-07lr_NfikI/edit?tab=t.0#heading=h.gw6ivvgpdre0).

## Special Detached Job Tracking Flow

### How to know if you need this flow

You likely need detached job tracking if either of these is true:
- You're submitting your job in detached mode, with the job client running on a remote machine meaning the client and the JobManager are in different JVMs.
- In the default flow, you're only seeing the `START` event and no `RUNNING`, `COMPLETE`, or `FAIL` events afterward, which is a strong signal that your job is being submitted remotely in detached mode.

### Enabling it

Enable the flow by setting `openlineage.flink.enableDetachedJobTracking` to `true`. See the
[configuration section](./configuration.md#flink-specific-configuration) for this option and the related
`openlineage.flink.detachedStartEventEmitTimeoutInSeconds` timeout.

With this enabled, the `START` event is emitted from the submitting (client) process, while later status
events (`RUNNING`, `COMPLETE`, `FAIL`) are handled by the JobManager listener.

### How it works

When you submit remotely in detached mode, the job client and the JobManager run in different JVMs,
typically on different machines. The default flow assumes both lifecycle stages happen in the same JVM:
the client-side `START` event carries the Flink job ID, and the JobManager-side listener relies on that
same event to initialize its tracking.

Because the client is remote and lives in a separate JVM, the JobManager-side listener never receives
that job ID. It cannot associate incoming status changes with the existing run, so it does not emit OpenLineage
events for them. This is why you see `START` event but nothing after it.

Detached job tracking fixes this by having the JobManager-side listener reconstruct the same OpenLineage
run id from the Flink job id and the job start time exposed by the Flink jobs API, so the
post-submission lifecycle events correlate back to the original `START`.

### Limitation

Unlike the default flow, detached job tracking does not emit an immediate `RUNNING` event right after `START`
to avoid race conditions. When checkpoint tracking is enabled, later `RUNNING` events are emitted from checkpoint tracking using the
`openlineage.trackingIntervalInSeconds` interval. Set `openlineage.flink.disableCheckpointTracking=true`
to suppress checkpoint polling and those checkpoint-driven `RUNNING` updates.
