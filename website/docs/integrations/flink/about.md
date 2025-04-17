---
sidebar_position: 1
title: About
---

**Apache Flink** is one of the most popular stream processing frameworks. Apache Flink jobs run on clusters,
which are composed of two types of nodes: `TaskManagers` and `JobManagers`. While clusters typically consists of
multiple `TaskManagers`, only reason to run multiple JobManagers is high availability. The jobs are _submitted_
to `JobManager` by `JobClient`, that compiles user application into dataflow graph which is understandable by `JobManager`.
`JobManager` then coordinates job execution: it splits the parallel units of a job
to `TaskManagers`, manages heartbeats, triggers checkpoints, reacts to failures and much more.

Apache Flink has multiple deployment modes - Session Mode, Application Mode and Per-Job mode. The most popular
are Session Mode and Application Mode. Session Mode consists of a `JobManager` managing multiple jobs sharing single
Flink cluster. In this mode, `JobClient` is executed on a machine that submits the job to the cluster.

Application Mode is used where cluster is utilized for a single job. In this mode, `JobClient`, where the main method runs,
is executed on the `JobManager`.

Flink jobs read data from `Sources` and write data to `Sinks`. In contrast to systems like Apache Spark, Flink jobs can write
data to multiple places - they can have multiple `Sinks`.

## Lineage metadata for Flink 1.x and 2.x

While there is a single OpenLineage connector for Flink, it offers two distinct implementations for Flink versions 1.x and 2.x.

The Flink 1.x connector is built on the JobListener interface, which Flink uses to notify users about job submissions, successful completions, or failures. 
However, `JobListener` does not provide lineage metadata. Consequently, the OpenLineage integration depends on the Transformations from the job’s `ExecutionEnvironment`. 
To enable this functionality, modifications to the Flink job code are necessary to incorporate `ExecutionEnvironment` within the `OpenLineageFlinkJobListener` instance. 
Additionally, this implementation does not support Flink SQL.

Conversely, the Flink 2.0 connector leverages Flink's native interfaces to access lineage metadata, which were introduced by [FLIP-314](https://cwiki.apache.org/confluence/display/FLINK/FLIP-314%3A+Support+Customized+Job+Lineage+Listener). 
One of the advantages of this implementation is that it requires no changes to the job code and does support Flink SQL.
Both implementations reside within the same package and share the same configuration options.