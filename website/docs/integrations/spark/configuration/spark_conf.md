---
sidebar_position: 2
title: Spark Config Parameters
---


The following parameters can be specified:

| Parameter                                          | Definition                                                                                                                                                                        | Example                                      |
----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------
| spark.openlineage.transport.type                   | The transport type used for event emit, default type is `console`                                                                                                                 | http                                         |
| spark.openlineage.namespace                        | The default namespace to be applied for any jobs submitted                                                                                                                        | MyNamespace                                  |
| spark.openlineage.parentJobNamespace               | The job namespace to be used for the parent job facet                                                                                                                             | ParentJobNamespace                           |
| spark.openlineage.parentJobName                    | The job name to be used for the parent job facet                                                                                                                                  | ParentJobName                                |
| spark.openlineage.parentRunId                      | The RunId of the parent job that initiated this Spark job                                                                                                                         | xxxx-xxxx-xxxx-xxxx                          |
| spark.openlineage.appName                          | Custom value overwriting Spark app name in events                                                                                                                                 | AppName                                      |
| spark.openlineage.facets.disabled                  | List of facets to disable, enclosed in `[]` (required from 0.21.x) and separated by `;`, default is `[spark_unknown;]` (currently must contain `;`)                               | \[spark_unknown;spark.logicalPlan\]          |
| spark.openlineage.capturedProperties               | comma separated list of properties to be captured in spark properties facet (default `spark.master`, `spark.app.name`)                                                            | "spark.example1,spark.example2"              |
| spark.openlineage.dataset.removePath.pattern       | Java regular expression that removes `?<remove>` named group from dataset path. Can be used to last path subdirectories from paths like `s3://my-whatever-path/year=2023/month=04` | `(.*)(?<remove>\/.*\/.*)`                    |
| spark.openlineage.jobName.appendDatasetName        | Decides whether output dataset name should be appended to job name. By default `true`.                                                                                            | false                                        |
| spark.openlineage.jobName.replaceDotWithUnderscore | Replaces dots in job name with underscore. Can be used to mimic legacy behaviour on Databricks platform. By default `false`.                                                      | false                                        |
| spark.openlineage.debugFacet                       | Determines whether debug facet shall be generated and included within the event. Set `enabled` to turn it on. By default, facet is disabled.                                      | enabled                                      |
| spark.openlineage.job.owners.<ownership-type\>     | Specifies ownership of the job. Multiple entries with different types are allowed. Config key name and value are used to create job ownership type and name (available since 1.13).  | spark.openlineage.job.owners.team="Some Team" |
