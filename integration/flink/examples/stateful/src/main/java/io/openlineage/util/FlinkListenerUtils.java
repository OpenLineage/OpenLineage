/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.util;

import io.openlineage.flink.OpenLineageFlinkJobListener;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The example applications are used within test phase of building flink integration, so they
 * cannot make use of OpenLineageFlinkJobListener classes as this would cause cyclic dependency.
 * This utils class uses reflection to instantiate {@link OpenLineageFlinkJobListener}.
 *
 * Please note that the recommended way to achieve that is by using the builder:
 * <pre>
*           OpenLineageFlinkJobListener.builder()
*             .executionEnvironment(streamExecutionEnvironment)
*             .jobNamespace(jobNamespace)
*             .jobName(jobName)
*             .build();
 * </pre>
 */
public class FlinkListenerUtils {

   public static JobListener instantiate(StreamExecutionEnvironment executionEnvironment) throws Exception {
     Object builder = MethodUtils.invokeStaticMethod(
         Class.forName("io.openlineage.flink.OpenLineageFlinkJobListener"),
         "builder"
     );

     builder = MethodUtils.invokeMethod(builder, "jobNamespace", "flink_job_namespace");
     builder = MethodUtils.invokeMethod(builder, "jobName", "flink_examples_stateful");
     builder = MethodUtils.invokeMethod(builder, "executionEnvironment", executionEnvironment);

     return (JobListener)MethodUtils.invokeMethod(builder, "build");
   }
}
