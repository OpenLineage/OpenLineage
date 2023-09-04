/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.util;

import io.openlineage.flink.OpenLineageFlinkJobListener;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The example applications are used within test phase of building flink integration, so they cannot
 * make use of OpenLineageFlinkJobListener classes as this would cause cyclic dependency. This utils
 * class uses reflection to instantiate {@link OpenLineageFlinkJobListener}.
 * <p>
 * Please note that the recommended way to achieve that is by using the builder:
 * <pre>
 *           OpenLineageFlinkJobListener.builder()
 *             .executionEnvironment(streamExecutionEnvironment)
 *             .jobNamespace(jobNamespace)
 *             .jobName(jobName)
 *             .build();
 * </pre>
 */
public class OpenLineageFlinkJobListenerBuilder {

  private Object builder;

  public static OpenLineageFlinkJobListenerBuilder create() {
    try {
      return new OpenLineageFlinkJobListenerBuilder();
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private OpenLineageFlinkJobListenerBuilder()
      throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, ClassNotFoundException {
    builder = MethodUtils.invokeStaticMethod(
        Class.forName("io.openlineage.flink.OpenLineageFlinkJobListener"),
        "builder"
    );

    // some defaults
    builder = MethodUtils.invokeMethod(builder, "jobNamespace", "flink_job_namespace");
    builder = MethodUtils.invokeMethod(builder, "jobTrackingInterval", Duration.ofSeconds(5));
  }

  public OpenLineageFlinkJobListenerBuilder jobName(String jobName)
      throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    builder = MethodUtils.invokeMethod(builder, "jobName", jobName);
    return this;
  }

  public OpenLineageFlinkJobListenerBuilder jobNamespace(String jobNamespace)
      throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    builder = MethodUtils.invokeMethod(builder, "jobNamespace", jobNamespace);
    return this;
  }

  public OpenLineageFlinkJobListenerBuilder jobTrackingInterval(Duration duration)
      throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    builder = MethodUtils.invokeMethod(builder, "jobTrackingInterval", duration);
    return this;
  }

  public OpenLineageFlinkJobListenerBuilder executionEnvironment(StreamExecutionEnvironment executionEnvironment)
      throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    builder = MethodUtils.invokeMethod(builder, "executionEnvironment", executionEnvironment);
    return this;
  }

  public JobListener build()
      throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    return (JobListener) MethodUtils.invokeMethod(builder, "build");
  }
}
