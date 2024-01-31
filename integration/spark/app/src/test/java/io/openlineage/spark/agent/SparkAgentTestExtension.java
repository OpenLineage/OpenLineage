/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalog.Table;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/** JUnit extension that sets up SparkSession for OpenLineage context. */
public class SparkAgentTestExtension
    implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {
  public static final EventEmitter EVENT_EMITTER = mock(EventEmitter.class);

  @SuppressWarnings("PMD") // always point locally
  private static final String LOCAL_IP = "127.0.0.1";

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    when(SparkAgentTestExtension.EVENT_EMITTER.getJobNamespace()).thenReturn("ns_name");
    when(SparkAgentTestExtension.EVENT_EMITTER.getParentJobName())
        .thenReturn(Optional.of("parent_name"));
    when(SparkAgentTestExtension.EVENT_EMITTER.getParentJobNamespace())
        .thenReturn(Optional.of("parent_namespace"));
    when(SparkAgentTestExtension.EVENT_EMITTER.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b")));
    when(SparkAgentTestExtension.EVENT_EMITTER.getApplicationRunId())
        .thenReturn(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    when(SparkAgentTestExtension.EVENT_EMITTER.getApplicationJobName()).thenReturn("test_rdd");

    OpenLineageSparkListener.init(new StaticExecutionContextFactory(EVENT_EMITTER));
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    Mockito.reset(EVENT_EMITTER);
    when(SparkAgentTestExtension.EVENT_EMITTER.getJobNamespace()).thenReturn("ns_name");
    when(SparkAgentTestExtension.EVENT_EMITTER.getParentJobName())
        .thenReturn(Optional.of("parent_name"));
    when(SparkAgentTestExtension.EVENT_EMITTER.getParentJobNamespace())
        .thenReturn(Optional.of("parent_namespace"));
    when(SparkAgentTestExtension.EVENT_EMITTER.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b")));
    when(SparkAgentTestExtension.EVENT_EMITTER.getApplicationRunId())
        .thenReturn(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    when(SparkAgentTestExtension.EVENT_EMITTER.getApplicationJobName()).thenReturn("test_rdd");
    Mockito.doAnswer(
            (arg) -> {
              LoggerFactory.getLogger(getClass())
                  .info(
                      "Emit called with args {}",
                      Arrays.stream(arg.getArguments())
                          .map(this::describe)
                          .collect(Collectors.toList()));
              return null;
            })
        .when(EVENT_EMITTER)
        .emit(any(RunEvent.class));
  }

  private Map describe(Object arg) {
    try {
      return BeanUtils.describe(arg);
    } catch (Exception e) {
      LoggerFactory.getLogger(getClass()).error("Unable to describe event {}", arg, e);
      return new HashMap();
    }
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    try {
      ScalaConversionUtils.asJavaOptional(SparkSession.getActiveSession())
          .ifPresent(
              session -> {
                Table[] tables = (Table[]) session.catalog().listTables().collect();
                Arrays.stream(tables)
                    .filter(Table::isTemporary)
                    .forEach(table -> session.catalog().dropTempView(table.name()));
              });
    } catch (Exception e) {
      // ignore
    }
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().equals(SparkSession.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    String testName = parameterContext.getDeclaringExecutable().getName();
    String warehouseDir =
        new File("spark-warehouse/")
            .getAbsoluteFile()
            .toPath()
            .resolve(testName)
            .resolve(String.valueOf(Instant.now().getEpochSecond()))
            .toString();
    return SparkSession.builder()
        .master("local[*]")
        .appName(testName)
        .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
        .config("spark.driver.host", LOCAL_IP)
        .config("spark.driver.bindAddress", LOCAL_IP)
        .config("spark.sql.warehouse.dir", warehouseDir)
        .config("spark.openlineage.facets.custom_environment_variables", "[TEST_VAR;]")
        .getOrCreate();
  }

  public static OpenLineageContext newContext(SparkSession sparkSession) {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    return OpenLineageContext.builder()
        .sparkSession(sparkSession)
        .sparkContext(sparkSession.sparkContext())
        .openLineage(openLineage)
        .customEnvironmentVariables(Arrays.asList("TEST_VAR"))
        .build();
  }
}
