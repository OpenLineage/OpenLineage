/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

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
  public static final EventEmitter OPEN_LINEAGE_SPARK_CONTEXT = mock(EventEmitter.class);

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    OpenLineageSparkListener.init(new StaticExecutionContextFactory(OPEN_LINEAGE_SPARK_CONTEXT));
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    Mockito.reset(OPEN_LINEAGE_SPARK_CONTEXT);
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
        .when(OPEN_LINEAGE_SPARK_CONTEXT)
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
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", warehouseDir)
        .getOrCreate();
  }

  public static OpenLineageContext newContext(SparkSession sparkSession) {
    return OpenLineageContext.builder()
        .sparkSession(Optional.of(sparkSession))
        .sparkContext(sparkSession.sparkContext())
        .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_PRODUCER_URI))
        .build();
  }
}
