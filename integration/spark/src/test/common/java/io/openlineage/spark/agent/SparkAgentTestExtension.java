/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
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
                  .info("Emit called with arg {}", BeanUtils.describe(arg));
              return null;
            })
        .when(OPEN_LINEAGE_SPARK_CONTEXT)
        .emit(any(RunEvent.class));
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
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
    return SparkSession.builder()
        .master("local[*]")
        .appName(parameterContext.getDeclaringExecutable().getName())
        .config("spark.extraListeners", OpenLineageSparkListener.class.getName())
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate();
  }

  public static OpenLineageContext newContext(SparkSession sparkSession) {
    return OpenLineageContext.builder()
        .sparkSession(Optional.of(sparkSession))
        .sparkContext(sparkSession.sparkContext())
        .openLineage(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
        .build();
  }
}
