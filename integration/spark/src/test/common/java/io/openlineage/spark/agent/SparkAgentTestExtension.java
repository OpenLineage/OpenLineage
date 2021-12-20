package io.openlineage.spark.agent;

import static org.mockito.Mockito.mock;

import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import net.bytebuddy.agent.ByteBuddyAgent;
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

/**
 * JUnit extension that invokes the {@link SparkAgent} by installing the {@link ByteBuddyAgent} to
 * instrument classes. This will allow the {@link java.lang.instrument.ClassFileTransformer}s in the
 * {@link openlineage.spark.agent.transformers} package to transform the byte code of target classes
 * as they're loaded.
 *
 * <p>Note that this extension has to be annotated on any class that interacts with any of the
 * transformed classes (i.e., {@link org.apache.spark.SparkContext}, {@link
 * org.apache.spark.sql.SparkSession}, etc.). Once a class has been loaded, it won't go through the
 * {@link java.lang.instrument.ClassFileTransformer} process again. If a test doesn't use this
 * extension and ends up running before other Spark tests, those subsequent tests will fail.
 */
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
}
