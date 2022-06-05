package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockserver.client.MockServerClient;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import scala.collection.immutable.HashMap;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static io.openlineage.spark.agent.util.TestOpenLineageEventHandlerFactory.FAILING_TABLE_NAME_FAIL_ON_APPLY;
import static io.openlineage.spark.agent.util.TestOpenLineageEventHandlerFactory.FAILING_TABLE_NAME_FAIL_ON_IS_DEFINED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockserver.model.HttpRequest.request;

/**
 * Class containing tests to verify that Spark job executes properly even when problems related to
 * OpenLineage encountered
 */
@Tag("integration-test")
@Testcontainers
@ExtendWith(SparkAgentTestExtension.class)
@Slf4j
public class SparkOpenLineageFailuresTest {

  private static final Network network = Network.newNetwork();

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      SparkContainerUtils.makeMockServerContainer(network);

  private static final String INCORRECT_PORT = "3000";
  private static final String CORRECT_PORT = "1080";

  private static MockServerClient mockServerClient;

  @AfterEach
  public void cleanupSpark() {
    if (mockServerClient != null) {
      mockServerClient.reset();
    }
  }

  @Test
  public void testIncorrectOpenLineageEndpointUrl() {
    SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            "http://openlineageclient:" + INCORRECT_PORT,
            ".*Spark is fine!.*",
            openLineageClientMockContainer,
            "testFailures",
            "/opt/spark_scripts/spark_test_failures.py")
        .start();
  }

  @Test
  public void testOpenLineageEndpointResponds400() {
    configureMockServerToRespondWith(400);

    SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            "http://openlineageclient:" + CORRECT_PORT,
            ".*Spark is fine!.*",
            openLineageClientMockContainer,
            "testFailures",
            "/opt/spark_scripts/spark_test_failures.py")
        .start();
  }

  @Test
  public void testOpenLineageEndpointResponds500() {
    configureMockServerToRespondWith(500);

    SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            "http://openlineageclient:" + CORRECT_PORT,
            ".*Spark is fine!.*",
            openLineageClientMockContainer,
            "testFailures",
            "/opt/spark_scripts/spark_test_failures.py")
        .start();
  }

  @Test
  public void testOpenLineageFailingVisitor(SparkSession spark)
      throws InterruptedException, TimeoutException {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("aString", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    // make sure table location in warehouse dir is empty
    cleanLocation(spark, FAILING_TABLE_NAME_FAIL_ON_APPLY);
    cleanLocation(spark, FAILING_TABLE_NAME_FAIL_ON_IS_DEFINED);

    Dataset<Row> dataset =
        spark.createDataFrame(Arrays.asList(new GenericRow(new Object[] {"one"})), schema);

    dataset.write().mode(SaveMode.Overwrite).saveAsTable(FAILING_TABLE_NAME_FAIL_ON_IS_DEFINED);
    dataset.write().mode(SaveMode.Overwrite).saveAsTable(FAILING_TABLE_NAME_FAIL_ON_APPLY);
    // creating table that way should fail because of a failing visitor defined in
    // TestOpenLineageEventHandlerFactory

    StaticExecutionContextFactory.waitForExecutionEnd();
    assertEquals(1, spark.table(FAILING_TABLE_NAME_FAIL_ON_IS_DEFINED).count());
    assertEquals(1, spark.table(FAILING_TABLE_NAME_FAIL_ON_APPLY).count());

    ArgumentMatcher<OpenLineage.RunEvent> matcher =
        new ArgumentMatcher<OpenLineage.RunEvent>() {
          @Override
          public boolean matches(OpenLineage.RunEvent event) {
            return event.getOutputs().size() == 1
                && event.getEventType().equals(OpenLineage.RunEvent.EventType.COMPLETE)
                && event.getOutputs().get(0).getName().contains("failing_table");
          }
        };

    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, Mockito.atLeast(2))
        .emit(argThat(matcher));
  }

  @SneakyThrows
  private void cleanLocation(SparkSession spark, String tableName) {
    Path locationPath = new Path(spark.conf().get("spark.sql.warehouse.dir") + "/" + tableName);
    FileSystem.get(spark.sparkContext().hadoopConfiguration()).delete(locationPath, true);
  }

  private void configureMockServerToRespondWith(int statusCode) {
    mockServerClient =
        new MockServerClient(
            openLineageClientMockContainer.getHost(),
            openLineageClientMockContainer.getServerPort());

    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(statusCode));

    Awaitility.await().until(openLineageClientMockContainer::isRunning);
  }
}
