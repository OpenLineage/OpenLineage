/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.MockServerUtils.verifyEvents;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.mockserver.model.HttpRequest.request;

import com.google.common.collect.ImmutableMap;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockserver.client.MockServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import scala.collection.immutable.HashMap;

@Tag("integration-test")
@Tag("kafka")
@ExtendWith(SparkAgentTestExtension.class)
@Testcontainers
class SparkKafkaIntegrationTest {

  private static final String EVENT_TYPE = "eventType";
  private static final String NAMESPACE = "namespace";
  private static final String NAME = "name";
  private static final Network network = Network.newNetwork();
  private static final KafkaContainer kafka = SparkContainerUtils.makeKafkaContainer(network);

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      SparkContainerUtils.makeMockServerContainer(network);

  private static final String PACKAGES = "--packages";
  private static GenericContainer<?> pyspark;
  private static MockServerClient mockServerClient;
  private static final Logger logger = LoggerFactory.getLogger(SparkKafkaIntegrationTest.class);

  @BeforeAll
  public static void setup() {
    mockServerClient =
        new MockServerClient(
            openLineageClientMockContainer.getHost(),
            openLineageClientMockContainer.getServerPort());
    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));

    Awaitility.await().until(openLineageClientMockContainer::isRunning);
  }

  @AfterEach
  public void cleanupSpark() {
    mockServerClient.reset();
    try {
      if (pyspark != null) pyspark.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down pyspark container", e2);
    }
  }

  @AfterAll
  public static void tearDown() {
    try {
      openLineageClientMockContainer.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down openlineage client container", e2);
    }
    try {
      if (kafka != null) kafka.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down kafka container", e2);
    }
    network.close();
  }

  @Test
  void testPysparkKafkaReadWrite() {
    kafka.start();

    pyspark =
        SparkContainerUtils.makePysparkContainerWithDefaultConf(
            network,
            openLineageClientMockContainer,
            "testPysparkKafkaReadWriteTest",
            PACKAGES,
            System.getProperty("kafka.package.version"),
            "/opt/spark_scripts/spark_kafka.py");
    pyspark.start();

    verifyEvents(
        mockServerClient,
        "pysparkKafkaWriteStartEvent.json",
        "pysparkKafkaWriteCompleteEvent.json",
        "pysparkKafkaReadStartEvent.json",
        "pysparkKafkaReadCompleteEvent.json");
  }

  @Test
  @SneakyThrows
  void testPysparkKafkaReadAssign() {
    kafka.start();

    ImmutableMap<String, Object> kafkaProps =
        ImmutableMap.of(
            "bootstrap.servers",
            kafka.getHost() + ":" + kafka.getMappedPort(KafkaContainer.KAFKA_PORT));
    AdminClient admin = AdminClient.create(kafkaProps);
    try {
      CreateTopicsResult topicsResult =
          admin.createTopics(
              Arrays.asList(
                  new NewTopic("topicA", 1, (short) 1), new NewTopic("topicB", 1, (short) 1)));
      topicsResult.topicId("topicA").get();
    } catch (TopicExistsException | ExecutionException e) {
      // it's OK -> topic exists
    }

    SparkContainerUtils.runPysparkContainerWithDefaultConf(
        network,
        openLineageClientMockContainer,
        "testPysparkKafkaReadAssignTest",
        "spark_kafk_assign_read.py");

    verifyEvents(
        mockServerClient,
        "pysparkKafkaAssignReadStartEvent.json",
        "pysparkKafkaAssignReadCompleteEvent.json");
  }

  @Test
  void testWriteWithKafkaSourceProvider(SparkSession spark)
      throws InterruptedException, TimeoutException {
    kafka.start();
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("key", StringType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("value", BinaryType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    spark
        .createDataFrame(
            Arrays.asList(
                new GenericRow(new Object[] {"seven", "seven".getBytes(StandardCharsets.UTF_8)}),
                new GenericRow(new Object[] {"one", "one".getBytes(StandardCharsets.UTF_8)}),
                new GenericRow(
                    new Object[] {"fourteen", "fourteen".getBytes(StandardCharsets.UTF_8)}),
                new GenericRow(
                    new Object[] {"sixteen", "sixteen".getBytes(StandardCharsets.UTF_8)})),
            schema)
        .write()
        .format("kafka")
        .option("topic", "topicA")
        .option("kafka.bootstrap.servers", kafka.getBootstrapServers())
        .save();

    StaticExecutionContextFactory.waitForExecutionEnd();
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(4))
        .emit(lineageEvent.capture());
    OpenLineage.RunEvent completeEvent = lineageEvent.getAllValues().get(2);
    assertThat(completeEvent).hasFieldOrPropertyWithValue(EVENT_TYPE, RunEvent.EventType.COMPLETE);
    String kafkaNamespace =
        "kafka://" + kafka.getHost() + ":" + kafka.getMappedPort(KafkaContainer.KAFKA_PORT);
    assertThat(completeEvent.getOutputs())
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue(NAME, "topicA")
        .hasFieldOrPropertyWithValue(NAMESPACE, kafkaNamespace);
  }

  @Test
  void testReadWithKafkaSourceProviderUsingAssignConfig(SparkSession spark)
      throws InterruptedException, TimeoutException, ExecutionException {
    kafka.start();
    Properties p = new Properties();
    p.setProperty("bootstrap.servers", kafka.getBootstrapServers());
    p.setProperty("key.serializer", StringSerializer.class.getName());
    p.setProperty("value.serializer", StringSerializer.class.getName());
    KafkaProducer<String, String> producer = new KafkaProducer<>(p);
    CompletableFuture.allOf(
            sendMessage(producer, new ProducerRecord<>("oneTopic", 0, "theKey", "theValue")),
            sendMessage(
                producer, new ProducerRecord<>("twoTopic", 0, "anotherKey", "anotherValue")))
        .get(10, TimeUnit.SECONDS);

    producer.flush();

    producer.close();
    Dataset<Row> kafkaDf =
        spark
            .read()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka.getBootstrapServers())
            .option("assign", "{\"oneTopic\": [0], \"twoTopic\": [0]}")
            .load();
    kafkaDf.collect();

    StaticExecutionContextFactory.waitForExecutionEnd();
    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(4))
        .emit(lineageEvent.capture());
    OpenLineage.RunEvent completeEvent = lineageEvent.getAllValues().get(2);
    assertThat(completeEvent).hasFieldOrPropertyWithValue(EVENT_TYPE, RunEvent.EventType.COMPLETE);
    String kafkaNamespace =
        "kafka://" + kafka.getHost() + ":" + kafka.getMappedPort(KafkaContainer.KAFKA_PORT);
    assertThat(completeEvent.getInputs())
        .hasSize(2)
        .satisfiesExactlyInAnyOrder(
            dataset ->
                assertThat(dataset)
                    .hasFieldOrPropertyWithValue(NAME, "oneTopic")
                    .hasFieldOrPropertyWithValue(NAMESPACE, kafkaNamespace),
            dataset -> assertThat(dataset.getName()).isEqualTo("twoTopic"));
  }

  private CompletableFuture sendMessage(
      KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
    CompletableFuture future = new CompletableFuture();
    producer.send(
        record,
        (md, e) -> {
          if (e != null) {
            future.completeExceptionally(e);
          } else {
            future.complete(md);
          }
        });
    return future;
  }
}
