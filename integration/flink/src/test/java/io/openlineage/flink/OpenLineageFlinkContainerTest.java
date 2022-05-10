package io.openlineage.flink;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import com.google.common.io.Resources;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.JsonBody;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration-test")
@Testcontainers
@Slf4j
public class OpenLineageFlinkContainerTest {

  private static final Network network = Network.newNetwork();
  private static MockServerClient mockServerClient;

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      FlinkContainerUtils.makeMockServerContainer(network);

  @Container
  private static final GenericContainer zookeeper =
      FlinkContainerUtils.makeZookeeperContainer(network);

  @Container
  private static final GenericContainer kafka =
      FlinkContainerUtils.makeKafkaContainer(network, zookeeper);

  @Container
  private static final GenericContainer schemaRegistry =
      FlinkContainerUtils.makeSchemaRegistryContainer(network, kafka);

  @Container
  private static final GenericContainer generateEvents =
      FlinkContainerUtils.makeGenerateEventsContainer(network, schemaRegistry);

  private static final GenericContainer jobManager =
      FlinkContainerUtils.makeFlinkJobManagerContainer(
          network, Arrays.asList(generateEvents, openLineageClientMockContainer));

  private static final GenericContainer taskManager =
      FlinkContainerUtils.makeFlinkTaskManagerContainer(network, Arrays.asList(jobManager));

  @BeforeAll
  public static void setup() {
    mockServerClient =
        new MockServerClient(
            openLineageClientMockContainer.getHost(),
            openLineageClientMockContainer.getServerPort());
    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));

    await().until(openLineageClientMockContainer::isRunning);
  }

  @AfterAll
  public static void tearDown() {
    FlinkContainerUtils.stopAll(
        Arrays.asList(
            openLineageClientMockContainer,
            zookeeper,
            schemaRegistry,
            kafka,
            generateEvents,
            jobManager,
            taskManager));

    network.close();
  }

  @Test
  @SneakyThrows
  public void testOpenLineageEventSent() {
    taskManager.start();
    await()
        .atMost(Duration.ofMinutes(2))
        .until(() -> jobManager.getLogs().contains("New checkpoint encountered"));

    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(
                readJson(Path.of(Resources.getResource("events/expected_event.json").getPath()))),
        request()
            .withPath("/api/v1/lineage")
            .withBody(
                json(
                    readJson(
                            Path.of(
                                Resources.getResource("events/expected_event_checkpoints.json")
                                    .getPath()))
                        .getValue())));
  }

  @SneakyThrows
  private JsonBody readJson(Path path) {
    return json(new String(readAllBytes(path)), MatchType.ONLY_MATCHING_FIELDS);
  }
}
