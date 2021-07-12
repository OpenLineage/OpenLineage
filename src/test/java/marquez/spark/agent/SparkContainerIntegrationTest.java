package marquez.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-test")
@Testcontainers
public class SparkContainerIntegrationTest {

  private static final Network network = Network.newNetwork();
  @Container private static final PostgreSQLContainer<?> postgres = makePostgresContainer();
  @Container private static final GenericContainer<?> marquez = makeMarquezContainer();
  private static GenericContainer<?> pyspark;
  private static HttpHost marquezHost;
  private static DefaultHttpClient httpClient;

  @BeforeAll
  public static void setup() {
    marquezHost = new HttpHost(marquez.getHost(), marquez.getFirstMappedPort());
    httpClient = new DefaultHttpClient(new PoolingClientConnectionManager());
  }

  @AfterEach
  public void cleanupSpark() {
    pyspark.stop();
  }

  @AfterAll
  public static void tearDown() {
    Logger logger = LoggerFactory.getLogger(SparkContainerIntegrationTest.class);
    try {
      postgres.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down postgres container", e2);
    }
    try {
      marquez.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down marquez container", e2);
    }
    try {
      pyspark.stop();
    } catch (Exception e2) {
      logger.error("Unable to shut down pyspark container", e2);
    }
    network.close();
  }

  private static PostgreSQLContainer<?> makePostgresContainer() {
    return new PostgreSQLContainer<>(DockerImageName.parse("postgres:11.8"))
        .withNetwork(network)
        .withNetworkAliases("postgres")
        .withDatabaseName("postgres")
        .withUsername("postgres")
        .withPassword("password")
        .withLogConsumer(SparkContainerIntegrationTest::consumeOutput)
        .withStartupTimeout(Duration.of(2, ChronoUnit.MINUTES))
        .withEnv(
            ImmutableMap.of(
                "POSTGRES_USER", "postgres",
                "POSTGRES_PASSWORD", "password",
                "MARQUEZ_DB", "marquez",
                "MARQUEZ_USER", "marquez",
                "MARQUEZ_PASSWORD", "marquez"))
        .withFileSystemBind("../../docker/init-db.sh", "/docker-entrypoint-initdb.d/init-db.sh");
  }

  private static GenericContainer<?> makeMarquezContainer() {
    return new GenericContainer<>(
            DockerImageName.parse(
                "marquezproject/marquez:" + System.getProperty("marquez.image.tag")))
        .withNetwork(network)
        .withNetworkAliases("marquez")
        .withExposedPorts(5000)
        .waitingFor(Wait.forHttp("/api/v1/namespaces"))
        .withStartupTimeout(Duration.of(2, ChronoUnit.MINUTES))
        .withLogConsumer(SparkContainerIntegrationTest::consumeOutput)
        .dependsOn(postgres);
  }

  private static GenericContainer<?> makePysparkContainer(String... command) {
    return new GenericContainer<>(
            DockerImageName.parse("godatadriven/pyspark:" + System.getProperty("spark.version")))
        .withNetwork(network)
        .withNetworkAliases("spark")
        .withFileSystemBind("src/test/resources/test_data", "/test_data")
        .withFileSystemBind("src/test/resources/spark_scripts", "/opt/spark_scripts")
        .withFileSystemBind("build/libs", "/opt/libs")
        .withLogConsumer(SparkContainerIntegrationTest::consumeOutput)
        .withStartupTimeout(Duration.of(2, ChronoUnit.MINUTES))
        .dependsOn(marquez)
        .withReuse(true)
        .withCommand(command);
  }

  private static void consumeOutput(org.testcontainers.containers.output.OutputFrame of) {
    try {
      switch (of.getType()) {
        case STDOUT:
          System.out.write(of.getBytes());
          break;
        case STDERR:
          System.err.write(of.getBytes());
          break;
        case END:
          System.out.println(of.getUtf8String());
          break;
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Test
  public void testPysparkWordCountWithCliArgs() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainer(
            "--master",
            "local",
            "--conf",
            "spark.openlineage.host=http://marquez:5000",
            "--conf",
            "spark.openlineage.namespace=testPysparkWordCountWithCliArgs",
            "--conf",
            "spark.extraListeners=" + SparkListener.class.getName(),
            "--jars",
            "/opt/libs/" + System.getProperty("marquez.spark.jar"),
            "/opt/spark_scripts/spark_word_count.py");
    pyspark.setWaitStrategy(Wait.forLogMessage(".*ShutdownHookManager: Shutdown hook called.*", 1));
    pyspark.start();

    HttpResponse response =
        httpClient.execute(
            marquezHost, new HttpGet("/api/v1/namespaces/testPysparkWordCountWithCliArgs/jobs"));
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    JsonNode jsonNode = new ObjectMapper().readTree(response.getEntity().getContent());
    assertThat(jsonNode).matches(j -> ((JsonNode) j).has("jobs"), "Has jobs key");
    assertThat(jsonNode.get("jobs"))
        .matches(j -> ((JsonNode) j).isArray())
        .hasSize(1)
        .first()
        .extracting(j -> j.get("name").textValue())
        .isEqualTo(
            "open_lineage_integration_word_count.execute_insert_into_hadoop_fs_relation_command");
  }

  @Test
  public void testPysparkRddToTable() throws IOException, InterruptedException {
    pyspark =
        makePysparkContainer(
            "--master",
            "local",
            "--conf",
            "spark.openlineage.host=http://marquez:5000",
            "--conf",
            "spark.openlineage.namespace=testPysparkRddToTable",
            "--conf",
            "spark.extraListeners=" + SparkListener.class.getName(),
            "--jars",
            "/opt/libs/" + System.getProperty("marquez.spark.jar"),
            "/opt/spark_scripts/spark_rdd_to_table.py");
    pyspark.setWaitStrategy(Wait.forLogMessage(".*ShutdownHookManager: Shutdown hook called.*", 1));
    pyspark.start();

    HttpResponse response =
        httpClient.execute(
            marquezHost, new HttpGet("/api/v1/namespaces/testPysparkRddToTable/jobs"));
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    JsonNode jsonNode = new ObjectMapper().readTree(response.getEntity().getContent());
    assertThat(jsonNode).matches(j -> ((JsonNode) j).has("jobs"), "Has jobs key");
    assertThat(jsonNode.get("jobs"))
        .matches(j -> ((JsonNode) j).isArray())
        .hasSize(2)
        .map(j -> j.get("name").textValue())
        .containsAll(
            Arrays.asList(
                "spark_rdd_to_table.map_partitions_python_list_of_random_words_and_numbers",
                "spark_rdd_to_table.execute_insert_into_hadoop_fs_relation_command"));
    for (JsonNode n : jsonNode.get("jobs")) {
      assertThat(n.get("outputs")).hasSize(1);
    }
  }
}
