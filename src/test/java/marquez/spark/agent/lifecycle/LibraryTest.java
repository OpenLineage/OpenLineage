package marquez.spark.agent.lifecycle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import marquez.spark.agent.MarquezAgent;
import marquez.spark.agent.MarquezContext;
import marquez.spark.agent.client.LineageEvent;
import marquez.spark.agent.client.OpenLineageClient;
import net.bytebuddy.agent.ByteBuddyAgent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.Tuple2;

public class LibraryTest {
  static MarquezContext marquezContext;

  @BeforeAll
  public static void setUp() throws Exception {
    marquezContext = mock(MarquezContext.class);
    ByteBuddyAgent.install();
    MarquezAgent.premain(
        "/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54",
        ByteBuddyAgent.getInstrumentation(),
        new StaticExecutionContextFactory(marquezContext));
  }

  @AfterEach
  public void tearDown() throws Exception {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @RepeatedTest(30)
  public void testSparkSql() throws IOException, TimeoutException {
    reset(marquezContext);
    when(marquezContext.getJobNamespace()).thenReturn("ns_name");
    when(marquezContext.getJobName()).thenReturn("job_name");
    when(marquezContext.getParentRunId()).thenReturn("ea445b5c-22eb-457a-8007-01c7c52b6e54");

    final SparkSession spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("Word Count")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();

    URL url = Resources.getResource("data.txt");
    final Dataset<String> data = spark.read().textFile(url.getPath());

    final long numAs = data.filter((FilterFunction<String>) s -> s.contains("a")).count();
    final long numBs = data.filter((FilterFunction<String>) s -> s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    spark.sparkContext().listenerBus().waitUntilEmpty(1000);
    spark.stop();

    ArgumentCaptor<LineageEvent> lineageEvent = ArgumentCaptor.forClass(LineageEvent.class);
    Mockito.verify(marquezContext, times(4)).emit(lineageEvent.capture());
    List<LineageEvent> events = lineageEvent.getAllValues();

    updateSnapshots("sparksql", events);

    assertEquals(4, events.size());

    ObjectMapper objectMapper = OpenLineageClient.getObjectMapper();
    for (int i = 0; i < events.size(); i++) {
      LineageEvent event = events.get(i);
      Map<String, Object> snapshot =
          objectMapper.readValue(
              Paths.get(String.format("integrations/%s/%d.json", "sparksql", i + 1)).toFile(),
              new TypeReference<Map<String, Object>>() {});
      assertEquals(
          snapshot,
          cleanSerializedMap(
              objectMapper.readValue(
                  objectMapper.writeValueAsString(event),
                  new TypeReference<Map<String, Object>>() {})));
    }
    verifySerialization(events);
  }

  private Map<String, Object> cleanSerializedMap(Map<String, Object> map) {
    // exprId and jvmId are not deterministic, so remove them from the maps to avoid failing
    map.remove("exprId");
    map.remove("resultId");

    // timezone is different in CI than local
    map.remove("timeZoneId");
    if (map.containsKey("namespace") && map.get("namespace").equals("file")) {
      map.put("name", "/path/to/data");
    }
    if (map.containsKey("uri") && ((String) map.get("uri")).startsWith("file:/")) {
      map.put("uri", "file:/path/to/data");
    }
    map.forEach(
        (k, v) -> {
          if (v instanceof Map) {
            cleanSerializedMap((Map<String, Object>) v);
          } else if (v instanceof List) {
            cleanSerializedList((List<?>) v);
          }
        });
    return map;
  }

  private void cleanSerializedList(List<?> l) {
    l.forEach(
        i -> {
          if (i instanceof Map) {
            cleanSerializedMap((Map<String, Object>) i);
          } else if (i instanceof List) {
            cleanSerializedList((List<?>) i);
          }
        });
  }

  @Test
  public void testRdd() throws IOException {
    reset(marquezContext);
    when(marquezContext.getJobNamespace()).thenReturn("ns_name");
    when(marquezContext.getJobName()).thenReturn("job_name");
    when(marquezContext.getParentRunId()).thenReturn("8d99e33e-2a1c-4254-9600-18f23435fc3b");

    URL url = Resources.getResource("data.txt");
    SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> textFile = sc.textFile(url.getPath());

    textFile
        .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum)
        .count();

    sc.stop();

    ArgumentCaptor<LineageEvent> lineageEvent = ArgumentCaptor.forClass(LineageEvent.class);
    Mockito.verify(marquezContext, times(2)).emit(lineageEvent.capture());
    List<LineageEvent> events = lineageEvent.getAllValues();
    assertEquals(2, events.size());

    updateSnapshots("sparkrdd", events);

    for (int i = 0; i < events.size(); i++) {
      LineageEvent event = events.get(i);
      String snapshot =
          new String(
              Files.readAllBytes(
                  Paths.get(String.format("integrations/%s/%d.json", "sparkrdd", i + 1))));
      assertEquals(
          snapshot,
          OpenLineageClient.getObjectMapper()
              .writerWithDefaultPrettyPrinter()
              .writeValueAsString(event));
    }

    verifySerialization(events);
  }

  private void verifySerialization(List<LineageEvent> events) throws JsonProcessingException {
    for (LineageEvent event : events) {
      assertNotNull(
          "Event can serialize", OpenLineageClient.getObjectMapper().writeValueAsString(event));
    }
  }

  private void updateSnapshots(String prefix, List<LineageEvent> events) {
    if (System.getenv().containsKey("UPDATE_SNAPSHOT")) {
      for (int i = 0; i < events.size(); i++) {
        LineageEvent event = events.get(i);
        try {
          String url = String.format("integrations/%s/%d.json", prefix, i + 1);
          FileWriter myWriter = new FileWriter(url);
          myWriter.write(
              OpenLineageClient.getObjectMapper()
                  .writerWithDefaultPrettyPrinter()
                  .writeValueAsString(event));
          myWriter.close();
        } catch (IOException e) {
          e.printStackTrace();
          fail();
        }
      }
    }
  }
}
