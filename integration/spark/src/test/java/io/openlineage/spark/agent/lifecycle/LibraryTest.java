package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.client.OpenLineageClient;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.Tuple2;

@ExtendWith(SparkAgentTestExtension.class)
public class LibraryTest {

  private final TypeReference<Map<String, Object>> mapTypeReference =
      new TypeReference<Map<String, Object>>() {};

  @AfterEach
  public void tearDown() throws Exception {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @RepeatedTest(30)
  public void testSparkSql() throws IOException, TimeoutException {
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getJobNamespace())
        .thenReturn("ns_name");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentJobName())
        .thenReturn("job_name");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("ea445b5c-22eb-457a-8007-01c7c52b6e54")));

    final SparkSession spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("Word Count")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();

    URL url = Resources.getResource("test_data/data.txt");
    final Dataset<String> data = spark.read().textFile(url.getPath());

    final long numAs = data.filter((FilterFunction<String>) s -> s.contains("a")).count();
    final long numBs = data.filter((FilterFunction<String>) s -> s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    spark.sparkContext().listenerBus().waitUntilEmpty(1000);
    spark.stop();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(4))
        .emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();

    updateSnapshots("sparksql", events);

    assertEquals(4, events.size());

    ObjectMapper objectMapper = OpenLineageClient.getObjectMapper();
    for (int i = 0; i < events.size(); i++) {
      OpenLineage.RunEvent event = events.get(i);
      Map<String, Object> snapshot =
          objectMapper.readValue(
              Paths.get(String.format("integrations/%s/%d.json", "sparksql", i + 1)).toFile(),
              mapTypeReference);
      assertEquals(
          snapshot,
          cleanSerializedMap(
              objectMapper.readValue(objectMapper.writeValueAsString(event), mapTypeReference)));
    }
    verifySerialization(events);
  }

  private Map<String, Object> cleanSerializedMap(Map<String, Object> map) {
    // exprId and jvmId are not deterministic, so remove them from the maps to avoid failing
    map.remove("exprId");
    map.remove("resultId");

    if (map.containsKey("facets") && map.containsKey("runId")) {
      map.put("runId", "fake_run_id");
    }

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
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getJobNamespace())
        .thenReturn("ns_name");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentJobName())
        .thenReturn("job_name");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b")));

    URL url = Resources.getResource("test_data/data.txt");
    SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> textFile = sc.textFile(url.getPath());

    textFile
        .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum)
        .count();

    sc.stop();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(2))
        .emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();
    assertEquals(2, events.size());

    updateSnapshots("sparkrdd", events);

    for (int i = 0; i < events.size(); i++) {
      OpenLineage.RunEvent event = events.get(i);
      String snapshot =
          new String(
              Files.readAllBytes(
                  Paths.get(String.format("integrations/%s/%d.json", "sparkrdd", i + 1))));

      Map<String, Object> eventFields =
          OpenLineageClient.getObjectMapper().convertValue(event, mapTypeReference);
      ((Map<String, Object>) eventFields.get("run")).replace("runId", "fake_run_id");

      assertEquals(
          OpenLineageClient.getObjectMapper().readValue(snapshot, mapTypeReference), eventFields);
    }

    verifySerialization(events);
  }

  @Test
  public void testRDDName() {
    SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<Integer> numbers =
        sc.parallelize(IntStream.range(1, 100).mapToObj(Integer::new).collect(Collectors.toList()));
    numbers.setName("numbers");
    JavaRDD<String> transformed =
        numbers.filter(n -> n > 10 && n < 90).map(i -> i * i).map(String::valueOf);
    String s = RddExecutionContext.nameRDD(transformed.rdd());
    assertThat(s).isEqualTo("map_partitions_numbers");
  }

  private void verifySerialization(List<OpenLineage.RunEvent> events)
      throws JsonProcessingException {
    for (OpenLineage.RunEvent event : events) {
      assertNotNull(
          "Event can serialize", OpenLineageClient.getObjectMapper().writeValueAsString(event));
    }
  }

  private void updateSnapshots(String prefix, List<OpenLineage.RunEvent> events) {
    if (System.getenv().containsKey("UPDATE_SNAPSHOT")) {
      for (int i = 0; i < events.size(); i++) {
        OpenLineage.RunEvent event = events.get(i);
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
