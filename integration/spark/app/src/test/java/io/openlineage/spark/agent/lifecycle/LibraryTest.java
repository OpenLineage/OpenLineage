/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.Tuple2;

@Slf4j
@ExtendWith(SparkAgentTestExtension.class)
class LibraryTest {
  private final TypeReference<Map<String, Object>> mapTypeReference =
      new TypeReference<Map<String, Object>>() {};

  //  @RepeatedTest(30)
  //  public void testSparkSql(SparkSession spark) throws IOException, TimeoutException {
  //    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getJobNamespace())
  //        .thenReturn("ns_name");
  //    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentJobName())
  //        .thenReturn("job_name");
  //    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentRunId())
  //        .thenReturn(Optional.of(UUID.fromString("ea445b5c-22eb-457a-8007-01c7c52b6e54")));
  //
  //    URL url = Resources.getResource("test_data/data.txt");
  //    final Dataset<String> data = spark.read().textFile(url.getPath());
  //
  //    final long numAs = data.filter((FilterFunction<String>) s -> s.contains("a")).count();
  //    final long numBs = data.filter((FilterFunction<String>) s -> s.contains("b")).count();
  //
  //    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
  //    spark.sparkContext().listenerBus().waitUntilEmpty(1000);
  //    spark.stop();
  //
  //    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
  //        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
  //    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(4))
  //        .emit(lineageEvent.capture());
  //    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();
  //
  //    assertEquals(4, events.size());
  //
  //    ObjectMapper objectMapper = OpenLineageClient.getObjectMapper();
  //
  //    for (int i = 0; i < events.size(); i++) {
  //      OpenLineage.RunEvent event = events.get(i);
  //      Map<String, Object> snapshot =
  //          objectMapper.readValue(
  //              Paths.get(String.format("integrations/%s/%d.json", "sparksql", i + 1)).toFile(),
  //              mapTypeReference);
  //      Map<String, Object> actual =
  //          objectMapper.readValue(objectMapper.writeValueAsString(event), mapTypeReference);
  //      assertThat(actual)
  //          .satisfies(
  //              new MatchesMapRecursively(
  //                  snapshot,
  //                  new HashSet<>(
  //                      Arrays.asList("runId", "nonInheritableMetadataKeys",
  // "validConstraints"))));
  //    }
  //    verifySerialization(events);
  //  }

  @Test
  @Disabled
  void testRdd(SparkSession spark) throws IOException {
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getJobNamespace())
        .thenReturn("ns_name");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentJobName())
        .thenReturn("job_name");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b")));

    URL url = Resources.getResource("test_data/data.txt");
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
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

    ObjectMapper objectMapper = OpenLineageClientUtils.newObjectMapper();
    for (int i = 0; i < events.size(); i++) {
      log.info("Iteration {}", i);
      OpenLineage.RunEvent event = events.get(i);
      Map<String, Object> snapshot =
          objectMapper.readValue(
              Paths.get(String.format("integrations/%s/%d.json", "sparkrdd", i + 1)).toFile(),
              mapTypeReference);
      Map<String, Object> actual =
          objectMapper.readValue(objectMapper.writeValueAsString(event), mapTypeReference);
      assertThat(actual)
          .satisfies(
              new MatchesMapRecursively(
                  snapshot,
                  new HashSet<>(
                      Arrays.asList("runId", "nonInheritableMetadataKeys", "validConstraints"))));
    }

    verifySerialization(events);
  }

  Map<String, Object> stripSchemaURL(Map<String, Object> map) {
    List<String> toRemove = new ArrayList<>();
    for (String key : map.keySet()) {
      if (key.endsWith("schemaURL")) {
        toRemove.add(key);
      } else {
        Object value = map.get(key);
        if (value instanceof Map) {
          stripSchemaURL((Map<String, Object>) value);
        }
      }
    }
    for (String key : toRemove) {
      map.remove(key);
    }
    return map;
  }

  @Test
  void testRDDName(SparkSession spark) {
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
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
          "Event can serialize",
          OpenLineageClientUtils.newObjectMapper().writeValueAsString(event));
    }
  }
}
