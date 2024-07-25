/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.Resources;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.EventEmitterProviderExtension;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.Tuple2;

@Slf4j
@ExtendWith(SparkAgentTestExtension.class)
class LibraryTest {
  public static EventEmitter emitter = mock(EventEmitter.class);

  @BeforeAll
  public static void beforeAll() {
    // create EventEmitter before starting the SparkSession, to capture
    // SparkListenerApplicationStart event
    EventEmitterProviderExtension.setup(emitter);
  }

  @AfterAll
  public static void afterAll() {
    EventEmitterProviderExtension.stop();
  }

  @Test
  @SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
  void testRdd(@TempDir Path tmpDir, SparkSession spark) throws IOException {
    URL url = Resources.getResource("test_data/data.txt");
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    JavaRDD<String> textFile = sc.textFile(url.getPath());

    textFile
        .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum)
        .saveAsTextFile(tmpDir.toString() + "/output");

    sc.stop();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(emitter, times(4)).emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();
    assertEquals(4, events.size());

    // verify first application event
    RunEvent applicationStart = events.get(0);

    assertThat(applicationStart.getJob())
        .hasFieldOrPropertyWithValue("namespace", "ns_name")
        .hasFieldOrPropertyWithValue("name", "test_rdd");

    assertThat(applicationStart.getRun().getFacets().getParent().getJob())
        .hasFieldOrPropertyWithValue("namespace", "parent_namespace")
        .hasFieldOrPropertyWithValue("name", "parent_name");

    assertThat(applicationStart.getEventType()).isEqualTo(EventType.START);

    // verify first job event
    RunEvent first = events.get(1);

    assertThat(first.getJob())
        .hasFieldOrPropertyWithValue("namespace", "ns_name")
        .hasFieldOrPropertyWithValue(
            "name", "test_rdd.map_partitions_shuffled_map_partitions_hadoop");

    assertThat(first.getRun().getFacets().getParent().getJob())
        .hasFieldOrPropertyWithValue("namespace", "ns_name")
        .hasFieldOrPropertyWithValue("name", "test_rdd");

    assertThat(first.getInputs())
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(first.getInputs().get(0).getName()).endsWith("test_data");
    assertThat(first.getEventType()).isEqualTo(EventType.START);

    assertThat(first.getJob().getFacets().getJobType())
        .hasFieldOrPropertyWithValue("jobType", "RDD_JOB")
        .hasFieldOrPropertyWithValue("processingType", "BATCH")
        .hasFieldOrPropertyWithValue("integration", "SPARK");

    // verify second job event
    RunEvent second = events.get(2);

    assertThat(second.getJob())
        .hasFieldOrPropertyWithValue("namespace", "ns_name")
        .hasFieldOrPropertyWithValue(
            "name", "test_rdd.map_partitions_shuffled_map_partitions_hadoop");

    assertThat(second.getRun().getFacets().getParent().getJob())
        .hasFieldOrPropertyWithValue("namespace", "ns_name")
        .hasFieldOrPropertyWithValue("name", "test_rdd");

    assertThat(second.getOutputs())
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(second.getOutputs().get(0).getName()).endsWith("output");
    assertThat(second.getEventType()).isEqualTo(EventType.COMPLETE);

    // verify complete application event
    RunEvent applicationComplete = events.get(3);

    assertThat(applicationComplete.getJob())
        .hasFieldOrPropertyWithValue("namespace", "ns_name")
        .hasFieldOrPropertyWithValue("name", "test_rdd");

    assertThat(applicationComplete.getRun().getFacets().getParent().getJob())
        .hasFieldOrPropertyWithValue("namespace", "parent_namespace")
        .hasFieldOrPropertyWithValue("name", "parent_name");

    assertThat(applicationComplete.getEventType()).isEqualTo(EventType.COMPLETE);

    verifySerialization(events);
  }

  @Test
  void testRDDName(SparkSession spark) {
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    JavaRDD<Integer> numbers =
        sc.parallelize(
            IntStream.range(1, 100).mapToObj(Integer::valueOf).collect(Collectors.toList()));
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
