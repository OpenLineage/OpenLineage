/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static io.openlineage.spark.agent.SparkAgentTestExtension.EVENT_EMITTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.Resources;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

@Slf4j
@ExtendWith(SparkAgentTestExtension.class)
class LibraryTest {
  @BeforeEach
  public void beforeEach() throws Exception {
    Mockito.reset(EVENT_EMITTER);
    when(EVENT_EMITTER.getJobNamespace()).thenReturn("ns_name");
    when(EVENT_EMITTER.getParentJobName()).thenReturn(Optional.of("parent_name"));
    when(EVENT_EMITTER.getParentJobNamespace()).thenReturn(Optional.of("parent_namespace"));
    when(EVENT_EMITTER.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b")));
    when(EVENT_EMITTER.getApplicationRunId())
        .thenReturn(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    when(EVENT_EMITTER.getApplicationJobName()).thenReturn("test_rdd");
    Mockito.doAnswer(
            (arg) -> {
              LoggerFactory.getLogger(getClass())
                  .info(
                      "Emit called with args {}",
                      Arrays.stream(arg.getArguments())
                          .map(this::describe)
                          .collect(Collectors.toList()));
              return null;
            })
        .when(EVENT_EMITTER)
        .emit(any(RunEvent.class));
  }

  @Test
  void testRdd(@TempDir Path tmpDir, SparkSession spark) throws IOException {
    URL url = Resources.getResource("test_data/data.txt");
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    String urlPath = url.getPath();
    JavaRDD<String> textFile = sc.textFile(urlPath);

    textFile
        .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum)
        .saveAsTextFile(tmpDir.toString() + "/output");

    sc.stop();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(EVENT_EMITTER, times(4)).emit(lineageEvent.capture());
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

    // This needed to change, because of the addition of additional source sets.
    // Previously, we were hard-coding the expectation to be 'test/test_data', but
    // with the additional source sets, the path to the resource changes.
    // This will also handle any issues that may arise, if someone runs these tests
    // on a Windows machine.
    Path path = Paths.get(urlPath);
    Path parentPath = path.getParent();
    assertThat(first.getInputs().get(0).getName()).endsWith(parentPath.toString());
    assertThat(first.getEventType()).isEqualTo(EventType.START);

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

  private Map describe(Object arg) {
    try {
      return BeanUtils.describe(arg);
    } catch (Exception e) {
      LoggerFactory.getLogger(getClass()).error("Unable to describe event {}", arg, e);
      return new HashMap();
    }
  }
}
