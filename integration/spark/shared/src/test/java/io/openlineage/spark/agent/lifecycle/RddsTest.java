/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.Dependency;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

@SuppressWarnings("PMD")
class RddsTest {

  @AfterEach
  void tearDown() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @Test
  void assertFindFileLikeRddsWhenDependeciesAreNull() {
    RDD<?> rdd = mock(RDD.class);
    when(rdd.getDependencies()).thenReturn(null);
    assertThat(Rdds.findFileLikeRdds(rdd)).isEmpty();
  }

  @Test
  void assertFlattenRdds() {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    List<Integer> seqNumList = IntStream.rangeClosed(10, 20).boxed().collect(Collectors.toList());

    JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

    JavaRDD<Integer> parallelRdd = jsc.parallelize(seqNumList, 5);
    JavaRDD<Integer> mappedRdd = parallelRdd.map(i -> i + 1);
    JavaRDD<Integer> filteredRdd = mappedRdd.filter(i -> i % 2 == 0);
    JavaRDD<Integer> mappedRdd2 = filteredRdd.map(i -> i + 10);

    assertThat(Rdds.flattenRDDs(mappedRdd2.rdd(), new HashSet<>()).size() == 4);
  }

  @Test
  void assertCyclesInRdds() {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    List<Integer> seqNumList = IntStream.rangeClosed(10, 20).boxed().collect(Collectors.toList());

    JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());
    Dependency mockDep = mock(Dependency.class);
    Seq<Dependency> mockDepSeq =
        JavaConverters.asScalaIteratorConverter(Arrays.asList(mockDep).iterator())
            .asScala()
            .toSeq();

    JavaRDD<Integer> parallelRdd = jsc.parallelize(seqNumList, 5);
    RDD<Integer> spyRdd = spy(parallelRdd.rdd());

    when(mockDep.rdd()).thenReturn(spyRdd);
    doReturn(mockDepSeq).when(spyRdd).dependencies();

    Set<RDD<?>> flattenedRdds = Rdds.flattenRDDs(spyRdd, new HashSet<>());

    assertThat(flattenedRdds.size() == 1);
  }
}
