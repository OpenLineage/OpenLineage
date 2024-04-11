/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PocQueryListener implements QueryExecutionListener {
  private static final Logger log = LoggerFactory.getLogger(PocQueryListener.class);

  @Override
  public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
    LogicalPlan output = qe.optimizedPlan(); // this is the output node
    List<LogicalPlan> leafNodes = toJavaList(output.collectLeaves()); // these are the input nodes

    // OK. Let's try and explain what's going on here.
    // When Spark is run in a streaming configuration AND when the user decides to use
    // `foreachBatch(ds: Dataset[Row], batchId: Long)` to process each micro-batch,
    // Spark loses track of the LOGICAL lineage for that micro-batch. (I'm not sure why they lose
    // the logical lineage, but same to say LogicalRDD.fromDataset() erases the logical lineage.
    // The physical lineage, however, is still retained. This listener tries to reconstruct the
    // logical lineage by looking at the physical partitioning information.
    // Specifically, it looks at the DataSourceRDDPartitions (which represent the physical
    // partitions read from external sources like Kafka)
    // It then dives into the InputPartitions of those DataSourceRDDPartitions. In the case of
    // Kafka, these are KafkaBatchInputPartition instances.
    // Now, the reason why we use a proxy and reflection is because for some reason we sometimes
    // cannot cast the InputPartition to its concrete type.
    // I suspect class loader issues, but this is all speculation.
    // This is all proof-of-concept code. We will need to find a home for it, in the main code base.
    // See integration/spark/scala-fixtures/src/main/scala/io/openlineage/spark/test/ForeachBatchKafkaSource.scala for an example

    List<KafkaBatchInputPartitionProxy> inputPartitions =
        leafNodes.stream()
            .filter(LogicalRDD.class::isInstance)
            .map(LogicalRDD.class::cast)
            .map(LogicalRDD::rdd)
            .flatMap(rdd -> Arrays.stream(rdd.partitions()))
            .filter(DataSourceRDDPartition.class::isInstance)
            .map(DataSourceRDDPartition.class::cast)
            .flatMap(partition -> toJavaList(partition.inputPartitions()).stream())
            .filter(
                inputPartition ->
                    inputPartition
                        .getClass()
                        .getCanonicalName()
                        .equals(
                            KafkaBatchInputPartitionProxy.KAFKA_BATCH_INPUT_PARTITION_CLASS_NAME))
            .map(KafkaBatchInputPartitionProxy::new)
            .collect(Collectors.toList());

    inputPartitions.forEach(
        partition -> {
          log.info("Topic: {}", partition.topic());
          log.info("Partition: {}", partition.partition());
          log.info("Bootstrap Servers: {}", partition.bootstrapServers());
        });
  }

  private static <E> List<E> toJavaList(scala.collection.Seq<E> seq) {
    return toJavaList(seq.iterator());
  }

  private static <E> List<E> toJavaList(scala.collection.immutable.Seq<E> seq) {
    return toJavaList(seq.iterator());
  }

  private static <E> List<E> toJavaList(scala.collection.Iterator<E> iterator) {
    List<E> list = new ArrayList<>();
    while (iterator.hasNext()) {
      E value = iterator.next();
      list.add(value);
    }
    return list;
  }

  @Override
  public void onFailure(String funcName, QueryExecution qe, Exception exception) {}
}
