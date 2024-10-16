/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.flink.util.TableLineageDatasetUtil;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.planner.lineage.TableLineageDataset;

/** Class to extract dataset identifier from {@link TableLineageDataset} stored on Kafka. */
@Slf4j
public class KafkaTableLineageDatasetIdentifierVisitor implements DatasetIdentifierVisitor {
  private static final String KAFKA_CONNECTOR = "kafka";
  private static final String KAFKA_DATASET_PREFIX = "kafka://";
  private static final String COMMA = ",";
  private static final String SEMICOLON = ";";

  @Override
  public boolean isDefinedAt(LineageDataset dataset) {
    if (!TableLineageDatasetUtil.isOnClasspath()) {
      return false;
    }

    if (!(dataset instanceof TableLineageDataset)) {
      return false;
    }

    TableLineageDataset table = (TableLineageDataset) dataset;

    if (table.table().getOptions() == null) {
      return false;
    }

    return table.table().getOptions().containsKey("connector")
        && KAFKA_CONNECTOR.equals(table.table().getOptions().get("connector"));
  }

  @Override
  public Collection<DatasetIdentifier> apply(LineageDataset dataset) {
    TableLineageDataset table = (TableLineageDataset) dataset;

    return Collections.singletonList(
        datasetIdentifierForKafka(
            table.table().getOptions().get("properties.bootstrap.servers"),
            table.table().getOptions().get("topic"),
            table.name()));
  }

  private DatasetIdentifier datasetIdentifierForKafka(
      String bootstrapServers, String topic, String tableName) {
    String kafkaHost = bootstrapServers;
    if (bootstrapServers.contains(COMMA)) {
      kafkaHost = bootstrapServers.split(COMMA)[0];
    } else if (bootstrapServers.contains(SEMICOLON)) {
      kafkaHost = bootstrapServers.split(SEMICOLON)[0];
    }

    String namespace = String.format(KAFKA_DATASET_PREFIX + kafkaHost);

    return new DatasetIdentifier(
        topic, namespace, Arrays.asList(new Symlink(tableName, namespace, SymlinkType.TABLE)));
  }
}
