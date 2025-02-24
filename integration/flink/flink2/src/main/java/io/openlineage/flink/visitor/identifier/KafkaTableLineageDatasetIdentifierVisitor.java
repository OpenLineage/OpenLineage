/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.flink.wrapper.TableLineageDatasetWrapper;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.catalog.CatalogBaseTable;
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
    log.debug("Calling isDefinedAt for dataset {}", dataset);

    CatalogBaseTable table = new TableLineageDatasetWrapper(dataset).getTable().orElse(null);

    if (table == null) {
      log.info("Table is null for dataset {}", dataset);
      return false;
    }

    // TODO: get comment from catalogBaseTable's comment field

    Map<String, String> options = table.getOptions();
    if (options == null) {
      log.info("Table options are null for dataset {}", dataset);
      return false;
    }

    if (!options.containsKey("connector")) {
      log.info("Table options does not contain connector for dataset {}", dataset);
      return false;
    }

    log.debug("Table options contains connector {}", options.get("connector"));
    return KAFKA_CONNECTOR.equals(options.get("connector"));
  }

  @Override
  public Collection<DatasetIdentifier> apply(LineageDataset dataset) {
    CatalogBaseTable table = new TableLineageDatasetWrapper(dataset).getTable().orElseThrow();

    if (log.isDebugEnabled()) {
      log.debug(
          "Extracting dataset identifier from table options bootstrap-servers={} topics={}",
          table.getOptions().get("properties.bootstrap.servers"),
          table.getOptions().get("topic"));
    }

    return Collections.singletonList(
        datasetIdentifierForKafka(
            table.getOptions().get("properties.bootstrap.servers"),
            table.getOptions().get("topic"),
            dataset.name()));
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
        topic, namespace, List.of(new Symlink(tableName, namespace, SymlinkType.TABLE)));
  }
}
