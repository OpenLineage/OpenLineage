/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.visitor.identifier.catalog.CatalogSymlinkProvider;
import io.openlineage.flink.visitor.identifier.catalog.GenericInMemoryCatalogSymlinkProvider;
import io.openlineage.flink.wrapper.TableLineageDatasetWrapper;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.listener.CatalogContext;
import org.apache.flink.table.planner.lineage.TableLineageDataset;

/** Class to extract dataset identifier from {@link TableLineageDataset} stored on Kafka. */
@Slf4j
public class KafkaTableLineageDatasetIdentifierVisitor implements DatasetIdentifierVisitor {
  private static final String KAFKA_CONNECTOR = "kafka";
  private static final String KAFKA_DATASET_PREFIX = "kafka://";
  private static final String COMMA = ",";
  private static final String SEMICOLON = ";";

  private final List<CatalogSymlinkProvider> catalogSymlinkProviders;

  public KafkaTableLineageDatasetIdentifierVisitor() {
    catalogSymlinkProviders = List.of(new GenericInMemoryCatalogSymlinkProvider());
  }

  @Override
  public boolean isDefinedAt(LineageDataset dataset) {
    log.debug("Calling isDefinedAt for dataset {}", dataset);

    CatalogBaseTable table = new TableLineageDatasetWrapper(dataset).getTable().orElse(null);

    if (table == null) {
      log.info("Table is null for dataset {}", dataset);
      return false;
    }

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
    TableLineageDatasetWrapper wrapper = new TableLineageDatasetWrapper(dataset);
    CatalogBaseTable table = wrapper.getTable().orElseThrow();
    Optional<CatalogContext> catalogContext = wrapper.getCatalogContext();

    if (log.isDebugEnabled()) {
      log.debug(
          "Extracting dataset identifier from table options bootstrap-servers={} topics={}",
          table.getOptions().get("properties.bootstrap.servers"),
          table.getOptions().get("topic"));
    }

    DatasetIdentifier identifier =
        datasetIdentifierForKafka(
            table.getOptions().get("properties.bootstrap.servers"),
            table.getOptions().get("topic"));

    catalogContext
        .flatMap(
            context ->
                catalogSymlinkProviders.stream()
                    .filter(c -> c.isDefinedAt(context.getClazz()))
                    .map(p -> p.getSymlink(context, dataset))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst())
        .ifPresent(identifier::withSymlink);

    return Collections.singletonList(identifier);
  }

  private DatasetIdentifier datasetIdentifierForKafka(String bootstrapServers, String topic) {
    String kafkaHost = bootstrapServers;
    if (bootstrapServers.contains(COMMA)) {
      kafkaHost = bootstrapServers.split(COMMA)[0];
    } else if (bootstrapServers.contains(SEMICOLON)) {
      kafkaHost = bootstrapServers.split(SEMICOLON)[0];
    }

    String namespace = String.format(KAFKA_DATASET_PREFIX + kafkaHost);

    return new DatasetIdentifier(topic, namespace);
  }
}
