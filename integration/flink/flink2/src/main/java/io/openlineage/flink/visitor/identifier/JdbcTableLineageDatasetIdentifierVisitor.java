/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.jdbc.JdbcDatasetUtils;
import io.openlineage.flink.wrapper.TableLineageDatasetWrapper;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.planner.lineage.TableLineageDataset;

/** Class to extract dataset identifier from {@link TableLineageDataset}. */
@Slf4j
public class JdbcTableLineageDatasetIdentifierVisitor implements DatasetIdentifierVisitor {
  private static final String JDBC_CONNECTOR = "jdbc";

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
    return JDBC_CONNECTOR.equals(options.get("connector"));
  }

  @Override
  public Collection<DatasetIdentifier> apply(LineageDataset dataset) {
    CatalogBaseTable table = new TableLineageDatasetWrapper(dataset).getTable().orElseThrow();

    if (log.isDebugEnabled()) {
      log.debug(
          "Extracting dataset identifier from table options bootstrap-servers={} topics={}",
          table.getOptions().get("url"),
          table.getOptions().get("table-name"));
    }

    return Collections.singletonList(
        JdbcDatasetUtils.getDatasetIdentifier(
            table.getOptions().get("url"), table.getOptions().get("table-name"), new Properties()));
  }
}
