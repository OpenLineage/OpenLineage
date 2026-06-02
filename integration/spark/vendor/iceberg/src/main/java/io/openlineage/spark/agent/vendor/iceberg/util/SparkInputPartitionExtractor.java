/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.InputPartitionExtractor;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ReflectionUtils;
import io.openlineage.spark.agent.vendor.iceberg.IcebergVendor;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class SparkInputPartitionExtractor implements InputPartitionExtractor {
  private static final String SPARK_INPUT_PARTITION =
      "org.apache.iceberg.spark.source.SparkInputPartition";

  private final TableExtractor tableExtractor;

  public SparkInputPartitionExtractor() {
    this(new TableExtractor());
  }

  public SparkInputPartitionExtractor(TableExtractor tableExtractor) {
    this.tableExtractor = tableExtractor;
  }

  @Override
  public boolean isDefinedAt(InputPartition inputPartition) {
    if (!new IcebergVendor().isVendorAvailable()) {
      return false;
    }

    try {
      Class<?> sparkInputPartitionClass = Class.forName(SPARK_INPUT_PARTITION);
      return sparkInputPartitionClass.isInstance(inputPartition);
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Could not load {}: {}", SPARK_INPUT_PARTITION, e.getMessage());
      }
      return false;
    }
  }

  @Override
  public List<DatasetIdentifier> extract(SparkContext sparkContext, InputPartition inputPartition) {
    return getTable(inputPartition)
        .map(table -> resolveIdentifier(table, sparkContext))
        .orElse(Collections.emptyList());
  }

  @Override
  public Optional<StructType> extractSchema(
      SparkContext sparkContext, InputPartition inputPartition) {
    return getTable(inputPartition).map(table -> SparkSchemaUtil.convert(table.schema()));
  }

  private Optional<Table> getTable(InputPartition inputPartition) {
    Optional<Object> maybeTable = ReflectionUtils.tryExecuteMethod(inputPartition, "table");
    if (!maybeTable.isPresent() || !(maybeTable.get() instanceof Table)) {
      return Optional.empty();
    }
    return Optional.of((Table) maybeTable.get());
  }

  private List<DatasetIdentifier> resolveIdentifier(Table table, SparkContext sparkContext) {
    Optional<URI> tableLocation = tableExtractor.extractTableLocation(table);
    Optional<TableIdentifier> tableIdentifier = tableExtractor.extractTableIdentifier(table);

    Optional<DatasetIdentifier> di =
        tableLocation.map(
            location ->
                tableIdentifier.isPresent()
                    ? PathUtils.fromTableIdentifier(tableIdentifier.get(), sparkContext, location)
                    : PathUtils.fromURI(location));
    return di.map(Collections::singletonList).orElse(Collections.emptyList());
  }
}
