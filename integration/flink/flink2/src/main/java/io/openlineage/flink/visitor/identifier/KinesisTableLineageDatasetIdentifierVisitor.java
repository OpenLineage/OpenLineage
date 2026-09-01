/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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

/**
 * Visitor to extract a physical dataset identifier from a table lineage dataset backed by the
 * Kinesis SQL connector ({@code connector = 'kinesis'}).
 *
 * <p>The identifier follows the convention emitted by the Kinesis connector's {@code
 * LineageVertexProvider} on the DataStream path (FLINK-39813), so DataStream and SQL jobs operating
 * on the same stream converge on the same dataset:
 *
 * <ul>
 *   <li>namespace: {@code arn:{partition}:kinesis:{region}:{account}}
 *   <li>name: {@code stream/{streamName}}
 * </ul>
 *
 * <p>The catalog table identity is preserved as a {@code TABLE} symlink, mirroring {@link
 * KafkaTableLineageDatasetIdentifierVisitor}.
 */
@Slf4j
public class KinesisTableLineageDatasetIdentifierVisitor implements DatasetIdentifierVisitor {

  private static final String KINESIS_CONNECTOR = "kinesis";
  private static final String STREAM_ARN_OPTION = "stream.arn";
  private static final int ARN_PARTS = 6;

  @Override
  public boolean isDefinedAt(LineageDataset dataset) {
    CatalogBaseTable table = new TableLineageDatasetWrapper(dataset).getTable().orElse(null);

    if (table == null) {
      return false;
    }

    Map<String, String> options = table.getOptions();
    if (options == null) {
      return false;
    }

    return KINESIS_CONNECTOR.equals(options.get("connector"))
        && isValidStreamArn(options.get(STREAM_ARN_OPTION));
  }

  @Override
  public Collection<DatasetIdentifier> apply(LineageDataset dataset) {
    CatalogBaseTable table = new TableLineageDatasetWrapper(dataset).getTable().orElseThrow();
    String streamArn = table.getOptions().get(STREAM_ARN_OPTION);

    log.debug("Extracting dataset identifier from table option stream.arn={}", streamArn);

    // arn:{partition}:kinesis:{region}:{account}:stream/{streamName}
    String[] parts = streamArn.split(":", ARN_PARTS);
    String namespace = String.format("arn:%s:kinesis:%s:%s", parts[1], parts[3], parts[4]);
    String name = parts[5];

    return Collections.singletonList(
        new DatasetIdentifier(
            name, namespace, List.of(new Symlink(dataset.name(), namespace, SymlinkType.TABLE))));
  }

  private boolean isValidStreamArn(String streamArn) {
    return streamArn != null
        && streamArn.startsWith("arn:")
        && streamArn.split(":", ARN_PARTS).length == ARN_PARTS;
  }
}
