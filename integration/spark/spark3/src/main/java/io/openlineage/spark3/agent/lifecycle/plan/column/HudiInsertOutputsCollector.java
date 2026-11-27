/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.utils.HudiUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Collects the output columns of Hudi's {@code InsertIntoHoodieTableCommand} the way Hudi writes
 * them.
 *
 * <p>Spark aligns the query of its own inserts with the target table during analysis, renaming its
 * columns to the table's, so matching the query's columns to the table's by name is enough. Hudi
 * 0.x keeps the query as written and aligns it only when the command runs ({@code
 * alignQueryOutput}): by name, ignoring case, if the query's columns are exactly the table's,
 * otherwise by position. So {@code INSERT INTO t SELECT id, name FROM s} writes {@code id} into
 * {@code t}'s first data column whatever its name is, and {@code SELECT TS, ...} writes {@code TS}
 * into {@code ts}. Hudi's meta columns and the static partition columns are not part of the query
 * and are skipped.
 */
final class HudiInsertOutputsCollector {
  private static final Set<String> META_COLUMNS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "_hoodie_commit_time",
                  "_hoodie_commit_seqno",
                  "_hoodie_record_key",
                  "_hoodie_partition_path",
                  "_hoodie_file_name")));

  private HudiInsertOutputsCollector() {}

  /**
   * Adds the query's output columns to the target's columns the way Hudi aligns them, under the
   * target's spelling of the column names.
   *
   * @return {@code true} if the outputs were added, {@code false} if the plan isn't a Hudi insert
   *     or the query has a different number of columns than the target, so that the usual
   *     collection applies
   */
  static boolean collect(
      ColumnLevelLineageContext context,
      LogicalPlan command,
      LogicalPlan query,
      OpenLineage.SchemaDatasetFacet schema) {
    if (!HudiUtils.isInsertIntoHoodieTableCommand(command)) {
      return false;
    }
    Set<String> staticPartitions =
        HudiUtils.staticPartitionColumns(command).stream()
            .map(HudiInsertOutputsCollector::normalize)
            .collect(Collectors.toSet());
    List<Attribute> outputs =
        ScalaConversionUtils.fromSeq(query.output()).stream()
            .filter(attribute -> !META_COLUMNS.contains(normalize(attribute.name())))
            .collect(Collectors.toList());
    List<String> columns =
        schema.getFields().stream()
            .map(OpenLineage.SchemaDatasetFacetFields::getName)
            .filter(name -> !META_COLUMNS.contains(normalize(name)))
            .filter(name -> !staticPartitions.contains(normalize(name)))
            .collect(Collectors.toList());
    if (outputs.size() != columns.size()) {
      return false;
    }
    Map<String, String> columnsByName =
        columns.stream()
            .collect(
                Collectors.toMap(
                    HudiInsertOutputsCollector::normalize, name -> name, (first, other) -> first));
    boolean byName = matchByName(outputs, columnsByName.keySet());
    for (int i = 0; i < outputs.size(); i++) {
      Attribute output = outputs.get(i);
      String column = byName ? columnsByName.get(normalize(output.name())) : columns.get(i);
      context.getBuilder().addOutput(output.exprId(), column);
    }
    return true;
  }

  /** Hudi aligns by name only if the query's column names are distinct and are the table's. */
  private static boolean matchByName(List<Attribute> outputs, Set<String> columnNames) {
    Set<String> outputNames =
        outputs.stream().map(attribute -> normalize(attribute.name())).collect(Collectors.toSet());
    return outputNames.size() == outputs.size() && outputNames.equals(columnNames);
  }

  private static String normalize(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
