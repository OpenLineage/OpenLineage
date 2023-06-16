/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateHiveTableAsSelectCommand} and extracts
 * the output {@link OpenLineage.Dataset} being written.
 */
public class CreateHiveTableAsSelectCommandVisitor
    extends QueryPlanVisitor<CreateHiveTableAsSelectCommand, OpenLineage.OutputDataset> {

  public CreateHiveTableAsSelectCommandVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    CreateHiveTableAsSelectCommand command = (CreateHiveTableAsSelectCommand) x;
    CatalogTable table = command.tableDesc();
    DatasetIdentifier di = PathUtils.fromCatalogTable(table);

    // zip query outputs with attribute names
    LogicalPlan query = command.query();
    List<Attribute> attributes = ScalaConversionUtils.fromSeq(command.query().output());

    List<Attribute> schemaAttributes = new ArrayList<>();
    IntStream.range(0, attributes.size())
        .filter(index -> index < query.output().size())
        .forEach(
            index ->
                schemaAttributes.add(
                    attributes.get(index).withName(command.outputColumnNames().apply(index))));

    return Collections.singletonList(
        outputDataset()
            .getDataset(
                di,
                outputSchema(schemaAttributes),
                OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE));
  }

  private StructType outputSchema(List<Attribute> attrs) {
    return new StructType(
        attrs.stream()
            .map(a -> new StructField(a.name(), a.dataType(), a.nullable(), a.metadata()))
            .toArray(StructField[]::new));
  }
}
