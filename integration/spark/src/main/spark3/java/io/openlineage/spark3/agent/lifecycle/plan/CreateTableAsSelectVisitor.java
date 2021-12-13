package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableAsSelect} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateTableAsSelectVisitor
    extends QueryPlanVisitor<CreateTableAsSelect, OpenLineage.Dataset> {

  private final SparkSession sparkSession;

  public CreateTableAsSelectVisitor(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    CreateTableAsSelect command = (CreateTableAsSelect) x;

    DatasetIdentifier datasetIdentifier;
    try {
      datasetIdentifier =
          CatalogUtils3.getDatasetIdentifier(
              sparkSession,
              command.catalog(),
              command.tableName(),
              ScalaConversionUtils.<String, String>fromMap(command.properties()));
    } catch (UnsupportedCatalogException ex) {
      log.error(String.format("Catalog %s is unsupported", ex.getMessage()), ex);
      return Collections.emptyList();
    }

    return Collections.singletonList(
        PlanUtils.getDataset(
            datasetIdentifier.getName(),
            datasetIdentifier.getNamespace(),
            PlanUtils.datasetFacet(command.tableSchema(), datasetIdentifier.getNamespace())));
  }
}
