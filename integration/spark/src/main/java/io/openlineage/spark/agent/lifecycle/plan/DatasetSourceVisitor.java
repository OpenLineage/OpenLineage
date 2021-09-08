package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2;
// import org.apache.spark.sql.sources.v2.DataSourceV2;
// import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import scala.runtime.AbstractPartialFunction;

/**
 * Find {@link org.apache.spark.sql.sources.BaseRelation}s and {@link DataSourceV2} readers and
 * writers that implement the {@link DatasetSource} interface.
 *
 * <p>Note that while the {@link DataSourceV2Relation} is a {@link
 * org.apache.spark.sql.catalyst.analysis.NamedRelation}, the returned name is that of the source,
 * not the specific dataset (e.g., "bigquery" not the table). While the {@link DataSourceV2Relation}
 * is a {@link LogicalPlan}, its {@link DataSourceReader} and {@link
 * org.apache.spark.sql.sources.v2.writer.DataSourceWriter} fields are not. Thus, the only (current)
 * way of extracting the actual dataset name is to attempt to cast the {@link DataSourceReader}
 * and/or {@link org.apache.spark.sql.sources.v2.writer.DataSourceWriter} instances to {@link
 * DatasetSource}s.
 */
@Slf4j
public class DatasetSourceVisitor
    extends AbstractPartialFunction<LogicalPlan, List<OpenLineage.Dataset>> {

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return findDatasetSource(x).isPresent();
  }

  private Optional<DatasetSource> findDatasetSource(LogicalPlan plan) {
    log.info("CALLED WITH " + plan.getClass().getName());
    if (plan instanceof LogicalRelation) {
      if (((LogicalRelation) plan).relation() instanceof DatasetSource) {
        return Optional.of((DatasetSource) ((LogicalRelation) plan).relation());
      }
      // Check the DataSourceV2Relation's reader.
      // Note that we don't check the writer here as it is always encapsulated by the
      // WriteToDataSourceV2 LogicalPlan below.
    } else if (plan instanceof DataSourceV2Relation) {
      DataSourceV2Relation relation = (DataSourceV2Relation) plan;
      log.error("Failed to find method DataSourceV2Relation.source() - probably using Spark 3");
      Table table = relation.table();
      log.error("TABLE");
      log.error(table.properties().toString());
      return Optional.of(new DatasetSource(){
        public String namespace() {
          String format = table.properties().getOrDefault("provider", "unknown");
          String location = table.properties().getOrDefault("location", "unknown");
          // On GCS: format=iceberg/parquet, (...) location=gs://test-bucket-ol/spark-wh/database/table 
          return String.format("%s://%s", format, location);
        }

        public String name() {
          return table.name();
        }
      });

        // return Optional.empty();
    }

      // Check the WriteToDataSourceV2's writer
    // } else if (plan instanceof WriteToDataSourceV2
    //     && ((WriteToDataSourceV2) plan).writer() instanceof DatasetSource) {
    //   return Optional.of((DatasetSource) ((WriteToDataSourceV2) plan).writer());
    // }
    return Optional.empty();
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    DatasetSource datasetSource =
        findDatasetSource(x)
            .orElseThrow(() -> new RuntimeException("Couldn't find DatasetSource in plan " + x));
    return Collections.singletonList(
        PlanUtils.getDataset(
            datasetSource.name(),
            datasetSource.namespace(),
            PlanUtils.datasetFacet(x.schema(), datasetSource.namespace())));
  }
}
