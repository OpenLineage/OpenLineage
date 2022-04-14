package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.StructType;

/**
 * Class extending {@link io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder}
 * with methods only available for Spark3. It is required to support datasetVersionFacet for delta
 * provider
 */
@Slf4j
public class LogicalRelationDatasetBuilder<D extends OpenLineage.Dataset>
    extends io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder<D> {

  public LogicalRelationDatasetBuilder(
      OpenLineageContext context, DatasetFactory datasetFactory, boolean searchDependencies) {
    super(context, datasetFactory, searchDependencies);
  }

  @Override
  public List<D> apply(LogicalRelation logRel) {
    List<D> datasets = super.apply(logRel);
    Optional<D> dataset = datasets.stream().findAny();

    if (!dataset.isPresent() || !(dataset.get() instanceof OpenLineage.OutputDataset)) {
      return datasets;
    }

    OpenLineage.OutputDataset outputDataset = (OpenLineage.OutputDataset) dataset.get();

    StructType schema;
    if (logRel.relation() instanceof HadoopFsRelation) {
      schema = ((HadoopFsRelation) logRel.relation()).schema();
    } else if (logRel.catalogTable().isDefined()) {
      schema = logRel.catalogTable().get().schema();
    } else {
      return datasets;
    }

    return ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, schema)
        .map(facet -> ColumnLevelLineageUtils.rewriteOutputDataset(outputDataset, facet))
        .map(el -> Collections.singletonList((D) el))
        .orElse(datasets);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return true;
  }

  @Override
  protected Optional<String> getDatasetVersion(LogicalRelation x) {
    return DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(x);
  }
}
