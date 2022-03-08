/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import scala.Option;

/**
 * {@link LogicalPlan} visitor that matches an {@link SaveIntoDataSourceCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written. Since the output datasource is a {@link
 * BaseRelation}, we wrap it with an artificial {@link LogicalRelation} so we can delegate to other
 * plan visitors.
 */
@Slf4j
public class SaveIntoDataSourceCommandVisitor
    extends AbstractQueryPlanDatasetBuilder<
        SparkListenerEvent, SaveIntoDataSourceCommand, OutputDataset> {

  public SaveIntoDataSourceCommandVisitor(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return context.getSparkSession().isPresent()
        && x instanceof SaveIntoDataSourceCommand
        && (((SaveIntoDataSourceCommand) x).dataSource() instanceof SchemaRelationProvider
            || ((SaveIntoDataSourceCommand) x).dataSource() instanceof RelationProvider);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent x) {
    return super.isDefinedAt(x)
        && context
            .getQueryExecution()
            .filter(qe -> isDefinedAtLogicalPlan(qe.optimizedPlan()))
            .isPresent();
  }

  public List<OutputDataset> apply(SaveIntoDataSourceCommand cmd) {
    return Collections.emptyList();
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(SparkListenerEvent event) {
    BaseRelation relation;
    SaveIntoDataSourceCommand command =
        (SaveIntoDataSourceCommand) context.getQueryExecution().get().optimizedPlan();

    // Kafka has some special handling because the Source and Sink relations require different
    // options. A KafkaRelation for writes uses the "topic" option, while the same relation for
    // reads requires the "subscribe" option. The KafkaSourceProvider never returns a KafkaRelation
    // for write operations (it executes the real writer, then returns a dummy relation), so we have
    // to use it to construct a reader, meaning we need to change the "topic" option to "subscribe".
    // Since it requires special handling anyway, we just go ahead and extract the Dataset(s)
    // directly.
    // TODO- it may be the case that we need to extend this pattern to support arbitrary relations,
    // as other impls of CreatableRelationProvider may not be able to be handled in the generic way.
    if (KafkaRelationVisitor.isKafkaSource(command.dataSource())) {
      return KafkaRelationVisitor.createKafkaDatasets(
          outputDataset(),
          command.dataSource(),
          command.options(),
          command.mode(),
          command.schema());
    }

    if (command.dataSource().getClass().getName().contains("DeltaDataSource")) {
      if (command.options().contains("path")) {
        URI uri = URI.create(command.options().get("path").get());
        return Collections.singletonList(
            outputDataset().getDataset(PathUtils.fromURI(uri, "file"), command.schema()));
      }
    }

    SQLContext sqlContext = context.getSparkSession().get().sqlContext();
    try {
      if (command.dataSource() instanceof RelationProvider) {
        RelationProvider p = (RelationProvider) command.dataSource();
        relation = p.createRelation(sqlContext, command.options());
      } else {
        SchemaRelationProvider p = (SchemaRelationProvider) command.dataSource();
        relation = p.createRelation(sqlContext, command.options(), command.schema());
      }
    } catch (Exception ex) {
      // Bad detection of errors in scala
      if (ex instanceof SQLException) {
        // This can happen on SparkListenerSQLExecutionStart for example for sqlite, when database
        // does not exist yet - it will be created as command execution
        // Still, we can just ignore it on start, because it will work on end
        // see SparkReadWriteIntegTest.testReadFromFileWriteToJdbc
        log.warn("Can't create relation: ", ex);
        return Collections.emptyList();
      }
      throw ex;
    }
    LogicalRelation logicalRelation =
        new LogicalRelation(
            relation, relation.schema().toAttributes(), Option.empty(), command.isStreaming());
    return delegate(
            context.getOutputDatasetQueryPlanVisitors(), context.getOutputDatasetBuilders(), event)
        .applyOrElse(
            logicalRelation,
            ScalaConversionUtils.toScalaFn((lp) -> Collections.<OutputDataset>emptyList()))
        .stream()
        // constructed datasets don't include the output stats, so add that facet here
        .map(
            ds -> {
              Builder<String, OpenLineage.DatasetFacet> facetsMap =
                  ImmutableMap.<String, OpenLineage.DatasetFacet>builder();
              if (ds.getFacets().getAdditionalProperties() != null) {
                facetsMap.putAll(ds.getFacets().getAdditionalProperties());
              }
              ds.getFacets().getAdditionalProperties().putAll(facetsMap.build());
              if (SaveMode.Overwrite == command.mode()) {
                // rebuild whole dataset with a LifecycleStateChange facet added
                OpenLineage.DatasetFacets facets =
                    context
                        .getOpenLineage()
                        .newDatasetFacets(
                            ds.getFacets().getDocumentation(),
                            ds.getFacets().getDataSource(),
                            ds.getFacets().getVersion(),
                            ds.getFacets().getSchema(),
                            context
                                .getOpenLineage()
                                .newLifecycleStateChangeDatasetFacet(
                                    OpenLineage.LifecycleStateChangeDatasetFacet
                                        .LifecycleStateChange.OVERWRITE,
                                    null));

                OpenLineage.OutputDataset newDs =
                    context
                        .getOpenLineage()
                        .newOutputDataset(
                            ds.getNamespace(), ds.getName(), facets, ds.getOutputFacets());
                return newDs;
              }
              return ds;
            })
        .collect(Collectors.toList());
  }
}
