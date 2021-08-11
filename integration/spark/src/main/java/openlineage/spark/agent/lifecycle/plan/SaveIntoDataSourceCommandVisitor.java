package openlineage.spark.agent.lifecycle.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.openlineage.client.OpenLineage;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import scala.Option;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that matches an {@link SaveIntoDataSourceCommand} and extracts the
 * output {@link OpenLineage.Dataset} being written. Since the output datasource is a {@link
 * BaseRelation}, we wrap it with an artificial {@link LogicalRelation} so we can delegate to other
 * plan visitors.
 */
public class SaveIntoDataSourceCommandVisitor
    extends AbstractPartialFunction<LogicalPlan, List<OpenLineage.Dataset>> {
  private final SQLContext sqlContext;
  private final List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> relationVisitors;

  public SaveIntoDataSourceCommandVisitor(
      SQLContext sqlContext,
      List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> relationVisitors) {
    this.sqlContext = sqlContext;
    this.relationVisitors = relationVisitors;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof SaveIntoDataSourceCommand
        && (((SaveIntoDataSourceCommand) x).dataSource() instanceof SchemaRelationProvider
            || ((SaveIntoDataSourceCommand) x).dataSource() instanceof RelationProvider);
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    BaseRelation relation;
    if (((SaveIntoDataSourceCommand) x).dataSource() instanceof RelationProvider) {
      RelationProvider p = (RelationProvider) ((SaveIntoDataSourceCommand) x).dataSource();
      relation = p.createRelation(sqlContext, ((SaveIntoDataSourceCommand) x).options());
    } else {
      SchemaRelationProvider p =
          (SchemaRelationProvider) ((SaveIntoDataSourceCommand) x).dataSource();
      relation =
          p.createRelation(sqlContext, ((SaveIntoDataSourceCommand) x).options(), x.schema());
    }
    return Optional.ofNullable(
            PlanUtils.applyFirst(
                relationVisitors,
                new LogicalRelation(
                    relation, relation.schema().toAttributes(), Option.empty(), x.isStreaming())))
        .orElse(Collections.emptyList()).stream()
        // constructed datasets don't include the output stats, so add that facet here
        .peek(
            ds -> {
              Builder<String, OpenLineage.CustomFacet> facetsMap =
                  ImmutableMap.<String, OpenLineage.CustomFacet>builder();
              if (ds.getFacets().getAdditionalProperties() != null) {
                facetsMap.putAll(ds.getFacets().getAdditionalProperties());
              }
              ds.getFacets().getAdditionalProperties().putAll(facetsMap.build());
            })
        .collect(Collectors.toList());
  }
}
