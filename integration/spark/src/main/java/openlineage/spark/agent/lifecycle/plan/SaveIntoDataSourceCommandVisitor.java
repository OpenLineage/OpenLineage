package openlineage.spark.agent.lifecycle.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import openlineage.spark.agent.client.LineageEvent.Dataset;
import openlineage.spark.agent.facets.OutputStatisticsFacet;
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
 * output {@link Dataset} being written. Since the output datasource is a {@link BaseRelation}, we
 * wrap it with an artificial {@link LogicalRelation} so we can delegate to other plan visitors.
 */
public class SaveIntoDataSourceCommandVisitor
    extends AbstractPartialFunction<LogicalPlan, List<Dataset>> {
  private final SQLContext sqlContext;
  private final List<PartialFunction<LogicalPlan, List<Dataset>>> relationVisitors;

  public SaveIntoDataSourceCommandVisitor(
      SQLContext sqlContext, List<PartialFunction<LogicalPlan, List<Dataset>>> relationVisitors) {
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
  public List<Dataset> apply(LogicalPlan x) {
    OutputStatisticsFacet outputStats =
        PlanUtils.getOutputStats(((SaveIntoDataSourceCommand) x).metrics());
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
              Builder<String, Object> facetsMap =
                  ImmutableMap.<String, Object>builder().put("stats", outputStats);
              if (ds.getFacets().getAdditionalFacets() != null) {
                facetsMap.putAll(ds.getFacets().getAdditionalFacets());
              }
              ds.getFacets().setAdditional(facetsMap.build());
            })
        .collect(Collectors.toList());
  }
}
