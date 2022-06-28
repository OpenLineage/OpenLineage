package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.CacheManager;
import org.apache.spark.sql.execution.CachedData;
import org.apache.spark.sql.execution.columnar.InMemoryRelation;
import org.apache.spark.sql.internal.SharedState;
import scala.collection.IndexedSeq;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class InMemoryRelationInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<InMemoryRelation> {

  public InMemoryRelationInputDatasetBuilder(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return true;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(InMemoryRelation x) {
    return Collections.emptyList();
  }

  @Override
  public List<OpenLineage.InputDataset> apply(
      SparkListenerEvent event, InMemoryRelation inMemoryRelation) {
    try {
      SharedState sharedState = context.getSparkSession().get().sharedState();
      CacheManager cacheManager =
          (CacheManager)
              FieldUtils.getField(SharedState.class, "cacheManager", true).get(sharedState);
      IndexedSeq<CachedData> cachedDataIndexedSeq =
          (IndexedSeq<CachedData>)
              FieldUtils.getField(CacheManager.class, "cachedData", true).get(cacheManager);

      return ScalaConversionUtils.<CachedData>fromSeq(cachedDataIndexedSeq).stream()
          .filter(
              cachedData ->
                  cachedData
                      .cachedRepresentation()
                      .cacheBuilder()
                      .cachedName()
                      .equals(inMemoryRelation.cacheBuilder().cachedName()))
          .map(cachedData -> (LogicalPlan) cachedData.plan())
          .findAny()
          .map(
              plan ->
                  ScalaConversionUtils.fromSeq(
                          plan.collect(
                              delegate(
                                  context.getInputDatasetQueryPlanVisitors(),
                                  context.getInputDatasetBuilders(),
                                  event)))
                      .stream()
                      .flatMap(Collection::stream)
                      .collect(Collectors.toList()))
          .orElse(Collections.<OpenLineage.InputDataset>emptyList());
    } catch (Exception e) {
      log.warn("cannot extract logical plan", e);
      // do nothing
    }
    return Collections.emptyList();
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof InMemoryRelation;
  }
}
