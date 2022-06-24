package io.openlineage.spark3.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static scala.collection.JavaConverters.collectionAsScalaIterableConverter;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.CacheManager;
import org.apache.spark.sql.execution.CachedData;
import org.apache.spark.sql.execution.columnar.CachedRDDBuilder;
import org.apache.spark.sql.execution.columnar.InMemoryRelation;
import org.apache.spark.sql.internal.SharedState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.collection.IndexedSeq;

public class InMemoryRelationInputDatasetBuilderTest {

  String CACHED_NAME = "some-table";

  OpenLineageContext context = mock(OpenLineageContext.class);
  SparkSession sparkSession = mock(SparkSession.class);
  SharedState sharedState = mock(SharedState.class);
  CacheManager cacheManager = mock(CacheManager.class);
  CachedData cachedData = mock(CachedData.class);
  LogicalPlan logicalPlan = mock(LogicalPlan.class);
  InMemoryRelationInputDatasetBuilder builder = new InMemoryRelationInputDatasetBuilder(context);

  InMemoryRelation inMemoryRelation = mock(InMemoryRelation.class);
  InMemoryRelation inMemoryRelationFromCacheManager = mock(InMemoryRelation.class);

  CachedRDDBuilder cacheBuilder1 = mock(CachedRDDBuilder.class);
  CachedRDDBuilder cacheBuilder2 = mock(CachedRDDBuilder.class);

  OpenLineage.InputDataset inputDataset = mock(OpenLineage.InputDataset.class);

  @BeforeEach
  public void setup() {
    when(context.getSparkSession()).thenReturn(Optional.of(sparkSession));
    when(context.getInputDatasetBuilders()).thenReturn(Collections.emptyList());
    when(context.getInputDatasetQueryPlanVisitors()).thenReturn(Collections.emptyList());

    when(sparkSession.sharedState()).thenReturn(sharedState);
    when(sharedState.cacheManager()).thenReturn(cacheManager);
    when(cachedData.plan()).thenReturn(logicalPlan);
    when(cachedData.cachedRepresentation()).thenReturn(inMemoryRelationFromCacheManager);

    when(inMemoryRelation.cacheBuilder()).thenReturn(cacheBuilder1);
    when(inMemoryRelationFromCacheManager.cacheBuilder()).thenReturn(cacheBuilder2);

    when(logicalPlan.collect(any()))
        .thenReturn(
            collectionAsScalaIterableConverter(
                    Arrays.asList((Object) Collections.singletonList(inputDataset)))
                .asScala()
                .toSeq());

    when(cacheBuilder1.cachedName()).thenReturn(CACHED_NAME);
    when(cacheBuilder2.cachedName()).thenReturn(CACHED_NAME);
  }

  @SneakyThrows
  @Test
  public void testApply() {
    IndexedSeq<CachedData> cachedItems =
        collectionAsScalaIterableConverter(Arrays.asList(cachedData)).asScala().toIndexedSeq();

    try (MockedStatic mocked = mockStatic(FieldUtils.class)) {
      Field cacheManagerField = mock(Field.class);
      when(FieldUtils.getField(SharedState.class, "cacheManager", true))
          .thenReturn(cacheManagerField);
      when(cacheManagerField.get(sharedState)).thenReturn(cacheManager);

      Field cachedDataField = mock(Field.class);
      when(FieldUtils.getField(CacheManager.class, "cachedData", true)).thenReturn(cachedDataField);
      when(cachedDataField.get(cacheManager)).thenReturn(cachedItems);

      List<OpenLineage.InputDataset> inputDatasets =
          builder.apply(mock(SparkListenerEvent.class), inMemoryRelation);
      assertEquals(1, inputDatasets.size());
      assertEquals(inputDataset, inputDatasets.get(0));
    }
  }

  @SneakyThrows
  @Test
  public void testApplyWithMultipleCachedPlans() {
    CachedRDDBuilder anotherCacheBuilder = mock(CachedRDDBuilder.class);
    CachedData anotherCachedData = mock(CachedData.class);
    InMemoryRelation anotherInMemoryRelation = mock(InMemoryRelation.class);

    when(anotherCachedData.cachedRepresentation()).thenReturn(anotherInMemoryRelation);
    when(anotherInMemoryRelation.cacheBuilder()).thenReturn(anotherCacheBuilder);
    when(anotherCacheBuilder.cachedName()).thenReturn("another-name");

    IndexedSeq<CachedData> cachedItems =
        collectionAsScalaIterableConverter(Arrays.asList(anotherCachedData, cachedData))
            .asScala()
            .toIndexedSeq();

    try (MockedStatic mocked = mockStatic(FieldUtils.class)) {
      Field cacheManagerField = mock(Field.class);
      when(FieldUtils.getField(SharedState.class, "cacheManager", true))
          .thenReturn(cacheManagerField);
      when(cacheManagerField.get(sharedState)).thenReturn(cacheManager);

      Field cachedDataField = mock(Field.class);
      when(FieldUtils.getField(CacheManager.class, "cachedData", true)).thenReturn(cachedDataField);
      when(cachedDataField.get(cacheManager)).thenReturn(cachedItems);

      List<OpenLineage.InputDataset> inputDatasets =
          builder.apply(mock(SparkListenerEvent.class), inMemoryRelation);
      assertEquals(1, inputDatasets.size());
      assertEquals(inputDataset, inputDatasets.get(0));
    }
  }

  @SneakyThrows
  @Test
  public void testApplyWhenNoCachedPlan() {
    IndexedSeq<CachedData> cachedItems =
        collectionAsScalaIterableConverter(Collections.<CachedData>emptyList())
            .asScala()
            .toIndexedSeq();

    try (MockedStatic mocked = mockStatic(FieldUtils.class)) {
      Field cacheManagerField = mock(Field.class);
      when(FieldUtils.getField(SharedState.class, "cacheManager", true))
          .thenReturn(cacheManagerField);
      when(cacheManagerField.get(sharedState)).thenReturn(cacheManager);

      Field cachedDataField = mock(Field.class);
      when(FieldUtils.getField(CacheManager.class, "cachedData", true)).thenReturn(cachedDataField);
      when(cachedDataField.get(cacheManager)).thenReturn(cachedItems);

      assertEquals(0, builder.apply(mock(SparkListenerEvent.class), inMemoryRelation).size());
    }
  }

  @SneakyThrows
  @Test
  public void testApplyWhenExceptionThrown() {
    try (MockedStatic mocked = mockStatic(FieldUtils.class)) {
      Field cacheManagerField = mock(Field.class);
      when(FieldUtils.getField(SharedState.class, "cacheManager", true))
          .thenReturn(cacheManagerField);
      when(cacheManagerField.get(sharedState)).thenThrow(new RuntimeException("message"));

      assertEquals(0, builder.apply(mock(SparkListenerEvent.class), inMemoryRelation).size());
    }
  }
}
