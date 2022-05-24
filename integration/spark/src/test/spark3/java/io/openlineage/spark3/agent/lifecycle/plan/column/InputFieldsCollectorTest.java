package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.lifecycle.Rdds;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

public class InputFieldsCollectorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  NamedExpression expression = mock(NamedExpression.class);
  ExprId exprId = mock(ExprId.class);
  DatasetIdentifier di = mock(DatasetIdentifier.class);
  AttributeReference attributeReference = mock(AttributeReference.class);

  @BeforeEach
  public void setup() {
    when(attributeReference.exprId()).thenReturn(exprId);
    when(attributeReference.name()).thenReturn("some-name");
  }

  @Test
  public void collectWhenGrandChildNodeIsDataSourceV2Relation() {
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    LogicalPlan plan = createPlanWithGrandChild(relation);
    InputFieldsCollector collector = new InputFieldsCollector(plan, context);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(context, relation)).thenReturn(Optional.of(di));
      collector.collect(builder);
    }
    verify(builder, times(1)).addInput(exprId, di, "some-name");
  }

  @Test
  public void collectWhenGrandChildNodeIsDataSourceV2ScanRelation() {
    DataSourceV2ScanRelation scanRelation = mock(DataSourceV2ScanRelation.class);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    when(scanRelation.relation()).thenReturn(relation);

    LogicalPlan plan = createPlanWithGrandChild(scanRelation);
    InputFieldsCollector collector = new InputFieldsCollector(plan, context);

    when(scanRelation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    try (MockedStatic mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(context, relation)).thenReturn(Optional.of(di));
      collector.collect(builder);
    }
    verify(builder, times(1)).addInput(exprId, di, "some-name");
  }

  @Test
  @SneakyThrows
  public void collectWhenGrandChildNodeIsHiveTableRelation() {
    HiveTableRelation relation = mock(HiveTableRelation.class);
    CatalogTable catalogTable = mock(CatalogTable.class);
    URI uri = new URI("file:/tmp");
    when(relation.tableMeta()).thenReturn(catalogTable);
    when(catalogTable.location()).thenReturn(uri);

    LogicalPlan plan = createPlanWithGrandChild(relation);
    InputFieldsCollector collector = new InputFieldsCollector(plan, context);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    collector.collect(builder);
    verify(builder, times(1)).addInput(exprId, new DatasetIdentifier("/tmp", "file"), "some-name");
  }

  @Test
  @SneakyThrows
  public void collectWhenGrandChildNodeIsLogicalRdd() {
    LogicalRDD relation = mock(LogicalRDD.class);
    RDD<InternalRow> rdd = mock(RDD.class);
    List<RDD<?>> listRDD = Collections.singletonList(rdd);
    Path path = new Path("file:///tmp");

    when(relation.rdd()).thenReturn(rdd);

    LogicalPlan plan = createPlanWithGrandChild(relation);
    InputFieldsCollector collector = new InputFieldsCollector(plan, context);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList((Attribute) attributeReference))
                .asScala()
                .toSeq());

    try (MockedStatic rdds = mockStatic(Rdds.class)) {
      try (MockedStatic planUtils = mockStatic(PlanUtils.class)) {
        when(Rdds.findFileLikeRdds(rdd)).thenReturn(listRDD);
        when(PlanUtils.findRDDPaths(listRDD)).thenReturn(Collections.singletonList(path));
        when(PlanUtils.namespaceUri(path.toUri())).thenReturn("file");

        collector.collect(builder);
        verify(builder, times(1))
            .addInput(exprId, new DatasetIdentifier("/tmp", "file"), "some-name");
      }
    }
  }

  @Test
  @SneakyThrows
  public void collectWhenGrandChildNodeIsLogicalRelation() {
    LogicalRelation relation = mock(LogicalRelation.class);
    CatalogTable catalogTable = mock(CatalogTable.class);
    URI uri = new URI("file:/tmp");
    when(relation.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(catalogTable.location()).thenReturn(uri);

    LogicalPlan plan = createPlanWithGrandChild(relation);
    InputFieldsCollector collector = new InputFieldsCollector(plan, context);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    collector.collect(builder);
    verify(builder, times(1)).addInput(exprId, new DatasetIdentifier("/tmp", "file"), "some-name");
  }

  @Test
  @SneakyThrows
  public void collectWhenGrandChildNodeIsLogicalRelationAndCatalogTableNotDefined() {
    LogicalRelation relation = mock(LogicalRelation.class);
    when(relation.catalogTable()).thenReturn(Option.empty());

    LogicalPlan plan = createPlanWithGrandChild(relation);
    InputFieldsCollector collector = new InputFieldsCollector(plan, context);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    collector.collect(builder);
    verify(builder, times(0)).addInput(any(), any(), any());
  }

  @Test
  @SneakyThrows
  public void collectWhenGrandChildNodeIsLogicalRelationAndLocationIsEmpty() {
    LogicalRelation relation = mock(LogicalRelation.class);
    CatalogTable catalogTable = mock(CatalogTable.class);
    when(relation.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(catalogTable.location()).thenReturn(null);

    LogicalPlan plan = createPlanWithGrandChild(relation);
    InputFieldsCollector collector = new InputFieldsCollector(plan, context);

    when(relation.output())
        .thenReturn(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(attributeReference))
                .asScala()
                .toSeq());

    collector.collect(builder);
    verify(builder, times(0)).addInput(any(), any(), any());
  }

  private LogicalPlan createPlanWithGrandChild(LogicalPlan grandChild) {
    LogicalPlan child =
        new Project(
            scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                    Arrays.asList(expression))
                .asScala()
                .toSeq(),
            grandChild);
    return new CreateTableAsSelect(null, null, null, child, null, null, false);
  }
}
