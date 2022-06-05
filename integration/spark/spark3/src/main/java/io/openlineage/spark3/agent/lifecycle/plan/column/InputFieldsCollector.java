package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.Rdds;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.plans.logical.LeafNode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;

/** Traverses LogicalPlan and collect input fields with the corresponding ExprId. */
@Slf4j
class InputFieldsCollector {

  static void collect(
      OpenLineageContext context, LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    discoverInputsFromNode(context, plan, builder);

    // hacky way to replace `plan instanceof UnaryNode` which fails for Spark 3.2.1
    // because of java.lang.IncompatibleClassChangeError: UnaryNode, but class was expected
    // probably related to single code base for different Spark versions
    if ((plan.getClass()).isAssignableFrom(UnaryNode.class)) {
      collect(context, ((UnaryNode) plan).child(), builder);
    } else if (plan.children() != null) {
      ScalaConversionUtils.<LogicalPlan>fromSeq(plan.children()).stream()
          .forEach(child -> collect(context, child, builder));
    }
  }

  private static void discoverInputsFromNode(
      OpenLineageContext context, LogicalPlan node, ColumnLevelLineageBuilder builder) {
    extractDatasetIdentifier(context, node).stream()
        .forEach(
            di ->
                ScalaConversionUtils.fromSeq(node.output()).stream()
                    .filter(attr -> attr instanceof AttributeReference)
                    .map(attr -> (AttributeReference) attr)
                    .forEach(attr -> builder.addInput(attr.exprId(), di, attr.name())));
  }

  private static List<DatasetIdentifier> extractDatasetIdentifier(
      OpenLineageContext context, LogicalPlan node) {
    if (node instanceof DataSourceV2Relation) {
      return extractDatasetIdentifier(context, (DataSourceV2Relation) node);
    } else if (node instanceof DataSourceV2ScanRelation) {
      return extractDatasetIdentifier(context, ((DataSourceV2ScanRelation) node).relation());
    } else if (node instanceof HiveTableRelation) {
      return extractDatasetIdentifier(((HiveTableRelation) node).tableMeta());
    } else if (node instanceof LogicalRelation
        && ((LogicalRelation) node).catalogTable().isDefined()) {
      return extractDatasetIdentifier(((LogicalRelation) node).catalogTable().get());
    } else if (node instanceof LogicalRDD) {
      return extractDatasetIdentifier((LogicalRDD) node);
    } else if (node instanceof LeafNode) {
      log.warn("Could not extract dataset identifier from {}", node.getClass().getCanonicalName());
    }
    return Collections.emptyList();
  }

  private static List<DatasetIdentifier> extractDatasetIdentifier(LogicalRDD logicalRDD) {
    List<RDD<?>> fileLikeRdds = Rdds.findFileLikeRdds(logicalRDD.rdd());
    return PlanUtils.findRDDPaths(fileLikeRdds).stream()
        .map(
            path ->
                new DatasetIdentifier(path.toUri().getPath(), PlanUtils.namespaceUri(path.toUri())))
        .collect(Collectors.toList());
  }

  private static List<DatasetIdentifier> extractDatasetIdentifier(
      OpenLineageContext context, DataSourceV2Relation relation) {
    return PlanUtils3.getDatasetIdentifier(context, relation)
        .map(Collections::singletonList)
        .orElse(Collections.emptyList());
  }

  private static List<DatasetIdentifier> extractDatasetIdentifier(CatalogTable catalogTable) {
    URI location = catalogTable.location();
    if (location == null) {
      return Collections.emptyList();
    } else {
      return Collections.singletonList(
          new DatasetIdentifier(
              catalogTable.location().getPath(), PlanUtils.namespaceUri(catalogTable.location())));
    }
  }
}
