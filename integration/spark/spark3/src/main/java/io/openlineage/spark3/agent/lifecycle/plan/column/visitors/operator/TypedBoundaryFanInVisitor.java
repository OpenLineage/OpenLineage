/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.ColumnLineageConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.AppendColumns;
import org.apache.spark.sql.catalyst.plans.logical.DeserializeToObject;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MapElements;
import org.apache.spark.sql.catalyst.plans.logical.MapGroups;
import org.apache.spark.sql.catalyst.plans.logical.MapPartitions;
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject;

/**
 * Emits a pessimistic fan-in across a typed Dataset boundary, i.e. the {@link
 * DeserializeToObject} / {@link SerializeFromObject} pair that {@code Dataset.map}, {@code
 * mapPartitions}, {@code flatMap} and {@code groupByKey().mapGroups} produce.
 *
 * <p><b>Why a fan-in and not per-field matching.</b> {@link DeserializeToObject} collapses the row
 * into a single opaque JVM object and the lambda body is bytecode, so per-field lineage is not
 * recoverable from the plan. The two encoder schemas look like a usable substitute but are not: the
 * fields of {@link SerializeFromObject#serializer()} have no {@code references()} at all - each is
 * rooted at {@code input[0, <BeanClass>, true]} - so the output side carries names and types only,
 * never attribute linkage. Matching those names against the input side was prototyped and measured
 * to be <em>unfalsifiable</em> rather than merely imprecise: an identity map and a map that swaps
 * two fields expose byte-identical boundary structure (their serializer trees are equal once exprId
 * numbering is normalised), so name matching emits the same {@code email <- email} claim for a
 * truthful and a false mapping. A specific wrong pairing is worse than an over-broad one once
 * compliance tooling consumes the facet.
 *
 * <p><b>What is emitted.</b> Every output attribute of the boundary is linked to every input
 * attribute the deserializers read, as {@link TransformationInfo.Types#INDIRECT}, {@link
 * TransformationInfo.Subtypes#TRANSFORMATION}, with the opaque operators named in the
 * description.
 * That over-claims breadth - for an opaque lambda "every output may derive from every input" is
 * literally true - while never asserting a specific pairing.
 *
 * <p><b>Off by default.</b> The fan-in is {@code N x M}, so it is gated behind {@link
 * ColumnLineageConfig#getTypedBoundaryFanInEnabled()} and ships dark.
 *
 * <p><b>Width cap.</b> Above {@link ColumnLineageConfig#getTypedBoundaryFanInMaxEdges()} the visitor
 * emits <b>nothing</b>. This is deliberate rather than a truncation: {@code
 * ColumnLevelLineageBuilder} drops the whole facet once the returned input fields exceed its own
 * {@code RETURNED_INPUT_FIELD_LIMIT} of 100 000, so an uncapped fan-in over a ~320 column table
 * would turn "too much lineage" into "no lineage" for every field of the dataset. Emitting nothing
 * for the single operator that cannot be traced anyway leaves the rest of the facet intact.
 *
 * <p>{@code TypedFilter} is deliberately <b>not</b> handled here. It does not cross the
 * serialize/deserialize boundary and passes its child's attributes through unchanged, sharing their
 * exprIds, so identity is already inferred without any edge. Adding fan-in there would replace a
 * faithful pass-through with an opaque one.
 */
@Slf4j
public class TypedBoundaryFanInVisitor implements OperatorVisitor {

  static final String DESCRIPTION_PREFIX = "typed operation: ";

  /** Default used when no {@link OpenLineageContext} is reachable from the builder. */
  private static final boolean DEFAULT_ENABLED = false;

  private static final int DEFAULT_MAX_EDGES = 10_000;

  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof SerializeFromObject;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    ColumnLineageConfig config = columnLineageConfig(builder);
    if (!isEnabled(config)) {
      return;
    }

    TypedBoundary boundary = describeBoundary((SerializeFromObject) operator);
    if (boundary.inputExprIds.isEmpty() || boundary.outputExprIds.isEmpty()) {
      // Nothing structural to link: either the deserializers reference no attribute (e.g. a
      // literal-only source) or the boundary produces no attributes.
      return;
    }

    long edges = (long) boundary.outputExprIds.size() * (long) boundary.inputExprIds.size();
    int maxEdges = maxEdges(config);
    if (edges > maxEdges) {
      log.debug(
          "Typed boundary fan-in skipped for {}: {} output x {} input fields would emit {} "
              + "edges, above the configured limit of {}. Emitting nothing rather than a fan-in "
              + "that would push the column lineage facet past its own returned-input-field limit.",
          boundary.description(),
          boundary.outputExprIds.size(),
          boundary.inputExprIds.size(),
          edges,
          maxEdges);
      return;
    }

    TransformationInfo transformationInfo =
        new TransformationInfo(
            TransformationInfo.Types.INDIRECT,
            TransformationInfo.Subtypes.TRANSFORMATION,
            boundary.description(),
            false);

    for (ExprId output : boundary.outputExprIds) {
      for (ExprId input : boundary.inputExprIds) {
        if (!output.equals(input)) {
          builder.addDependency(output, input, transformationInfo);
        }
      }
    }
  }

  /**
   * Collects the attributes on both sides of the boundary by walking down from {@link
   * SerializeFromObject} through the chain of opaque object operators. The walk stops as soon as it
   * leaves that chain, so a nested typed hop lower in the plan is left to its own {@link
   * SerializeFromObject}.
   */
  private static TypedBoundary describeBoundary(SerializeFromObject serialize) {
    TypedBoundary boundary = new TypedBoundary();
    ScalaConversionUtils.<Attribute>fromSeq(serialize.output())
        .forEach(attr -> boundary.outputExprIds.add(attr.exprId()));
    collectInputs(serialize.child(), boundary);
    return boundary;
  }

  private static void collectInputs(LogicalPlan node, TypedBoundary boundary) {
    if (node == null) {
      return;
    }

    if (node instanceof DeserializeToObject) {
      // Bottom of the chain: the only node whose expressions reference real attributes of the
      // relation below. Terminal - do not descend, so a nested boundary keeps its own visitor.
      boundary.opaqueOperators.add(node.getClass().getSimpleName());
      addReferences(((DeserializeToObject) node).deserializer(), boundary);
      return;
    }

    if (node instanceof AppendColumns) {
      boundary.opaqueOperators.add(node.getClass().getSimpleName());
      addReferences(((AppendColumns) node).deserializer(), boundary);
      return;
    }

    if (node instanceof MapGroups) {
      MapGroups mapGroups = (MapGroups) node;
      boundary.opaqueOperators.add(node.getClass().getSimpleName());
      addReferences(mapGroups.keyDeserializer(), boundary);
      addReferences(mapGroups.valueDeserializer(), boundary);
    } else if (node instanceof MapElements || node instanceof MapPartitions) {
      // Purely opaque: no deserializer of their own, they only consume and produce the object.
      boundary.opaqueOperators.add(node.getClass().getSimpleName());
    } else {
      // Left the typed chain.
      return;
    }

    ScalaConversionUtils.<LogicalPlan>fromSeq(node.children())
        .forEach(child -> collectInputs(child, boundary));
  }

  private static void addReferences(Expression expression, TypedBoundary boundary) {
    if (expression == null) {
      return;
    }
    ScalaConversionUtils.<Attribute>fromSeq(expression.references().toSeq())
        .forEach(attr -> boundary.inputExprIds.add(attr.exprId()));
  }

  private static ColumnLineageConfig columnLineageConfig(ColumnLevelLineageBuilder builder) {
    try {
      return Optional.ofNullable(builder.getContext())
          .map(OpenLineageContext::getOpenLineageConfig)
          .map(SparkOpenLineageConfig::getColumnLineageConfig)
          .orElse(null);
    } catch (RuntimeException e) {
      log.debug("Could not read column lineage configuration", e);
      return null;
    }
  }

  private static boolean isEnabled(ColumnLineageConfig config) {
    return Optional.ofNullable(config)
        .map(ColumnLineageConfig::getTypedBoundaryFanInEnabled)
        .orElse(DEFAULT_ENABLED);
  }

  private static int maxEdges(ColumnLineageConfig config) {
    return Optional.ofNullable(config)
        .map(ColumnLineageConfig::getTypedBoundaryFanInMaxEdges)
        .orElse(DEFAULT_MAX_EDGES);
  }

  /** Both sides of one typed boundary, plus the opaque operators sitting between them. */
  private static final class TypedBoundary {
    private final Set<ExprId> outputExprIds = new LinkedHashSet<>();
    private final Set<ExprId> inputExprIds = new LinkedHashSet<>();
    private final List<String> opaqueOperators = new ArrayList<>();

    private String description() {
      return DESCRIPTION_PREFIX
          + (opaqueOperators.isEmpty()
              ? SerializeFromObject.class.getSimpleName()
              : opaqueOperators.stream().distinct().collect(Collectors.joining(", ")));
    }
  }
}
