/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static java.util.Optional.ofNullable;
import static scala.collection.JavaConversions.mapAsJavaMap;
import static scala.collection.JavaConversions.seqAsJavaList;

import io.openlineage.spark.agent.facets.LogicalPlanFacet;
import io.openlineage.spark.agent.facets.UnknownEntryFacet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.AttributeSet;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import scala.collection.JavaConversions;

/**
 * Holds information about visited nodes in {@link LogicalPlan}, using {@link LogicalPlanSerializer}
 * serialize unvisited leaves and root and build an {@link UnknownEntryFacet} for the sake of
 * tracking leaf and root nodes (inputs and outputs) that don't yet have a function defined for
 * collecting {@link io.openlineage.client.OpenLineage.Dataset} attributes. In this way, OpenLineage
 * data collectors can gain some visibility into gaps in coverage of the Spark plans being visited.
 * Even though the {@link LogicalPlanFacet} collects the entire plan for the run, it relies on the
 * {@link LogicalPlan#toJSON()} traversal, which only visits {@link LogicalPlan#children()}, which
 * omits other fields present in the nodes. An example includes the {@link
 * org.apache.spark.sql.execution.LogicalRDD}, which encapsulates {@link org.apache.spark.rdd.RDD}s,
 * which may point to parseable data sources.
 */
@Slf4j
public class UnknownEntryFacetListener implements Consumer<LogicalPlan> {

  private final Map<LogicalPlan, Object> visitedNodes = new IdentityHashMap<>();
  private final LogicalPlanSerializer planSerializer = new LogicalPlanSerializer();

  private static final UnknownEntryFacetListener INSTANCE = new UnknownEntryFacetListener();

  public static UnknownEntryFacetListener getInstance() {
    return INSTANCE;
  }

  @Override
  public void accept(LogicalPlan logicalPlan) {
    visitedNodes.put(logicalPlan, null);
  }

  public Optional<UnknownEntryFacet> build(LogicalPlan root) {
    Optional<UnknownEntryFacet.FacetEntry> output = mapEntry(root);
    List<UnknownEntryFacet.FacetEntry> inputs =
        seqAsJavaList(root.collectLeaves()).stream()
            .map(this::mapEntry)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    return !output.isPresent() && inputs.isEmpty()
        ? Optional.empty()
        : Optional.of(new UnknownEntryFacet(output.orElse(null), inputs));
  }

  private Optional<UnknownEntryFacet.FacetEntry> mapEntry(LogicalPlan x) {
    if (visitedNodes.containsKey(x)) {
      log.debug("Node was visited - ignoring {}", x);
      return Optional.empty();
    }
    List<UnknownEntryFacet.AttributeField> output = attributeFields(x.outputSet());
    List<UnknownEntryFacet.AttributeField> input = attributeFields(x.inputSet());
    String serializedNode = planSerializer.serialize(x);
    log.debug("Adding serialized node for unknown facet entry {}", serializedNode);
    return Optional.of(new UnknownEntryFacet.FacetEntry(serializedNode, input, output));
  }

  private List<UnknownEntryFacet.AttributeField> attributeFields(AttributeSet set) {
    return JavaConversions.<AttributeReference>asJavaCollection(set.toSet()).stream()
        .map(this::mapAttributeReference)
        .collect(Collectors.toList());
  }

  private UnknownEntryFacet.AttributeField mapAttributeReference(AttributeReference ar) {
    return new UnknownEntryFacet.AttributeField(
        ar.name(),
        ofNullable(ar.dataType()).map(DataType::typeName).orElse(null),
        new HashMap<>(mapAsJavaMap(ar.metadata().map())));
  }
}
