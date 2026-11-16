/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.FabricWarehouseUtils;
import io.openlineage.spark.agent.util.FacetUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * Adds the {@code fabric_spark} run facet to every event of a Microsoft Fabric Spark session: the
 * Fabric item the session runs for, its Livy session and its job instance. The values come from the
 * Spark settings Fabric sets when it starts the session, so the facet doesn't depend on {@code
 * spark.openlineage.capturedProperties}.
 *
 * <p>It reads the {@code spark.hadoop.trident.*} settings, not the bare {@code trident.*} ones:
 * Fabric sets the bare settings on a notebook's session only after the application starts, so the
 * application's START event wouldn't carry them.
 */
@Slf4j
public class FabricSparkRunFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, OpenLineage.FabricSparkRunFacet> {

  static final String FACET_NAME = "fabric_spark";
  static final String JOB_IDENTIFIER_CONF = "spark.synapse.jobidentifier";
  static final String WORKSPACE_ID_CONF = "spark.hadoop.trident.artifact.workspace.id";
  static final String ITEM_ID_CONF = "spark.hadoop.trident.artifact.id";
  static final String ITEM_TYPE_CONF = "spark.hadoop.trident.artifact.type";
  static final String JOB_INSTANCE_ID_CONF = "spark.hadoop.trident.activity.id";

  private final OpenLineageContext context;
  // The settings don't change during a session, so the facet is built once.
  private Optional<OpenLineage.FabricSparkRunFacet> facet;

  public FabricSparkRunFacetBuilder(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  protected void build(
      SparkListenerEvent event,
      BiConsumer<String, ? super OpenLineage.FabricSparkRunFacet> consumer) {
    if (FacetUtils.isFacetDisabled(context, FACET_NAME)) {
      return;
    }
    if (facet == null) {
      facet = facetFor(context.getOpenLineage(), sparkConf());
    }
    facet.ifPresent(f -> consumer.accept(FACET_NAME, f));
  }

  // This runs for every event of every Spark job, so failing to read the settings must not break
  // lineage for jobs outside Fabric; the events just don't get the facet.
  @SuppressWarnings("PMD.AvoidCatchingGenericException")
  private Function<String, Optional<String>> sparkConf() {
    try {
      return context
          .getSparkContext()
          .map(
              sc -> {
                SparkConf conf = sc.getConf();
                return (Function<String, Optional<String>>)
                    key -> ScalaConversionUtils.asJavaOptional(conf.getOption(key));
              })
          .orElse(key -> Optional.empty());
    } catch (RuntimeException e) {
      log.warn("Cannot read the Spark settings; events get no fabric_spark facet", e);
      return key -> Optional.empty();
    }
  }

  /**
   * Builds the facet from the session's settings, or returns empty outside Fabric, where there is
   * neither a workspace nor a Livy session.
   */
  static Optional<OpenLineage.FabricSparkRunFacet> facetFor(
      OpenLineage openLineage, Function<String, Optional<String>> conf) {
    Optional<String> workspaceId = guid(conf, WORKSPACE_ID_CONF);
    Optional<String> livyId =
        conf.apply(JOB_IDENTIFIER_CONF).flatMap(FabricSparkRunFacetBuilder::livyId);
    if (!workspaceId.isPresent() && !livyId.isPresent()) {
      return Optional.empty();
    }
    return Optional.of(
        openLineage
            .newFabricSparkRunFacetBuilder()
            .workspaceId(workspaceId.orElse(null))
            .itemId(guid(conf, ITEM_ID_CONF).orElse(null))
            .itemType(
                conf.apply(ITEM_TYPE_CONF).map(String::trim).filter(t -> !t.isEmpty()).orElse(null))
            .livyId(livyId.orElse(null))
            .jobInstanceId(guid(conf, JOB_INSTANCE_ID_CONF).orElse(null))
            .build());
  }

  /**
   * The Livy session ID at the end of {@code spark.synapse.jobidentifier}, for example {@code
   * 22dynamicdatacloudworkspace.datacloudpool1-4353128699960801747.<livyId>}, in lowercase.
   */
  static Optional<String> livyId(String jobIdentifier) {
    return FabricWarehouseUtils.parseGuid(
            jobIdentifier.substring(jobIdentifier.lastIndexOf('.') + 1))
        .map(UUID::toString);
  }

  private static Optional<String> guid(Function<String, Optional<String>> conf, String key) {
    return conf.apply(key).flatMap(FabricWarehouseUtils::parseGuid).map(UUID::toString);
  }
}
