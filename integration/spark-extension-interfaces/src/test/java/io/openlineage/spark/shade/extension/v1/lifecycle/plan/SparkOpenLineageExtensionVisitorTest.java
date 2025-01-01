/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.list;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.shade.extension.v1.InputDatasetWithDelegate;
import io.openlineage.spark.shade.extension.v1.InputDatasetWithIdentifier;
import io.openlineage.spark.shade.extension.v1.InputLineageNode;
import io.openlineage.spark.shade.extension.v1.LineageRelation;
import io.openlineage.spark.shade.extension.v1.LineageRelationProvider;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;

class SparkOpenLineageExtensionVisitorTest {
  OpenLineage openLineage = new OpenLineage(URI.create("spark://some-uri"));
  SQLContext sqlContext = mock(SQLContext.class);
  SparkListenerEvent event = mock(SparkListenerEvent.class);
  SparkOpenLineageExtensionVisitor visitor = new SparkOpenLineageExtensionVisitor();
  Map<String, String> options = Collections.singletonMap("path", "some-path");

  @Test
  void testDatasetIdentifierReturnedByLineageRelationProvider() {
    // given
    LineageRelationProvider provider =
        (sparkListenerEventName, openLineage, sqlContext, parameters) ->
            new DatasetIdentifier("name", "namespace")
                .withSymlink(
                    new DatasetIdentifier.Symlink(
                        "name1", "namespace1", DatasetIdentifier.SymlinkType.TABLE));
    assertThat(visitor.isDefinedAt(provider)).isTrue();
    Map<String, Object> result =
        visitor.apply(provider, event.getClass().getName(), sqlContext, options);

    // then
    assertThat(result).extracting("name").isEqualTo("name");
    assertThat(result).extracting("namespace").isEqualTo("namespace");
    assertThat(result)
        .extracting("symlinks")
        .isEqualTo(
            list(
                ImmutableMap.builder()
                    .put("name", "name1")
                    .put("namespace", "namespace1")
                    .put("type", "TABLE")
                    .build()));
  }

  @Test
  void testDatasetIdentifierReturnedByLineageRelation() {
    // given
    LineageRelation lineageRelation =
        (sparkListenerEventName, openLineage) ->
            new DatasetIdentifier("name", "namespace")
                .withSymlink(
                    new DatasetIdentifier.Symlink(
                        "name1", "namespace1", DatasetIdentifier.SymlinkType.TABLE));

    // when
    assertThat(visitor.isDefinedAt(lineageRelation)).isTrue();
    Map<String, Object> result = visitor.apply(lineageRelation, event.getClass().getName());

    // then
    assertThat(result).extracting("name").isEqualTo("name");
    assertThat(result).extracting("namespace").isEqualTo("namespace");
    assertThat(result)
        .extracting("symlinks")
        .isEqualTo(
            list(
                ImmutableMap.builder()
                    .put("name", "name1")
                    .put("namespace", "namespace1")
                    .put("type", "TABLE")
                    .build()));
  }

  @Test
  void testFacetsReturnedByInputLineageNodeWithIdentifier() {
    // given
    InputLineageNode inputLineageNode =
        (sparkListenerEventName, openLineage) ->
            Collections.singletonList(
                new InputDatasetWithIdentifier(
                    new DatasetIdentifier("a", "b"),
                    openLineage.newDatasetFacetsBuilder(),
                    openLineage.newInputDatasetInputFacetsBuilder()));

    // when
    assertThat(visitor.isDefinedAt(inputLineageNode)).isTrue();
    Map<String, Object> result = visitor.apply(inputLineageNode, event.getClass().getName());

    // then
    assertThat(result).extracting("delegateNodes").isEqualTo(list());
  }

  @Test
  void testFacetsReturnedByInputLineageNodeWithDelegates() {
    // given
    LogicalPlan delegate = mock(LogicalPlan.class);
    InputLineageNode inputLineageNode =
        (sparkListenerEventName, openLineage) ->
            Collections.singletonList(
                new InputDatasetWithDelegate(
                    delegate,
                    openLineage.newDatasetFacetsBuilder(),
                    openLineage.newInputDatasetInputFacetsBuilder()));

    // when
    assertThat(visitor.isDefinedAt(inputLineageNode)).isTrue();
    Map<String, Object> result = visitor.apply(inputLineageNode, event.getClass().getName());

    // then
    assertThat(result).extracting("delegateNodes").isEqualTo(list(delegate));
  }
}
