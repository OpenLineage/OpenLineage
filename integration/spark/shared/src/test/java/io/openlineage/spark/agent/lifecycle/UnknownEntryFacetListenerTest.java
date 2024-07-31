/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.spark.agent.facets.UnknownEntryFacet;
import io.openlineage.spark.agent.facets.UnknownEntryFacetConfig;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.command.ListFilesCommand;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata$;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq$;

class UnknownEntryFacetListenerTest {

  @Test
  void testBuildUnknownFacet() {
    UnknownEntryFacetListener underTest =
        new UnknownEntryFacetListener(new UnknownEntryFacetConfig(true));

    NamedExpression reference =
        new AttributeReference(
            "test",
            DataType.fromDDL("`gender` STRING"),
            false,
            Metadata$.MODULE$.fromJson("{\"__CHAR_VARCHAR_TYPE_STRING\":\"varchar(64)\"}"),
            ExprId.apply(1L),
            Seq$.MODULE$.<String>newBuilder().result());

    ListFilesCommand logicalPlan =
        new ListFilesCommand(
            ScalaConversionUtils.fromList(Collections.singletonList("./test.file")));
    Project project =
        new Project(
            ScalaConversionUtils.fromList(Collections.singletonList(reference)), logicalPlan);

    UnknownEntryFacet facet = underTest.build(project).get();

    assertThat(facet.getOutput().getInputAttributes())
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue("name", "Results")
        .hasFieldOrPropertyWithValue("type", "string");
    assertThat(facet.getOutput().getOutputAttributes())
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue("name", "test")
        .hasFieldOrPropertyWithValue("type", "struct")
        .extracting("metadata")
        .asInstanceOf(InstanceOfAssertFactories.MAP)
        .containsEntry("__CHAR_VARCHAR_TYPE_STRING", "varchar(64)");

    assertThat(facet.getInputs())
        .hasSize(1)
        .first()
        .extracting("inputAttributes")
        .asList()
        .hasSize(0);
    assertThat(facet.getInputs())
        .hasSize(1)
        .first()
        .extracting("outputAttributes")
        .asList()
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue("name", "Results")
        .hasFieldOrPropertyWithValue("type", "string");
  }

  @Test
  void testReturnNullIfProcessedUnknownFacet() {
    UnknownEntryFacetListener underTest =
        new UnknownEntryFacetListener(new UnknownEntryFacetConfig(true));

    NamedExpression reference =
        new AttributeReference(
            "test",
            DataType.fromDDL("`gender` STRING"),
            false,
            Metadata$.MODULE$.fromJson("{\"__CHAR_VARCHAR_TYPE_STRING\":\"varchar(64)\"}"),
            ExprId.apply(1L),
            ScalaConversionUtils.asScalaSeqEmpty());

    ListFilesCommand logicalPlan =
        new ListFilesCommand(ScalaConversionUtils.fromList(Collections.singletonList("./test")));
    Project project =
        new Project(
            ScalaConversionUtils.fromList(Collections.singletonList(reference)), logicalPlan);
    underTest.accept(project);
    underTest.accept(logicalPlan);

    Optional<UnknownEntryFacet> facet = underTest.build(project);
    assertThat(facet.isPresent()).isFalse();
  }
}
