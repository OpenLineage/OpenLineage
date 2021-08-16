package openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;

import openlineage.spark.agent.facets.UnknownEntryFacet;
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
    UnknownEntryFacetListener underTest = new UnknownEntryFacetListener();

    NamedExpression reference =
        new AttributeReference(
            "test",
            DataType.fromDDL("`gender` STRING"),
            false,
            Metadata$.MODULE$.fromJson("{\"__CHAR_VARCHAR_TYPE_STRING\":\"varchar(64)\"}"),
            ExprId.apply(1L),
            Seq$.MODULE$.<String>newBuilder().result());

    ListFilesCommand logicalPlan =
        new ListFilesCommand(Seq$.MODULE$.<String>newBuilder().$plus$eq("./test.file").result());
    Project project =
        new Project(
            Seq$.MODULE$.<NamedExpression>newBuilder().$plus$eq(reference).result(), logicalPlan);

    UnknownEntryFacet facet = underTest.build(project);

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
    UnknownEntryFacetListener underTest = new UnknownEntryFacetListener();

    NamedExpression reference =
        new AttributeReference(
            "test",
            DataType.fromDDL("`gender` STRING"),
            false,
            Metadata$.MODULE$.fromJson("{\"__CHAR_VARCHAR_TYPE_STRING\":\"varchar(64)\"}"),
            ExprId.apply(1L),
            Seq$.MODULE$.<String>newBuilder().result());

    ListFilesCommand logicalPlan =
        new ListFilesCommand(Seq$.MODULE$.<String>newBuilder().$plus$eq("./test").result());
    Project project =
        new Project(
            Seq$.MODULE$.<NamedExpression>newBuilder().$plus$eq(reference).result(), logicalPlan);
    underTest.accept(project);
    underTest.accept(logicalPlan);

    UnknownEntryFacet facet = underTest.build(project);
    assertThat(facet).isNull();
  }
}
