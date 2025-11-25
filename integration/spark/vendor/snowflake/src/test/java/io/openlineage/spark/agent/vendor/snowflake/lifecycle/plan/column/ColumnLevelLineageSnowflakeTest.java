/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle.plan.column;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.Vendors;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.spark.snowflake.SnowflakeRelation;
import net.snowflake.spark.snowflake.TableName;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;

@Slf4j
class ColumnLevelLineageSnowflakeTest {

  SparkSession spark = mock(SparkSession.class);
  SparkContext sparkContext = mock(SparkContext.class);
  QueryExecution queryExecution = mock(QueryExecution.class);
  SnowflakeRelation snowflakeRelation =
      mock(
          SnowflakeRelation.class,
          withSettings().defaultAnswer(RETURNS_DEEP_STUBS).name("SnowflakeRelation"));

  OpenLineageContext context;
  SparkListenerEvent event = mock(SparkListenerSQLExecutionEnd.class);
  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  StructType snowflakeSchema =
      new StructType(
          new StructField[] {
            new StructField("COLORID", IntegerType$.MODULE$, false, Metadata.empty()),
            new StructField("COLORNAME", StringType$.MODULE$, false, Metadata.empty())
          });

  @BeforeEach
  public void beforeEach() {
    when(spark.sparkContext()).thenReturn(sparkContext);

    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    context =
        OpenLineageContext.builder()
            .sparkSession(spark)
            .sparkContext(sparkContext)
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(queryExecution)
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(config)
            .vendors(Vendors.getVendors())
            .sparkExtensionVisitorWrapper(
                mock(
                    io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper
                        .class))
            .build();
    context
        .getColumnLevelLineageVisitors()
        .addAll(
            Vendors.getVendors().getEventHandlerFactories().stream()
                .map(factory -> factory.createColumnLevelLineageVisitors(context))
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));

    when(snowflakeRelation.params().sfDatabase()).thenReturn("snowflake_db");
    when(snowflakeRelation.params().sfSchema()).thenReturn("snowflake_schema");
    when(snowflakeRelation.params().sfFullURL())
        .thenReturn("https://orgname-accountname.snowflakecomputing.com");
    when(snowflakeRelation.schema()).thenReturn(snowflakeSchema);
  }

  @Test
  void testSnowflakeColumnLineageWithDbtable() {
    givenALogicalRelationWith(
        aSnowflakeRelation("dbtable", "COLORS"),
        anAttribute("COLORID", 21L),
        anAttribute("COLORNAME", 22L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("COLORID"), aField("COLORNAME")));

    assertFieldsPresent(facet, "COLORID", "COLORNAME");
    assertFieldDependsOn(facet, "COLORID", "COLORID");
    assertFieldDependsOn(facet, "COLORNAME", "COLORNAME");
  }

  @Test
  void testSnowflakeColumnLineageWithQuery() {
    givenALogicalRelationWith(
        aSnowflakeRelation("query", "SELECT COLORID, COLORNAME FROM COLORS"),
        anAttribute("COLORID", 21L),
        anAttribute("COLORNAME", 22L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("COLORID"), aField("COLORNAME")));

    assertFieldsPresent(facet, "COLORID", "COLORNAME");
    assertFieldDependsOn(facet, "COLORID", "COLORID");
    assertFieldDependsOn(facet, "COLORNAME", "COLORNAME");
  }

  @Test
  void testSnowflakeSingleColumnSelection() {
    givenALogicalRelationWith(
        aSnowflakeRelation("query", "SELECT COLORNAME FROM COLORS"), anAttribute("COLORNAME", 21L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("COLORNAME")));

    assertFieldsPresent(facet, "COLORNAME");
    assertFieldDependsOn(facet, "COLORNAME", "COLORNAME");
  }

  @Test
  void testSnowflakeQueryWithAliases() {
    givenALogicalRelationWith(
        aSnowflakeRelation("query", "SELECT COLORID AS ID, COLORNAME AS NAME FROM COLORS"),
        anAttribute("ID", 21L),
        anAttribute("NAME", 22L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("ID"), aField("NAME")));

    assertFieldsPresent(facet, "ID", "NAME");
    assertFieldDependsOn(facet, "ID", "COLORID");
    assertFieldDependsOn(facet, "NAME", "COLORNAME");
  }

  @Test
  void testSnowflakeQueryWithMultipleInputs() {
    givenALogicalRelationWith(
        aSnowflakeRelation("query", "SELECT CONCAT(COLORID, COLORNAME) AS COLOR FROM COLORS"),
        anAttribute("COLOR", 21L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("COLOR")));

    assertFieldsPresent(facet, "COLOR");
    assertFieldDependsOn(facet, "COLOR", "COLORID", "COLORNAME");
  }

  @Test
  void testSnowflakeQueryFromNonDefaultSchema() {
    givenALogicalRelationWith(
        aSnowflakeRelation("query", "SELECT COLORNAME FROM my_schema.COLORS"),
        anAttribute("COLORNAME", 21L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("COLORNAME")));

    assertFieldsPresent(facet, "COLORNAME");
    assertFieldDependsOn(facet, "COLORNAME", "snowflake_db.my_schema.COLORS.COLORNAME");
  }

  @Test
  void testSnowflakeQueryFromNonDefaultDatabase() {
    givenALogicalRelationWith(
        aSnowflakeRelation("query", "SELECT COLORNAME FROM my_db.snowflake_schema.COLORS"),
        anAttribute("COLORNAME", 21L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("COLORNAME")));

    assertFieldsPresent(facet, "COLORNAME");
    assertFieldDependsOn(facet, "COLORNAME", "my_db.snowflake_schema.COLORS.COLORNAME");
  }

  @Test
  void testSnowflakeQueryFromNonDefaultDatabaseAndSchema() {
    givenALogicalRelationWith(
        aSnowflakeRelation("query", "SELECT COLORNAME FROM my_db.my_schema.COLORS"),
        anAttribute("COLORNAME", 21L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("COLORNAME")));

    assertFieldsPresent(facet, "COLORNAME");
    assertFieldDependsOn(facet, "COLORNAME", "my_db.my_schema.COLORS.COLORNAME");
  }

  @Test
  void testSnowflakeQueryFromMultipleDatabases() {
    givenALogicalRelationWith(
        aSnowflakeRelation(
            "query",
            "SELECT "
                + "    db1_colors.COLORID AS COLORID, "
                + "    CONCAT(db2_colors.COLORTINT, db1_colors.COLORNAME) AS COLORNAME "
                + "FROM "
                + "    snowflake_db.snowflake_schema.COLORS AS db1_colors "
                + "JOIN "
                + "    different_snowflake_db.different_snowflake_schema.COLORS AS db2_colors "
                + "ON "
                + "    db1_colors.COLORID = db2_colors.COLORID"),
        anAttribute("COLORID", 21L),
        anAttribute("COLORNAME", 22L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("COLORID"), aField("COLORNAME")));

    assertFieldsPresent(facet, "COLORID", "COLORNAME");
    assertFieldDependsOn(facet, "COLORID", "snowflake_db.snowflake_schema.COLORS.COLORID");
    assertFieldDependsOn(
        facet,
        "COLORNAME",
        "snowflake_db.snowflake_schema.COLORS.COLORNAME",
        "different_snowflake_db.different_snowflake_schema.COLORS.COLORTINT");
  }

  @Test
  void testSnowflakeQueryFromDefaultAndASecondDatabase() {
    givenALogicalRelationWith(
        aSnowflakeRelation(
            "query",
            "SELECT "
                + "    db1_colors.COLORID AS COLORID, "
                + "    CONCAT(db2_colors.COLORTINT, db1_colors.COLORNAME) AS COLORNAME "
                + "FROM "
                + "    COLORS AS db1_colors "
                + "JOIN "
                + "    different_snowflake_db.different_snowflake_schema.COLORS AS db2_colors "
                + "ON "
                + "    db1_colors.COLORID = db2_colors.COLORID"),
        anAttribute("COLORID", 21L),
        anAttribute("COLORNAME", 22L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("COLORID"), aField("COLORNAME")));

    assertFieldsPresent(facet, "COLORID", "COLORNAME");
    assertFieldDependsOn(facet, "COLORID", "snowflake_db.snowflake_schema.COLORS.COLORID");
    assertFieldDependsOn(
        facet,
        "COLORNAME",
        "snowflake_db.snowflake_schema.COLORS.COLORNAME",
        "different_snowflake_db.different_snowflake_schema.COLORS.COLORTINT");
  }

  @Test
  void testSnowflakeQueryWithQuotedIdentifiers() {
    givenALogicalRelationWith(
        aSnowflakeRelation(
            "query",
            "SELECT  "
                + "    \"db1_colors\".\"COLORID\" AS \"COLORID\",  "
                + "    CONCAT(\"db2_colors\".\"COLORTINT\", \"db1_colors\".\"COLORNAME\") AS \"COLORNAME\" "
                + "FROM  "
                + "    \"COLORS\" AS \"db1_colors\" "
                + "JOIN  "
                + "    \"different_snowflake_db\".\"different_snowflake_schema\".\"COLORS\" AS \"db2_colors\" "
                + "ON  "
                + "    \"db1_colors\".\"COLORID\" = \"db2_colors\".\"COLORID\""),
        anAttribute("COLORID", 21L),
        anAttribute("COLORNAME", 22L));

    OpenLineage.ColumnLineageDatasetFacet facet =
        buildFacetFor(anOutputSchemaWith(aField("COLORID"), aField("COLORNAME")));

    assertFieldsPresent(facet, "COLORID", "COLORNAME");
    assertFieldDependsOn(facet, "COLORID", "snowflake_db.snowflake_schema.COLORS.COLORID");
    assertFieldDependsOn(
        facet,
        "COLORNAME",
        "snowflake_db.snowflake_schema.COLORS.COLORNAME",
        "different_snowflake_db.different_snowflake_schema.COLORS.COLORTINT");
  }

  private SnowflakeRelation aSnowflakeRelation(String option, String value) {
    if ("query".equals(option)) {
      when(snowflakeRelation.params().table()).thenReturn(Option.empty());
      when(snowflakeRelation.params().query()).thenReturn(Option.apply(value));
    } else if ("dbtable".equals(option)) {
      TableName tableName = mock(TableName.class);
      when(tableName.toString()).thenReturn(value);
      when(snowflakeRelation.params().table()).thenReturn(Option.apply(tableName));
      when(snowflakeRelation.params().query()).thenReturn(Option.empty());
    }
    return snowflakeRelation;
  }

  private AttributeReference anAttribute(String name, long exprId) {
    return new AttributeReference(
        name,
        IntegerType$.MODULE$,
        false,
        Metadata.empty(),
        ExprId.apply(exprId),
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  private void givenALogicalRelationWith(
      SnowflakeRelation relation, AttributeReference... attributes) {
    LogicalRelation logicalRelation =
        new LogicalRelation(
            relation,
            ScalaConversionUtils.fromList(Arrays.asList(attributes)),
            Option.empty(),
            false);
    when(queryExecution.optimizedPlan()).thenReturn(logicalRelation);
  }

  private OpenLineage.SchemaDatasetFacetFields aField(String name) {
    return openLineage.newSchemaDatasetFacetFieldsBuilder().name(name).type("int").build();
  }

  private OpenLineage.SchemaDatasetFacet anOutputSchemaWith(
      OpenLineage.SchemaDatasetFacetFields... fields) {
    return openLineage.newSchemaDatasetFacet(Arrays.asList(fields));
  }

  private OpenLineage.ColumnLineageDatasetFacet buildFacetFor(
      OpenLineage.SchemaDatasetFacet outputSchema) {
    Optional<OpenLineage.ColumnLineageDatasetFacet> facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(event, context, outputSchema);

    assertTrue(facet.isPresent(), "ColumnLineageDatasetFacet should be present");
    return facet.get();
  }

  private void assertFieldsPresent(
      OpenLineage.ColumnLineageDatasetFacet facet, String... expectedFieldNames) {
    assertEquals(expectedFieldNames.length, facet.getFields().getAdditionalProperties().size());
    for (String fieldName : expectedFieldNames) {
      assertTrue(
          facet.getFields().getAdditionalProperties().containsKey(fieldName),
          "Field " + fieldName + " should be present in facet");
    }
  }

  private void assertFieldDependsOn(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String outputField,
      String... expectedInputFields) {

    List<OpenLineage.InputField> actualInputFields =
        facet.getFields().getAdditionalProperties().get(outputField).getInputFields();

    assertEquals(
        expectedInputFields.length,
        actualInputFields.size(),
        "Invalid number of input fields for output field " + outputField);

    for (int i = 0; i < expectedInputFields.length; i++) {
      String expectedInput = expectedInputFields[i];
      String expectedNamespace = "snowflake://orgname-accountname";
      String expectedName;
      String expectedField;

      if (expectedInput.contains(".")) {
        // Fully qualified name: db.schema.table.column
        String[] parts = expectedInput.split("\\.", 4);
        expectedName = parts[0] + "." + parts[1] + "." + parts[2];
        expectedField = parts[3];
      } else {
        // Simple column name: default to snowflake_db.snowflake_schema.COLORS
        expectedName = "snowflake_db.snowflake_schema.COLORS";
        expectedField = expectedInput;
      }

      OpenLineage.InputField actualInput = actualInputFields.get(i);

      assertEquals(
          expectedNamespace,
          actualInput.getNamespace(),
          "Invalid namespace for input field " + actualInput.getField());
      assertEquals(
          expectedName,
          actualInput.getName(),
          "Invalid name for input field " + actualInput.getField());
      assertEquals(expectedField, actualInput.getField(), "Invalid field name");
    }
  }
}
