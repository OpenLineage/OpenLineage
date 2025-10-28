/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.snowflake.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.SnowflakeUtils;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.agent.vendor.snowflake.lifecycle.SnowflakeDataset;
import io.openlineage.spark.agent.vendor.snowflake.lifecycle.SnowflakeRelationVisitor;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.Vendors;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.spark.snowflake.SnowflakeRelation;
import net.snowflake.spark.snowflake.TableName;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import scala.Option;

@Slf4j
public class SnowflakeRelationVisitorTest {

  private static final String FIELD_NAME = "name";

  SparkSession session = mock(SparkSession.class);
  OpenLineageContext context = mock(OpenLineageContext.class);
  SnowflakeRelation relation = mock(SnowflakeRelation.class, RETURNS_DEEP_STUBS);

  @BeforeEach
  public void setUp() {
    when(session.sparkContext()).thenReturn(mock(SparkContext.class));
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(context.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());

    when(relation.params().sfDatabase()).thenReturn("snowflake_db");
    when(relation.params().sfSchema()).thenReturn("snowflake_schema");
    when(relation.schema())
        .thenReturn(
            (new StructType(
                new StructField[] {new StructField("name", StringType$.MODULE$, false, null)})));
  }

  @ParameterizedTest()
  @CsvSource({
    "https://microsoft_partner.east-us-2.azure.snowflakecomputing.com,snowflake://microsoft_partner.east-us-2.azure",
    "https://orgname-accountname.snowflakecomputing.com,snowflake://orgname-accountname",
  })
  void testApplyDbTable(String fullUrl, String namespace) {
    when(relation.params().sfFullURL()).thenReturn(fullUrl);

    TableName tableName = mock(TableName.class, RETURNS_DEEP_STUBS);
    when(tableName.toString()).thenReturn("table");

    Option<TableName> table = Option.apply(tableName);
    when(relation.params().table()).thenReturn(table);

    OpenLineageContext openLineageContext =
        OpenLineageContext.builder()
            .sparkSession(session)
            .sparkContext(session.sparkContext())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .customEnvironmentVariables(Collections.singletonList("TEST_VAR"))
            .vendors(Vendors.getVendors())
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .build();

    SnowflakeRelationVisitor visitor =
        new SnowflakeRelationVisitor<>(openLineageContext, DatasetFactory.output(context));

    LogicalRelation lr =
        new LogicalRelation(
            relation,
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    new AttributeReference(
                        FIELD_NAME,
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        ScalaConversionUtils.<String>asScalaSeqEmpty()))),
            Option.empty(),
            false);

    List<OpenLineage.Dataset> datasets = visitor.apply(lr);

    OpenLineage.Dataset ds = datasets.get(0);

    assertEquals(namespace, ds.getNamespace());

    assertEquals("snowflake_db.snowflake_schema.table", ds.getName());
  }

  @ParameterizedTest()
  @CsvSource({
    "https://microsoft_partner.east-us-2.azure.snowflakecomputing.com,snowflake://microsoft_partner.east-us-2.azure",
    "https://orgname-accountname.snowflakecomputing.com,snowflake://orgname-accountname",
  })
  void testApplyQuery(String fullUrl, String namespace) {
    when(relation.params().sfFullURL()).thenReturn(fullUrl);

    when(relation.params().table()).thenReturn(Option.empty());
    when(relation.params().query()).thenReturn(Option.apply("select * from some_table"));

    OpenLineageContext openLineageContext =
        OpenLineageContext.builder()
            .sparkSession(session)
            .sparkContext(session.sparkContext())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .customEnvironmentVariables(Collections.singletonList("TEST_VAR"))
            .vendors(Vendors.getVendors())
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .build();

    SnowflakeRelationVisitor visitor =
        new SnowflakeRelationVisitor<>(openLineageContext, DatasetFactory.output(context));

    LogicalRelation lr =
        new LogicalRelation(
            relation,
            ScalaConversionUtils.fromList(
                Collections.singletonList(
                    new AttributeReference(
                        FIELD_NAME,
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        ScalaConversionUtils.<String>asScalaSeqEmpty()))),
            Option.empty(),
            false);

    List<OpenLineage.Dataset> datasets = visitor.apply(lr);

    OpenLineage.Dataset ds = datasets.get(0);

    assertEquals(namespace, ds.getNamespace());

    assertEquals("snowflake_db.snowflake_schema.some_table", ds.getName());
  }

  @ParameterizedTest
  @CsvSource({
    // Quoted identifiers (should strip quotes)
    "\"MyTable\", MyTable",
    "\"my_database\", my_database",
    "\"UPPERCASE_TABLE\", UPPERCASE_TABLE",
    "\"table_with_special$chars\", table_with_special$chars",

    // Unquoted identifiers (should remain unchanged)
    "normal_table, normal_table",
    "TableName, TableName",
    "TABLE123, TABLE123",

    // Edge cases
    "\"\", ''", // Empty quotes should result in empty string (represented as '' in CSV)
    "single, single", // No quotes

    // Null and short strings
    "null, null", // Null input (will be handled separately)
  })
  void testStripQuotes(String input, String expected) {
    // Handle null case
    if ("null".equals(input)) {
      assertNull(SnowflakeUtils.stripQuotes(null));
      return;
    }

    // Handle empty string representation in CSV
    if ("''".equals(expected)) {
      expected = "";
    }

    String result = SnowflakeUtils.stripQuotes(input);
    assertEquals(expected, result, String.format("Failed to strip quotes from: %s", input));
  }

  @Test
  void testStripQuotesFromSchema() {
    // Create schema with quoted field names
    StructType originalSchema =
        new StructType(
            new StructField[] {
              new StructField("\"report_date\"", DataTypes.DateType, false, Metadata.empty()),
              new StructField("\"model_portfolio\"", DataTypes.StringType, true, Metadata.empty()),
              new StructField("unquoted_field", DataTypes.IntegerType, true, Metadata.empty()),
              new StructField("\"bor_nav\"", DataTypes.DoubleType, true, Metadata.empty())
            });

    // Strip quotes from schema
    StructType normalizedSchema = SnowflakeDataset.stripQuotesFromSchema(originalSchema);

    // Verify field names are normalized
    assertEquals(4, normalizedSchema.fields().length);
    assertEquals("report_date", normalizedSchema.fields()[0].name());
    assertEquals("model_portfolio", normalizedSchema.fields()[1].name());
    assertEquals("unquoted_field", normalizedSchema.fields()[2].name());
    assertEquals("bor_nav", normalizedSchema.fields()[3].name());

    // Verify data types and nullable are preserved
    assertEquals(DataTypes.DateType, normalizedSchema.fields()[0].dataType());
    assertEquals(false, normalizedSchema.fields()[0].nullable());
    assertEquals(DataTypes.StringType, normalizedSchema.fields()[1].dataType());
    assertEquals(true, normalizedSchema.fields()[1].nullable());
    assertEquals(DataTypes.IntegerType, normalizedSchema.fields()[2].dataType());
    assertEquals(DataTypes.DoubleType, normalizedSchema.fields()[3].dataType());
  }

  @Test
  void testStripQuotesFromSchemaWithNull() {
    assertNull(SnowflakeDataset.stripQuotesFromSchema(null));
  }

  @Test
  void testStripQuotesFromSchemaEmpty() {
    StructType emptySchema = new StructType(new StructField[] {});
    StructType result = SnowflakeDataset.stripQuotesFromSchema(emptySchema);
    assertEquals(0, result.fields().length);
  }
}
