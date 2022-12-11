/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.spark.snowflake.SnowflakeRelation;
import net.snowflake.spark.snowflake.TableName;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.Seq$;

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

    when(relation.params().sfDatabase()).thenReturn("snowflake_db");
    when(relation.params().sfSchema()).thenReturn("snowflake_schema");
    when(relation.schema())
        .thenReturn(
            (new StructType(
                new StructField[] {new StructField("name", StringType$.MODULE$, false, null)})));

    TableName tableName = mock(TableName.class, RETURNS_DEEP_STUBS);
    when(tableName.toString()).thenReturn("table");

    Option<TableName> table = Option.apply(tableName);

    when(relation.params().table()).thenReturn(table);

    when(relation.params().sfFullURL())
        .thenReturn("https://microsoft_partner.east-us-2.azure.snowflakecomputing.com");
  }

  @Test
  void testApplyDbTable() {

    SnowflakeRelationVisitor visitor =
        new SnowflakeRelationVisitor<>(
            SparkAgentTestExtension.newContext(session), DatasetFactory.output(context));

    LogicalRelation lr =
        new LogicalRelation(
            relation,
            Seq$.MODULE$
                .<AttributeReference>newBuilder()
                .$plus$eq(
                    new AttributeReference(
                        FIELD_NAME,
                        StringType$.MODULE$,
                        false,
                        null,
                        ExprId.apply(1L),
                        Seq$.MODULE$.<String>empty()))
                .result(),
            Option.empty(),
            false);

    List<OpenLineage.Dataset> datasets = visitor.apply(lr);

    OpenLineage.Dataset ds = datasets.get(0);

    assertEquals(
        "snowflake://microsoft_partner.east-us-2.azure.snowflakecomputing.com", ds.getNamespace());

    assertEquals("snowflake_db.snowflake_schema.table", ds.getName());
  }
}
