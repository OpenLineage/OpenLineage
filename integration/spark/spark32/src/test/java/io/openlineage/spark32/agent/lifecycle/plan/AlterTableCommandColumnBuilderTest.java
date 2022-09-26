/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
 */
package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils;
import java.util.Arrays;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AlterTableCommandColumnBuilderTest {

    @SuppressWarnings("PMD")
    private static final String LOCAL_IP = "127.0.0.1";

    private static final String INT_TYPE = "int";
    private static final String FILE = "file";
    private static final String T1_EXPECTED_NAME = "/tmp/column_level_lineage/db.t1";
    private static final String CREATE_T1_FROM_TEMP
            = "CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp";
    SparkSession spark;
    QueryExecution queryExecution = mock(QueryExecution.class);

    OpenLineageContext context;
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    StructType structTypeSchema
            = new StructType(
                    new StructField[]{
                        new StructField("a", IntegerType$.MODULE$, false, new Metadata(new scala.collection.immutable.HashMap<>())),
                        new StructField("b", IntegerType$.MODULE$, false, new Metadata(new scala.collection.immutable.HashMap<>()))
                    });

    @BeforeAll
    @SneakyThrows
    public static void beforeAll() {
        SparkSession$.MODULE$.cleanupAnyExistingSession();
    }

    @AfterAll
    @SneakyThrows
    public static void afterAll() {
        SparkSession$.MODULE$.cleanupAnyExistingSession();
    }

    @BeforeEach
    @SneakyThrows
    public void beforeEach() {
        spark
                = SparkSession.builder()
                        .master("local[*]")
                        .appName("ColumnLevelLineage")
                        .config("spark.extraListeners", LastQueryExecutionSparkEventListener.class.getName())
                        .config("spark.driver.host", LOCAL_IP)
                        .config("spark.driver.bindAddress", LOCAL_IP)
                        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
                        .config("spark.sql.catalog.spark_catalog.type", "hive")
                        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                        .config("spark.sql.catalog.local.warehouse", "/tmp/column_level_lineage/")
                        .config("spark.sql.catalog.local.type", "hadoop")
                        .config(
                                "spark.sql.extensions",
                                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/col_v2/derby")
                        .getOrCreate();

        context
                = OpenLineageContext.builder()
                        .sparkSession(Optional.of(spark))
                        .sparkContext(spark.sparkContext())
                        .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                        .queryExecution(queryExecution)
                        .build();

        FileSystem.get(spark.sparkContext().hadoopConfiguration())
                .delete(new Path("/tmp/column_level_lineage/"), true);

            spark.sql("DROP TABLE IF EXISTS local.db.t1");
            spark.sql("DROP TABLE IF EXISTS local.db.t2");
            spark.sql("DROP TABLE IF EXISTS local.db.t");
        try {
        spark
                .createDataFrame(Arrays.asList(new GenericRow(new Object[]{1, 2})), structTypeSchema)
                .createOrReplaceTempView("temp");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    void testAlterQuery() {
        spark.sql(CREATE_T1_FROM_TEMP);
        spark.sql("INSERT INTO local.db.t1 VALUES (1,2),(3,4),(5,6)");
        spark.sql("CREATE TABLE local.db.t2 AS SELECT CONCAT(CAST(a AS STRING), CAST(b AS STRING)) as `c`, a+b as `d` FROM local.db.t1");

        spark.sql(
                "ALTER TABLE local.db.t2 RENAME COLUMN `d` TO `adb`");

        OpenLineage.SchemaDatasetFacet outputSchema
                = openLineage.newSchemaDatasetFacet(
                        Arrays.asList(
                                openLineage.newSchemaDatasetFacetFieldsBuilder().name("c").type(INT_TYPE).build()));

        LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
        when(queryExecution.optimizedPlan()).thenReturn(plan);
        OpenLineage.ColumnLineageDatasetFacet facet
                = ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, outputSchema).get();
        assertTrue(facet != null);
        assertColumnDependsOn(facet, "adb", FILE, T1_EXPECTED_NAME, "d");
    }

    private void assertColumnDependsOn(
            OpenLineage.ColumnLineageDatasetFacet facet,
            String outputColumn,
            String expectedNamespace,
            String expectedName,
            String expectedInputField) {

        assertTrue(
                facet.getFields().getAdditionalProperties().get(outputColumn).getInputFields().stream()
                        .filter(f -> f.getNamespace().equalsIgnoreCase(expectedNamespace))
                        .filter(f -> f.getName().equals(expectedName))
                        .filter(f -> f.getField().equalsIgnoreCase(expectedInputField))
                        .findAny()
                        .isPresent());
    }
}
