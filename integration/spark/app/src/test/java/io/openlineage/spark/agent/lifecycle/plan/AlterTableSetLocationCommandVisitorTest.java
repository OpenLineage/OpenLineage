/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.command.AlterTableSetLocationCommand;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;
import scala.collection.Map$;
import scala.collection.immutable.HashMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@ExtendWith(SparkAgentTestExtension.class)
class AlterTableSetLocationCommandVisitorTest {

    private static final String TABLE_1 = "table1";
    SparkSession session;
    AlterTableSetLocationCommandVisitor visitor;
    String database;

    @AfterEach
    public void afterEach() {
        dropTables();
    }

    private void dropTables() {
        session
                .sessionState()
                .catalog()
                .dropTable(new TableIdentifier(TABLE_1, Option.apply(database)), true, true);
    }

    @BeforeEach
    public void setup() {
        session =
                SparkSession.builder()
                        .config("spark.sql.warehouse.dir", "/tmp/warehouse")
                        .master("local")
                        .getOrCreate();

        dropTables();

        StructType schema =
                new StructType(
                        new StructField[]{
                                new StructField("col1", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
                        });

        session.catalog().createTable(TABLE_1, "csv", schema, Map$.MODULE$.empty());
        database = session.catalog().currentDatabase();
        visitor = new AlterTableSetLocationCommandVisitor(SparkAgentTestExtension.newContext(session));
    }

    @Test
    void testAlterTableSetLocation() {
        AlterTableSetLocationCommand command =
                new AlterTableSetLocationCommand(
                        new TableIdentifier(TABLE_1, Option.apply(database)),
                        Option.empty(),
                        "file:///tmp/dir");

        command.run(session);

        assertThat(visitor.isDefinedAt(command)).isTrue();
        List<OpenLineage.OutputDataset> datasets = visitor.apply(command);
        assertEquals(1, datasets.get(0).getFacets().getSchema().getFields().size());
        assertEquals("default.table1", datasets.get(0).getFacets().getSymlinks().getIdentifiers().get(0).getName());
        assertThat(datasets)
                .singleElement()
                .hasFieldOrPropertyWithValue("name", "/tmp/dir")
                .hasFieldOrPropertyWithValue("namespace", "file");
    }
}
