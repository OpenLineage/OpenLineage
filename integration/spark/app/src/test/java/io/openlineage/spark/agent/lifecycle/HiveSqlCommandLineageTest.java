/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.spark.agent.Spark4CompatUtils;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Hive counterpart of {@link SqlCommandLineageTest}.
 *
 * <p>Tagged as an integration test, as a Hive session started in a shared test JVM breaks loading
 * of {@code OpenLineageExtensionProvider}s in {@link SparkOpenLineageExtensionVisitorWrapper} for
 * tests that run after it.
 */
@Tag("integration-test")
@EnabledIfSystemProperty(named = "spark.version", matches = "3.*")
@ExtendWith(SparkAgentTestExtension.class)
class HiveSqlCommandLineageTest {

  @Test
  void hiveCommands(@TempDir Path tmp) throws IOException {
    SparkSession spark =
        SparkAgentTestExtension.sessionBuilder(
                Spark4CompatUtils.builderWithHiveSupport(), "hiveCommands")
            .getOrCreate();
    SqlCommandLineage lineage = new SqlCommandLineage(spark, tmp);
    SoftAssertions softly = new SoftAssertions();
    Path data = tmp.resolve("data.txt");
    Files.write(data, "1\u0001x\n".getBytes(StandardCharsets.UTF_8));

    // Spark 3.4+ attaches the catalog facet to outputs of these commands as well
    String catalog = lineage.isSparkAtLeast(3, 4) ? "catalog," : "";

    softly
        .assertThat(lineage.run("CREATE TABLE h1 (a INT, b STRING) STORED AS textfile"))
        .containsExactlyInAnyOrder(
            "COMPLETE hive_commands.execute_create_table_command.WH_h1 OUT file:<wh>/h1 CREATE schema=a,b facets="
                + catalog
                + "dataSource,lifecycleStateChange,schema,symlinks");

    softly
        .assertThat(lineage.run("INSERT INTO h1 VALUES (1, 'x')"))
        .containsExactlyInAnyOrder(
            "COMPLETE hive_commands.execute_insert_into_hive_table.WH_h1 OUT file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
            "START hive_commands.execute_insert_into_hive_table.WH_h1 OUT file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks");

    softly
        .assertThat(lineage.run("CREATE TABLE h2 STORED AS textfile AS SELECT * FROM h1"))
        .containsExactlyInAnyOrder(
            "COMPLETE hive_commands.execute_create_hive_table_as_select_command.default_h2 IN file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
            "COMPLETE hive_commands.execute_create_hive_table_as_select_command.default_h2 OUT file:<wh>/h2 CREATE schema=a,b facets=catalog,columnLineage,dataSource,lifecycleStateChange,schema,symlinks",
            "START hive_commands.execute_create_hive_table_as_select_command.default_h2 IN file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
            "START hive_commands.execute_create_hive_table_as_select_command.default_h2 OUT file:<wh>/h2 CREATE schema=a,b facets=catalog,columnLineage,dataSource,lifecycleStateChange,schema,symlinks");

    if (lineage.isSparkAtLeast(3, 4)) {
      softly
          .assertThat(lineage.run("CREATE TABLE h3 STORED AS parquet AS SELECT * FROM h1"))
          .containsExactlyInAnyOrder(
              "COMPLETE hive_commands.execute_create_hive_table_as_select_command.default_h3 IN file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
              "COMPLETE hive_commands.execute_create_hive_table_as_select_command.default_h3 OUT file:<wh>/h3 CREATE schema=a,b facets=catalog,columnLineage,dataSource,lifecycleStateChange,schema,symlinks",
              "START hive_commands.execute_create_hive_table_as_select_command.default_h3 IN file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
              "START hive_commands.execute_create_hive_table_as_select_command.default_h3 OUT file:<wh>/h3 CREATE schema=a,b facets=catalog,columnLineage,dataSource,lifecycleStateChange,schema,symlinks");
    } else {
      softly
          .assertThat(lineage.run("CREATE TABLE h3 STORED AS parquet AS SELECT * FROM h1"))
          .containsExactlyInAnyOrder(
              "COMPLETE hive_commands.execute_optimized_create_hive_table_as_select_command IN file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
              "COMPLETE hive_commands.execute_optimized_create_hive_table_as_select_command OUT file:<wh>/h3 schema=a,b facets=columnLineage,dataSource,schema,symlinks",
              "START hive_commands.execute_optimized_create_hive_table_as_select_command IN file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
              "START hive_commands.execute_optimized_create_hive_table_as_select_command OUT file:<wh>/h3 schema=a,b facets=columnLineage,dataSource,schema,symlinks");
    }

    softly
        .assertThat(
            lineage.run(
                "INSERT OVERWRITE LOCAL DIRECTORY '"
                    + tmp.resolve("hdir")
                    + "' STORED AS textfile SELECT * FROM h1"))
        .containsExactlyInAnyOrder(
            "COMPLETE hive_commands.execute_insert_into_hive_dir_command.TMP_hdir IN file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
            "COMPLETE hive_commands.execute_insert_into_hive_dir_command.TMP_hdir OUT file:<tmp>/hdir OVERWRITE schema=a,b facets=catalog,columnLineage,dataSource,lifecycleStateChange,schema",
            "START hive_commands.execute_insert_into_hive_dir_command.TMP_hdir IN file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
            "START hive_commands.execute_insert_into_hive_dir_command.TMP_hdir OUT file:<tmp>/hdir OVERWRITE schema=a,b facets=catalog,columnLineage,dataSource,lifecycleStateChange,schema");

    softly
        .assertThat(lineage.run("LOAD DATA LOCAL INPATH '" + data + "' INTO TABLE h1"))
        .containsExactlyInAnyOrder(
            "COMPLETE hive_commands.execute_load_data_command.default_h1 IN file:<tmp>/data.txt facets=dataSource",
            "COMPLETE hive_commands.execute_load_data_command.default_h1 OUT file:<wh>/h1 schema=a,b facets="
                + catalog
                + "dataSource,schema,symlinks",
            "START hive_commands.execute_load_data_command.default_h1 IN file:<tmp>/data.txt facets=dataSource",
            "START hive_commands.execute_load_data_command.default_h1 OUT file:<wh>/h1 schema=a,b facets="
                + catalog
                + "dataSource,schema,symlinks");

    spark.sql("CREATE TABLE t5 (a INT, b STRING) USING parquet");
    softly
        .assertThat(lineage.run("INSERT INTO t5 SELECT * FROM h1"))
        .containsExactlyInAnyOrder(
            "COMPLETE hive_commands.execute_insert_into_hadoop_fs_relation_command.WH_t5 IN file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
            "COMPLETE hive_commands.execute_insert_into_hadoop_fs_relation_command.WH_t5 OUT file:<wh>/t5 schema=a,b facets=columnLineage,dataSource,schema,symlinks",
            "START hive_commands.execute_insert_into_hadoop_fs_relation_command.WH_t5 IN file:<wh>/h1 schema=a,b facets=catalog,dataSource,schema,symlinks",
            "START hive_commands.execute_insert_into_hadoop_fs_relation_command.WH_t5 OUT file:<wh>/t5 schema=a,b facets=columnLineage,dataSource,schema,symlinks");
    softly.assertAll();
  }
}
