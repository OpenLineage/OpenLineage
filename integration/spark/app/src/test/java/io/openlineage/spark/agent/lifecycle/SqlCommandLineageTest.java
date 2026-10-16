/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the input and output datasets, their main facets and the job names emitted for SQL commands
 * handled by query plan visitors and dataset builders, so that moving extraction logic between them
 * does not change emitted lineage unnoticed. Hive commands are covered in {@link
 * HiveSqlCommandLineageTest}.
 */
@ExtendWith(SparkAgentTestExtension.class)
class SqlCommandLineageTest {

  @Test
  void datasourceTableCommands(@TempDir Path tmp, SparkSession spark) {
    SqlCommandLineage lineage = new SqlCommandLineage(spark, tmp);
    SoftAssertions softly = new SoftAssertions();
    softly
        .assertThat(lineage.run("CREATE TABLE t1 (a INT, b STRING) USING parquet"))
        .containsExactlyInAnyOrder(
            "COMPLETE datasource_table_commands.execute_create_data_source_table_command.default_t1 OUT file:<wh>/t1 CREATE schema=a,b facets=dataSource,lifecycleStateChange,schema,symlinks",
            "START datasource_table_commands.execute_create_data_source_table_command.default_t1 OUT file:<wh>/t1 CREATE schema=a,b facets=dataSource,lifecycleStateChange,schema,symlinks");

    softly
        .assertThat(lineage.run("INSERT INTO t1 VALUES (1, 'x')"))
        .containsExactlyInAnyOrder(
            "COMPLETE datasource_table_commands.execute_insert_into_hadoop_fs_relation_command.WH_t1 OUT file:<wh>/t1 schema=a,b facets=dataSource,schema,symlinks",
            "START datasource_table_commands.execute_insert_into_hadoop_fs_relation_command.WH_t1 OUT file:<wh>/t1 schema=a,b facets=dataSource,schema,symlinks");

    softly
        .assertThat(lineage.run("CREATE TABLE t2 USING parquet AS SELECT * FROM t1"))
        .containsExactlyInAnyOrder(
            "COMPLETE datasource_table_commands.execute_create_data_source_table_as_select_command.default_t2 IN file:<wh>/t1 schema=a,b facets=dataSource,schema,symlinks",
            "COMPLETE datasource_table_commands.execute_create_data_source_table_as_select_command.default_t2 OUT file:<wh>/t2 CREATE schema=a,b facets=columnLineage,dataSource,lifecycleStateChange,schema,symlinks",
            "START datasource_table_commands.execute_create_data_source_table_as_select_command.default_t2 IN file:<wh>/t1 schema=a,b facets=dataSource,schema,symlinks",
            "START datasource_table_commands.execute_create_data_source_table_as_select_command.default_t2 OUT file:<wh>/t2 CREATE schema=a,b facets=columnLineage,dataSource,lifecycleStateChange,schema,symlinks");

    softly
        .assertThat(
            lineage.run(
                "INSERT OVERWRITE DIRECTORY '"
                    + tmp.resolve("dir")
                    + "' USING parquet SELECT * FROM t1"))
        .containsExactlyInAnyOrder(
            "COMPLETE datasource_table_commands.execute_insert_into_data_source_dir_command IN file:<wh>/t1 schema=a,b facets=dataSource,schema,symlinks",
            "COMPLETE datasource_table_commands.execute_insert_into_data_source_dir_command OUT file:<tmp>/dir OVERWRITE schema= facets=dataSource,lifecycleStateChange,schema",
            "START datasource_table_commands.execute_insert_into_data_source_dir_command IN file:<wh>/t1 schema=a,b facets=dataSource,schema,symlinks",
            "START datasource_table_commands.execute_insert_into_data_source_dir_command OUT file:<tmp>/dir OVERWRITE schema= facets=dataSource,lifecycleStateChange,schema");

    softly
        .assertThat(lineage.run("CREATE TABLE t3 LIKE t1"))
        .containsExactlyInAnyOrder(
            "COMPLETE datasource_table_commands.execute_create_table_like_command.t3 OUT file:<wh>/t3 CREATE schema=a,b facets=dataSource,lifecycleStateChange,schema");

    softly
        .assertThat(lineage.run("ALTER TABLE t3 ADD COLUMNS (c INT)"))
        .containsExactlyInAnyOrder(
            "COMPLETE datasource_table_commands.execute_alter_table_add_columns_command OUT file:<wh>/t3 ALTER schema=a,b,c facets=dataSource,lifecycleStateChange,schema,symlinks");

    softly
        .assertThat(lineage.run("ALTER TABLE t3 RENAME TO t4"))
        .containsExactlyInAnyOrder(
            "COMPLETE datasource_table_commands.execute_alter_table_rename_command OUT file:<wh>/t4 RENAME schema=a,b,c facets=dataSource,lifecycleStateChange,schema,symlinks");

    // START sees the table before the location changes
    softly
        .assertThat(lineage.run("ALTER TABLE t4 SET LOCATION '" + tmp.resolve("loc") + "'"))
        .containsExactlyInAnyOrder(
            "COMPLETE datasource_table_commands.execute_alter_table_set_location_command OUT file:<tmp>/loc ALTER schema=a,b,c facets=dataSource,lifecycleStateChange,schema",
            "START datasource_table_commands.execute_alter_table_set_location_command OUT file:<wh>/t4 ALTER schema=a,b,c facets=dataSource,lifecycleStateChange,schema,symlinks");

    softly
        .assertThat(lineage.run("TRUNCATE TABLE t2"))
        .containsExactlyInAnyOrder(
            "COMPLETE datasource_table_commands.execute_truncate_table_command.WH_t2 OUT file:<wh>/t2 TRUNCATE facets=dataSource,lifecycleStateChange,symlinks",
            "START datasource_table_commands.execute_truncate_table_command.WH_t2 OUT file:<wh>/t2 TRUNCATE facets=dataSource,lifecycleStateChange,symlinks");

    // SparkNodesFilter drops RefreshTableCommand events before RefreshTableCommandDatasetBuilder
    // applies
    softly.assertThat(lineage.run("REFRESH TABLE t2")).isEmpty();

    if (lineage.isSparkAtLeast(3, 4)) {
      // resolved to the v2 DropTable node, handled by DropTableDatasetBuilder
      softly
          .assertThat(lineage.run("DROP TABLE t4"))
          .containsExactlyInAnyOrder(
              "COMPLETE datasource_table_commands.drop_table OUT file:<wh>/t4 DROP schema= facets=dataSource,lifecycleStateChange,schema,symlinks",
              "START datasource_table_commands.drop_table OUT file:<wh>/t4 DROP schema= facets=dataSource,lifecycleStateChange,schema,symlinks");
    } else {
      // the table no longer exists when COMPLETE is built
      softly
          .assertThat(lineage.run("DROP TABLE t4"))
          .containsExactlyInAnyOrder(
              "START datasource_table_commands.execute_drop_table_command OUT file:<tmp>/loc DROP facets=dataSource,lifecycleStateChange");
    }

    spark.sql("CREATE TABLE p1 (a INT, p INT) USING parquet PARTITIONED BY (p)");
    softly
        .assertThat(lineage.run("ALTER TABLE p1 ADD PARTITION (p = 1)"))
        .containsExactlyInAnyOrder(
            "COMPLETE datasource_table_commands.execute_alter_table_add_partition_command OUT file:<wh>/p1 ALTER schema=a,p facets=dataSource,lifecycleStateChange,schema,symlinks",
            "START datasource_table_commands.execute_alter_table_add_partition_command OUT file:<wh>/p1 ALTER schema=a,p facets=dataSource,lifecycleStateChange,schema,symlinks");

    if (lineage.isSparkAtLeast(3, 3)) {
      // RepairTableCommandDatasetBuilder is registered for Spark 3.2 only
      softly.assertThat(lineage.run("MSCK REPAIR TABLE p1")).isEmpty();
    } else {
      softly
          .assertThat(lineage.run("MSCK REPAIR TABLE p1"))
          .containsExactlyInAnyOrder(
              "COMPLETE datasource_table_commands.execute_repair_table_command OUT file:<wh>/p1 ALTER schema=a,p facets=dataSource,lifecycleStateChange,schema,symlinks",
              "START datasource_table_commands.execute_repair_table_command OUT file:<wh>/p1 ALTER schema=a,p facets=dataSource,lifecycleStateChange,schema,symlinks");
    }
    softly.assertAll();
  }

  @Test
  void insertIntoJdbcRelation(@TempDir Path tmp, SparkSession spark) {
    SqlCommandLineage lineage = new SqlCommandLineage(spark, tmp);
    SoftAssertions softly = new SoftAssertions();
    String url = "jdbc:derby:memory:insert_into_jdbc_relation;create=true";
    String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    spark.sql("CREATE TABLE src (a INT) USING parquet");
    spark.sql("INSERT INTO src VALUES (1)");
    spark
        .table("src")
        .write()
        .format("jdbc")
        .option("url", url)
        .option("driver", driver)
        .option("dbtable", "target")
        .save();
    spark.sql(
        "CREATE TEMPORARY VIEW jdbc_target USING org.apache.spark.sql.jdbc OPTIONS (url '"
            + url
            + "', driver '"
            + driver
            + "', dbtable 'target')");

    // the target relation is resolved by delegating to LogicalRelationDatasetBuilder
    softly
        .assertThat(lineage.run("INSERT INTO jdbc_target SELECT * FROM src"))
        .containsExactlyInAnyOrder(
            "COMPLETE insert_into_jdbc_relation.execute_insert_into_data_source_command IN file:<wh>/src schema=a facets=dataSource,schema,symlinks",
            "COMPLETE insert_into_jdbc_relation.execute_insert_into_data_source_command OUT file:<derby>/metastore_db:target schema=a facets=dataSource,schema",
            "START insert_into_jdbc_relation.execute_insert_into_data_source_command IN file:<wh>/src schema=a facets=dataSource,schema,symlinks",
            "START insert_into_jdbc_relation.execute_insert_into_data_source_command OUT file:<derby>/metastore_db:target schema=a facets=dataSource,schema");

    softly
        .assertThat(lineage.run("INSERT OVERWRITE TABLE jdbc_target SELECT * FROM src"))
        .containsExactlyInAnyOrder(
            "COMPLETE insert_into_jdbc_relation.execute_insert_into_data_source_command IN file:<wh>/src schema=a facets=dataSource,schema,symlinks",
            "COMPLETE insert_into_jdbc_relation.execute_insert_into_data_source_command OUT file:<derby>/metastore_db:target OVERWRITE schema=a facets=dataSource,lifecycleStateChange,schema",
            "START insert_into_jdbc_relation.execute_insert_into_data_source_command IN file:<wh>/src schema=a facets=dataSource,schema,symlinks",
            "START insert_into_jdbc_relation.execute_insert_into_data_source_command OUT file:<derby>/metastore_db:target OVERWRITE schema=a facets=dataSource,lifecycleStateChange,schema");
    softly.assertAll();
  }

  @Test
  void rddInputs(@TempDir Path tmp, SparkSession spark) throws IOException {
    SqlCommandLineage lineage = new SqlCommandLineage(spark, tmp);
    SoftAssertions softly = new SoftAssertions();
    Path input = tmp.resolve("input.txt");
    Files.write(input, "a\nb\n".getBytes(StandardCharsets.UTF_8));

    spark
        .createDataset(spark.sparkContext().textFile(input.toString(), 1), Encoders.STRING())
        .createOrReplaceTempView("external_rdd");
    softly
        .assertThat(
            lineage.run(
                "INSERT OVERWRITE DIRECTORY '"
                    + tmp.resolve("external_out")
                    + "' USING json SELECT * FROM external_rdd"))
        .containsExactlyInAnyOrder(
            "COMPLETE rdd_inputs.execute_insert_into_data_source_dir_command IN file:<tmp> schema=obj facets=dataSource,schema",
            "COMPLETE rdd_inputs.execute_insert_into_data_source_dir_command OUT file:<tmp>/external_out OVERWRITE schema= facets=dataSource,lifecycleStateChange,schema",
            "START rdd_inputs.execute_insert_into_data_source_dir_command IN file:<tmp> schema=obj facets=dataSource,schema",
            "START rdd_inputs.execute_insert_into_data_source_dir_command OUT file:<tmp>/external_out OVERWRITE schema= facets=dataSource,lifecycleStateChange,schema");

    spark
        .createDataFrame(
            new JavaSparkContext(spark.sparkContext())
                .textFile(input.toString(), 1)
                .map(RowFactory::create),
            new StructType().add("v", "string"))
        .createOrReplaceTempView("logical_rdd");
    softly
        .assertThat(
            lineage.run(
                "INSERT OVERWRITE DIRECTORY '"
                    + tmp.resolve("logical_out")
                    + "' USING json SELECT * FROM logical_rdd"))
        .containsExactlyInAnyOrder(
            "COMPLETE rdd_inputs.execute_insert_into_data_source_dir_command IN file:<tmp> facets=dataSource",
            "COMPLETE rdd_inputs.execute_insert_into_data_source_dir_command OUT file:<tmp>/logical_out OVERWRITE schema= facets=dataSource,lifecycleStateChange,schema",
            "START rdd_inputs.execute_insert_into_data_source_dir_command IN file:<tmp> facets=dataSource",
            "START rdd_inputs.execute_insert_into_data_source_dir_command OUT file:<tmp>/logical_out OVERWRITE schema= facets=dataSource,lifecycleStateChange,schema");
    softly.assertAll();
  }
}
