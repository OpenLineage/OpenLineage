/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.openlineage.flink.StreamEnvironment.setupEnv;

import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.ParameterTool;
import org.apache.kafka.common.errors.TimeoutException;

public class FlinkSqlApplication {

  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = setupEnv(args);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // ---------- Produce an event time stream into Kafka -------------------
    String groupId = "testSql";
    String bootstraps = parameters.getRequired("bootstraps");

    final String createInputTable =
        String.format(
            "create table kafka_input (\n"
                + "  `computed-price` as price + 1.0,\n"
                + "  price decimal(38, 18),\n"
                + "  currency string,\n"
                + "  log_date date,\n"
                + "  log_time time(3),\n"
                + "  log_ts timestamp(3),\n"
                + "  ts as log_ts + INTERVAL '1' SECOND,\n"
                + "  watermark for ts as ts\n"
                + ") comment 'My Complex Table' with (\n"
                + "  'connector' = '%s',\n"
                + "  'topic' = '%s',\n"
                + "  'properties.bootstrap.servers' = '%s',\n"
                + "  'properties.group.id' = '%s',\n"
                + "  'scan.startup.mode' = 'earliest-offset',\n"
                + "  'format' = 'json'\n"
                + ")",
            KafkaDynamicTableFactory.IDENTIFIER, parameters.getRequired("input-topics"), bootstraps, groupId);

    tEnv.executeSql(createInputTable);

    final String createOutputTable =
        String.format(
            "create table kafka_output (\n"
                + "  ts_interval string,\n"
                + "  max_log_date string,\n"
                + "  max_log_time string,\n"
                + "  max_ts string,\n"
                + "  counter bigint,\n"
                + "  max_price decimal(38, 18)\n"
                + ") with (\n"
                + "  'connector' = '%s',\n"
                + "  'topic' = '%s',\n"
                + "  'properties.bootstrap.servers' = '%s',\n"
                + "  'properties.group.id' = '%s',\n"
                + "  'scan.startup.mode' = 'earliest-offset',\n"
                + "  'format' = 'json'\n"
                + ")",
            KafkaDynamicTableFactory.IDENTIFIER, parameters.getRequired("output-topics"), bootstraps, groupId);
    tEnv.executeSql(createOutputTable);

    String initialValues =
        "INSERT INTO kafka_input\n"
            + "SELECT CAST(price AS DECIMAL(10, 2)), currency, "
            + " CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3))\n"
            + "FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001'), \n"
            + "  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001'), \n"
            + "  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001'), \n"
            + "  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001'), \n"
            + "  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001'), \n"
            + "  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10'))\n"
            + "  AS orders (price, currency, d, t, ts)";
    tEnv.executeSql(initialValues).await();

    // ---------- Consume stream from Kafka -------------------

    String query =
        "INSERT INTO kafka_output SELECT\n"
            + "  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n"
            + "  CAST(MAX(log_date) AS VARCHAR),\n"
            + "  CAST(MAX(log_time) AS VARCHAR),\n"
            + "  CAST(MAX(ts) AS VARCHAR),\n"
            + "  COUNT(*),\n"
            + "  CAST(MAX(price) AS DECIMAL(10, 2))\n"
            + "FROM kafka_input\n"
            + "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

    try {
      tEnv.executeSql(query).await(60, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // ok
    }
  }
}
