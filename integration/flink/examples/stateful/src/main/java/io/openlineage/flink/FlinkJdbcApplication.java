/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import io.openlineage.util.OpenLineageFlinkJobListenerBuilder;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.types.Row;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.util.Properties;

import static io.openlineage.common.config.ConfigWrapper.fromResource;

public class FlinkJdbcApplication {
    public static final String INPUT_QUERY = "select * from source_event";
    public static final String OUTPUT_QUERY = "insert into sink_event(event_uid, content, created_at) values (?, ?, ?)";

    public static final JdbcStatementBuilder<Row> TEST_ENTRY_JDBC_STATEMENT_BUILDER =
            (ps, row) -> {
                if (row.getArity() == 3) {
                    ps.setString(1, (String) row.getField(0));
                    ps.setString(2, (String) row.getField(1));
                    ps.setTimestamp(3, (Timestamp) row.getField(2));
                }
            };
    public static void main(String[] args) throws Exception {
        Properties properties = fromResource("postgres.conf").toProperties();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                SqlTimeTypeInfo.TIMESTAMP
        };

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
        JdbcInputFormat eventInputFormat =
                new JdbcInputFormat.JdbcInputFormatBuilder()
                        .setDBUrl(properties.getProperty("postgres.url"))
                        .setDrivername(properties.getProperty("postgres.driver"))
                        .setQuery(INPUT_QUERY)
                        .setUsername(properties.getProperty("postgres.user"))
                        .setPassword(properties.getProperty("postgres.password"))
                        .setRowTypeInfo(rowTypeInfo)
                        .finish();

        DataStreamSource dataStreamSource = env.createInput(eventInputFormat);
        dataStreamSource.addSink(JdbcSink.sink(
                OUTPUT_QUERY,
                TEST_ENTRY_JDBC_STATEMENT_BUILDER,
                new InternalJdbcConnectionOptions.Builder()
                        .setDBUrl(properties.getProperty("postgres.url"))
                        .setDriverName(properties.getProperty("postgres.driver"))
                        .setUsername(properties.getProperty("postgres.user"))
                        .setTableName("sink_event")
                        .setPassword(properties.getProperty("postgres.password")).build()));

        env.registerJobListener(
                OpenLineageFlinkJobListenerBuilder.create()
                        .executionEnvironment(env)
                        .jobName("flink_examples_jdbc")
                        .build());
        env.execute("flink_examples_jdbc");
    }
}
