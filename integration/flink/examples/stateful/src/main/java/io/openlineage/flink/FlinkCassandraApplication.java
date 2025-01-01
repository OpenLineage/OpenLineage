/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.openlineage.common.config.ConfigWrapper.fromResource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.mapping.Mapper;
import io.openlineage.cassandra.Event;
import io.openlineage.util.OpenLineageFlinkJobListenerBuilder;
import java.net.InetSocketAddress;
import java.util.Properties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class FlinkCassandraApplication {
  private static String CASSANDRA_HOST_NAME = "casandra.hostname";
  private static String CASSANDRA_POST = "casandra.port";
  private static String CASSANDRA_CONNECT_TIMEOUT = "casandra.connect.timeout.milliseconds";
  private static String CASSANDRA_READ_TIMEOUT = "casandra.read.timeout.milliseconds";
  private static final String query = "select id, content, timestamp from flink.source_event;";

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    CassandraPojoInputFormat<Event> eventInputFormat =
        new CassandraPojoInputFormat<>(
            query, createClusterBuilder(), Event.class, () -> new Mapper.Option[] {});

    CassandraSink.addSink(
            env.createInput(eventInputFormat, TypeInformation.of(Event.class))
                .uid("cassandra-source"))
        .setClusterBuilder(createClusterBuilder())
        .setMapperOptions(() -> new Mapper.Option[] {Mapper.Option.saveNullFields(true)})
        .build();

    env.registerJobListener(
        OpenLineageFlinkJobListenerBuilder.create()
            .executionEnvironment(env)
            .jobName("flink_examples_cassandra")
            .build());
    env.execute("flink_examples_cassandra");
  }

  private static ClusterBuilder createClusterBuilder() {
    Properties properties = fromResource("cassandra.conf").toProperties();
    return new ClusterBuilder() {
      @Override
      protected Cluster buildCluster(Cluster.Builder builder) {
        return builder
            .addContactPointsWithPorts(
                new InetSocketAddress(
                    properties.getProperty(CASSANDRA_HOST_NAME),
                    Integer.parseInt(properties.getProperty(CASSANDRA_POST))))
            .withQueryOptions(
                new QueryOptions()
                    .setConsistencyLevel(ConsistencyLevel.ONE)
                    .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
            .withSocketOptions(
                new SocketOptions()
                    .setConnectTimeoutMillis(
                        Integer.parseInt(properties.getProperty(CASSANDRA_CONNECT_TIMEOUT)))
                    .setReadTimeoutMillis(
                        Integer.parseInt((properties.getProperty(CASSANDRA_READ_TIMEOUT)))))
            .build();
      }
    };
  }
}
