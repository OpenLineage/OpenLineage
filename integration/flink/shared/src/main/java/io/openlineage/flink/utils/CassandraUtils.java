/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.mapping.annotations.Table;
import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

@Slf4j
public class CassandraUtils {
  public static final String CASSANDRA_MANAGER_CLASS = "com.datastax.driver.core.Cluster$Manager";
  public static final String CASSANDRA_NAMESPACE_PREFIX = "cassandra://";

  public static boolean hasClasses() {
    try {
      CassandraUtils.class
          .getClassLoader()
          .loadClass("org.apache.flink.batch.connectors.cassandra.CassandraInputFormat");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
      log.debug(
          "Can't load class org.apache.flink.batch.connectors.cassandra.CassandraInputFormat");
    }
    return false;
  }

  public static Optional<Table> extractTableAnnotation(Class pojo) {
    Annotation[] annotations = pojo.getAnnotations();
    for (Annotation annotation : annotations) {
      if (annotation instanceof Table) {
        return Optional.of((Table) annotation);
      }
    }

    return Optional.empty();
  }

  public static Optional<String> findNamespaceFromBuilder(
      Optional<ClusterBuilder> clusterBuilderOpt) {
    try {
      if (clusterBuilderOpt.isPresent()) {
        Cluster cluster = clusterBuilderOpt.get().getCluster();
        if (cluster != null) {
          Optional<Object> managerOpt =
              WrapperUtils.<Object>getFieldValue(Cluster.class, cluster, "manager");
          if (managerOpt.isPresent()) {
            Class managerClass = Class.forName(CASSANDRA_MANAGER_CLASS);
            Optional<List<Object>> endpointsOpt =
                WrapperUtils.<List<Object>>getFieldValue(
                    managerClass, managerOpt.get(), "contactPoints");
            return CassandraUtils.convertToNamespace(endpointsOpt);
          }
        }
      }
    } catch (ClassNotFoundException e) {
      log.error("Failed load class required to infer the Cassandra namespace name", e);
    }

    return Optional.of("");
  }

  public static ClusterBuilder createClusterBuilder(String contactPoints) {
    return new ClusterBuilder() {
      @Override
      protected Cluster buildCluster(Cluster.Builder builder) {
        return builder.addContactPoint(contactPoints).build();
      }
    };
  }

  private static Optional<String> convertToNamespace(Optional<List<Object>> endpointsOpt) {
    if (endpointsOpt.isPresent() && !endpointsOpt.isEmpty()) {
      Object endpoint = endpointsOpt.get().get(0);
      // endpoint is in the format of hostname/ip:port
      if (endpoint != null) {
        String[] parts = endpoint.toString().split("/");
        if (parts.length != 2) {
          log.debug("Unrecgonized cassandra endpoint {}", endpoint);
          return Optional.of(CASSANDRA_NAMESPACE_PREFIX + endpoint);
        } else if (StringUtils.isBlank(parts[0]) && !StringUtils.isBlank(parts[1])) {
          // Use ip:port if hostname is not provided
          return Optional.of(CASSANDRA_NAMESPACE_PREFIX + parts[1]);
        } else if (!StringUtils.isBlank(parts[0]) && !StringUtils.isBlank(parts[1])) {
          // Use hostname:port if cname is provided for readability
          if (parts[1].contains(":")) {
            return Optional.of(
                CASSANDRA_NAMESPACE_PREFIX + parts[0] + ":" + parts[1].split(":")[1]);
          }
        }
      }
    }

    return Optional.of("");
  }
}
