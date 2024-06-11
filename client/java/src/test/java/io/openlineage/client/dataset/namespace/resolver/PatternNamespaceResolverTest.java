/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.namespace.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PatternNamespaceResolverTest {

  PatternNamespaceResolver resolver =
      new PatternNamespaceResolver(
          "cassandra-cluster-prod",
          new PatternNamespaceResolverConfig("cassandra-prod(\\d)+\\.company\\.com", null));

  @Test
  void testNonMatchingPattern() {
    String namespace = "kafka://kafka-prod893472.company.com:9091";
    assertThat(resolver.resolve(namespace)).isEqualTo(namespace);
  }

  @Test
  void testMatchingPattern() {
    String host = "cassandra://cassandra-prod893472.company.com";
    assertThat(resolver.resolve(host)).isEqualTo("cassandra://cassandra-cluster-prod");
  }

  @Test
  void testMatchingPatternWithDifferentSchema() {
    resolver =
        new PatternNamespaceResolver(
            "cassandra-cluster-prod",
            new PatternNamespaceResolverConfig("cassandra-prod(\\d)+\\.company\\.com", "kafka"));

    String host = "cassandra://cassandra-prod893472.company.com";
    assertThat(resolver.resolve(host)).isEqualTo("cassandra://cassandra-prod893472.company.com");
  }

  @Test
  void testMatchingPatternWithSameSchema() {
    resolver =
        new PatternNamespaceResolver(
            "cassandra-cluster-prod",
            new PatternNamespaceResolverConfig(
                "cassandra-prod(\\d)+\\.company\\.com", "cassandra"));

    String host = "cassandra://cassandra-prod893472.company.com";
    assertThat(resolver.resolve(host)).isEqualTo("cassandra://cassandra-cluster-prod");
  }
}
