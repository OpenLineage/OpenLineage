/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.namespace.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PatternMatchingGroupNamespaceResolverTest {

  PatternMatchingGroupNamespaceResolver resolver =
      new PatternMatchingGroupNamespaceResolver(
          new PatternMatchingGroupNamespaceResolverConfig(
              "(?<cluster>[a-zA-Z-]+)-(\\d)+\\.company\\.com:[\\d]*", "cluster", null));

  @Test
  void testNonMatchingPattern() {
    String namespace = "cassandra://cassandra-prod-893472.other-domain.com:8080/whatever";
    assertThat(resolver.resolve(namespace)).isEqualTo(namespace);
  }

  @Test
  void testMatchingPattern() {
    String namespace = "cassandra://cassandra-prod-893472.company.com:8080/whatever";
    assertThat(resolver.resolve(namespace)).isEqualTo("cassandra://cassandra-prod/whatever");
  }

  @Test
  void testMatchingPatternWithDifferentSchema() {
    resolver =
        new PatternMatchingGroupNamespaceResolver(
            new PatternMatchingGroupNamespaceResolverConfig(
                "(?<cluster>[a-zA-Z-]+)-(\\d)+\\.company\\.com:[\\d]*", "cluster", "kafka"));

    String namespace = "cassandra://cassandra-prod-893472.company.com:8080/whatever";
    assertThat(resolver.resolve(namespace))
        .isEqualTo("cassandra://cassandra-prod-893472.company.com:8080/whatever");
  }

  @Test
  void testMatchingPatternWithSameSchema() {
    resolver =
        new PatternMatchingGroupNamespaceResolver(
            new PatternMatchingGroupNamespaceResolverConfig(
                "(?<cluster>[a-zA-Z-]+)-(\\d)+\\.company\\.com:[\\d]*", "cluster", "cassandra"));

    String namespace = "cassandra://cassandra-prod-893472.company.com:8080/whatever";
    assertThat(resolver.resolve(namespace)).isEqualTo("cassandra://cassandra-prod/whatever");
  }
}
