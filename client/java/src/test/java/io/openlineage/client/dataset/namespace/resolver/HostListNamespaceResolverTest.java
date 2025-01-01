/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.namespace.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class HostListNamespaceResolverTest {

  HostListNamespaceResolver resolver =
      new HostListNamespaceResolver(
          "cluster", new HostListNamespaceResolverConfig(Arrays.asList("host1", "host2"), null));

  @Test
  void testHostNotOnList() {
    assertThat(resolver.resolve("postgres://host3:5432")).isEqualTo("postgres://host3:5432");
  }

  @Test
  void testHostOnTheList() {
    assertThat(resolver.resolve("postgres://host2:5432")).isEqualTo("postgres://cluster:5432");
  }

  @Test
  void testHostOnTheListWithDifferentSchema() {
    resolver =
        new HostListNamespaceResolver(
            "cluster",
            new HostListNamespaceResolverConfig(Arrays.asList("host1", "host2"), "kafka"));
    assertThat(resolver.resolve("postgres://host2:5432")).isEqualTo("postgres://host2:5432");
  }

  @Test
  void testHostOnTheListWithSameSchema() {
    resolver =
        new HostListNamespaceResolver(
            "cluster",
            new HostListNamespaceResolverConfig(Arrays.asList("host1", "host2"), "postgres"));
    assertThat(resolver.resolve("postgres://host2:5432")).isEqualTo("postgres://cluster:5432");
  }

  @Test
  void testHostOnTheListWithDifferentCase() {
    assertThat(resolver.resolve("postgres://HOST1:5432")).isEqualTo("postgres://cluster:5432");
  }

  @Test
  void testResolveWhenNoHostsProvided() {
    assertThat(
            new HostListNamespaceResolver(
                    "cluster", new HostListNamespaceResolverConfig(Collections.emptyList(), null))
                .resolve("postgres://host3:5432"))
        .isEqualTo("postgres://host3:5432");
  }
}
