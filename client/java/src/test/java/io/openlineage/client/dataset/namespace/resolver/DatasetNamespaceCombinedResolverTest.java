/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.namespace.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class DatasetNamespaceCombinedResolverTest {
  HostListNamespaceResolver resolver1 = mock(HostListNamespaceResolver.class);
  HostListNamespaceResolver resolver2 = mock(HostListNamespaceResolver.class);
  DatasetConfig config = new DatasetConfig();

  @Test
  void testNoConfigProvided() {
    config.setNamespaceResolvers(Collections.emptyMap());
    assertThat(new DatasetNamespaceCombinedResolver((DatasetConfig) null).resolve("some-namespace"))
        .isEqualTo("some-namespace");

    config = new DatasetConfig();
    config.setNamespaceResolvers(null);
    assertThat(new DatasetNamespaceCombinedResolver(config).resolve("some-namespace"))
        .isEqualTo("some-namespace");

    HostListNamespaceResolverConfig resolverConfig =
        new HostListNamespaceResolverConfig(Arrays.asList("host1", "host2"), null);
    this.config = new DatasetConfig();
    this.config.setNamespaceResolvers(Collections.singletonMap("cluster", resolverConfig));
    assertThat(new DatasetNamespaceCombinedResolver(config).resolve("some-namespace"))
        .isEqualTo("some-namespace");
  }

  @Test
  void testResolveWhenNoResolversDefined() {
    config.setNamespaceResolvers(Collections.emptyMap());
    assertThat(new DatasetNamespaceCombinedResolver(config).resolve("host5")).isEqualTo("host5");
  }

  @Test
  void testResolveWhenForSingleResolver() {
    when(resolver1.resolve("host1")).thenReturn("cluster");
    when(resolver1.resolve("host5")).thenReturn("host5");
    try (MockedStatic loader = mockStatic(DatasetNamespaceResolverLoader.class)) {
      when(DatasetNamespaceResolverLoader.loadDatasetNamespaceResolvers(config))
          .thenReturn(Collections.singletonList(resolver1));
      DatasetNamespaceCombinedResolver resolvers = new DatasetNamespaceCombinedResolver(config);

      Assertions.assertThat(
              resolvers.resolve(new DatasetIdentifier("name", "host1")).getNamespace())
          .isEqualTo("cluster");
      assertThat(resolvers.resolve(new DatasetIdentifier("name", "host5")).getNamespace())
          .isEqualTo("host5");
    }
  }

  @Test
  void testResolveWhenForMultipleResolvers() {
    when(resolver1.resolve("host1")).thenReturn("cluster1");
    when(resolver1.resolve("host2")).thenReturn("host2");
    when(resolver1.resolve("host5")).thenReturn("host5");

    when(resolver2.resolve("host1")).thenReturn("host1");
    when(resolver1.resolve("host2")).thenReturn("cluster2");
    when(resolver2.resolve("host5")).thenReturn("host5");

    try (MockedStatic loader = mockStatic(DatasetNamespaceResolverLoader.class)) {
      when(DatasetNamespaceResolverLoader.loadDatasetNamespaceResolvers(config))
          .thenReturn(Arrays.asList(resolver1, resolver2));
      DatasetNamespaceCombinedResolver resolvers = new DatasetNamespaceCombinedResolver(config);

      assertThat(resolvers.resolve("host1")).isEqualTo("cluster1");
      assertThat(resolvers.resolve("host2")).isEqualTo("cluster2");
      assertThat(resolvers.resolve("host5")).isEqualTo("host5");
    }
  }

  @Test
  void testResolveForUriString() throws URISyntaxException {
    HostListNamespaceResolver resolver =
        new HostListNamespaceResolver(
            "kafka-prod",
            new HostListNamespaceResolverConfig(Collections.singletonList("kafka"), null));

    try (MockedStatic loader = mockStatic(DatasetNamespaceResolverLoader.class)) {
      when(DatasetNamespaceResolverLoader.loadDatasetNamespaceResolvers(config))
          .thenReturn(Arrays.asList(resolver));
      DatasetNamespaceCombinedResolver resolvers = new DatasetNamespaceCombinedResolver(config);

      // assert that only host part of the URI gets resolved
      assertThat(resolvers.resolveHost(new URI("kafka://kafka:9091")).toString())
          .isEqualTo("kafka://kafka-prod:9091");

      // assert non-matching uri is not changed
      URI someUri = new URI("kafka://other-host:9091");
      assertThat(resolvers.resolveHost(someUri)).isEqualTo(someUri);
    }
  }
}
