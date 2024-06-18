/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceResolverLoader.DatasetNamespaceResolverServiceLoader;
import java.util.Collections;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class DatasetNamespaceResolverLoaderTest {

  @Test
  void testBuilderLoadedWithServiceLoader() {
    DatasetNamespaceResolverBuilder builder = mock(DatasetNamespaceResolverBuilder.class);
    DatasetConfig datasetConfig = mock(DatasetConfig.class);
    DatasetNamespaceResolverConfig config = mock(DatasetNamespaceResolverConfig.class);
    DatasetNamespaceResolver resolver = mock(DatasetNamespaceResolver.class);
    when(builder.getConfig()).thenReturn(config);
    when(builder.build("name", config)).thenReturn(resolver);
    when(datasetConfig.getNamespaceResolvers())
        .thenReturn(Collections.singletonMap("name", config));

    ServiceLoader<DatasetNamespaceResolverBuilder> serviceLoader = mock(ServiceLoader.class);
    try (MockedStatic loader = mockStatic(DatasetNamespaceResolverServiceLoader.class)) {
      when(DatasetNamespaceResolverServiceLoader.load()).thenReturn(serviceLoader);
      when(serviceLoader.spliterator())
          .thenReturn(Collections.singletonList(builder).spliterator());

      assertThat(DatasetNamespaceResolverLoader.loadDatasetNamespaceResolvers(datasetConfig))
          .hasSize(1)
          .first()
          .isEqualTo(resolver);
    }
  }
}
