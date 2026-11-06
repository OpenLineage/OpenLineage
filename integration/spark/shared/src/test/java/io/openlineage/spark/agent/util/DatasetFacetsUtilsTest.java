/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class DatasetFacetsUtilsTest {

  private final OpenLineage openLineage = new OpenLineage(URI.create("https://test"));
  private final OpenLineageContext context = mock(OpenLineageContext.class);

  @Test
  @SneakyThrows
  void testCopyToBuilderCopiesEveryFacet() {
    when(context.getOpenLineage()).thenReturn(openLineage);
    // set every facet the builder knows about, so a facet added to the spec can't be dropped
    OpenLineage.DatasetFacetsBuilder builder = openLineage.newDatasetFacetsBuilder();
    List<Method> facetSetters = facetSetters();
    for (Method setter : facetSetters) {
      setter.invoke(builder, mock(setter.getParameterTypes()[0]));
    }
    OpenLineage.DatasetFacet customFacet = mock(OpenLineage.DatasetFacet.class);
    builder.put("custom", customFacet);
    OpenLineage.DatasetFacets facets = builder.build();

    OpenLineage.DatasetFacets copy = DatasetFacetsUtils.copyToBuilder(context, facets).build();

    assertThat(facetSetters).isNotEmpty();
    for (Method setter : facetSetters) {
      Method getter = OpenLineage.DatasetFacets.class.getMethod(getterName(setter));
      assertThat(getter.invoke(copy)).as(setter.getName()).isSameAs(getter.invoke(facets));
    }
    assertThat(copy.getAdditionalProperties()).containsEntry("custom", customFacet);
  }

  @Test
  void testCopyToBuilderOfNullFacetsIsEmpty() {
    when(context.getOpenLineage()).thenReturn(openLineage);

    OpenLineage.DatasetFacets copy = DatasetFacetsUtils.copyToBuilder(context, null).build();

    assertThat(copy.getSchema()).isNull();
    assertThat(copy.getAdditionalProperties()).isEmpty();
  }

  private static List<Method> facetSetters() {
    return Arrays.stream(OpenLineage.DatasetFacetsBuilder.class.getMethods())
        .filter(m -> m.getReturnType().equals(OpenLineage.DatasetFacetsBuilder.class))
        .filter(m -> m.getParameterCount() == 1)
        .filter(m -> OpenLineage.DatasetFacet.class.isAssignableFrom(m.getParameterTypes()[0]))
        .collect(Collectors.toList());
  }

  private static String getterName(Method setter) {
    String name = setter.getName();
    return "get" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
  }
}
