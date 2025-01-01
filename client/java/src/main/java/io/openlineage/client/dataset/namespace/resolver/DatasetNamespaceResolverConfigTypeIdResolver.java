/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;

public class DatasetNamespaceResolverConfigTypeIdResolver extends TypeIdResolverBase {
  private JavaType superType;

  @Override
  public void init(JavaType baseType) {
    superType = baseType;
  }

  @Override
  public String idFromValue(Object value) {
    return DatasetNamespaceResolverLoader.loadDatasetNamespaceResolverTypeByConfigClass(
        value.getClass());
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType) {
    return idFromValue(value);
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    Class<? extends DatasetNamespaceResolverConfig> clazz =
        DatasetNamespaceResolverLoader.loadDatasetNamespaceResolverConfigByType(id);
    return context.constructSpecializedType(superType, clazz);
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.CUSTOM;
  }
}
