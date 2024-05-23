/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import java.util.Objects;

public class TokenProviderTypeIdResolver extends TypeIdResolverBase {
  private JavaType superType;

  @Override
  public void init(JavaType baseType) {
    superType = baseType;
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.NAME;
  }

  @Override
  public String idFromValue(Object obj) {
    return idFromValueAndType(obj, obj.getClass());
  }

  @Override
  public String idFromValueAndType(Object obj, Class<?> subType) {
    return subType.getCanonicalName();
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    if (Objects.equals(id, "api_key")) { // backwards compatibility
      return context.constructSpecializedType(superType, ApiKeyTokenProvider.class);
    }
    try {
      return context.constructSpecializedType(superType, Class.forName(id));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          String.format("No class matches %s from spark.openlineage.transport.auth.type", id), e);
    }
  }
}
