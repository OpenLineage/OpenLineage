/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.circuitBreaker;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;

public class CircuitBreakerConfigTypeIdResolver extends TypeIdResolverBase {

  private JavaType superType;

  @Override
  public void init(JavaType baseType) {
    superType = baseType;
  }

  @Override
  public String idFromValue(Object value) {
    if (value instanceof SimpleJvmCircuitBreaker) {
      return "simple";
    } else if (value instanceof TestCircuitBreaker) {
      return "test";
    }
    throw new UnsupportedOperationException("Unsupported circuit breaker " + value);
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType) {
    return idFromValue(value);
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    if (id.equalsIgnoreCase("simple")) {
      return context.constructSpecializedType(superType, SimpleJvmCircuitBreakerConfig.class);
    } else if (id.equalsIgnoreCase("test")) {
      return context.constructSpecializedType(superType, TestCircuitBreakerConfig.class);
    }
    throw new UnsupportedOperationException("Unsupported circuit breaker of type " + id);
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.CUSTOM;
  }
}
