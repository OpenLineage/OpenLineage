/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import java.io.IOException;

public class TransportConfigTypeIdResolver extends TypeIdResolverBase {

  private JavaType superType;

  @Override
  public void init(JavaType baseType) {
    superType = baseType;
  }

  @Override
  public String idFromValue(Object value) {
    return CustomTransportResolver.resolveCustomTransportTypeByConfigClass(value.getClass());
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType) {
    return idFromValue(value);
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) throws IOException {
    Class<? extends TransportConfig> clazz = null;
    switch (id.toLowerCase()) {
      case "console":
        clazz = ConsoleConfig.class;
        break;
      case "http":
        clazz = HttpConfig.class;
        break;
      case "kafka":
        clazz = KafkaConfig.class;
        break;
      case "kinesis":
        clazz = KinesisConfig.class;
        break;
      default:
        clazz = CustomTransportResolver.resolveCustomTransportConfigByType(id);
    }
    return context.constructSpecializedType(superType, clazz);
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.CUSTOM;
  }
}
