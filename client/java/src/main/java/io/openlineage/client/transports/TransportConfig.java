/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import java.util.WeakHashMap;

@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonTypeIdResolver(TransportConfigTypeIdResolver.class)
public interface TransportConfig {
  WeakHashMap<TransportConfig, String> nameRegistry = new WeakHashMap<>();

  default String getName() {
    return nameRegistry.get(this);
  }

  default void setName(String name) {
    nameRegistry.put(this, name);
  }
}
