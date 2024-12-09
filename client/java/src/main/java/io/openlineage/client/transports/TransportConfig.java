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
public interface TransportConfig extends Comparable<TransportConfig> {
  WeakHashMap<TransportConfig, String> nameRegistry = new WeakHashMap<>();

  default String getName() {
    return nameRegistry.get(this);
  }

  default void setName(String name) {
    nameRegistry.put(this, name);
  }

  @Override
  default int compareTo(TransportConfig o) {
    if (o == null || o.getName() == null) {
      return 1;
    }
    if (getName() == null) {
      return -1;
    }
    return getName().compareTo(o.getName());
  }
}
