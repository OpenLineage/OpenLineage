/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class CustomTransportResolver {

  public static Class<? extends TransportConfig> resolveCustomTransportConfigByType(String type) {
    CustomTransportBuilder builder =
        getCustomTransportBuilder(b -> b.getType().equalsIgnoreCase(type));
    return builder.getConfig().getClass();
  }

  public static String resolveCustomTransportTypeByConfigClass(Class transportConfigClass) {
    CustomTransportBuilder builder =
        getCustomTransportBuilder(b -> b.getConfig().getClass().equals(transportConfigClass));
    return builder.getType();
  }

  public static Transport resolveCustomTransportByConfig(TransportConfig transportConfig) {
    CustomTransportBuilder builder =
        getCustomTransportBuilder(b -> b.getConfig().getClass().equals(transportConfig.getClass()));
    return builder.build(transportConfig);
  }

  private static CustomTransportBuilder getCustomTransportBuilder(
      Predicate<CustomTransportBuilder> predicate) {
    ServiceLoader<CustomTransportBuilder> loader = ServiceLoader.load(CustomTransportBuilder.class);
    Optional<CustomTransportBuilder> optionalBuilder =
        StreamSupport.stream(loader.spliterator(), false).filter(predicate).findFirst();

    return optionalBuilder.orElseThrow(
        () -> new IllegalArgumentException("Failed to find CustomTransportBuilder"));
  }
}
