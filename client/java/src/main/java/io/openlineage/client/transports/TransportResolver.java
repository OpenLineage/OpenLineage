/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class TransportResolver {

  public static Class<? extends TransportConfig> resolveTransportConfigByType(String type) {
    TransportBuilder builder = getTransportBuilder(b -> b.getType().equalsIgnoreCase(type));
    return builder.getConfig().getClass();
  }

  public static String resolveTransportTypeByConfigClass(Class transportConfigClass) {
    TransportBuilder builder =
        getTransportBuilder(b -> b.getConfig().getClass().equals(transportConfigClass));
    return builder.getType();
  }

  public static Transport resolveTransportByConfig(TransportConfig transportConfig) {
    TransportBuilder builder =
        getTransportBuilder(b -> b.getConfig().getClass().equals(transportConfig.getClass()));
    return builder.build(transportConfig);
  }

  private static TransportBuilder getTransportBuilder(Predicate<TransportBuilder> predicate) {
    ServiceLoader<TransportBuilder> loader = ServiceLoader.load(TransportBuilder.class);
    Optional<TransportBuilder> optionalBuilder =
        StreamSupport.stream(loader.spliterator(), false).filter(predicate).findFirst();

    return optionalBuilder.orElseThrow(
        () -> new IllegalArgumentException("Failed to find TransportBuilder"));
  }
}
