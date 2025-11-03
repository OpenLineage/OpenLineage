/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class TransportResolver {

  // The Service Loader mechanism can often become cumbersome. For instance, class loader issues may
  // arise when the Kinesis transport loader is missing from the classpath, yet a corresponding
  // definition exists in the manifest — resulting in a ServiceConfigurationError.
  // To prevent such issues, it’s preferable to support the most common transports directly, without
  // relying on the Service Loader mechanism.
  // This method is the default approach used by the Spark integration through
  @SuppressWarnings("PMD.DoubleBraceInitialization")
  private static final Collection<TransportBuilder> COMMON_BUILDERS =
      Arrays.asList(
          new HttpTransportBuilder(),
          new CompositeTransportBuilder(),
          new FileTransportBuilder(),
          new TransformTransportBuilder(),
          new KafkaTransportBuilder());

  public static Class<? extends TransportConfig> resolveTransportConfigByType(String type) {
    for (TransportBuilder builder : COMMON_BUILDERS) {
      if (builder.getType().equalsIgnoreCase(type)) {
        return builder.getConfig().getClass();
      }
    }
    TransportBuilder builder = getTransportBuilder(b -> b.getType().equalsIgnoreCase(type));
    return builder.getConfig().getClass();
  }

  public static String resolveTransportTypeByConfigClass(Class transportConfigClass) {
    TransportBuilder builder =
        getTransportBuilder(b -> b.getConfig().getClass().equals(transportConfigClass));
    return builder.getType();
  }

  public static Transport resolveTransportByConfig(TransportConfig transportConfig) {
    for (TransportBuilder builder : COMMON_BUILDERS) {
      if (transportConfig.getClass().equals(builder.getConfig().getClass())) {
        return builder.build(transportConfig);
      }
    }
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
