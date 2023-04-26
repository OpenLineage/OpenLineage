/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import lombok.NonNull;

/**
 * A factory for creating {@link Transport} instances. A {@code Transport} must define a {@link
 * TransportConfig} defining the set of parameters needed to construct a new {@code Transport}
 * instance. For example, {@link HttpConfig} defines the parameters for constructing a new {@link
 * HttpTransport} instance when invoking {@link TransportFactory#build()}. Below, we define a list
 * of supported {@code Transport}s. Note, when defining your own {@code TransportConfig}, the {@code
 * type} parameter <b>must</b> be specified.
 *
 * <ul>
 *   <li>A default {@link ConsoleTransport} transport
 *   <li>A {@link HttpTransport} transport
 *   <li>A {@link KafkaTransport} transport
 * </ul>
 */
public final class TransportFactory {

  private final TransportConfig transportConfig;

  public TransportFactory(@NonNull final TransportConfig transportConfig) {
    this.transportConfig = transportConfig;
  }

  public Transport build() {
    return TransportResolver.resolveTransportByConfig(transportConfig);
  }
}
