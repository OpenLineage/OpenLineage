/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import io.openlineage.client.transports.NoopTransport;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportFactory;

/** Factory class for creating new {@link OpenLineageClient} objects. */
public final class Clients {
  private Clients() {}

  /** Returns a new {@code OpenLineageClient} object. */
  public static OpenLineageClient newClient() {
    return newClient(new DefaultConfigPathProvider());
  }

  public static OpenLineageClient newClient(ConfigPathProvider configPathProvider) {
    String isDisabled = Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED");
    if (Boolean.parseBoolean(isDisabled)) {
      return OpenLineageClient.builder().transport(new NoopTransport()).build();
    }
    final OpenLineageYaml openLineageYaml =
        OpenLineageClientUtils.loadOpenLineageYaml(configPathProvider);
    final TransportFactory factory = new TransportFactory(openLineageYaml.getTransportConfig());
    final Transport transport = factory.build();
    // ...
    return OpenLineageClient.builder().transport(transport).build();
  }
}
