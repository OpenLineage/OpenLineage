package io.openlineage.client;

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
    final OpenLineageYaml openLineageYaml = Utils.loadOpenLineageYaml(configPathProvider);
    final TransportFactory factory = new TransportFactory(openLineageYaml.getTransportConfig());
    final Transport transport = factory.build();
    // ...
    return OpenLineageClient.builder().transport(transport).build();
  }
}
