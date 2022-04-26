package io.openlineage.client;

import java.net.URI;

public class Events {
  public static OpenLineage.RunEvent event() {
    return new OpenLineage(URI.create("http://test.producer")).newRunEventBuilder().build();
  }
}
