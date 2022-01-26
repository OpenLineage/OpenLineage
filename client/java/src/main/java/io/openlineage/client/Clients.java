package io.openlineage.client;

import java.net.URL;
import javax.annotation.Nullable;
import lombok.NonNull;

/** Factory class for creating new {@link OpenLineageClient} objects. */
public final class Clients {
  private Clients() {}

  /** Returns a new {@code OpenLineageClient} . */
  public static OpenLineageClient newClient() {
    return OpenLineageClient.builder().build();
  }

  /** Returns a new {@code OpenLineageClient} object with the given {@code url}. */
  public static OpenLineageClient newClient(@NonNull final URL url) {
    return newClient(url, null);
  }

  /**
   * Returns a new {@code OpenLineageClient} object with the given {@code url} and {@code apiKey}.
   */
  public static OpenLineageClient newClient(@NonNull final URL url, @Nullable final String apiKey) {
    return OpenLineageClient.builder().url(url).apiKey(apiKey).build();
  }
}
