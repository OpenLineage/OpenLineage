package io.openlineage.client;

import java.net.URL;
import javax.annotation.Nullable;
import lombok.NonNull;

public final class Clients {
  private Clients() {}

  public static OpenLineageClient newClient(@NonNull final URL baseUrl) {
    return newClient(baseUrl, null);
  }

  public static OpenLineageClient newClient(
      @NonNull final URL baseUrl, @Nullable final String apiKey) {
    return OpenLineageClient.builder().baseUrl(baseUrl).apiKey(apiKey).build();
  }
}
