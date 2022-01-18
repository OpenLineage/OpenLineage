package io.openlineage.client;

import java.net.URL;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class OpenLineageClient {
  private final OpenLineageHttp http;

  public OpenLineageClient() {
    this(Utils.toUrl(System.getenv("OPENLINEAGE_URL")), System.getenv("OPENLINEAGE_API_KEY"));
  }

  public OpenLineageClient(@NonNull final URL baseUrl) {
    this(baseUrl, null);
  }

  public OpenLineageClient(@NonNull final URL baseUrl, @Nullable final String apiKey) {
    this(OpenLineageHttp.create(baseUrl, apiKey));
  }

  OpenLineageClient(@NonNull final OpenLineageHttp http) {
    this.http = http;
  }

  public void emit(@NonNull OpenLineage.RunEvent run) {
    http.post(http.url("/lineage"), Utils.toJson(run));
  }
}
