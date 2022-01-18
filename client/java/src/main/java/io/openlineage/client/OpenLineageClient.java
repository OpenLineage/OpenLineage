package io.openlineage.client;

import java.net.URL;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

@Slf4j
public final class OpenLineageClient {
  private static final URL DEFAULT_BASE_URL = Utils.toUrl("http://localhost:8080");

  private final OpenLineageHttp http;


  public OpenLineageClient() {
    this(DEFAULT_BASE_URL, null);
  }

  public OpenLineageClient(final String baseUrlString) {
    this(baseUrlString, null);
  }

  public OpenLineageClient(final String baseUrlString, @Nullable final String apiKey) {
    this(Utils.toUrl(baseUrlString), apiKey);
  }

  public OpenLineageClient(final URL baseUrl) {
    this(baseUrl, null);
  }

  public OpenLineageClient(final URL baseUrl, @Nullable final String apiKey) {
    this(OpenLineageHttp.create(baseUrl, apiKey));
  }

  OpenLineageClient(@NonNull final OpenLineageHttp http) {
    this.http = http;
  }

  public void emit(@NonNull OpenLineage.RunEvent run) {
    http.post(http.url("/lineage"), Utils.toJson(run));
  }
}
