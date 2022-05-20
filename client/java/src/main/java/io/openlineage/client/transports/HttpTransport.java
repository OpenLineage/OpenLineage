package io.openlineage.client.transports;

import static org.apache.http.Consts.UTF_8;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

@Slf4j
public final class HttpTransport extends Transport implements Closeable {
  private static final String API_V1 = "/api/v1";

  private final HttpClient http;
  private final URI uri;
  private @Nullable final TokenProvider tokenProvider;

  public HttpTransport(@NonNull final HttpConfig httpConfig) {
    this(HttpClientBuilder.create().build(), httpConfig);
  }

  public HttpTransport(@NonNull final HttpClient httpClient, @NonNull final HttpConfig httpConfig) {
    super(Type.HTTP);
    this.http = httpClient;
    try {
      URI configUri = httpConfig.getUrl();
      if (configUri.getPath() != null && !configUri.getPath().equals("")) {
        this.uri = httpConfig.getUrl();
      } else {
        this.uri =
            new URIBuilder(httpConfig.getUrl())
                .setPath(httpConfig.getUrl().getPath() + API_V1 + "/lineage")
                .build();
      }
    } catch (URISyntaxException e) {
      throw new OpenLineageClientException(e);
    }
    this.tokenProvider = httpConfig.getAuth();
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    final String eventAsJson = Utils.toJson(runEvent);
    log.debug("POST {}: {}", uri, eventAsJson);
    try {
      final HttpPost request = new HttpPost();
      request.setURI(uri);
      request.addHeader(ACCEPT, APPLICATION_JSON.toString());
      request.addHeader(CONTENT_TYPE, APPLICATION_JSON.toString());
      request.setEntity(new StringEntity(eventAsJson, APPLICATION_JSON));

      if (tokenProvider != null) {
        request.addHeader(AUTHORIZATION, tokenProvider.getToken());
      }

      final HttpResponse response = http.execute(request);
      throwOnHttpError(response);
    } catch (IOException e) {
      throw new OpenLineageClientException(e);
    }
  }

  private void throwOnHttpError(@NonNull HttpResponse response) throws IOException {
    final int code = response.getStatusLine().getStatusCode();
    if (code >= 400 && code < 600) { // non-2xx
      throw new OpenLineageClientException(EntityUtils.toString(response.getEntity(), UTF_8));
    }
  }

  @Override
  public void close() throws IOException {
    if (http instanceof Closeable) {
      ((Closeable) http).close();
    }
  }

  /** Returns an new {@link HttpTransport.Builder} object for building {@link HttpTransport}s. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link HttpTransport} instances.
   *
   * <p>Usage:
   *
   * <pre>{@code
   * HttpTransport httpTransport = HttpTransport().builder()
   *   .url("http://localhost:5000")
   *   .build()
   * }</pre>
   */
  public static final class Builder {
    private static final URI DEFAULT_OPENLINEAGE_URI = Utils.toUri("http://localhost:8080");

    private URI uri;

    private @Nullable HttpClient httpClient;
    private @Nullable TokenProvider tokenProvider;

    private Builder() {
      this.uri = DEFAULT_OPENLINEAGE_URI;
    }

    public Builder uri(@NonNull String urlAsString) {
      return uri(Utils.toUri(urlAsString));
    }

    public Builder uri(@NonNull String urlAsString, @NonNull Map<String, String> queryParams) {
      return uri(Utils.toUri(urlAsString), queryParams);
    }

    public Builder uri(@NonNull URI uri) {
      return uri(uri, Collections.emptyMap());
    }

    public Builder uri(@NonNull URI uri, @NonNull Map<String, String> queryParams) {
      try {
        final URIBuilder builder = new URIBuilder(uri);
        queryParams.forEach(builder::addParameter);
        this.uri = builder.build();
      } catch (URISyntaxException e) {
        throw new OpenLineageClientException(e);
      }
      return this;
    }

    public Builder http(@NonNull HttpClient httpClient) {
      this.httpClient = httpClient;
      return this;
    }

    public Builder tokenProvider(@Nullable TokenProvider tokenProvider) {
      this.tokenProvider = tokenProvider;
      return this;
    }

    public Builder apiKey(@Nullable String apiKey) {
      final ApiKeyTokenProvider apiKeyTokenProvider = new ApiKeyTokenProvider();
      apiKeyTokenProvider.setApiKey(apiKey);
      this.tokenProvider = apiKeyTokenProvider;
      return this;
    }

    /**
     * Returns an {@link HttpTransport} object with the properties of this {@link
     * HttpTransport.Builder}.
     */
    public HttpTransport build() {
      final HttpConfig httpConfig = new HttpConfig();
      httpConfig.setUrl(uri);
      httpConfig.setAuth(tokenProvider);
      if (httpClient != null) {
        return new HttpTransport(httpClient, httpConfig);
      }
      return new HttpTransport(httpConfig);
    }
  }
}
