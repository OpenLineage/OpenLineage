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
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
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
  private final URL url;
  private @Nullable final String apiKey;

  public HttpTransport(@NonNull final HttpConfig httpConfig) {
    super(Type.HTTP);
    this.http = HttpClientBuilder.create().build();
    try {
      this.url =
          new URIBuilder(httpConfig.getUrl().toURI())
              .setPath(httpConfig.getUrl().getPath() + API_V1 + "/lineage")
              .build()
              .toURL();
    } catch (URISyntaxException | MalformedURLException e) {
      throw new OpenLineageClientException(e);
    }
    this.apiKey = httpConfig.getApiKey();
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    final String eventAsJson = Utils.toJson(runEvent);
    log.debug("POST {}: {}", url, eventAsJson);
    try {
      final HttpPost request = new HttpPost();
      request.setURI(url.toURI());
      request.addHeader(ACCEPT, APPLICATION_JSON.toString());
      request.addHeader(CONTENT_TYPE, APPLICATION_JSON.toString());
      request.setEntity(new StringEntity(eventAsJson, APPLICATION_JSON));

      if (apiKey != null) {
        request.addHeader(AUTHORIZATION, "Bearer " + apiKey);
      }

      final HttpResponse response = http.execute(request);
      throwOnHttpError(response);
    } catch (URISyntaxException | IOException e) {
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
    private static final URL DEFAULT_OPENLINEAGE_URL = Utils.toUrl("http://localhost:8080");

    private URL url;
    private @Nullable String apiKey;

    private Builder() {
      this.url = DEFAULT_OPENLINEAGE_URL;
    }

    public Builder url(@NonNull String urlAsString) {
      return url(Utils.toUrl(urlAsString));
    }

    public Builder url(@NonNull String urlAsString, @NonNull Map<String, String> queryParams) {
      return url(Utils.toUrl(urlAsString), queryParams);
    }

    public Builder url(@NonNull URL url) {
      return url(url, Collections.emptyMap());
    }

    public Builder url(@NonNull URL url, @NonNull Map<String, String> queryParams) {
      try {
        final URIBuilder builder = new URIBuilder(url.toURI());
        queryParams.forEach(builder::addParameter);
        this.url = builder.build().toURL();
      } catch (URISyntaxException | MalformedURLException e) {
        throw new OpenLineageClientException(e);
      }
      return this;
    }

    public Builder apiKey(@Nullable String apiKey) {
      this.apiKey = apiKey;
      return this;
    }

    /**
     * Returns an {@link HttpTransport} object with the properties of this {@link
     * HttpTransport.Builder}.
     */
    public HttpTransport build() {
      final HttpConfig httpConfig = new HttpConfig();
      httpConfig.setUrl(url);
      httpConfig.setApiKey(apiKey);
      return new HttpTransport(httpConfig);
    }
  }
}
