/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.apache.http.Consts.UTF_8;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

@Slf4j
public final class HttpTransport extends Transport {
  private static final String API_V1 = "/api/v1";

  private final CloseableHttpClient http;
  private final URI uri;
  private @Nullable final TokenProvider tokenProvider;

  private final Map<String, String> headers;
  private @Nullable final HttpConfig.Compression compression;

  public HttpTransport(@NonNull final HttpConfig httpConfig) {
    this(withTimeout(httpConfig), httpConfig);
  }

  private static CloseableHttpClient withTimeout(HttpConfig httpConfig) {
    int timeoutMs;
    if (httpConfig.getTimeout() != null) {
      // deprecated approach, value in seconds as double provided
      timeoutMs = (int) (httpConfig.getTimeout() * 1000);
    } else if (httpConfig.getTimeoutInMillis() != null) {
      timeoutMs = httpConfig.getTimeoutInMillis();
    } else {
      // default one
      timeoutMs = 5000;
    }

    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(timeoutMs)
            .setConnectionRequestTimeout(timeoutMs)
            .setSocketTimeout(timeoutMs)
            .build();
    return HttpClientBuilder.create().setDefaultRequestConfig(config).build();
  }

  public HttpTransport(
      @NonNull final CloseableHttpClient httpClient, @NonNull final HttpConfig httpConfig) {
    super(Type.HTTP);
    this.http = httpClient;
    try {
      this.uri = getUri(httpConfig);
    } catch (URISyntaxException e) {
      throw new OpenLineageClientException(e);
    }
    this.tokenProvider = httpConfig.getAuth();
    this.headers = httpConfig.getHeaders() != null ? httpConfig.getHeaders() : new HashMap<>();
    this.compression = httpConfig.getCompression();
  }

  private URI getUri(HttpConfig httpConfig) throws URISyntaxException {
    URI url = httpConfig.getUrl();
    if (url == null) {
      throw new OpenLineageClientException(
          "url can't be null, try setting transport.url in config");
    }
    URIBuilder builder = new URIBuilder(url);
    if (StringUtils.isNotBlank(url.getPath())) {
      if (StringUtils.isNotBlank(httpConfig.getEndpoint())) {
        throw new OpenLineageClientException("You can't pass both uri and endpoint parameters.");
      }
    } else {
      String endpoint =
          StringUtils.isNotBlank(httpConfig.getEndpoint())
              ? httpConfig.getEndpoint()
              : API_V1 + "/lineage";
      builder.setPath(endpoint);
    }
    if (httpConfig.getUrlParams() != null) {
      httpConfig.getUrlParams().entrySet().stream()
          .forEach(e -> builder.addParameter(e.getKey().replace("url.param.", ""), e.getValue()));
    }
    return builder.build();
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emit(OpenLineageClientUtils.toJson(runEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    emit(OpenLineageClientUtils.toJson(datasetEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    emit(OpenLineageClientUtils.toJson(jobEvent));
  }

  private void emit(String eventAsJson) {
    log.debug("POST event on URL {}", uri);
    try {
      final HttpPost request = new HttpPost();
      request.setURI(uri);
      setHeaders(request);
      setBody(request, eventAsJson);

      try (CloseableHttpResponse response = http.execute(request)) {
        throwOnHttpError(response);
        EntityUtils.consume(response.getEntity());
      }
    } catch (IOException e) {
      throw new OpenLineageClientException(e);
    }
  }

  private void setBody(HttpPost request, String body) {
    HttpEntity entity = new StringEntity(body, APPLICATION_JSON);
    if (compression == HttpConfig.Compression.GZIP) {
      entity = new GzipCompressingEntity(entity);
    }
    request.setEntity(entity);
  }

  private void setHeaders(HttpPost request) {
    // set headers to accept json
    headers.put(ACCEPT, APPLICATION_JSON.toString());
    headers.put(CONTENT_TYPE, APPLICATION_JSON.toString());
    // if tokenProvider preset overwrite authorization
    if (tokenProvider != null) {
      headers.put(AUTHORIZATION, tokenProvider.getToken());
    }
    headers.forEach(request::addHeader);
  }

  private void throwOnHttpError(@NonNull HttpResponse response) throws IOException {
    final int code = response.getStatusLine().getStatusCode();
    if (code >= 400 && code < 600) { // non-2xx
      throw new HttpTransportResponseException(
          code, EntityUtils.toString(response.getEntity(), UTF_8));
    }
  }

  @Override
  public void close() throws IOException {
    http.close();
  }

  /**
   * @return an new {@link HttpTransport.Builder} object for building {@link HttpTransport}s.
   */
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
   *
   * @deprecated Use {@link HttpConfig} instead
   */
  @Deprecated
  public static final class Builder {
    private static final URI DEFAULT_OPENLINEAGE_URI =
        OpenLineageClientUtils.toUri("http://localhost:8080");

    private @Nullable CloseableHttpClient httpClient;

    @Delegate private final HttpConfig httpConfig = new HttpConfig();

    private Builder() {
      httpConfig.setUrl(DEFAULT_OPENLINEAGE_URI);
    }

    public Builder uri(@NonNull String urlAsString) {
      return uri(OpenLineageClientUtils.toUri(urlAsString));
    }

    public Builder uri(@NonNull String urlAsString, @NonNull Map<String, String> queryParams) {
      return uri(OpenLineageClientUtils.toUri(urlAsString), queryParams);
    }

    public Builder uri(@NonNull URI uri) {
      return uri(uri, Collections.emptyMap());
    }

    public Builder uri(@NonNull URI uri, @NonNull Map<String, String> queryParams) {
      try {
        final URIBuilder builder = new URIBuilder(uri);
        queryParams.forEach(builder::addParameter);
        httpConfig.setUrl(builder.build());
      } catch (URISyntaxException e) {
        throw new OpenLineageClientException(e);
      }
      return this;
    }

    public Builder timeout(@Nullable Double timeout) {
      httpConfig.setTimeout(timeout);
      return this;
    }

    public Builder http(@NonNull CloseableHttpClient httpClient) {
      this.httpClient = httpClient;
      return this;
    }

    public Builder tokenProvider(@Nullable TokenProvider tokenProvider) {
      httpConfig.setAuth(tokenProvider);
      return this;
    }

    public Builder apiKey(@Nullable String apiKey) {
      final ApiKeyTokenProvider apiKeyTokenProvider = new ApiKeyTokenProvider();
      apiKeyTokenProvider.setApiKey(apiKey);
      return tokenProvider(apiKeyTokenProvider);
    }

    /**
     * @return an {@link HttpTransport} object with the properties of this {@link
     *     HttpTransport.Builder}.
     */
    public HttpTransport build() {
      if (httpClient != null) {
        return new HttpTransport(httpClient, httpConfig);
      }
      return new HttpTransport(httpConfig);
    }
  }
}
