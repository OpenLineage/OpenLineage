/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openlineage.client;

import static org.apache.http.Consts.UTF_8;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

@Slf4j
class OpenLineageHttp implements Closeable {
  private static final String BASE_PATH = "/api/v1";

  final HttpClient http;
  final URL baseUrl;
  @Nullable final String apiKey;

  OpenLineageHttp(
      @NonNull final HttpClient http, @NonNull final URL baseUrl, @Nullable final String apiKey) {
    this.http = http;
    this.baseUrl = baseUrl;
    this.apiKey = apiKey;
  }

  static OpenLineageHttp create(@NonNull final URL baseUrl, @Nullable final String apiKey) {
    final CloseableHttpClient http = HttpClientBuilder.create().build();
    return new OpenLineageHttp(http, baseUrl, apiKey);
  }

  String post(@NonNull URL url, @NonNull String json) {
    log.debug("POST {}: {}", url, json);
    try {
      final HttpPost request = new HttpPost();
      request.setURI(url.toURI());
      request.addHeader(ACCEPT, APPLICATION_JSON.toString());
      request.addHeader(CONTENT_TYPE, APPLICATION_JSON.toString());
      request.setEntity(new StringEntity(json, APPLICATION_JSON));

      addAuthToReqIfKeyPresent(request);

      final HttpResponse response = http.execute(request);
      throwOnHttpError(response);

      final String bodyAsJson = EntityUtils.toString(response.getEntity(), UTF_8);
      log.debug("Response: {}", bodyAsJson);
      return bodyAsJson;
    } catch (URISyntaxException | IOException e) {
      throw new OpenLineageHttpException();
    }
  }

  URL url(String pathTemplate, @Nullable String... pathArgs) {
    return url(String.format(pathTemplate, (Object[]) pathArgs), Collections.emptyMap());
  }

  URL url(String pathTemplate, Map<String, Object> queryParams, @Nullable String... pathArgs) {
    return url(String.format(pathTemplate, (Object[]) pathArgs), queryParams);
  }

  URL url(String path, Map<String, Object> queryParams) {
    try {
      final URIBuilder builder =
          new URIBuilder(baseUrl.toURI()).setPath(baseUrl.getPath() + BASE_PATH + path);
      queryParams.forEach((name, value) -> builder.addParameter(name, String.valueOf(value)));
      return builder.build().toURL();
    } catch (URISyntaxException | MalformedURLException e) {
      throw new OpenLineageHttpException();
    }
  }

  private void addAuthToReqIfKeyPresent(@NonNull HttpRequestBase request) {
    if (apiKey != null) {
      Utils.addAuthTo(request, apiKey);
    }
  }

  private void throwOnHttpError(@NonNull HttpResponse response) throws IOException {
    final int code = response.getStatusLine().getStatusCode();
    if (code >= 400 && code < 600) { // non-2xx
      throw new OpenLineageHttpException(HttpError.of(response));
    }
  }

  @Override
  public void close() throws IOException {
    if (http instanceof Closeable) {
      ((Closeable) http).close();
    }
  }

  @Value
  static class HttpError {
    @Getter @Nullable Integer code;
    @Getter @Nullable String message;
    @Getter @Nullable String details;

    @JsonCreator
    HttpError(
        @Nullable final Integer code,
        @Nullable final String message,
        @Nullable final String details) {
      this.code = code;
      this.message = message;
      this.details = details;
    }

    static HttpError of(final HttpResponse response) throws IOException {
      final String json = EntityUtils.toString(response.getEntity(), UTF_8);
      return fromJson(json);
    }

    static HttpError fromJson(final String json) {
      return Utils.fromJson(json, new TypeReference<HttpError>() {});
    }
  }
}
