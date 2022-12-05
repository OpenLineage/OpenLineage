/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.event;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class HttpTransportTest {

  @Test
  void transportCreatedWithHttpConfig() {
    HttpConfig httpConfig = new HttpConfig();
    try {
      httpConfig.setUrl(new URI("http://localhost:5000"));
      httpConfig.setEndpoint("/api/v1/lineage");
      httpConfig.setTimeout(5000.0);
      httpConfig.setUrlParams(singletonMap("param", "value"));
      ApiKeyTokenProvider auth = new ApiKeyTokenProvider();
      auth.setApiKey("test");
      httpConfig.setAuth(auth);
      HttpTransport httpTransport = new HttpTransport(httpConfig);
      Field uri = httpTransport.getClass().getDeclaredField("uri");
      uri.setAccessible(true);
      String target = uri.get(httpTransport).toString();
      assertEquals("http://localhost:5000/api/v1/lineage?param=value", target);

    } catch (URISyntaxException | NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void clientEmitsHttpTransport() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getStatusLine().getStatusCode()).thenReturn(200);

    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    client.emit(event());

    verify(http, times(1)).execute(any());
  }

  @Test
  void httpTransportRaisesOnBothUriAndEndpoint() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    config.setEndpoint("/");
    assertThrows(OpenLineageClientException.class, () -> new HttpTransport(http, config));
  }

  @Test
  void httpTransportDefaultEndpoint() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    ArgumentCaptor<HttpUriRequest> captor = ArgumentCaptor.forClass(HttpUriRequest.class);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(200);
    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    client.emit(event());

    verify(http, times(1)).execute(captor.capture());

    assertThat(captor.getValue().getURI())
        .isEqualTo(URI.create("https://localhost:1500/api/v1/lineage"));
  }

  @Test
  void httpTransportAcceptsExplicitEndpoint() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500"));
    config.setEndpoint("/");
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    ArgumentCaptor<HttpUriRequest> captor = ArgumentCaptor.forClass(HttpUriRequest.class);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(200);
    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    client.emit(event());

    verify(http, times(1)).execute(captor.capture());

    assertThat(captor.getValue().getURI()).isEqualTo(URI.create("https://localhost:1500/"));
  }

  @Test
  void httpTransportRaisesOn500() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getStatusLine().getStatusCode()).thenReturn(500);
    when(response.getEntity()).thenReturn(mock(HttpEntity.class));

    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    assertThrows(OpenLineageClientException.class, () -> client.emit(event()));

    verify(http, times(1)).execute(any());
  }

  @Test
  void httpTransportRaisesOnConnectionFail() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    Transport transport = HttpTransport.builder().uri("http://localhost:1500").http(http).build();
    OpenLineageClient client = new OpenLineageClient(transport);

    when(http.execute(any(HttpUriRequest.class))).thenThrow(new IOException(""));

    assertThrows(OpenLineageClientException.class, () -> client.emit(event()));

    verify(http, times(1)).execute(any());
  }

  @Test
  void httpTransportBuilderRaisesOnBadUri() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpTransport.Builder builder = HttpTransport.builder().http(http);
    assertThrows(OpenLineageClientException.class, () -> builder.uri("!http://localhost:1500!"));
  }

  @Test
  void httpTransportSendsAuthAndQueryParams() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    Transport transport =
        HttpTransport.builder()
            .uri("http://localhost:1500", singletonMap("param", "value"))
            .http(http)
            .apiKey("apiKey")
            .build();

    OpenLineageClient client = new OpenLineageClient(transport);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getStatusLine().getStatusCode()).thenReturn(200);
    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    ArgumentCaptor<HttpUriRequest> captor = ArgumentCaptor.forClass(HttpUriRequest.class);

    client.emit(event());

    verify(http, times(1)).execute(captor.capture());

    assertThat(captor.getValue().getFirstHeader("Authorization").getValue())
        .isEqualTo("Bearer apiKey");
    assertThat(captor.getValue().getURI())
        .isEqualTo(URI.create("http://localhost:1500/api/v1/lineage?param=value"));
  }

  @Test
  void clientClosesNetworkResources() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getStatusLine().getStatusCode()).thenReturn(200);
    when(response.getEntity().isStreaming()).thenReturn(true);

    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    client.emit(event());

    verify(response, times(1)).close();
    verify(response.getEntity().getContent(), times(1)).close();
  }
}
