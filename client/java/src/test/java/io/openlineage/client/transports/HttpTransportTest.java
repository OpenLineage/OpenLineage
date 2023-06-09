/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.datasetEvent;
import static io.openlineage.client.Events.jobEvent;
import static io.openlineage.client.Events.runEvent;
import static java.util.Collections.singletonMap;
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class HttpTransportTest {

  @Test
  void transportCreatedWithHttpConfig()
      throws URISyntaxException, NoSuchFieldException, IllegalAccessException {
    HttpConfig httpConfig = new HttpConfig();
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

    client.emit(runEvent());

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

    client.emit(runEvent());

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

    client.emit(runEvent());

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

    assertThrows(OpenLineageClientException.class, () -> client.emit(runEvent()));

    verify(http, times(1)).execute(any());
  }

  @Test
  void httpTransportRaisesOnConnectionFail() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    Transport transport = HttpTransport.builder().uri("http://localhost:1500").http(http).build();
    OpenLineageClient client = new OpenLineageClient(transport);

    when(http.execute(any(HttpUriRequest.class))).thenThrow(new IOException(""));

    assertThrows(OpenLineageClientException.class, () -> client.emit(runEvent()));

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

    client.emit(runEvent());

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

    client.emit(runEvent());

    verify(response, times(1)).close();
    verify(response.getEntity().getContent(), times(1)).close();
  }

  @Test
  void customHeaders() throws IOException {
    HashMap<String, String> headers = new HashMap<>();
    headers.put(ACCEPT, "not-application/json");
    headers.put(CONTENT_TYPE, "not-application/json");
    headers.put("testHeader1", "test1");
    headers.put("testHeader2", "test2");

    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    config.setHeaders(headers);

    CloseableHttpClient http = mock(CloseableHttpClient.class);
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getStatusLine().getStatusCode()).thenReturn(200);
    when(response.getEntity().isStreaming()).thenReturn(true);
    Map<String, HttpPost> map = new HashMap<>();
    when(http.execute(any(HttpUriRequest.class)))
        .thenAnswer(
            invocation -> {
              map.put("test", invocation.getArgument(0));
              return response;
            });

    client.emit(runEvent());
    Map<String, String> resultHeaders =
        Arrays.stream(map.get("test").getAllHeaders())
            .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));
    assertThat(resultHeaders)
        .containsEntry(ACCEPT, APPLICATION_JSON.toString())
        .containsEntry(CONTENT_TYPE, APPLICATION_JSON.toString())
        .containsEntry("testHeader1", "test1")
        .containsEntry("testHeader2", "test2");
  }

  @Test
  void clientEmitsDatasetEventHttpTransport() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getStatusLine().getStatusCode()).thenReturn(200);

    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    client.emit(datasetEvent());

    verify(http, times(1)).execute(any());
  }

  @Test
  void clientEmitsJobEventHttpTransport() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getStatusLine().getStatusCode()).thenReturn(200);

    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    client.emit(jobEvent());

    verify(http, times(1)).execute(any());
  }
}
