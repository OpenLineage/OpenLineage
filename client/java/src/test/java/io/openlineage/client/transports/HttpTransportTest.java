/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.*;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class HttpTransportTest {

  @Test
  @SuppressWarnings("PMD")
  void transportCreatedWithHttpConfig()
      throws URISyntaxException, NoSuchFieldException, IllegalAccessException {
    HttpConfig httpConfig = new HttpConfig();
    httpConfig.setUrl(new URI("http://localhost:5000"));
    httpConfig.setEndpoint("/api/v1/lineage");
    httpConfig.setTimeout(5000.0);
    httpConfig.setUrlParams(singletonMap("param", "value"));
    httpConfig.setCompression(HttpConfig.Compression.GZIP);
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
    when(response.getEntity()).thenReturn(new StringEntity("whoops!", ContentType.TEXT_PLAIN));

    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    HttpTransportResponseException thrown =
        assertThrows(HttpTransportResponseException.class, () -> client.emit(runEvent()));
    assertThat(thrown.getStatusCode()).isEqualTo(500);
    assertThat(thrown.getBody()).isEqualTo("whoops!");
    assertThat(thrown.getMessage()).contains("500");
    assertThat(thrown.getMessage()).contains("whoops!");

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
  void gzipCompression() throws IOException {
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    config.setCompression(HttpConfig.Compression.GZIP);

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

    HttpEntity resultEntity = map.get("test").getEntity();
    assertThat(resultEntity).isInstanceOf(GzipCompressingEntity.class);

    GzipCompressingEntity gzippedEntity = (GzipCompressingEntity) resultEntity;
    ByteArrayOutputStream compressedData = new ByteArrayOutputStream();
    gzippedEntity.writeTo(compressedData);

    GZIPInputStream uncompressedStream =
        new GZIPInputStream(new ByteArrayInputStream(compressedData.toByteArray()));
    ByteArrayOutputStream uncompressedData = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int nRead = uncompressedStream.read(buffer, 0, buffer.length);
    while (nRead != -1) {
      uncompressedData.write(buffer, 0, nRead);
      nRead = uncompressedStream.read(buffer, 0, buffer.length);
    }
    uncompressedData.flush();

    String body = new String(uncompressedData.toByteArray(), "UTF-8");
    assertThat(body)
        .isEqualTo(
            "{\"producer\":\"http://test.producer\",\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent\",\"run\":{\"runId\":\"ea445b5c-22eb-457a-8007-01c7c52b6e54\"},\"job\":{\"namespace\":\"test-namespace\",\"name\":\"test-job\"}}");
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

  @Test
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void testTimeout() {
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    config.setTimeout(2.5d); // 2.5 seconds

    Builder builder = mock(Builder.class);
    try (MockedStatic mocked = mockStatic(RequestConfig.class)) {
      when(RequestConfig.custom()).thenReturn(builder);
      when(builder.setConnectTimeout(2500)).thenReturn(builder);
      when(builder.setConnectionRequestTimeout(2500)).thenReturn(builder);
      when(builder.setSocketTimeout(2500)).thenReturn(builder);

      new HttpTransport(config);
    }
  }

  @Test
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void testTimeoutInMillis() {
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    config.setTimeoutInMillis(3000); // 3 seconds

    Builder builder = mock(Builder.class);
    try (MockedStatic mocked = mockStatic(RequestConfig.class)) {
      when(RequestConfig.custom()).thenReturn(builder);
      when(builder.setConnectTimeout(3000)).thenReturn(builder);
      when(builder.setConnectionRequestTimeout(3000)).thenReturn(builder);
      when(builder.setSocketTimeout(3000)).thenReturn(builder);

      new HttpTransport(config);
    }
  }
}
