/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.datasetEvent;
import static io.openlineage.client.Events.jobEvent;
import static io.openlineage.client.Events.runEvent;
import static java.util.Collections.singletonMap;
import static org.apache.hc.core5.http.ContentType.APPLICATION_JSON;
import static org.apache.hc.core5.http.HttpHeaders.ACCEPT;
import static org.apache.hc.core5.http.HttpHeaders.CONTENT_TYPE;
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
import lombok.SneakyThrows;
import org.apache.hc.client5.http.entity.GzipCompressingEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
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
    when(response.getCode()).thenReturn(200);

    when(http.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenReturn(response);

    client.emit(runEvent());

    verify(http, times(1)).execute(any(), any(HttpClientResponseHandler.class));
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
  @SneakyThrows
  void httpTransportDefaultEndpoint() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    ArgumentCaptor<ClassicHttpRequest> captor = ArgumentCaptor.forClass(ClassicHttpRequest.class);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getCode()).thenReturn(200);
    when(http.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenReturn(response);

    client.emit(runEvent());

    verify(http, times(1)).execute(captor.capture(), any(HttpClientResponseHandler.class));

    assertThat(captor.getValue().getUri())
        .isEqualTo(URI.create("https://localhost:1500/api/v1/lineage"));
  }

  @Test
  @SneakyThrows
  void httpTransportAcceptsExplicitEndpoint() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500"));
    config.setEndpoint("/");
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    ArgumentCaptor<ClassicHttpRequest> captor = ArgumentCaptor.forClass(ClassicHttpRequest.class);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getCode()).thenReturn(200);
    when(http.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenReturn(response);

    client.emit(runEvent());

    verify(http, times(1)).execute(captor.capture(), any(HttpClientResponseHandler.class));

    assertThat(captor.getValue().getUri()).isEqualTo(URI.create("https://localhost:1500/"));
  }

  @Test
  void httpTransportRaisesOn500() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getCode()).thenReturn(500);
    when(response.getEntity()).thenReturn(new StringEntity("whoops!", ContentType.TEXT_PLAIN));

    when(http.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenThrow(new HttpTransportResponseException(500, "whoops!"));

    HttpTransportResponseException thrown =
        assertThrows(HttpTransportResponseException.class, () -> client.emit(runEvent()));
    assertThat(thrown.getStatusCode()).isEqualTo(500);
    assertThat(thrown.getBody()).isEqualTo("whoops!");
    assertThat(thrown.getMessage()).contains("500");
    assertThat(thrown.getMessage()).contains("whoops!");

    verify(http, times(1)).execute(any(), any(HttpClientResponseHandler.class));
  }

  @Test
  void httpTransportRaisesOnConnectionFail() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("http://localhost:1500"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(http.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenThrow(new IOException("Connection failed"));

    assertThrows(OpenLineageClientException.class, () -> client.emit(runEvent()));

    verify(http, times(1)).execute(any(), any(HttpClientResponseHandler.class));
  }

  @Test
  void httpTransportBuilderRaisesOnBadUri() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    config.setEndpoint("/");
    assertThrows(OpenLineageClientException.class, () -> new HttpTransport(http, config));
  }

  @Test
  @SneakyThrows
  void httpTransportSendsAuthAndQueryParams() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500"));
    config.setUrlParams(singletonMap("param", "value"));
    ApiKeyTokenProvider auth = new ApiKeyTokenProvider();
    auth.setApiKey("apiKey");
    config.setAuth(auth);
    Transport transport = new HttpTransport(http, config);

    OpenLineageClient client = new OpenLineageClient(transport);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);

    when(response.getCode()).thenReturn(200);
    when(http.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenReturn(response);

    ArgumentCaptor<ClassicHttpRequest> captor = ArgumentCaptor.forClass(ClassicHttpRequest.class);

    client.emit(runEvent());

    verify(http, times(1)).execute(captor.capture(), any(HttpClientResponseHandler.class));

    assertThat(captor.getValue().getFirstHeader("Authorization").getValue())
        .isEqualTo("Bearer apiKey");
    assertThat(captor.getValue().getUri())
        .isEqualTo(URI.create("https://localhost:1500/api/v1/lineage?param=value"));
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
    when(response.getCode()).thenReturn(200);
    when(response.getEntity().isStreaming()).thenReturn(true);
    Map<String, ClassicHttpRequest> map = new HashMap<>();
    when(http.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              map.put("test", invocation.getArgument(0));
              return response;
            });

    client.emit(runEvent());
    Map<String, String> resultHeaders =
        Arrays.stream(map.get("test").getHeaders())
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
    when(response.getCode()).thenReturn(200);
    when(response.getEntity().isStreaming()).thenReturn(true);
    Map<String, ClassicHttpRequest> map = new HashMap<>();
    when(http.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
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
    when(response.getCode()).thenReturn(200);

    when(http.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenReturn(response);

    client.emit(datasetEvent());

    verify(http, times(1)).execute(any(), any(HttpClientResponseHandler.class));
  }

  @Test
  void clientEmitsJobEventHttpTransport() throws IOException {
    CloseableHttpClient http = mock(CloseableHttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getCode()).thenReturn(200);

    when(http.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenReturn(response);

    client.emit(jobEvent());

    verify(http, times(1)).execute(any(), any(HttpClientResponseHandler.class));
  }
}
