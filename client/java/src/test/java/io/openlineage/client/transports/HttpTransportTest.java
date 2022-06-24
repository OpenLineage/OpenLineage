/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.event;
import static org.assertj.core.api.Assertions.assertThat;
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
import java.net.URI;
import java.util.Collections;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class HttpTransportTest {

  @Test
  void clientEmitsHttpTransport() throws IOException {
    HttpClient http = mock(HttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getStatusLine().getStatusCode()).thenReturn(200);

    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    client.emit(event());

    verify(http, times(1)).execute(any());
  }

  @Test
  void httpTransportRaisesOn500() throws IOException {
    HttpClient http = mock(HttpClient.class);
    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create("https://localhost:1500/api/v1/lineage"));
    Transport transport = new HttpTransport(http, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);
    when(response.getStatusLine().getStatusCode()).thenReturn(500);
    when(response.getEntity()).thenReturn(mock(HttpEntity.class));

    when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

    assertThrows(OpenLineageClientException.class, () -> client.emit(event()));

    verify(http, times(1)).execute(any());
  }

  @Test
  void httpTransportRaisesOnConnectionFail() throws IOException {
    HttpClient http = mock(HttpClient.class);
    Transport transport = HttpTransport.builder().uri("http://localhost:1500").http(http).build();
    OpenLineageClient client = new OpenLineageClient(transport);

    when(http.execute(any(HttpUriRequest.class))).thenThrow(new IOException(""));

    assertThrows(OpenLineageClientException.class, () -> client.emit(event()));

    verify(http, times(1)).execute(any());
  }

  @Test
  void httpTransportBuilderRaisesOnBadUri() throws IOException {
    HttpClient http = mock(HttpClient.class);
    HttpTransport.Builder builder = HttpTransport.builder().http(http);
    assertThrows(OpenLineageClientException.class, () -> builder.uri("!http://localhost:1500!"));
  }

  @Test
  void httpTransportSendsAuthAndQueryParams() throws IOException {
    HttpClient http = mock(HttpClient.class);
    Transport transport =
        HttpTransport.builder()
            .uri("http://localhost:1500", Collections.singletonMap("param", "value"))
            .http(http)
            .apiKey("apiKey")
            .build();

    OpenLineageClient client = new OpenLineageClient(transport);
    HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);

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
}
