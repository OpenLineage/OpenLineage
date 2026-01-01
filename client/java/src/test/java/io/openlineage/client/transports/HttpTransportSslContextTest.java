/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import java.net.URI;
import javax.net.ssl.HttpsURLConnection;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.logging.MockServerLogger;
import org.mockserver.model.ClearType;
import org.mockserver.model.HttpRequest;
import org.mockserver.socket.tls.KeyStoreFactory;
import org.slf4j.event.Level;

@Slf4j
class HttpTransportSslContextTest {

  private static final int MOCK_SERVER_PORT = 1087;
  public static final String API_V1_LINEAGE = "/api/v1/lineage";
  private static ClientAndServer mockServer;

  private static KeyStoreFactory keyStoreFactory;
  HttpConfig httpConfig = new HttpConfig();
  OpenLineage.RunEvent event =
      new OpenLineage(URI.create("http://test.producer")).newRunEventBuilder().build();

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    keyStoreFactory = new KeyStoreFactory(new MockServerLogger());

    Configuration config = Configuration.configuration();
    config.tlsMutualAuthenticationRequired(true);
    config.logLevel(Level.ERROR);

    // ensure all connection using HTTPS will use the SSL context defined by
    // MockServer to allow dynamically generated certificates to be accepted
    HttpsURLConnection.setDefaultSSLSocketFactory(keyStoreFactory.sslContext().getSocketFactory());

    mockServer = ClientAndServer.startClientAndServer(config, MOCK_SERVER_PORT);
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    mockServer
        .withSecure(true)
        .when(new HttpRequest().withSecure(true).withPath(API_V1_LINEAGE))
        .respond(response().withStatusCode(201));

    httpConfig.setUrl(new URI("https://localhost:" + mockServer.getPort()));
    httpConfig.setEndpoint(API_V1_LINEAGE);
    httpConfig.setSslContextConfig(
        new HttpSslContextConfig(
            KeyStoreFactory.KEY_STORE_PASSWORD,
            KeyStoreFactory.KEY_STORE_PASSWORD, // same password is set for keystore and truststore
            KeyStoreFactory.KEY_STORE_TYPE,
            keyStoreFactory.keyStoreFileName));
  }

  @AfterEach
  public void clear() {
    mockServer.clear(request(API_V1_LINEAGE), ClearType.LOG);
  }

  @AfterAll
  public static void stopMockServer() {
    mockServer.stop();
  }

  /**
   * This test is to ensure that the testing environment is set up correctly. Within the test we
   * instantiate Http Transport without SSL context, call MockServer and verify an exception is
   * thrown on the client side together with mock server logs containing a message about TLS
   * handshake failure.
   */
  @Test
  void smokeTestTestingEnvironment() {
    // clear SSL context from config
    httpConfig.setSslContextConfig(null);

    HttpTransport httpTransport = new HttpTransport(httpConfig);

    // make sure client exception is thrown
    assertThrows(OpenLineageClientException.class, () -> httpTransport.emit(event));

    // make sure logs contain handshake failure message
    assertThat(mockServer.retrieveLogMessages(request(API_V1_LINEAGE)))
        .contains("TLS handshake failure while a client attempted to connect to");
  }

  @Test
  @SneakyThrows
  void testSslContext() {
    HttpTransport httpTransport = new HttpTransport(httpConfig);
    httpTransport.emit(event);

    // make sure event is sent to mock server
    assertThat(mockServer.retrieveRecordedRequests(request(API_V1_LINEAGE)).length).isEqualTo(1);

    assertThat(mockServer.retrieveLogMessages(request(API_V1_LINEAGE)))
        .doesNotContain("TLS handshake failure");
  }
}
