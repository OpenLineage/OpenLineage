/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class OAuth2ClientCredentialsTokenProviderTest {

  private static final URI TOKEN_ENDPOINT = URI.create("https://auth.example.com/token");
  private static final String TOKEN_RESPONSE =
      "{\"access_token\": \"access-token-value\", \"token_type\": \"Bearer\", \"expires_in\": 600}";

  private TestableOAuth2ClientCredentialsTokenProvider provider;
  private CloseableHttpClient mockHttpClient;

  @BeforeEach
  void setUp() {
    mockHttpClient = mock(CloseableHttpClient.class);
    provider =
        new TestableOAuth2ClientCredentialsTokenProvider(
            "test-client-id", "test-client-secret", TOKEN_ENDPOINT, mockHttpClient);
  }

  /** Testable subclass that allows injecting a mock HTTP client. */
  private static class TestableOAuth2ClientCredentialsTokenProvider
      extends OAuth2ClientCredentialsTokenProvider {
    private final CloseableHttpClient mockClient;

    TestableOAuth2ClientCredentialsTokenProvider(
        String clientId, String clientSecret, URI tokenEndpoint, CloseableHttpClient mockClient) {
      super(clientId, clientSecret, tokenEndpoint);
      this.mockClient = mockClient;
    }

    @Override
    protected CloseableHttpClient createHttpClient() {
      return mockClient;
    }
  }

  private void mockTokenEndpoint(int statusCode, String... responseBodies) throws IOException {
    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(statusCode);
    HttpEntity[] entities =
        Arrays.stream(responseBodies)
            .map(body -> new StringEntity(body, ContentType.APPLICATION_JSON))
            .toArray(HttpEntity[]::new);
    when(mockHttpResponse.getEntity())
        .thenReturn(entities[0], Arrays.copyOfRange(entities, 1, entities.length));
    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });
  }

  private HttpPost capturedTokenRequest() throws IOException {
    ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor.forClass(HttpPost.class);
    verify(mockHttpClient).execute(requestCaptor.capture(), any(HttpClientResponseHandler.class));
    return requestCaptor.getValue();
  }

  @Test
  void testGetTokenSendsClientCredentialsInAuthorizationHeader() throws Exception {
    mockTokenEndpoint(200, TOKEN_RESPONSE);

    String token = provider.getToken();

    assertThat(token).isEqualTo("Bearer access-token-value");
    HttpPost request = capturedTokenRequest();
    String expectedCredentials =
        Base64.getEncoder()
            .encodeToString("test-client-id:test-client-secret".getBytes(StandardCharsets.UTF_8));
    assertThat(request.getUri()).isEqualTo(TOKEN_ENDPOINT);
    assertThat(request.getHeader("Authorization").getValue())
        .isEqualTo("Basic " + expectedCredentials);
    assertThat(EntityUtils.toString(request.getEntity()))
        .isEqualTo("grant_type=client_credentials");
  }

  @Test
  void testGetTokenSendsClientCredentialsInBody() throws Exception {
    provider.setClientAuthMethod(OAuth2ClientCredentialsTokenProvider.CLIENT_SECRET_POST);
    provider.setScope("openid");
    mockTokenEndpoint(200, TOKEN_RESPONSE);

    String token = provider.getToken();

    assertThat(token).isEqualTo("Bearer access-token-value");
    HttpPost request = capturedTokenRequest();
    assertThat(request.getHeader("Authorization")).isNull();
    assertThat(EntityUtils.toString(request.getEntity()))
        .isEqualTo(
            "grant_type=client_credentials&scope=openid"
                + "&client_id=test-client-id&client_secret=test-client-secret");
  }

  @Test
  void testGetTokenCachesToken() throws IOException {
    mockTokenEndpoint(200, TOKEN_RESPONSE);

    assertThat(provider.getToken()).isEqualTo("Bearer access-token-value");
    assertThat(provider.getToken()).isEqualTo("Bearer access-token-value");

    verify(mockHttpClient, times(1))
        .execute(any(HttpPost.class), any(HttpClientResponseHandler.class));
  }

  @Test
  void testGetTokenRefreshesTokenBeforeExpiry() throws IOException {
    TestableOAuth2ClientCredentialsTokenProvider spyProvider = spy(provider);
    mockTokenEndpoint(
        200,
        "{\"access_token\": \"token-1\", \"expires_in\": 600}",
        "{\"access_token\": \"token-2\", \"expires_in\": 600}");

    long initialTime = 1000000L;
    doReturn(initialTime).when(spyProvider).getCurrentTimeSeconds();
    assertThat(spyProvider.getToken()).isEqualTo("Bearer token-1");

    // Token expires at 600, default buffer is 120: still cached at 479, refreshed at 480
    doReturn(initialTime + 479).when(spyProvider).getCurrentTimeSeconds();
    assertThat(spyProvider.getToken()).isEqualTo("Bearer token-1");
    verify(mockHttpClient, times(1))
        .execute(any(HttpPost.class), any(HttpClientResponseHandler.class));

    doReturn(initialTime + 480).when(spyProvider).getCurrentTimeSeconds();
    assertThat(spyProvider.getToken()).isEqualTo("Bearer token-2");
    verify(mockHttpClient, times(2))
        .execute(any(HttpPost.class), any(HttpClientResponseHandler.class));
  }

  @Test
  void testGetTokenThrowsOnHttpError() throws IOException {
    mockTokenEndpoint(401, "{\"error\": \"invalid_client\"}");

    assertThatThrownBy(() -> provider.getToken())
        .isInstanceOf(OpenLineageClientException.class)
        .hasMessageContaining("Failed to fetch OAuth2 access token")
        .hasRootCauseMessage(
            "OAuth2 access token endpoint returned status 401: {\"error\": \"invalid_client\"}");
  }

  @Test
  void testGetTokenThrowsOnMissingAccessToken() throws IOException {
    mockTokenEndpoint(200, "{\"token_type\": \"Bearer\"}");

    assertThatThrownBy(() -> provider.getToken())
        .isInstanceOf(OpenLineageClientException.class)
        .hasMessageContaining("Failed to fetch OAuth2 access token")
        .hasRootCauseMessage(
            "OAuth2 access token field '[access_token]' not found or invalid in response");
  }

  @Test
  void testConstructorRequiresClientIdClientSecretAndTokenEndpoint() {
    assertThatThrownBy(() -> new OAuth2ClientCredentialsTokenProvider("", "secret", TOKEN_ENDPOINT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("clientId");
    assertThatThrownBy(() -> new OAuth2ClientCredentialsTokenProvider("id", null, TOKEN_ENDPOINT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("clientSecret");
    assertThatThrownBy(() -> new OAuth2ClientCredentialsTokenProvider("id", "secret", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tokenEndpoint");
  }

  @Test
  void testRejectsUnknownClientAuthMethod() {
    assertThatThrownBy(() -> provider.setClientAuthMethod("private_key_jwt"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("clientAuthMethod");
  }

  @Test
  void testToStringDoesNotExposeClientSecret() {
    assertThat(provider.toString()).contains("test-client-id").doesNotContain("test-client-secret");
  }

  @Test
  void testLoadOAuth2ClientCredentialsAuthFromYamlConfig() {
    String yaml =
        String.join(
            "\n",
            "transport:",
            "  type: http",
            "  url: http://backend:5000",
            "  auth:",
            "    type: oauth2",
            "    clientId: yaml-client-id",
            "    clientSecret: yaml-client-secret",
            "    tokenEndpoint: https://auth.example.com/token",
            "    clientAuthMethod: client_secret_post",
            "    scope: openid",
            "    tokenRefreshBuffer: 180",
            "");

    OpenLineageConfig config =
        OpenLineageClientUtils.loadOpenLineageConfigYaml(
            new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)),
            new TypeReference<OpenLineageConfig>() {});

    assertThat(config.getTransportConfig()).isInstanceOf(HttpConfig.class);
    HttpConfig httpConfig = (HttpConfig) config.getTransportConfig();
    assertThat(httpConfig.getAuth()).isInstanceOf(OAuth2ClientCredentialsTokenProvider.class);
    OAuth2ClientCredentialsTokenProvider auth =
        (OAuth2ClientCredentialsTokenProvider) httpConfig.getAuth();
    assertThat(auth.getClientId()).isEqualTo("yaml-client-id");
    assertThat(auth.getClientSecret()).isEqualTo("yaml-client-secret");
    assertThat(auth.getTokenEndpoint()).isEqualTo(TOKEN_ENDPOINT);
    assertThat(auth.getClientAuthMethod())
        .isEqualTo(OAuth2ClientCredentialsTokenProvider.CLIENT_SECRET_POST);
    assertThat(auth.getScope()).isEqualTo("openid");
    assertThat(auth.getTokenRefreshBuffer()).isEqualTo(180);
    assertThat(auth.getTokenFields()).containsExactly("access_token");
  }

  @Test
  void testUrlEncodesClientCredentialsInAuthorizationHeader() throws Exception {
    TestableOAuth2ClientCredentialsTokenProvider specialProvider =
        new TestableOAuth2ClientCredentialsTokenProvider(
            "id+with/special", "secret=with&special?chars", TOKEN_ENDPOINT, mockHttpClient);
    mockTokenEndpoint(200, TOKEN_RESPONSE);

    specialProvider.getToken();

    String expectedCredentials =
        Base64.getEncoder()
            .encodeToString(
                "id%2Bwith%2Fspecial:secret%3Dwith%26special%3Fchars"
                    .getBytes(StandardCharsets.UTF_8));
    assertThat(capturedTokenRequest().getHeader("Authorization").getValue())
        .isEqualTo("Basic " + expectedCredentials);
  }

  @Test
  void testLoadOAuth2ClientCredentialsAuthFromEnvironmentVariables() throws Exception {
    Map<String, String> envVars = new HashMap<>();
    envVars.put("OPENLINEAGE__TRANSPORT__TYPE", "http");
    envVars.put("OPENLINEAGE__TRANSPORT__URL", "http://backend:5000");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TYPE", "oauth2");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__CLIENT_ID", "env-client-id");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__CLIENT_SECRET", "env-client-secret");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TOKEN_ENDPOINT", TOKEN_ENDPOINT.toString());
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__CLIENT_AUTH_METHOD", "client_secret_post");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__SCOPE", "openid");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TOKEN_REFRESH_BUFFER", "180");

    setEnvironmentVariables(envVars);

    try {
      OpenLineageConfig config =
          OpenLineageClientUtils.loadOpenLineageConfigFromEnvVars(
              new TypeReference<OpenLineageConfig>() {});

      assertThat(config.getTransportConfig()).isInstanceOf(HttpConfig.class);
      HttpConfig httpConfig = (HttpConfig) config.getTransportConfig();
      assertThat(httpConfig.getAuth()).isInstanceOf(OAuth2ClientCredentialsTokenProvider.class);
      OAuth2ClientCredentialsTokenProvider auth =
          (OAuth2ClientCredentialsTokenProvider) httpConfig.getAuth();

      assertThat(auth.getClientId()).isEqualTo("env-client-id");
      assertThat(auth.getClientSecret()).isEqualTo("env-client-secret");
      assertThat(auth.getTokenEndpoint()).isEqualTo(TOKEN_ENDPOINT);
      assertThat(auth.getClientAuthMethod())
          .isEqualTo(OAuth2ClientCredentialsTokenProvider.CLIENT_SECRET_POST);
      assertThat(auth.getScope()).isEqualTo("openid");
      assertThat(auth.getTokenRefreshBuffer()).isEqualTo(180);
    } finally {
      clearEnvironmentVariables(envVars.keySet());
    }
  }

  @Test
  void testLoadOAuth2ClientCredentialsAuthFromInvalidEnvironmentVariablesFails() throws Exception {
    Map<String, String> envVars = new HashMap<>();
    envVars.put("OPENLINEAGE__TRANSPORT__TYPE", "http");
    envVars.put("OPENLINEAGE__TRANSPORT__URL", "http://backend:5000");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TYPE", "oauth2");
    // Without underscores the keys stay lowercase and do not map onto the provider properties
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__CLIENTID", "env-client-id");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__CLIENTSECRET", "env-client-secret");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TOKENENDPOINT", TOKEN_ENDPOINT.toString());

    setEnvironmentVariables(envVars);

    try {
      assertThatThrownBy(
              () ->
                  OpenLineageClientUtils.loadOpenLineageConfigFromEnvVars(
                      new TypeReference<OpenLineageConfig>() {}))
          .isInstanceOf(OpenLineageClientException.class)
          .rootCause()
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("clientId must not be null or empty");
    } finally {
      clearEnvironmentVariables(envVars.keySet());
    }
  }

  /**
   * Helper method to set environment variables using reflection. This is a workaround for the
   * immutable System.getenv() Map.
   */
  @SuppressWarnings({"unchecked", "PMD"})
  private void setEnvironmentVariables(Map<String, String> newEnv) throws Exception {
    Class<?> classOfMap = System.getenv().getClass();
    Field field = classOfMap.getDeclaredField("m");
    field.setAccessible(true);
    Map<String, String> writeableEnvironmentVariables =
        (Map<String, String>) field.get(System.getenv());
    writeableEnvironmentVariables.putAll(newEnv);
  }

  /** Helper method to clear environment variables using reflection. */
  @SuppressWarnings({"unchecked", "PMD"})
  private void clearEnvironmentVariables(Set<String> keys) throws Exception {
    Class<?> classOfMap = System.getenv().getClass();
    Field field = classOfMap.getDeclaredField("m");
    field.setAccessible(true);
    Map<String, String> writeableEnvironmentVariables =
        (Map<String, String>) field.get(System.getenv());
    keys.forEach(writeableEnvironmentVariables::remove);
  }
}
