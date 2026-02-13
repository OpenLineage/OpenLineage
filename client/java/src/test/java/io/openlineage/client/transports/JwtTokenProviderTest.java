/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageConfig;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class JwtTokenProviderTest {

  private TestableJwtTokenProvider provider;
  private CloseableHttpClient mockHttpClient;

  @BeforeEach
  void setUp() {
    mockHttpClient = mock(CloseableHttpClient.class);
    provider =
        new TestableJwtTokenProvider(
            "test-api-key", URI.create("https://auth.example.com/token"), mockHttpClient);
  }

  /**
   * Helper method to set environment variables using reflection. This is a workaround for the
   * immutable System.getenv() Map.
   */
  @SuppressWarnings({"unchecked", "PMD"}) // Suppress warnings for reflection and PMD
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

  /** Testable subclass that allows injecting a mock HTTP client. */
  private static class TestableJwtTokenProvider extends JwtTokenProvider {
    private final CloseableHttpClient mockClient;

    TestableJwtTokenProvider(String apiKey, URI tokenEndpoint, CloseableHttpClient mockClient) {
      super(apiKey, tokenEndpoint);
      this.mockClient = mockClient;
    }

    @Override
    protected CloseableHttpClient createHttpClient() {
      return mockClient;
    }
  }

  @Test
  void testGetTokenFetchesJwtFromEndpoint() throws IOException {
    // Mock response with JWT token
    String mockResponse =
        "{\"token\": \"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiZXhwIjoxNzM1NTc4MDAwfQ.signature\", \"expiresIn\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    // Inject mock client via reflection

    String token = provider.getToken();

    assertThat(token).startsWith("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9");
    verify(mockHttpClient, times(1))
        .execute(any(HttpPost.class), any(HttpClientResponseHandler.class));
  }

  @Test
  void testGetTokenCachesToken() throws IOException {
    // Mock response with JWT token and expiry
    String mockResponse = "{\"token\": \"test-jwt-token\", \"expiresIn\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    // First call should fetch token
    String token1 = provider.getToken();
    assertThat(token1).isEqualTo("Bearer test-jwt-token");

    // Second call should use cached token
    String token2 = provider.getToken();
    assertThat(token2).isEqualTo("Bearer test-jwt-token");

    // Should only call the endpoint once
    verify(mockHttpClient, times(1))
        .execute(any(HttpPost.class), any(HttpClientResponseHandler.class));
  }

  @Test
  void testGetTokenRefreshesExpiredToken() throws IOException {
    // Create a spy to mock time progression
    TestableJwtTokenProvider spyProvider = spy(provider);

    // Mock response with token that expires in 3600 seconds
    String mockResponse = "{\"token\": \"test-jwt-token\", \"expiresIn\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    // Mock current time at T=0
    long initialTime = 1000000L;
    doReturn(initialTime).when(spyProvider).getCurrentTimeSeconds();

    // First call - should fetch token
    spyProvider.getToken();

    // Mock time progression to after token expiry (3600 - 120 buffer + 1 = 3481 seconds later)
    doReturn(initialTime + 3481).when(spyProvider).getCurrentTimeSeconds();

    // Second call should refresh because token is within buffer time
    spyProvider.getToken();

    // Should call endpoint twice
    verify(mockHttpClient, times(2))
        .execute(any(HttpPost.class), any(HttpClientResponseHandler.class));
  }

  @Test
  void testGetTokenRespectsCustomRefreshBuffer() throws IOException {
    // Create a spy to mock time progression
    TestableJwtTokenProvider spyProvider = spy(provider);
    spyProvider.setTokenRefreshBuffer(300); // Set custom 300 second buffer

    // Mock response with token that expires in 3600 seconds
    String mockResponse = "{\"token\": \"test-jwt-token\", \"expiresIn\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    // Mock current time at T=0
    long initialTime = 1000000L;
    doReturn(initialTime).when(spyProvider).getCurrentTimeSeconds();

    // First call - should fetch token
    spyProvider.getToken();

    // Mock time at 3200 seconds (within 300s buffer but outside 120s default buffer)
    // Token expires at 3600, so at 3200 we're 400s before expiry
    // With 300s buffer, token is still valid (400 > 300)
    doReturn(initialTime + 3200).when(spyProvider).getCurrentTimeSeconds();

    // Should still use cached token
    spyProvider.getToken();
    // Should only call endpoint once
    verify(mockHttpClient, times(1))
        .execute(any(HttpPost.class), any(HttpClientResponseHandler.class));

    // Mock time at 3301 seconds (within 300s buffer)
    // Token expires at 3600, so at 3301 we're 299s before expiry
    // With 300s buffer, token should be refreshed (299 < 300)
    doReturn(initialTime + 3301).when(spyProvider).getCurrentTimeSeconds();

    // Should refresh token
    spyProvider.getToken();
    // Should call endpoint twice now
    verify(mockHttpClient, times(2))
        .execute(any(HttpPost.class), any(HttpClientResponseHandler.class));
  }

  @Test
  void testGetTokenWithCustomFields() throws IOException {
    provider.setTokenFields(new String[] {"access_token"});
    provider.setExpiresInField("expires_in");

    String mockResponse = "{\"access_token\": \"custom-jwt-token\", \"expires_in\": 7200}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    String token = provider.getToken();

    assertThat(token).isEqualTo("Bearer custom-jwt-token");
  }

  @Test
  void testGetTokenWithUrlEncodedParameters() throws Exception {
    String mockResponse = "{\"token\": \"url-encoded-jwt-token\", \"expiresIn\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor.forClass(HttpPost.class);

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    provider.getToken();

    verify(mockHttpClient).execute(requestCaptor.capture(), any(HttpClientResponseHandler.class));
    HttpPost request = requestCaptor.getValue();

    // Verify Content-Type is application/x-www-form-urlencoded
    assertThat(request.getFirstHeader("Content-Type").getValue())
        .contains("application/x-www-form-urlencoded");

    // Verify request body contains URL-encoded parameters with new defaults
    String requestBody = EntityUtils.toString(request.getEntity());
    assertThat(requestBody).contains("apikey=test-api-key");
    assertThat(requestBody)
        .contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer");
    assertThat(requestBody).contains("response_type=token");
  }

  @Test
  void testGetTokenWithCustomGrantType() throws Exception {
    provider.setGrantType("custom_grant_type");

    String mockResponse = "{\"token\": \"custom-grant-jwt-token\", \"expiresIn\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor.forClass(HttpPost.class);

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    provider.getToken();

    verify(mockHttpClient).execute(requestCaptor.capture(), any(HttpClientResponseHandler.class));
    HttpPost request = requestCaptor.getValue();

    String requestBody = EntityUtils.toString(request.getEntity());
    assertThat(requestBody).contains("grant_type=custom_grant_type");
  }

  @Test
  void testGetTokenWithCustomResponseType() throws Exception {
    provider.setResponseType("custom_response");

    String mockResponse = "{\"token\": \"custom-response-jwt-token\", \"expiresIn\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor.forClass(HttpPost.class);

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    provider.getToken();

    verify(mockHttpClient).execute(requestCaptor.capture(), any(HttpClientResponseHandler.class));
    HttpPost request = requestCaptor.getValue();

    String requestBody = EntityUtils.toString(request.getEntity());
    assertThat(requestBody).contains("response_type=custom_response");
  }

  @Test
  void testUrlEncodingOfSpecialCharacters() throws Exception {
    // Create a new provider with special characters in the API key
    provider =
        new TestableJwtTokenProvider(
            "test+key=with&special?chars",
            URI.create("https://auth.example.com/token"),
            mockHttpClient);

    String mockResponse = "{\"token\": \"encoded-jwt-token\", \"expiresIn\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor.forClass(HttpPost.class);

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    provider.getToken();

    verify(mockHttpClient).execute(requestCaptor.capture(), any(HttpClientResponseHandler.class));
    HttpPost request = requestCaptor.getValue();

    String requestBody = EntityUtils.toString(request.getEntity());
    // Verify special characters are properly URL-encoded
    assertThat(requestBody).contains("apikey=test%2Bkey%3Dwith%26special%3Fchars");
  }

  @Test
  void testGetTokenThrowsOnHttpError() throws IOException {
    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(401);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity("Unauthorized", ContentType.TEXT_PLAIN));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    assertThrows(OpenLineageClientException.class, () -> provider.getToken());
  }

  @Test
  void testGetTokenThrowsOnMissingTokenField() throws IOException {
    // With multi-key support, this should now succeed by finding "access_token"
    // To test failure, use a response with no valid token fields
    String mockResponse = "{\"some_other_field\": \"jwt-token\"}"; // No valid token field

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    assertThrows(OpenLineageClientException.class, () -> provider.getToken());
  }

  @Test
  void testGetTokenExtractsExpiryFromJwt() throws IOException {
    // Create a JWT with exp claim set to 1 hour from now
    long expiry = Instant.now().getEpochSecond() + 3600;
    String payload = String.format("{\"sub\":\"test\",\"exp\":%d}", expiry);
    String encodedPayload =
        Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes());
    String jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." + encodedPayload + ".signature";

    // Response without expiresIn field
    String mockResponse = String.format("{\"token\": \"%s\"}", jwt);

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    // First call should fetch and cache
    String token1 = provider.getToken();
    assertThat(token1).startsWith("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9");

    // Second call should use cache (token not expired)
    String token2 = provider.getToken();
    assertThat(token2).isEqualTo(token1);

    // Should only call endpoint once
    verify(mockHttpClient, times(1))
        .execute(any(HttpPost.class), any(HttpClientResponseHandler.class));
  }

  @Test
  void testGetTokenWithMultipleFieldNameSupport() throws IOException {
    // Test that provider can find token with "token" field when it's in the tokenFields array
    provider.setTokenFields(new String[] {"access_token", "token"});

    String mockResponse = "{\"token\": \"fallback-jwt-token\", \"expires_in\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    String token = provider.getToken();

    assertThat(token).isEqualTo("Bearer fallback-jwt-token");
  }

  @Test
  void testGetTokenPrefersConfiguredFieldName() throws IOException {
    // Test that configured field name takes precedence (first in array)
    provider.setTokenFields(new String[] {"custom_token", "token", "access_token"});

    String mockResponse =
        "{\"custom_token\": \"custom-jwt\", \"token\": \"fallback-jwt\", \"access_token\": \"another-jwt\"}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    String token = provider.getToken();

    // Should use the configured field name first
    assertThat(token).isEqualTo("Bearer custom-jwt");
  }

  @Test
  void testGetTokenFallsBackToAccessToken() throws IOException {
    // Test fallback to "access_token" when first field not found
    provider.setTokenFields(new String[] {"missing_field", "access_token"});

    String mockResponse = "{\"access_token\": \"access-jwt-token\", \"expires_in\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    String token = provider.getToken();

    assertThat(token).isEqualTo("Bearer access-jwt-token");
  }

  @Test
  void testGetTokenFallsBackToToken() throws IOException {
    // Test fallback to "token" when other fields not found
    provider.setTokenFields(new String[] {"missing_field", "token"});

    String mockResponse = "{\"token\": \"token-jwt-value\", \"expires_in\": 3600}";

    ClassicHttpResponse mockHttpResponse = mock(ClassicHttpResponse.class);
    when(mockHttpResponse.getCode()).thenReturn(200);
    when(mockHttpResponse.getEntity())
        .thenReturn(new StringEntity(mockResponse, ContentType.APPLICATION_JSON));

    when(mockHttpClient.execute(any(HttpPost.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpClientResponseHandler handler = invocation.getArgument(1);
              return handler.handleResponse(mockHttpResponse);
            });

    String token = provider.getToken();

    assertThat(token).isEqualTo("Bearer token-jwt-value");
  }

  @Test
  void testLoadJwtAuthFromEnvironmentVariables() throws Exception {
    String expectedTokenEndpoint = "https://auth.example.com/token/with/underscores";
    String expectedGrantType = "jwt-bearer-with-underscores";
    String expectedApiKey = "test-env-api-key-with-underscores";
    String expectedResponseType = "token-with-underscores";
    String expectedTokenRefreshBuffer = "180";

    // Use reflection to set environment variables
    Map<String, String> envVars = new HashMap<>();
    envVars.put("OPENLINEAGE__TRANSPORT__TYPE", "http");
    envVars.put("OPENLINEAGE__TRANSPORT__URL", "http://backend:5000");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TYPE", "jwt");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__API_KEY", expectedApiKey);
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TOKEN_ENDPOINT", expectedTokenEndpoint);
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__GRANT_TYPE", expectedGrantType);
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__RESPONSE_TYPE", expectedResponseType);
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TOKEN_REFRESH_BUFFER", expectedTokenRefreshBuffer);

    setEnvironmentVariables(envVars);

    try {
      // Parse config from environment variables
      OpenLineageConfig config =
          OpenLineageClientUtils.loadOpenLineageConfigFromEnvVars(
              new com.fasterxml.jackson.core.type.TypeReference<OpenLineageConfig>() {});

      // Verify that underscores are preserved and values are correct
      assertThat(config.getTransportConfig()).isInstanceOf(HttpConfig.class);
      HttpConfig httpConfig = (HttpConfig) config.getTransportConfig();

      assertThat(httpConfig.getAuth()).isInstanceOf(JwtTokenProvider.class);
      JwtTokenProvider provider = (JwtTokenProvider) httpConfig.getAuth();

      assertThat(provider.getTokenEndpoint()).isEqualTo(URI.create(expectedTokenEndpoint));
      assertThat(provider.getApiKey()).isEqualTo(expectedApiKey);
      assertThat(provider.getGrantType()).isEqualTo(expectedGrantType);
      assertThat(provider.getResponseType()).isEqualTo(expectedResponseType);
      assertThat(provider.getTokenRefreshBuffer())
          .isEqualTo(Integer.valueOf(expectedTokenRefreshBuffer));
    } finally {
      // Clean up environment variables
      clearEnvironmentVariables(envVars.keySet());
    }
  }

  @Test
  void testLoadJwtAuthFromInvialidEnvironmentVariablesFails() throws Exception {
    String expectedTokenEndpoint = "https://auth.example.com/token/with/underscores";
    String expectedGrantType = "jwt-bearer-with-underscores";
    String expectedApiKey = "test-env-api-key-with-underscores";
    String expectedResponseType = "token-with-underscores";
    String expectedTokenRefreshBuffer = "180";

    // Use reflection to set environment variables
    Map<String, String> envVars = new HashMap<>();
    envVars.put("OPENLINEAGE__TRANSPORT__TYPE", "http");
    envVars.put("OPENLINEAGE__TRANSPORT__URL", "http://backend:5000");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TYPE", "jwt");
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__APIKEY", expectedApiKey);
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TOKENENDPOINT", expectedTokenEndpoint);
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__GRANTTYPE", expectedGrantType);
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__RESPONSETYPE", expectedResponseType);
    envVars.put("OPENLINEAGE__TRANSPORT__AUTH__TOKEN_REFRESHBUFFER", expectedTokenRefreshBuffer);

    setEnvironmentVariables(envVars);

    // Should fail because environment variables use wrong field names (no underscores)
    OpenLineageClientException exception =
        assertThrows(
            OpenLineageClientException.class,
            () -> {
              // Parse config from environment variables
              OpenLineageClientUtils.loadOpenLineageConfigFromEnvVars(
                  new com.fasterxml.jackson.core.type.TypeReference<OpenLineageConfig>() {});
            });

    // Verify the root cause is IllegalArgumentException with the expected message
    Throwable cause = exception.getCause();
    while (cause != null && !(cause instanceof IllegalArgumentException)) {
      cause = cause.getCause();
    }
    assertThat(cause).isInstanceOf(IllegalArgumentException.class);
    assertThat(cause.getMessage()).contains("apiKey must not be null or empty");
  }
}
