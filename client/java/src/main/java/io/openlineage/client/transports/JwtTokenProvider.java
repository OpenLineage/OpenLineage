/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineageClientException;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicNameValuePair;

/**
 * TokenProvider that exchanges an API key for a JWT token via a POST endpoint.
 *
 * <p>Sends the API key and OAuth parameters as URL-encoded form data.
 *
 * <p>The provider automatically tries multiple common JSON field names for the token: the
 * configured tokenFields (default ["token", "access_token"]). This ensures compatibility with
 * various OAuth providers.
 *
 * <p>Configuration example:
 *
 * <pre>{@code
 * transport:
 *   type: http
 *   url: https://api.example.com
 *   auth:
 *     type: jwt
 *     apiKey: your-api-key
 *     tokenEndpoint: https://auth.example.com/token
 *     tokenFields: ["token", "access_token"]  # optional, defaults to ["token", "access_token"]
 *     expiresInField: expires_in  # optional, defaults to "expires_in"
 *     grantType: urn:ietf:params:oauth:grant-type:jwt-bearer  # optional, defaults to "urn:ietf:params:oauth:grant-type:jwt-bearer"
 *     responseType: token  # optional, defaults to "token"
 * }</pre>
 *
 * <p>For IBM Cloud IAM, use these settings:
 *
 * <pre>{@code
 * auth:
 *   type: jwt
 *   apiKey: your-ibm-api-key
 *   tokenEndpoint: https://iam.cloud.ibm.com/identity/token
 *   grantType: urn:ibm:params:oauth:grant-type:apikey
 *   responseType: cloud_iam
 * }</pre>
 */
@Slf4j
@NoArgsConstructor
@ToString(exclude = {"apiKey", "cachedToken"})
public class JwtTokenProvider implements TokenProvider {

  private static final int TOKEN_REFRESH_BUFFER_SECONDS = 60; // Refresh 60s before expiry

  @Getter @Setter private String apiKey;
  @Getter @Setter private URI tokenEndpoint;

  /**
   * The JSON field name containing the JWT token in the response. Defaults to "access_token". The
   * provider will also try "token" and "access_token" as fallback options.
   */
  @Getter @Setter private String[] tokenFields = new String[] {"token", "access_token"};

  /**
   * The JSON field name containing the token expiration time in seconds. Defaults to "expires_in".
   * If not present in response, token will be refreshed on every call.
   */
  @Getter @Setter private String expiresInField = "expires_in";

  /**
   * OAuth grant type parameter sent in the token request. Optional, default:
   * "urn:ietf:params:oauth:grant-type:jwt-bearer"
   */
  @Getter @Setter private String grantType = "urn:ietf:params:oauth:grant-type:jwt-bearer";

  /** OAuth response type parameter sent in the token request. Optional, default: "token" */
  @Getter @Setter private String responseType = "token";

  // Cached token state
  private transient String cachedToken;
  private transient long tokenExpiryEpochSeconds;

  @Override
  public String getToken() {
    if (isTokenValid()) {
      log.debug("Using cached JWT token");
      return formatToken(cachedToken);
    }

    log.debug("Fetching new JWT token from endpoint: {}", tokenEndpoint);
    try {
      fetchAndCacheToken();
      return formatToken(cachedToken);
    } catch (Exception e) {
      throw new OpenLineageClientException("Failed to fetch JWT token", e);
    }
  }

  private boolean isTokenValid() {
    if (cachedToken == null) {
      return false;
    }

    // If we don't have expiry info, consider token invalid (will refresh)
    if (tokenExpiryEpochSeconds == 0) {
      return false;
    }

    long currentTime = Instant.now().getEpochSecond();
    long timeUntilExpiry = tokenExpiryEpochSeconds - currentTime;

    // Refresh if token expires within buffer time
    return timeUntilExpiry > TOKEN_REFRESH_BUFFER_SECONDS;
  }

  /** Creates an HTTP client for token requests. Protected to allow test overrides. */
  protected CloseableHttpClient createHttpClient() {
    return HttpClients.createDefault();
  }

  private void fetchAndCacheToken() throws IOException {
    try (CloseableHttpClient httpClient = createHttpClient()) {
      HttpPost request = new HttpPost(tokenEndpoint);
      request.setHeader("Content-Type", ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
      request.setHeader("Accept", ContentType.APPLICATION_JSON.getMimeType());

      // Build URL-encoded request body
      request.setEntity(
          new UrlEncodedFormEntity(
              List.of(
                  new BasicNameValuePair("apikey", apiKey),
                  new BasicNameValuePair("grant_type", grantType),
                  new BasicNameValuePair("response_type", responseType))));

      String responseBody =
          httpClient.execute(
              request,
              response -> {
                int statusCode = response.getCode();
                String body = EntityUtils.toString(response.getEntity());

                if (statusCode < 200 || statusCode >= 300) {
                  throw new OpenLineageClientException(
                      String.format("JWT token endpoint returned status %d: %s", statusCode, body));
                }

                return body;
              });

      parseAndCacheToken(responseBody);
    }
  }

  private void parseAndCacheToken(String responseBody) throws IOException {
    // Use helper class to parse the JWT token response
    JwtTokenResponse tokenResponse =
        new JwtTokenResponse(responseBody, tokenFields, expiresInField);

    cachedToken = tokenResponse.getToken();
    if (cachedToken == null || cachedToken.isEmpty()) {
      throw new OpenLineageClientException(
          String.format(
              "JWT token field '%s' not found or invalid in response",
              Arrays.deepToString(tokenFields)));
    }

    // Get expiry from response or JWT payload
    tokenExpiryEpochSeconds = tokenResponse.getExpiryEpochSeconds();
    if (tokenExpiryEpochSeconds > 0) {
      long expiresIn = tokenExpiryEpochSeconds - Instant.now().getEpochSecond();
      log.debug("JWT token cached, expires in {} seconds", expiresIn);
    } else {
      log.debug("JWT token cached, no expiry information available");
    }
  }

  private String formatToken(String token) {
    return String.format("Bearer %s", token);
  }

  /**
   * Helper class to parse JWT token response from OAuth endpoint. Parses the JSON response and
   * extracts token value and expiry with case-insensitive field matching.
   */
  private static class JwtTokenResponse {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Getter private final String token;

    @Getter private final long expiryEpochSeconds;

    /**
     * Constructs a JwtTokenResponse by parsing the JSON response body.
     *
     * @param responseBody JSON response from token endpoint
     * @param tokenFieldNames Expected field name for the token (case-insensitive)
     * @param expiresInFieldName Expected field name for expiry (case-insensitive)
     * @throws IOException if JSON parsing fails
     */
    JwtTokenResponse(String responseBody, String[] tokenFieldNames, String expiresInFieldName)
        throws IOException {
      JsonNode jsonResponse = MAPPER.readTree(responseBody);

      // Extract token with case-insensitive field matching
      this.token = extractFieldCaseInsensitive(jsonResponse, tokenFieldNames);

      // Extract expiry from response or JWT payload
      String expiresInStr = extractFieldCaseInsensitive(jsonResponse, expiresInFieldName);
      long expiresInSeconds = expiresInStr != null ? Long.decode(expiresInStr) : 0;

      if (expiresInSeconds > 0) {
        this.expiryEpochSeconds = Instant.now().getEpochSecond() + expiresInSeconds;
      } else if (token != null) {
        // Try to extract expiry from JWT token itself
        this.expiryEpochSeconds = extractExpiryFromJwtPayload(token);
      } else {
        this.expiryEpochSeconds = 0;
      }
    }

    /**
     * Extracts a string field from JSON with case-insensitive matching. Tries multiple field names
     * in order, returning the first match found.
     */
    private static String extractFieldCaseInsensitive(JsonNode jsonNode, String... fieldNames) {
      for (String fieldName : fieldNames) {
        // Try exact match first
        JsonNode node = jsonNode.get(fieldName);
        if (node != null && (node.isTextual() || node.isNumber())) {
          return node.asText();
        }

        // Try case-insensitive match
        String normalizedFieldName = normalizeFieldName(fieldName);
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> entry = fields.next();
          if ((entry.getKey().equalsIgnoreCase(fieldName)
                  || normalizeFieldName(entry.getKey()).equals(normalizedFieldName))
              && (entry.getValue().isTextual() || entry.getValue().isNumber())) {
            return entry.getValue().asText();
          }
        }
      }

      return null;
    }

    /**
     * Normalizes field name by removing underscores and converting to lowercase. This allows
     * matching "expires_in" with "expiresIn".
     */
    private static String normalizeFieldName(String fieldName) {
      return fieldName.replace("_", "").toLowerCase(Locale.US);
    }

    /** Extracts expiry time from JWT token payload using Jackson. Returns 0 if unable to parse. */
    private static long extractExpiryFromJwtPayload(String jwt) {
      try {
        String[] parts = jwt.split("\\.");
        // Decode the payload (second part) and parse using Jackson
        String payload = new String(Base64.getUrlDecoder().decode(parts[1]));
        JsonNode payloadJson = MAPPER.readTree(payload);

        // Look for standard "exp" claim (case-insensitive)
        String exp = extractFieldCaseInsensitive(payloadJson, "exp");
        return exp != null ? Long.decode(exp) : 0;

      } catch (Exception e) {
        log.debug("Unable to extract expiry from JWT token: {}", e.getMessage());
        return 0;
      }
    }
  }
}
