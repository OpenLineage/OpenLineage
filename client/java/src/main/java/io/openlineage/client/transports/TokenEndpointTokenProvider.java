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
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for TokenProviders that obtain a short-lived bearer token from a token endpoint.
 *
 * <p>The token is cached and fetched again {@code tokenRefreshBuffer} seconds before it expires.
 * Subclasses provide the URL-encoded form parameters of the token request and, optionally, the
 * {@code Authorization} header sent to the token endpoint.
 */
@ToString(exclude = {"log", "cachedToken"})
public abstract class TokenEndpointTokenProvider implements TokenProvider {

  // Instance logger so each concrete provider keeps its own log category
  private final Logger log = LoggerFactory.getLogger(getClass());

  // Default: Refresh 120s before expiry
  private static final int DEFAULT_TOKEN_REFRESH_BUFFER_SECONDS = 120;

  @Getter private final URI tokenEndpoint;

  /** The JSON field names to search for the token in the response, tried in order. */
  @Getter @Setter private String[] tokenFields;

  /**
   * The JSON field name containing the token expiration time in seconds. Defaults to "expires_in".
   * If not present in response, token will be refreshed on every call.
   */
  @Getter @Setter private String expiresInField = "expires_in";

  /**
   * Number of seconds before token expiry to trigger a refresh. Optional, default: 120 seconds.
   * This buffer ensures tokens are refreshed before they expire to avoid authentication failures.
   */
  @Getter @Setter private int tokenRefreshBuffer = DEFAULT_TOKEN_REFRESH_BUFFER_SECONDS;

  // Cached token state
  private transient String cachedToken;
  private transient long tokenExpiryEpochSeconds;

  /**
   * @param tokenEndpoint The token endpoint URI, validated by the subclass constructor together
   *     with its other required parameters
   * @param defaultTokenFields JSON field names to search for the token unless {@code tokenFields}
   *     is configured
   */
  protected TokenEndpointTokenProvider(URI tokenEndpoint, String... defaultTokenFields) {
    this.tokenEndpoint = tokenEndpoint;
    this.tokenFields = defaultTokenFields.clone();
  }

  /** Name of the token used in log and error messages, for example "JWT token". */
  protected abstract String getTokenName();

  /** URL-encoded form parameters sent to the token endpoint. */
  protected abstract List<NameValuePair> getTokenRequestParameters();

  /** Value of the {@code Authorization} header sent to the token endpoint, or null for none. */
  protected @Nullable String getTokenRequestAuthorization() {
    return null;
  }

  @Override
  public synchronized String getToken() {
    if (isTokenValid()) {
      log.debug("Using cached {}", getTokenName());
      return formatToken(cachedToken);
    }

    log.debug("Fetching new {} from endpoint: {}", getTokenName(), tokenEndpoint);
    try {
      fetchAndCacheToken();
      return formatToken(cachedToken);
    } catch (Exception e) {
      throw new OpenLineageClientException("Failed to fetch " + getTokenName(), e);
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

    long currentTime = getCurrentTimeSeconds();
    long timeUntilExpiry = tokenExpiryEpochSeconds - currentTime;

    // Refresh if token expires within buffer time
    return timeUntilExpiry > tokenRefreshBuffer;
  }

  /** Gets current time in epoch seconds. Protected to allow test overrides. */
  protected long getCurrentTimeSeconds() {
    return Instant.now().getEpochSecond();
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
      String authorization = getTokenRequestAuthorization();
      if (authorization != null) {
        request.setHeader("Authorization", authorization);
      }
      request.setEntity(new UrlEncodedFormEntity(getTokenRequestParameters()));

      String responseBody =
          httpClient.execute(
              request,
              response -> {
                int statusCode = response.getCode();
                String body = EntityUtils.toString(response.getEntity());

                if (statusCode < 200 || statusCode >= 300) {
                  throw new OpenLineageClientException(
                      String.format(
                          "%s endpoint returned status %d: %s", getTokenName(), statusCode, body));
                }
                return body;
              });

      parseAndCacheToken(responseBody);
    }
  }

  private void parseAndCacheToken(String responseBody) throws IOException {
    TokenResponse tokenResponse =
        new TokenResponse(responseBody, tokenFields, expiresInField, getCurrentTimeSeconds());

    cachedToken = tokenResponse.getToken();
    if (cachedToken == null || cachedToken.isEmpty()) {
      throw new OpenLineageClientException(
          String.format(
              "%s field '%s' not found or invalid in response",
              getTokenName(), Arrays.deepToString(tokenFields)));
    }

    // Get expiry from response or JWT payload
    tokenExpiryEpochSeconds = tokenResponse.getExpiryEpochSeconds();

    if (tokenExpiryEpochSeconds > 0) {
      long expiresIn = tokenExpiryEpochSeconds - getCurrentTimeSeconds();
      log.debug("{} cached, expires in {} seconds", getTokenName(), expiresIn);
    } else {
      log.warn(
          "{} endpoint returned no expiry information, so the token cannot be cached and a new one "
              + "is requested for every event. Set expiresInField if the response names it differently.",
          getTokenName());
    }
  }

  private String formatToken(String token) {
    return String.format("Bearer %s", token);
  }

  /**
   * Helper class to parse a token response. Parses the JSON response and extracts token value and
   * expiry with case-insensitive field matching.
   */
  @Slf4j
  private static class TokenResponse {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Getter private final String token;
    @Getter private final long expiryEpochSeconds;

    /**
     * Constructs a TokenResponse by parsing the JSON response body.
     *
     * @param responseBody JSON response from token endpoint
     * @param tokenFieldNames Expected field names for the token (case-insensitive)
     * @param expiresInFieldName Expected field name for expiry (case-insensitive)
     * @param currentTimeSeconds Current time in epoch seconds
     * @throws IOException if JSON parsing fails
     */
    TokenResponse(
        String responseBody,
        String[] tokenFieldNames,
        String expiresInFieldName,
        long currentTimeSeconds)
        throws IOException {
      JsonNode jsonResponse = MAPPER.readTree(responseBody);

      // Extract token with case-insensitive field matching
      this.token = extractFieldCaseInsensitive(jsonResponse, tokenFieldNames);

      // Extract expiry from response or JWT payload
      String expiresInStr = extractFieldCaseInsensitive(jsonResponse, expiresInFieldName);
      long expiresInSeconds = expiresInStr != null ? Long.decode(expiresInStr) : 0;

      if (expiresInSeconds > 0) {
        this.expiryEpochSeconds = currentTimeSeconds + expiresInSeconds;
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
