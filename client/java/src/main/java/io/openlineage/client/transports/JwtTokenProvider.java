/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.common.util.StringUtils;
import java.net.URI;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hc.core5.http.NameValuePair;
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
 * <p>The provider caches tokens and automatically refreshes them before expiry. By default, tokens
 * are refreshed 120 seconds before they expire. This can be configured using the tokenRefreshBuffer
 * parameter.
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
 *     tokenRefreshBuffer: 120  # optional, defaults to 120 seconds
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
@ToString(
    callSuper = true,
    exclude = {"apiKey"})
public class JwtTokenProvider extends TokenEndpointTokenProvider {

  @Getter private final String apiKey;

  /**
   * Constructor that requires mandatory parameters apiKey and tokenEndpoint. Used by Jackson for
   * deserialization.
   *
   * @param apiKey The API key for authentication (required)
   * @param tokenEndpoint The token endpoint URI (required)
   * @throws IllegalArgumentException if apiKey is null/empty or tokenEndpoint is null
   */
  @JsonCreator
  public JwtTokenProvider(
      @JsonProperty("apiKey") String apiKey, @JsonProperty("tokenEndpoint") URI tokenEndpoint) {
    super(tokenEndpoint, "token", "access_token");
    if (StringUtils.isBlank(apiKey)) {
      throw new IllegalArgumentException("apiKey must not be null or empty");
    }
    if (tokenEndpoint == null) {
      throw new IllegalArgumentException("tokenEndpoint must not be null");
    }
    this.apiKey = apiKey;
  }

  /**
   * OAuth grant type parameter sent in the token request. Optional, default:
   * "urn:ietf:params:oauth:grant-type:jwt-bearer"
   */
  @Getter @Setter private String grantType = "urn:ietf:params:oauth:grant-type:jwt-bearer";

  /** OAuth response type parameter sent in the token request. Optional, default: "token" */
  @Getter @Setter private String responseType = "token";

  @Override
  protected String getTokenName() {
    return "JWT token";
  }

  @Override
  protected List<NameValuePair> getTokenRequestParameters() {
    return List.of(
        new BasicNameValuePair("apikey", apiKey),
        new BasicNameValuePair("grant_type", grantType),
        new BasicNameValuePair("response_type", responseType));
  }
}
