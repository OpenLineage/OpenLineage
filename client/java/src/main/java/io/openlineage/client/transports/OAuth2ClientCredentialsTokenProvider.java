/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.common.util.StringUtils;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;

/**
 * TokenProvider that obtains an access token with the OAuth 2.0 client credentials grant (RFC 6749,
 * section 4.4).
 *
 * <p>The client credentials are sent in the {@code Authorization} header ({@code
 * client_secret_basic}, the default) or in the request body ({@code client_secret_post}). The
 * access token is cached and fetched again before it expires, as the client credentials grant does
 * not issue refresh tokens.
 *
 * <p>Configuration example:
 *
 * <pre>{@code
 * transport:
 *   type: http
 *   url: https://api.example.com
 *   auth:
 *     type: oauth2
 *     clientId: your-client-id
 *     clientSecret: your-client-secret
 *     tokenEndpoint: https://auth.example.com/token
 *     scope: openid  # optional
 *     clientAuthMethod: client_secret_basic  # optional, or client_secret_post
 *     tokenRefreshBuffer: 120  # optional, defaults to 120 seconds
 * }</pre>
 */
@ToString(
    callSuper = true,
    exclude = {"clientSecret"})
public class OAuth2ClientCredentialsTokenProvider extends TokenEndpointTokenProvider {

  public static final String CLIENT_SECRET_BASIC = "client_secret_basic";
  public static final String CLIENT_SECRET_POST = "client_secret_post";

  @Getter private final String clientId;
  @Getter private final String clientSecret;

  /** Space separated OAuth 2.0 scopes to request. Optional. */
  @Getter @Setter private String scope;

  /**
   * How the client credentials are sent to the token endpoint: "client_secret_basic" (HTTP basic
   * Authorization header) or "client_secret_post" (request body). Optional, default:
   * "client_secret_basic".
   */
  @Getter private String clientAuthMethod = CLIENT_SECRET_BASIC;

  /**
   * Constructor that requires mandatory parameters clientId, clientSecret and tokenEndpoint. Used
   * by Jackson for deserialization.
   *
   * @param clientId The OAuth 2.0 client ID (required)
   * @param clientSecret The OAuth 2.0 client secret (required)
   * @param tokenEndpoint The token endpoint URI (required)
   * @throws IllegalArgumentException if clientId or clientSecret is null/empty or tokenEndpoint is
   *     null
   */
  @JsonCreator
  public OAuth2ClientCredentialsTokenProvider(
      @JsonProperty("clientId") String clientId,
      @JsonProperty("clientSecret") String clientSecret,
      @JsonProperty("tokenEndpoint") URI tokenEndpoint) {
    super(tokenEndpoint, "access_token");
    if (StringUtils.isBlank(clientId)) {
      throw new IllegalArgumentException("clientId must not be null or empty");
    }
    if (StringUtils.isBlank(clientSecret)) {
      throw new IllegalArgumentException("clientSecret must not be null or empty");
    }
    if (tokenEndpoint == null) {
      throw new IllegalArgumentException("tokenEndpoint must not be null");
    }
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }

  public void setClientAuthMethod(String clientAuthMethod) {
    if (!CLIENT_SECRET_BASIC.equals(clientAuthMethod)
        && !CLIENT_SECRET_POST.equals(clientAuthMethod)) {
      throw new IllegalArgumentException(
          String.format(
              "clientAuthMethod must be %s or %s, got %s",
              CLIENT_SECRET_BASIC, CLIENT_SECRET_POST, clientAuthMethod));
    }
    this.clientAuthMethod = clientAuthMethod;
  }

  @Override
  protected String getTokenName() {
    return "OAuth2 access token";
  }

  @Override
  protected List<NameValuePair> getTokenRequestParameters() {
    List<NameValuePair> parameters = new ArrayList<>();
    parameters.add(new BasicNameValuePair("grant_type", "client_credentials"));
    if (StringUtils.isNotBlank(scope)) {
      parameters.add(new BasicNameValuePair("scope", scope));
    }
    if (CLIENT_SECRET_POST.equals(clientAuthMethod)) {
      parameters.add(new BasicNameValuePair("client_id", clientId));
      parameters.add(new BasicNameValuePair("client_secret", clientSecret));
    }
    return parameters;
  }

  @Override
  protected @Nullable String getTokenRequestAuthorization() {
    if (CLIENT_SECRET_POST.equals(clientAuthMethod)) {
      return null;
    }
    String credentials = formUrlEncode(clientId) + ":" + formUrlEncode(clientSecret);
    return "Basic "
        + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
  }

  /** Encodes a client credential as required by RFC 6749, section 2.3.1. */
  private static String formUrlEncode(String value) {
    try {
      return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("UTF-8 is always supported", e);
    }
  }
}
