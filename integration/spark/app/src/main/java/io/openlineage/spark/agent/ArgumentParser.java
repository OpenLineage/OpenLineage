/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URLEncodedUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

@AllArgsConstructor
@Slf4j
@Getter
@ToString
public class ArgumentParser {
  public static final ArgumentParser DEFAULTS = getDefaultArguments();

  private final String host;
  private final String version;
  private final String namespace;
  private final String jobName;
  private final String parentRunId;
  private final Optional<String> apiKey;
  private final Optional<Map<String, String>> urlParams;

  public static ArgumentParser parse(String clientUrl) {
    URI uri = URI.create(clientUrl);
    String host = uri.getScheme() + "://" + uri.getAuthority();

    String path = uri.getPath();
    String[] elements = path.split("/");
    String version = get(elements, "api", 1, DEFAULTS.getVersion());
    String namespace = get(elements, "namespaces", 3, DEFAULTS.getNamespace());
    String jobName = get(elements, "jobs", 5, DEFAULTS.getJobName());
    String runId = get(elements, "runs", 7, DEFAULTS.getParentRunId());

    List<NameValuePair> nameValuePairList = URLEncodedUtils.parse(uri, StandardCharsets.UTF_8);
    Optional<String> apiKey = getApiKey(nameValuePairList);
    Optional<Map<String, String>> urlParams = getUrlParams(nameValuePairList);

    log.info(
        String.format(
            "%s/api/%s/namespaces/%s/jobs/%s/runs/%s", host, version, namespace, jobName, runId));

    return new ArgumentParser(host, version, namespace, jobName, runId, apiKey, urlParams);
  }

  public static UUID getRandomUuid() {
    return UUID.randomUUID();
  }

  private static Optional<String> getApiKey(List<NameValuePair> nameValuePairList) {
    return Optional.ofNullable(getNamedParameter(nameValuePairList, "api_key"))
        .filter(StringUtils::isNoneBlank);
  }

  public String getUrlParam(String urlParamName) {
    String param = null;
    if (urlParams.isPresent()) {
      param = urlParams.get().get(urlParamName);
    }
    return param;
  }

  private static Optional<Map<String, String>> getUrlParams(List<NameValuePair> nameValuePairList) {
    final Map<String, String> urlParams = new HashMap<String, String>();
    nameValuePairList.stream()
        .filter(pair -> !(pair.getName().equals("api_key")))
        .forEach(pair -> urlParams.put(pair.getName(), pair.getValue()));

    return urlParams.isEmpty() ? Optional.empty() : Optional.ofNullable(urlParams);
  }

  protected static String getNamedParameter(List<NameValuePair> nameValuePairList, String param) {
    for (NameValuePair nameValuePair : nameValuePairList) {
      if (nameValuePair.getName().equalsIgnoreCase(param)) {
        return nameValuePair.getValue();
      }
    }
    return null;
  }

  private static String get(String[] elements, String name, int index, String defaultValue) {
    boolean check = elements.length > index + 1 && name.equals(elements[index]);
    if (check) {
      return elements[index + 1];
    } else {
      log.warn("missing " + name + " in " + Arrays.toString(elements) + " at " + index);
      return defaultValue;
    }
  }

  private static ArgumentParser getDefaultArguments() {
    return new ArgumentParser(
        "", "v1", "default", "default", null, Optional.empty(), Optional.empty());
  }
}
