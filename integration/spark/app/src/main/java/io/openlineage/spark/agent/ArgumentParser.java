/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URLEncodedUtils;
import org.jetbrains.annotations.NotNull;

@AllArgsConstructor
@Slf4j
@Getter
@ToString
public class ArgumentParser {
  public static final ArgumentParser DEFAULTS = getDefaultArguments();
  public static final Set<String> namedParams =
      new HashSet<>(Arrays.asList("timeout", "api_key", "overwrite_name"));

  private final String host;
  private final String version;
  private final String namespace;
  private final String jobName;
  private final String parentRunId;
  private final Optional<Double> timeout;
  private final Optional<String> apiKey;
  private final Optional<String> overwriteName;
  private final Optional<Map<String, String>> urlParams;
  private final boolean consoleMode;

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
    Optional<Double> timeout = getTimeout(nameValuePairList);
    Optional<String> apiKey = getNamedStringParameter(nameValuePairList, "api_key");
    Optional<String> overwriteName = getNamedStringParameter(nameValuePairList, "overwrite_name");
    Optional<Map<String, String>> urlParams = getUrlParams(nameValuePairList);

    log.info(
        String.format(
            "%s/api/%s/namespaces/%s/jobs/%s/runs/%s", host, version, namespace, jobName, runId));

    return new ArgumentParser(
        host, version, namespace, jobName, runId, timeout, apiKey, overwriteName, urlParams, false);
  }

  public static UUID getRandomUuid() {
    return UUID.randomUUID();
  }

  @NotNull
  private static Optional<String> getNamedStringParameter(
      List<NameValuePair> nameValuePairList, String name) {
    return Optional.ofNullable(getNamedParameter(nameValuePairList, name))
        .filter(StringUtils::isNoneBlank);
  }

  private static Optional<Double> getTimeout(List<NameValuePair> nameValuePairList) {
    return Optional.ofNullable(
        ArgumentParser.extractTimeout(getNamedParameter(nameValuePairList, "timeout")));
  }

  private static Double extractTimeout(String timeoutString) {
    try {
      if (StringUtils.isNotBlank(timeoutString)) {
        return Double.parseDouble(timeoutString);
      }
    } catch (NumberFormatException e) {
      log.warn("Value of timeout is not parsable");
    }
    return null;
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
        .filter(pair -> !namedParams.contains(pair.getName()))
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
        "",
        "v1",
        "default",
        "default",
        null,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false);
  }
}
