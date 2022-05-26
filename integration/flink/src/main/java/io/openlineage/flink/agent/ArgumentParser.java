/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.flink.agent;

import static io.openlineage.flink.utils.FlinkOpenLineageConfigs.FLINK_CONF_API_KEY;
import static io.openlineage.flink.utils.FlinkOpenLineageConfigs.FLINK_CONF_API_VERSION_KEY;
import static io.openlineage.flink.utils.FlinkOpenLineageConfigs.FLINK_CONF_HOST_KEY;
import static io.openlineage.flink.utils.FlinkOpenLineageConfigs.FLINK_CONF_JOB_NAME_KEY;
import static io.openlineage.flink.utils.FlinkOpenLineageConfigs.FLINK_CONF_NAMESPACE_KEY;
import static io.openlineage.flink.utils.FlinkOpenLineageConfigs.FLINK_CONF_PARENT_RUN_ID_KEY;
import static io.openlineage.flink.utils.FlinkOpenLineageConfigs.FLINK_CONF_URL_KEY;
import static io.openlineage.flink.utils.FlinkOpenLineageConfigs.FLINK_CONF_URL_PARAM_PREFIX;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URLEncodedUtils;

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

  public static ArgumentParser parseConfiguration(Map<String, String> configuration) {
    Optional<String> url = findConfigurationFor(configuration, FLINK_CONF_URL_KEY.getConfigPath());
    if (url.isPresent()) {
      return parse(url.get());
    } else {
      String host =
          findConfigurationFor(
              configuration, FLINK_CONF_HOST_KEY.getConfigPath(), DEFAULTS.getHost());
      String version =
          findConfigurationFor(
              configuration, FLINK_CONF_API_VERSION_KEY.getConfigPath(), DEFAULTS.getVersion());
      String namespace =
          findConfigurationFor(
              configuration, FLINK_CONF_NAMESPACE_KEY.getConfigPath(), DEFAULTS.getNamespace());
      String jobName =
          findConfigurationFor(
              configuration, FLINK_CONF_JOB_NAME_KEY.getConfigPath(), DEFAULTS.getJobName());
      String runId =
          findConfigurationFor(
              configuration,
              FLINK_CONF_PARENT_RUN_ID_KEY.getConfigPath(),
              DEFAULTS.getParentRunId());
      Optional<String> apiKey =
          findConfigurationFor(configuration, FLINK_CONF_API_KEY.getConfigPath())
              .filter(str -> !str.isEmpty());
      Optional<Map<String, String>> urlParams =
          findFlinkUrlParams(configuration, FLINK_CONF_URL_PARAM_PREFIX.getConfigPath());
      return new ArgumentParser(host, version, namespace, jobName, runId, apiKey, urlParams);
    }
  }

  private static Optional<String> findConfigurationFor(Map<String, String> config, String name) {
    Optional<String> parameter = Optional.ofNullable(config.get(name));
    if (parameter.isEmpty()) {
      return Optional.ofNullable(config.get("flink." + name));
    }
    return Optional.empty();
  }

  private static String findConfigurationFor(
      Map<String, String> config, String name, String defaultValue) {
    return findConfigurationFor(config, name).orElse(defaultValue);
  }

  private static Optional<Map<String, String>> findFlinkUrlParams(
      Map<String, String> config, String prefix) {
    Map<String, String> urlParams = new HashMap<>();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      if (entry.getKey().startsWith("flink." + prefix + ".")) {
        urlParams.put(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }
    return urlParams.isEmpty() ? Optional.empty() : Optional.of(urlParams);
  }

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
    final Map<String, String> urlParams = new HashMap<>();
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
