package openlineage.spark.agent;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
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
  private final String runId;
  private final Optional<String> apiKey;

  public static ArgumentParser parse(String clientUrl) {
    URI uri = URI.create(clientUrl);
    String host = uri.getScheme() + "://" + uri.getAuthority();

    String path = uri.getPath();
    String[] elements = path.split("/");
    String version = get(elements, "api", 1, DEFAULTS.getVersion());
    String namespace = get(elements, "namespaces", 3, DEFAULTS.getNamespace());
    String jobName = get(elements, "jobs", 5, DEFAULTS.getJobName());
    String runId = get(elements, "runs", 7, DEFAULTS.getRunId());

    List<NameValuePair> nameValuePairList = URLEncodedUtils.parse(uri, StandardCharsets.UTF_8);
    Optional<String> apiKey = getApiKey(nameValuePairList);

    log.info(
        String.format(
            "%s/api/%s/namespaces/%s/jobs/%s/runs/%s", host, version, namespace, jobName, runId));

    return new ArgumentParser(host, version, namespace, jobName, runId, apiKey);
  }

  public static UUID getRandomUuid() {
    return UUID.randomUUID();
  }

  private static Optional<String> getApiKey(List<NameValuePair> nameValuePairList) {
    return Optional.ofNullable(getNamedParameter(nameValuePairList, "api_key"))
        .filter(StringUtils::isNoneBlank);
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
    return new ArgumentParser("", "v1", "default", "default", "", Optional.empty());
  }
}
