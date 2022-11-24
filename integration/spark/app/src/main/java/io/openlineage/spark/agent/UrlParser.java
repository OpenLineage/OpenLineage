package io.openlineage.spark.agent;

import io.openlineage.client.transports.HttpConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URLEncodedUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Getter
public class UrlParser {

    public static final Set<String> namedParams =
            new HashSet<>(Arrays.asList("timeout", "api_key", "app_name"));
    public static final String disabledFacetsSeparator = ";";

    public String host = "";
    public String version = "v1";
    public String namespace = "default";
    public String jobName = "default";
    public String parentRunId = null;
    public Optional<String> appName = Optional.empty();

    public static ArgumentParser.ArgumentParserBuilder parseUrl(ArgumentParser.ArgumentParserBuilder builder, String clientUrl) {
        HttpConfig httpConfig = new HttpConfig();
        URI uri = URI.create(clientUrl);
        String path = uri.getPath();
        String[] elements = path.split("/");
        List<NameValuePair> nameValuePairList = URLEncodedUtils.parse(uri, StandardCharsets.UTF_8);

        httpConfig.setUrl(uri);
        httpConfig.setHost(uri.getScheme() + "://" + uri.getAuthority());
        httpConfig.setTimeout(getTimeout(nameValuePairList));
        get(elements, "api", 1).ifPresent(httpConfig::setVersion);
        getNamedStringParameter(nameValuePairList, "app_name").ifPresent(builder::appName);
        Properties properties = new Properties();
        Map<String, Map.Entry<String, String>> params = getUrlParams(nameValuePairList)
                .orElseGet(HashMap::new)
                .entrySet().stream()
                .collect(Collectors.toMap(key -> "transport.http.url.param." + key, value -> value));
        properties.putAll(params);
        httpConfig.setProperties(properties);


        get(elements, "namespaces", 3).ifPresent(builder::namespace);
        get(elements, "jobs", 5).ifPresent(builder::jobName);
        get(elements, "runs", 7).ifPresent(builder::parentRunId);

        builder.transportConfig(httpConfig);
        return builder;
    }

    public static UUID getRandomUuid() {
        return UUID.randomUUID();
    }

    private static Optional<String> getNamedStringParameter(
            List<NameValuePair> nameValuePairList, String name) {
        return Optional.ofNullable(getNamedParameter(nameValuePairList, name))
                .filter(StringUtils::isNoneBlank);
    }

    private static Double getTimeout(List<NameValuePair> nameValuePairList) {
        String timeoutString = getNamedParameter(nameValuePairList, "timeout");
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

    public String[] extractDisabledFacets() {
        return disabledFacets.split(disabledFacetsSeparator);
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

    private static Optional<String> get(String[] elements, String name, int index) {
        boolean check = elements.length > index + 1 && name.equals(elements[index]);
        if (check) {
            return Optional.of(elements[index + 1]);
        } else {
            log.warn("missing " + name + " in " + Arrays.toString(elements) + " at " + index);
            return Optional.empty();
        }
    }
}
