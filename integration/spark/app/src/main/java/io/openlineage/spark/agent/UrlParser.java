package io.openlineage.spark.agent;

import io.openlineage.client.transports.HttpConfig;
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
    public static final String SPARK_CONF_API_ENDPOINT = "spark.openlineage.transport.endpoint";
    public static final String SPARK_CONF_TIMEOUT = "spark.openlineage.transport.timeout";
    public static final String SPARK_CONF_API_KEY = "spark.openlineage.transport.auth.apiKey";
    public static final String SPARK_CONF_AUTH_TYPE = "spark.openlineage.transport.auth.type";
    public static final String SPARK_CONF_URL_PARAM_PREFIX = "spark.openlineage.transport.properties.url.param";
    public static final String SPARK_CONF_HTTP_URL = "spark.openlineage.transport.url";
    private static final String SPARK_CONF_DISABLED_FACETS = "spark.openlineage.facets.disabled.";
    private static final String TRANSPORT_PREFIX = "spark.openlineage.transport.";
    private static final String OPENLINEAGE_PREFIX = "spark.openlineage.";
    public static final Set<String> namedParams =
            new HashSet<>(Arrays.asList("timeout", "api_key", "app_name", "disabled"));
    public static final String disabledFacetsSeparator = ";";

    public String host = "";
    public String version = "v1";
    public String namespace = "default";
    public String jobName = "default";
    public String parentRunId = null;
    public Optional<String> appName = Optional.empty();

    public static Map<String,String> parseUrl(String clientUrl) {
        URI uri = URI.create(clientUrl);
        String path = uri.getPath();
        String[] elements = path.split("/");
        List<NameValuePair> nameValuePairList = URLEncodedUtils.parse(uri, StandardCharsets.UTF_8);
        
        Map<String, String> parsedProperties = new HashMap<>();
        parsedProperties.put(SPARK_CONF_HTTP_URL, uri.getScheme() + "://" + uri.getAuthority());
        get(elements, "namespaces", 3).ifPresent(p -> parsedProperties.put(ArgumentParser.SPARK_CONF_NAMESPACE, p));
        get(elements, "jobs", 5).ifPresent(p -> parsedProperties.put(ArgumentParser.SPARK_CONF_JOB_NAME, p));
        get(elements, "runs", 7).ifPresent(p -> parsedProperties.put(ArgumentParser.SPARK_CONF_PARENT_RUN_ID, p));
        get(elements, "api", 1).ifPresent(p -> parsedProperties.put(SPARK_CONF_API_ENDPOINT, String.format("/api/%s/lineage", p)));
        
        getNamedStringParameter(nameValuePairList, "disabled").ifPresent(p->parsedProperties.put(SPARK_CONF_DISABLED_FACETS, p));
        getNamedStringParameter(nameValuePairList, "timeout").ifPresent(p->parsedProperties.put(SPARK_CONF_TIMEOUT, p));
        getNamedStringParameter(nameValuePairList, "api_key").ifPresent(p->{
            parsedProperties.put(SPARK_CONF_API_KEY, p);
            parsedProperties.put(SPARK_CONF_AUTH_TYPE, "api_key");
        });
        getNamedStringParameter(nameValuePairList, "app_name").ifPresent(p->parsedProperties.put(ArgumentParser.SPARK_CONF_APP_NAME, p));
        
        getUrlParams(nameValuePairList)
                .orElseGet(HashMap::new)
                .forEach((key, value) -> parsedProperties.put(SPARK_CONF_URL_PARAM_PREFIX + key, value));
        return parsedProperties;
    }
    
    private static Optional<String> getNamedStringParameter(
            List<NameValuePair> nameValuePairList, String name) {
        return Optional.ofNullable(getNamedParameter(nameValuePairList, name))
                .filter(StringUtils::isNoneBlank);
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
