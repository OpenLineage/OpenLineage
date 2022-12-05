package io.openlineage.spark.agent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class UrlParserTest {

    private static final String NS_NAME = "ns_name";
    private static final String JOB_NAME = "job_name";
    private static final String URL = "http://localhost:5000";
    private static final String RUN_ID = "ea445b5c-22eb-457a-8007-01c7c52b6e54";
    private static final String APP_NAME = "test";
    private static final String NA = "NOT_DEFINED";
    public static Collection<Object[]> data() {
        List<Object[]> pass = new ArrayList<>();

        pass.add(
                new Object[] {
                        "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc",
                        URL,
                        NS_NAME,
                        JOB_NAME,
                        RUN_ID,
                        NA,
                        "/api/v1",
                        "abc",
                        "apiKey",
                        NA,
                        NA,
                        getMapOf()
                });
        pass.add(
                new Object[] {
                        "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54",
                        URL,
                        NS_NAME,
                        JOB_NAME,
                        RUN_ID,
                        NA,
                        "/api/v1",
                        "abc",
                        "apiKey",
                        NA,
                        NA,
                        getMapOf()
                });
        pass.add(
                new Object[] {
                        "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=",
                        URL,
                        NS_NAME,
                        JOB_NAME,
                        RUN_ID,
                        NA,
                        "/api/v1",
                        "abc",
                        "apiKey",
                        NA,
                        NA,
                        getMapOf()
                });
        pass.add(
                new Object[] {
                        "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name?api_key=",
                        URL,
                        NS_NAME,
                        JOB_NAME,
                        RUN_ID,
                        NA,
                        "/api/v1",
                        "abc",
                        "apiKey",
                        NA,
                        NA,
                        getMapOf()
                });
        pass.add(
                new Object[] {
                        "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=abc&myParam=xyz",
                        URL,
                        NS_NAME,
                        JOB_NAME,
                        RUN_ID,
                        NA,
                        "/api/v1",
                        "abc",
                        "apiKey",
                        NA,
                        NA,
                        getMapOf()
                });
        pass.add(
                new Object[] {
                        "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?api_key=&myParam=xyz",
                        URL,
                        NS_NAME,
                        JOB_NAME,
                        RUN_ID,
                        NA,
                        "/api/v1",
                        "abc",
                        "apiKey",
                        NA,
                        NA,
                        getMapOf()
                });
        pass.add(
                new Object[] {
                        "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?timeout=5000",
                        URL,
                        NS_NAME,
                        JOB_NAME,
                        RUN_ID,
                        NA,
                        "/api/v1",
                        "abc",
                        "apiKey",
                        NA,
                        NA,
                        getMapOf()
                });
        pass.add(
                new Object[] {
                        "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?timeout=",
                        URL,
                        NS_NAME,
                        JOB_NAME,
                        RUN_ID,
                        NA,
                        "/api/v1",
                        "abc",
                        "apiKey",
                        NA,
                        NA,
                        getMapOf()
                });
        pass.add(
                new Object[] {
                        "http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/ea445b5c-22eb-457a-8007-01c7c52b6e54?app_name="
                                + APP_NAME,
                        URL,
                        NS_NAME,
                        JOB_NAME,
                        RUN_ID,
                        NA,
                        "/api/v1",
                        "abc",
                        "apiKey",
                        NA,
                        NA,
                        getMapOf()
                });
        return pass;
    }

    @ParameterizedTest
    @MethodSource("data")
    void testArgument(
            String input,
            String url,
            String namespace,
            String jobName,
            String runId,
            String appName,
            String endpoint,
            String apiKey,
            String authType,
            String timeout,
            String disabledFacets,
            Map<String, String> urlParams) {
        Map<String, String> parsed = UrlParser.parseUrl(input);
        assertEquals(parsed.getOrDefault(ArgumentParser.SPARK_CONF_HTTP_URL, "NOT_DEFINED"), url);
        assertEquals(parsed.getOrDefault(ArgumentParser.SPARK_CONF_NAMESPACE, "NOT_DEFINED"), namespace);
        assertEquals(parsed.getOrDefault(ArgumentParser.SPARK_CONF_JOB_NAME, "NOT_DEFINED"), jobName);
        assertEquals(parsed.getOrDefault(ArgumentParser.SPARK_CONF_PARENT_RUN_ID, "NOT_DEFINED"), runId);
        assertEquals(parsed.getOrDefault(ArgumentParser.SPARK_CONF_APP_NAME, "NOT_DEFINED"), appName);
        assertEquals(parsed.getOrDefault(UrlParser.SPARK_CONF_API_ENDPOINT, "NOT_DEFINED" ), endpoint);
        assertEquals(parsed.getOrDefault(UrlParser.SPARK_CONF_API_KEY, "NOT_DEFINED"), apiKey);
        assertEquals(parsed.getOrDefault(UrlParser.SPARK_CONF_AUTH_TYPE, "NOT_DEFINED"), authType);
        assertEquals(parsed.getOrDefault(UrlParser.SPARK_CONF_TIMEOUT, "NOT_DEFINED"), timeout);
        assertEquals(parsed.getOrDefault(UrlParser.SPARK_CONF_DISABLED_FACETS, "NOT_DEFINED"), disabledFacets);
        boolean urlParamsMatch = urlParams.entrySet().stream()
                .filter(e -> e.getKey().contains(UrlParser.SPARK_CONF_URL_PARAM_PREFIX))
                .allMatch(e -> parsed.get(e.getKey()).equals(e.getValue()));
        assertTrue(urlParamsMatch);
    }

    private static Map<String, String> getMapOf(String... s){
        return Arrays.stream(s).collect(Collectors.toMap(e -> e.split(":")[0], e->e.split(":")[1]));
    }

    private static Map<String, String> getMapOf(){
        return Collections.emptyMap();
    }

}