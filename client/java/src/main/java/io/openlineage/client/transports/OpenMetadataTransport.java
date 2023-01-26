/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.http.Consts.UTF_8;
import static org.apache.http.HttpHeaders.*;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

@Slf4j
@Builder
public final class OpenMetadataTransport extends Transport implements Closeable {

    private final CloseableHttpClient http;
    private final URI uri;
    private final String pipelineServiceName;
    private final String pipelineName;
    private final String airflowHost;
    private @Nullable
    final TokenProvider tokenProvider;

    public enum LineageType {
        OUTLET,
        INLET
    }

    public OpenMetadataTransport(@NonNull final OpenMetadataConfig openMetadataConfig) {
        this(withTimeout(openMetadataConfig.getTimeout()), openMetadataConfig);
    }

    public OpenMetadataTransport(CloseableHttpClient http, URI uri, String pipelineServiceName,
                                 String pipelineName, String airflowHost, TokenProvider tokenProvider) {
        super(Type.OPEN_METADATA);
        this.http = http;
        this.uri = uri;
        this.tokenProvider = tokenProvider;
        this.pipelineServiceName = pipelineServiceName;
        this.pipelineName = pipelineName;
        this.airflowHost = airflowHost;
    }

    private static CloseableHttpClient withTimeout(Double timeout) {
        int timeoutMs;
        if (timeout == null) {
            timeoutMs = 5000;
        } else {
            timeoutMs = (int) (timeout * 1000);
        }

        RequestConfig config =
                RequestConfig.custom()
                        .setConnectTimeout(timeoutMs)
                        .setConnectionRequestTimeout(timeoutMs)
                        .setSocketTimeout(timeoutMs)
                        .build();
        return HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    }

    public OpenMetadataTransport(
            @NonNull final CloseableHttpClient httpClient, @NonNull final OpenMetadataConfig openMetadataConfig) {
        super(Type.OPEN_METADATA);
        this.http = httpClient;
        this.uri = openMetadataConfig.getUrl();
        this.tokenProvider = openMetadataConfig.getAuth();
        this.pipelineName = openMetadataConfig.getPipelineName();
        this.pipelineServiceName = openMetadataConfig.getPipelineServiceName();
        this.airflowHost = openMetadataConfig.getAirflowHost();
    }

    private Set<String> getTableNames(List<? extends OpenLineage.Dataset> datasets) {
        if (datasets == null) {
            return Collections.emptySet();
        }
        return datasets.stream().filter(d -> d.getFacets() != null && d.getFacets().getSymlinks() != null &&
                        d.getFacets().getSymlinks().getIdentifiers() != null)
                .flatMap(d -> d.getFacets().getSymlinks().getIdentifiers().stream())
                .map(i -> i.getName())
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Override
    public void emit(@NonNull OpenLineage.RunEvent runEvent) {
        try {
            if (runEvent.getEventType() != OpenLineage.RunEvent.EventType.START) {
                return;
            }

            Set<String> inputTableNames = getTableNames(runEvent.getInputs());
            inputTableNames.forEach(tableName -> {
                sendToOpenMetadata(tableName, pipelineName, LineageType.INLET);
            });

            Set<String> outputTableNames = getTableNames(runEvent.getOutputs());
            outputTableNames.forEach(tableName -> {
                sendToOpenMetadata(tableName, pipelineName, LineageType.OUTLET);
            });
        } catch (Exception e) {
            log.error("failed to emit event to OpenMetadata: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void throwOnHttpError(@NonNull HttpResponse response) throws IOException {
        final int code = response.getStatusLine().getStatusCode();
        if (code >= 400 && code < 600) { // non-2xx
            String message =
                    String.format(
                            "code: %d, response: %s", code, EntityUtils.toString(response.getEntity(), UTF_8));

            throw new OpenLineageClientException(message);
        }
    }

    private void sendToOpenMetadata(String tableName, String pipelineName, LineageType lineageType) {
        try {
            Set<String> tableIds = getTableIds(tableName);

            tableIds.forEach(tableId -> {
                String pipelineServiceId = createOrUpdatePipelineService();
                String pipelineId = createOrUpdatePipeline(pipelineServiceId, pipelineName);
                createOrUpdateLineage(pipelineId, tableId, lineageType);
                log.info("{} lineage was sent successfully to OpenMetadata for pipeline: {}, table: {}", lineageType, pipelineName, tableName);
            });
        } catch (Exception e) {
            log.error("Failed to send {} lineage to OpenMetadata for table {} pipeline {} due to: {}", lineageType, tableName, pipelineName, e.getMessage(), e);
            throw e;
        }
    }

    private Map sendRequest(HttpRequestBase request) throws IOException {
        try (CloseableHttpResponse response = http.execute(request)) {
            throwOnHttpError(response);
            String jsonResponse = EntityUtils.toString(response.getEntity());
            return fromJsonString(jsonResponse);
        }
    }

    private Set<String> getTableIds(String tableName) {
        try {
            HttpGet request = createGetTableRequest(tableName);
            Map response = sendRequest(request);
            Map<String, Object> hitsResult = (Map<String, Object>) response.get("hits");
            int totalHits = Integer.parseInt(((Map<String, Object>) hitsResult.get("total")).get("value").toString());
            if (totalHits == 0) {
                log.error("Failed to get id of table {} from OpenMetadata.", tableName);
                return Collections.emptySet();
            }
            List<Map<String, Object>> tablesData = (List<Map<String, Object>>) hitsResult.get("hits");
            return tablesData.stream().map(t -> ((Map<String, Object>) t.get("_source")).get("id").toString()).collect(Collectors.toSet());

        } catch (Exception e) {
            log.error("Failed to get id of table {} from OpenMetadata: ", tableName, e);
            throw new OpenLineageClientException(e);
        }
    }

    private String createOrUpdatePipelineService() {
        try {
            HttpPut request = createPipelineServiceRequest();
            Map response = sendRequest(request);
            return response.get("id").toString();
        } catch (Exception e) {
            log.error("Failed to create/update service pipeline {} in OpenMetadata: ", pipelineServiceName, e);
            throw new OpenLineageClientException(e);
        }
    }

    private String createOrUpdatePipeline(String pipelineServiceId, String pipelineName) {
        try {
            HttpPut request = createPipelineRequest(pipelineServiceId, pipelineName);
            Map response = sendRequest(request);
            return response.get("id").toString();
        } catch (Exception e) {
            log.error("Failed to create/update pipeline {} in OpenMetadata: ", pipelineName, e);
            throw new OpenLineageClientException(e);
        }
    }

    private void createOrUpdateLineage(String pipelineId, String tableId, LineageType lineageType) {
        try {
            HttpPut request = createLineageRequest(pipelineId, tableId, lineageType);
            sendRequest(request);
        } catch (Exception e) {
            log.error("Failed to create/update lineage in OpenMetadata for pipeline id {} and tableId {}: ", pipelineId, tableId, e);
            throw new OpenLineageClientException(e);
        }
    }

    public HttpPut createLineageRequest(String pipelineId, String tableId, LineageType lineageType) throws Exception {
        Map edgeMap = new HashMap<>();
        if (lineageType == LineageType.OUTLET) {
            edgeMap.put("fromEntity", createEntityMap("pipeline", pipelineId));
            edgeMap.put("toEntity", createEntityMap("table", tableId));
        } else {
            edgeMap.put("toEntity", createEntityMap("pipeline", pipelineId));
            edgeMap.put("fromEntity", createEntityMap("table", tableId));
        }

        Map requestMap = new HashMap<>();
        requestMap.put("edge", edgeMap);

        String jsonRequest = toJsonString(requestMap);
        return createPutRequest("/api/v1/lineage", jsonRequest);
    }

    private String toJsonString(Map map) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(map);
    }

    private Map fromJsonString(String jsonString) throws JsonProcessingException {
        if (jsonString == null || jsonString.isEmpty()) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonString, Map.class);
    }

    private Map createEntityMap(String type, String id) {
        Map entityMap = new HashMap();
        entityMap.put("type", type);
        entityMap.put("id", id);
        return entityMap;
    }

    public HttpGet createGetRequest(String path, Map<String, String> queryParams) throws Exception {
        return (HttpGet) createHttpRequest(HttpGet::new, path, queryParams);
    }

    public HttpPut createPutRequest(String path, String jsonRequest) throws URISyntaxException, MalformedURLException {
        HttpPut request = (HttpPut) createHttpRequest(HttpPut::new, path, null);
        request.setEntity(new StringEntity(jsonRequest, APPLICATION_JSON));
        return request;
    }

    private HttpRequestBase createHttpRequest(Supplier<HttpRequestBase> supplier, String path,
                                              Map<String, String> queryParams) throws URISyntaxException, MalformedURLException {
        URIBuilder uriBuilder = new URIBuilder(this.uri);
        uriBuilder.setPath(path);
        if (queryParams != null) {
            queryParams.entrySet().forEach(e -> uriBuilder.addParameter(e.getKey(), e.getValue()));
        }
        URI omUri = uriBuilder.build();
        final HttpRequestBase request = supplier.get();
        request.setURI(omUri);
        request.addHeader(ACCEPT, APPLICATION_JSON.toString());
        request.addHeader(CONTENT_TYPE, APPLICATION_JSON.toString());

        if (tokenProvider != null) {
            request.addHeader(AUTHORIZATION, tokenProvider.getToken());
        }
        return request;
    }

    public HttpPut createPipelineServiceRequest() throws Exception {
        Map requestMap = new HashMap<>();
        requestMap.put("name", pipelineServiceName);
        requestMap.put("serviceType", "Airflow");

        Map connectionConfig = new HashMap<>();
        connectionConfig.put("config", new HashMap<String, String>() {{
            put("type", "Airflow");
            put("hostPort", airflowHost);
        }});
        requestMap.put("connection", connectionConfig);
        String jsonRequest = toJsonString(requestMap);
        return createPutRequest("/api/v1/services/pipelineServices", jsonRequest);
    }

    public HttpPut createPipelineRequest(String pipelineServiceId, String pipelineName) throws Exception {
        Map requestMap = new HashMap<>();
        requestMap.put("name", pipelineName);
        requestMap.put("service", new HashMap<String, String>() {{
            put("id", pipelineServiceId);
            put("type", "pipelineService");
        }});
        String jsonRequest = toJsonString(requestMap);
        return createPutRequest("/api/v1/pipelines", jsonRequest);
    }

    public HttpGet createGetTableRequest(String tableName) throws Exception {
        String path = "api/v1/search/query";
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("size", "10");
        queryParams.put("q", "fullyQualifiedName:*" + tableName);
        return createGetRequest(path, queryParams);
    }

    @Override
    public void close() throws IOException {
        http.close();
    }
}
