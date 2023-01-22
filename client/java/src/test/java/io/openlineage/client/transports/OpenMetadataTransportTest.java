/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OpenMetadataTransportTest {

    private final OpenLineage ol = new OpenLineage(URI.create("http://test.producer"));
    private final String INPUT_TABLE_NAME = "test_db.input_table1";
    private final String OUTPUT_TABLE_NAME = "test_db.output_table1";

    @Test
    void transportCreatedWithOpenMetadataConfig()
            throws URISyntaxException, NoSuchFieldException, IllegalAccessException {
        OpenMetadataConfig openMetadataConfig = new OpenMetadataConfig();
        openMetadataConfig.setUrl(new URI("http://localhost:8081"));
        openMetadataConfig.setTimeout(5000.0);
        openMetadataConfig.setAirflowHost("localhost:8080");
        openMetadataConfig.setPipelineServiceName("my-airflow-playground");
        openMetadataConfig.setPipelineName("my-pipeline");
        ApiKeyTokenProvider auth = new ApiKeyTokenProvider();
        auth.setApiKey("test");
        openMetadataConfig.setAuth(auth);
        OpenMetadataTransport openMetadataTransport = new OpenMetadataTransport(openMetadataConfig);
        Field uri = openMetadataTransport.getClass().getDeclaredField("uri");
        uri.setAccessible(true);
        String target = uri.get(openMetadataTransport).toString();
        assertEquals("http://localhost:8081", target);
    }

    @Test
    void openMetadataTransportIgnoresNonStartEventType() throws IOException {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        OpenMetadataConfig config = createConfig();
        Transport transport = new OpenMetadataTransport(http, config);
        OpenLineageClient client = new OpenLineageClient(transport);

        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(response.getStatusLine().getStatusCode()).thenReturn(200);
        when(http.execute(any(HttpUriRequest.class))).thenReturn(response);

        OpenLineage.RunEvent event = createRunEvent(OpenLineage.RunEvent.EventType.RUNNING, createInput(), createOutput());
        client.emit(event);

        verify(http, times(0)).execute(any());
    }

    @Test
    void clientEmitsOpenMetadataTransport() throws Exception {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        OpenMetadataConfig config = createConfig();
        OpenMetadataTransport transport = new OpenMetadataTransport(http, config);
        OpenLineageClient client = new OpenLineageClient(transport);

        String inputTableId = "inTable1";
        CloseableHttpResponse getResponseInput =
                createMockResponse("{ \"hits\": { \"total\": { \"value\": 1 }, \"hits\": [ { \"_source\": { \"id\": \"" + inputTableId + "\" } } ] } }");

        String pipelineServiceId = "pipelineService1";
        CloseableHttpResponse putPipelineServiceResponse = createMockResponse("{\"id\": \"" + pipelineServiceId + "\"}");

        String pipelineId = "pipeline1";
        CloseableHttpResponse putPipelineResponse = createMockResponse("{\"id\": \"" + pipelineId + "\"}");

        CloseableHttpResponse putInputLineageResponse = createMockResponse("");

        when(http.execute(any())).thenReturn(getResponseInput)
                .thenReturn(putPipelineServiceResponse)
                .thenReturn(putPipelineResponse)
                .thenReturn(putInputLineageResponse);
        OpenLineage.RunEvent event = createRunEvent(OpenLineage.RunEvent.EventType.START, createInput(), null);
        client.emit(event);

        verify(http, times(1)).execute(any(HttpGet.class));
        verify(http, times(3)).execute(any(HttpPut.class));
    }

    @Test
    void openMetadataTransportRaisesOn500() throws IOException {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        OpenMetadataConfig config = new OpenMetadataConfig();
        config.setUrl(URI.create("https://localhost:8081"));
        Transport transport = new OpenMetadataTransport(http, config);
        OpenLineageClient client = new OpenLineageClient(transport);

        CloseableHttpResponse getResponseInput = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(getResponseInput.getStatusLine().getStatusCode()).thenReturn(500);

        when(http.execute(any())).thenReturn(getResponseInput);
        OpenLineage.RunEvent event = createRunEvent(OpenLineage.RunEvent.EventType.START, createInput(), null);

        assertThrows(OpenLineageClientException.class, () -> client.emit(event));

        verify(http, times(1)).execute(any(HttpGet.class));
        verify(http, times(0)).execute(any(HttpPut.class));
    }

    @Test
    void openMetadataTransportRaisesOnConnectionFail() throws IOException {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        OpenMetadataConfig config = new OpenMetadataConfig();
        config.setUrl(URI.create("https://localhost:8081"));
        Transport transport = new OpenMetadataTransport(http, config);
        OpenLineageClient client = new OpenLineageClient(transport);

        when(http.execute(any())).thenThrow(new IOException(""));
        OpenLineage.RunEvent event = createRunEvent(OpenLineage.RunEvent.EventType.START, createInput(), null);

        assertThrows(OpenLineageClientException.class, () -> client.emit(event));

        verify(http, times(1)).execute(any(HttpGet.class));
        verify(http, times(0)).execute(any(HttpPut.class));
    }

    @Test
    void openMetadataTransportSendsAuthAndQueryParams() throws Exception {
        CloseableHttpClient http = mock(CloseableHttpClient.class);
        OpenMetadataConfig config = createConfig();
        ApiKeyTokenProvider tokenProvider = new ApiKeyTokenProvider();
        tokenProvider.setApiKey("apiKey");
        config.setAuth(tokenProvider);
        OpenMetadataTransport transport = new OpenMetadataTransport(http, config);
        OpenLineageClient client = new OpenLineageClient(transport);

        String outputTableId = "outTable1";
        CloseableHttpResponse getResponseOutput =
                createMockResponse("{ \"hits\": { \"total\": { \"value\": 1 }, \"hits\": [ { \"_source\": { \"id\": \"" + outputTableId + "\" } } ] } }");

        String pipelineServiceId = "pipelineService1";
        CloseableHttpResponse putPipelineServiceResponse = createMockResponse("{\"id\": \"" + pipelineServiceId + "\"}");

        String pipelineId = "pipeline1";
        CloseableHttpResponse putPipelineResponse = createMockResponse("{\"id\": \"" + pipelineId + "\"}");

        CloseableHttpResponse putInputLineageResponse = createMockResponse("");

        when(http.execute(any())).thenReturn(getResponseOutput)
                .thenReturn(putPipelineServiceResponse)
                .thenReturn(putPipelineResponse)
                .thenReturn(putInputLineageResponse);
        OpenLineage.RunEvent event = createRunEvent(OpenLineage.RunEvent.EventType.START, null, createOutput());

        ArgumentCaptor<HttpUriRequest> captor = ArgumentCaptor.forClass(HttpUriRequest.class);
        client.emit(event);

        verify(http, times(4)).execute(captor.capture());
        assertThat(captor.getValue().getFirstHeader("Authorization").getValue()).isEqualTo("Bearer apiKey");
        assertThat(captor.getValue().getURI()).isEqualTo(URI.create("https://localhost:8081/api/v1/lineage"));
    }

    private OpenMetadataConfig createConfig() {
        OpenMetadataConfig config = new OpenMetadataConfig();
        config.setUrl(URI.create("https://localhost:8081"));
        config.setPipelineName("my-pipeline");
        config.setPipelineServiceName("my-airflow-local");
        config.setAirflowHost("localhost:8080");
        return config;
    }

    private OpenLineage.RunEvent createRunEvent(OpenLineage.RunEvent.EventType eventType, List<OpenLineage.InputDataset> inputs, List<OpenLineage.OutputDataset> outputs) {
        OpenLineage.RunEvent runEvent =
                ol.newRunEventBuilder()
                        .eventType(eventType)
                        .inputs(inputs)
                        .outputs(outputs)
                        .build();
        return runEvent;
    }

    private List<OpenLineage.InputDataset> createInput() {
        List<OpenLineage.InputDataset> inputs =
                Arrays.asList(
                        ol.newInputDatasetBuilder()
                                .namespace("test")
                                .name("input_table")
                                .facets(createFacets(INPUT_TABLE_NAME))
                                .build());
        return inputs;
    }

    private List<OpenLineage.OutputDataset> createOutput() {
        List<OpenLineage.OutputDataset> outputs =
                Arrays.asList(
                        ol.newOutputDatasetBuilder()
                                .namespace("test")
                                .name("output_table")
                                .facets(createFacets(OUTPUT_TABLE_NAME))
                                .build());
        return outputs;
    }

    private OpenLineage.DatasetFacets createFacets(String tableName) {
        OpenLineage.DatasetFacets facets =
                ol.newDatasetFacetsBuilder()
                        .symlinks(
                                ol
                                        .newSymlinksDatasetFacetBuilder()
                                        .identifiers(
                                                Arrays.asList(
                                                        ol
                                                                .newSymlinksDatasetFacetIdentifiersBuilder()
                                                                .name(tableName)
                                                                .build())
                                        )
                                        .build()
                        )
                        .build();
        return facets;
    }

    private CloseableHttpResponse createMockResponse(String content) throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(response.getStatusLine().getStatusCode()).thenReturn(200);
        HttpEntity httpEntity = mock(HttpEntity.class, RETURNS_DEEP_STUBS);
        when(response.getEntity()).thenReturn(httpEntity);
        InputStream inputStream = new ByteArrayInputStream(content.getBytes());
        when(httpEntity.getContent()).thenReturn(inputStream);
        when(httpEntity.getContentType()).thenReturn(null);
        return response;
    }
}
