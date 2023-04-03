package io.openlineage.datahub;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.Downstream;
import com.linkedin.dataset.DownstreamArray;
import com.linkedin.dataset.DownstreamLineage;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.datahub.OpenLineageToDataHubMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


@Slf4j
public class DataHubMappingTest {
    @Test
    public void testSendValidDataHubEvent() throws IOException, URISyntaxException {
        RestEmitter emitter = RestEmitter.createWithDefaults();
        ObjectMapper mapped = new ObjectMapper();
        OpenLineage.RunEvent runEvent = mapped.readValue(new File("datahub/input.json"), new TypeReference<OpenLineage.RunEvent>() {});

        List<MetadataChangeProposalWrapper> collect = OpenLineageToDataHubMapper.getMCPWs(runEvent);
        collect.forEach(mcpw -> {
            try {
                Future<MetadataWriteResponse> emit = emitter.emit(mcpw, new DatahubEmitterCallback(mcpw));
                boolean success = emit.get().isSuccess();
                log.info("emiting completed");
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

//        DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("dupa1"),  "dupa2", FabricType.NON_PROD);
//        String entityUrn1 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table1,PROD)";
//        String entityUrn2 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table2,PROD)";
//        String entityUrn3 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table3,PROD)";
//        String entityUrn4 = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table4,PROD)";
//        
//        Upstream upstream1 = new Upstream().setDataset(DatasetUrn.createFromString(entityUrn1)).setType(DatasetLineageType.TRANSFORMED);
//        Upstream upstream2 = new Upstream().setDataset(DatasetUrn.createFromString(entityUrn2)).setType(DatasetLineageType.TRANSFORMED);
//      
//        UpstreamArray upstreams = new UpstreamArray();
//        upstreams.add(upstream1);
//        upstreams.add(upstream2);
//
//        Downstream downstream1 = new Downstream().setDataset(DatasetUrn.createFromString(entityUrn4));
//        
//        DownstreamArray downstreams = new DownstreamArray();
//        downstreams.add(downstream1);
        
//        MetadataChangeProposalWrapper mcpw1 = getEntityDescription(entityUrn1);
//        MetadataChangeProposalWrapper mcpw2 = getEntityDescription(entityUrn2);
//        MetadataChangeProposalWrapper mcpw3 = getEntityDescription(entityUrn3);
//        MetadataChangeProposalWrapper mcpw4 = getEntityUpstreams(entityUrn3, upstreams);
//        MetadataChangeProposalWrapper mcpw5 = getEntityDownstreams(entityUrn4, downstreams);
//        
//        Callback callback1 = getCallback(mcpw1);
//        Callback callback2 = getCallback(mcpw2);
//        Callback callback3 = getCallback(mcpw3);
//        Callback callback4 = getCallback(mcpw4);
//        Callback callback5 = getCallback(mcpw5);
        
//        emitter.emit(mcpw1, callback1);
//        emitter.emit(mcpw2, callback2);
//        emitter.emit(mcpw3, callback3);
//        emitter.emit(mcpw4 , callback4);
//        emitter.emit(mcpw5, callback5);



    }


    private static MetadataChangeProposalWrapper getEntityDescription(String entityUrn) {
        MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
                .entityType("dataset")
                .entityUrn(entityUrn)
                .upsert()
                .aspect(new DatasetProperties().setDescription("This is the canonical User profile dataset"))
                .build();
        return mcpw;
    }
    
    private static MetadataChangeProposalWrapper getEntityUpstreams(String entityUrn, UpstreamArray upstreams) {
        MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
                .entityType("dataset")
                .entityUrn(entityUrn)
                .upsert()
                .aspect(new UpstreamLineage().setUpstreams(upstreams))
                .build();
        return mcpw;
    }
    
    private static MetadataChangeProposalWrapper getEntityDownstreams(String entityUrn, DownstreamArray downstreams) {
        DownstreamLineage aspect = new DownstreamLineage().setDownstreams(downstreams);
        MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
                .entityType("dataset")
                .entityUrn(entityUrn)
                .upsert()
                .aspect(aspect)
                .build();
        return mcpw;
    }

}
