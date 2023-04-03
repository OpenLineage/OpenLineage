package io.openlineage.datahub;

import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import datahub.event.MetadataChangeProposalWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
@Slf4j
public class DatahubEmitterCallback implements Callback {

    MetadataChangeProposalWrapper mcpw;
    
    public DatahubEmitterCallback(MetadataChangeProposalWrapper mcpw){
        this.mcpw = mcpw;
    }
    
    @Override
    public void onCompletion(MetadataWriteResponse response) {
        if (response.isSuccess()) {
            log.info(String.format("Successfully emitted metadata event for %s", mcpw.getEntityUrn()));
        } else {
            // Get the underlying http response
            HttpResponse httpResponse = (HttpResponse) response.getUnderlyingResponse();
            log.info(String.format("Failed to emit metadata event for %s, aspect: %s with status code: %d",
                    mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));
        }
    }

    @Override
    public void onFailure(Throwable exception) {
        log.info(
                String.format("Failed to emit metadata event for %s, aspect: %s due to %s", mcpw.getEntityUrn(),
                        mcpw.getAspectName(), exception.getMessage()));
    }
}
