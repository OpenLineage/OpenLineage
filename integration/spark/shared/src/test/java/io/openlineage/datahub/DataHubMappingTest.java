package io.openlineage.datahub;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import io.openlineage.client.OpenLineage;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class DataHubMappingTest {
  @Test
  public void testSendValidDataHubEvent() throws IOException {
    RestEmitter emitter = RestEmitter.createWithDefaults();
    ObjectMapper mapped = new ObjectMapper();
    OpenLineage.RunEvent runEvent =
        mapped.readValue(
            new File("datahub/input.json"), new TypeReference<OpenLineage.RunEvent>() {});

    List<MetadataChangeProposalWrapper> collect = OpenLineageToDataHubMapper.getMCPWs(runEvent);
    collect.forEach(
        mcpw -> {
          try {
            MetadataWriteResponse metadataWriteResponse =
                emitter.emit(mcpw, new DatahubEmitterCallback(mcpw)).get();
            if (metadataWriteResponse.isSuccess() == true) {
              log.info("emiting completed");
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
