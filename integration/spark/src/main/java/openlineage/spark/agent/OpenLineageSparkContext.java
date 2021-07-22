package openlineage.spark.agent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ForkJoinPool;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import openlineage.spark.agent.client.LineageEvent;
import openlineage.spark.agent.client.OpenLineageClient;
import openlineage.spark.agent.client.OpenLineageHttpException;
import openlineage.spark.agent.client.ResponseMessage;

@Slf4j
public class OpenLineageSparkContext {
  @Getter private OpenLineageClient client;
  @Getter private URI lineageURI;
  @Getter private String jobNamespace;
  @Getter private String parentJobName;
  @Getter private String parentRunId;

  private final ObjectMapper mapper = OpenLineageClient.createMapper();

  public OpenLineageSparkContext(ArgumentParser argument) throws URISyntaxException {
    this.client = OpenLineageClient.create(argument.getApiKey(), ForkJoinPool.commonPool());
    this.lineageURI =
        new URI(String.format("%s/api/%s/lineage", argument.getHost(), argument.getVersion()));
    this.jobNamespace = argument.getNamespace();
    this.parentJobName = argument.getJobName();
    this.parentRunId = argument.getRunId();
    log.info(
        String.format(
            "Init OpenLineageSparkContext: Args: %s URI: %s", argument, lineageURI.toString()));
  }

  public void emit(LineageEvent event) {
    try {
      // Todo: move to async client
      log.debug("Posting LineageEvent {}", event);
      ResponseMessage resp = client.post(lineageURI, event);
      if (!resp.completedSuccessfully()) {
        log.error(
            "Could not emit lineage: {}",
            mapper.writeValueAsString(event),
            new OpenLineageHttpException(resp, resp.getError()));
      } else {
        log.info("Lineage completed successfully: {} {}", resp, mapper.writeValueAsString(event));
      }
    } catch (OpenLineageHttpException | JsonProcessingException e) {
      log.error("Could not emit lineage w/ exception", e);
    }
  }

  public void close() {
    client.close();
  }
}
