package io.openlineage.spark.agent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.client.OpenLineageHttpException;
import io.openlineage.spark.agent.client.ResponseMessage;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventEmitter {
  @Getter private OpenLineageClient client;
  @Getter private URI lineageURI;
  @Getter private String jobNamespace;
  @Getter private String parentJobName;
  @Getter private Optional<UUID> parentRunId;

  private final ObjectMapper mapper = OpenLineageClient.createMapper();

  public EventEmitter(ArgumentParser argument) throws URISyntaxException {
    this.client = OpenLineageClient.create(argument.getApiKey(), ForkJoinPool.commonPool());
    // Extract url parameters other than api_key to append to lineageURI
    String queryParams = null;
    if (argument.getUrlParams().isPresent()) {
      Map<String, String> urlParams = argument.getUrlParams().get();

      StringJoiner query = new StringJoiner("&");
      urlParams.forEach((k, v) -> query.add(k + "=" + v));

      queryParams = query.toString();
    }

    // Convert host to a URI to extract scheme and authority
    URI hostURI = new URI(argument.getHost());
    String uriPath = String.format("/api/%s/lineage", argument.getVersion());

    this.lineageURI =
        new URI(hostURI.getScheme(), hostURI.getAuthority(), uriPath, queryParams, null);
    this.jobNamespace = argument.getNamespace();
    this.parentJobName = argument.getJobName();
    this.parentRunId = convertToUUID(argument.getParentRunId());
    log.info(
        String.format(
            "Init OpenLineageContext: Args: %s URI: %s", argument, lineageURI.toString()));
  }

  public void emit(OpenLineage.RunEvent event) {
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

  private static Optional<UUID> convertToUUID(String uuid) {
    try {
      return Optional.ofNullable(uuid).map(UUID::fromString);
    } catch (Exception e) {
      return Optional.empty();
    }
  }
}
