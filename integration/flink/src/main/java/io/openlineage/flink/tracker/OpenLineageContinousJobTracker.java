/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.flink.agent.facets.CheckpointFacet;
import io.openlineage.flink.agent.lifecycle.FlinkExecutionContext;
import io.openlineage.flink.tracker.restapi.Checkpoints;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestOptions;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;

@Slf4j
/**
 * Tracker class which spawns extra thread which call Flink Rest API endpoint to collect some
 * OpenLineage information.
 */
public class OpenLineageContinousJobTracker {

  private final ReadableConfig config;
  private final Duration trackingInterval;
  private Thread trackingThread;
  private Optional<Checkpoints> latestCheckpoints = Optional.empty();
  private boolean shouldContinue = true;

  public OpenLineageContinousJobTracker(ReadableConfig config, Duration trackingInterval) {
    this.config = config;
    this.trackingInterval = trackingInterval;
  }

  /**
   * Starts tracking flink JOB rest API
   *
   * @param context flink execution context
   */
  public void startTracking(FlinkExecutionContext context) {
    CloseableHttpClient httpClient = HttpClients.createDefault();

    String checkpointApiUrl =
        String.format(
            "http://%s:%s/jobs/%s/checkpoints",
            config.get(RestOptions.ADDRESS),
            config.get(RestOptions.PORT),
            context.getJobId().toString());
    HttpGet request = new HttpGet(checkpointApiUrl);

    trackingThread =
        (new Thread(
            () -> {
              while (shouldContinue) {
                try {
                  Thread.sleep(trackingInterval.toMillis());

                  CloseableHttpResponse response = httpClient.execute(request);
                  String json = EntityUtils.toString(response.getEntity());

                  Optional.of(
                          new ObjectMapper()
                              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                              .readValue(json, Checkpoints.class))
                      .filter(
                          newCheckpoints ->
                              latestCheckpoints.isEmpty()
                                  || latestCheckpoints.get().getCounts().getTotal()
                                      != newCheckpoints.getCounts().getTotal())
                      .ifPresentOrElse(
                          newCheckpoints -> emitNewCheckpointEvent(context, newCheckpoints),
                          () -> log.info("no new checkpoint found"));
                } catch (IOException | ParseException e) {
                  log.error("Connecting REST API failed", e);
                } catch (InterruptedException e) {
                  log.info("Stopping tracker thread", e);
                  shouldContinue = false;
                } catch (Exception e) {
                  log.error("tracker thread failed due not unknown exception", e);
                  shouldContinue = false;
                }
              }
            }));
    trackingThread.start();
  }

  private void emitNewCheckpointEvent(FlinkExecutionContext context, Checkpoints newCheckpoints) {
    log.info(
        "New checkpoint encountered total-checkpoint:{}", newCheckpoints.getCounts().getTotal());
    latestCheckpoints = Optional.of(newCheckpoints);

    context.onJobCheckpoint(
        new CheckpointFacet(
            newCheckpoints.getCounts().getCompleted(),
            newCheckpoints.getCounts().getFailed(),
            newCheckpoints.getCounts().getIn_progress(),
            newCheckpoints.getCounts().getRestored(),
            newCheckpoints.getCounts().getTotal()));
  }

  /** Stops the tracking thread */
  public void stopTracking() {
    log.info("stop tracking");
    shouldContinue = false;
  }
}
