/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.flink.tracker.restapi.JobDetails;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;

@Slf4j
public class FlinkRestApiClient {
  private final String jobsApiUrl;

  public FlinkRestApiClient(String jobsApiUrl) {
    this.jobsApiUrl = jobsApiUrl;
  }

  public Optional<Instant> getJobStartTime(JobID jobId) {
    String url = String.format("%s/%s", jobsApiUrl, jobId.toString());
    Optional<String> json = getApiResponse(url, "Flink job details");
    if (json.isEmpty()) {
      return Optional.empty();
    }

    try {
      return Optional.of(
              new ObjectMapper()
                  .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                  .readValue(json.get(), JobDetails.class))
          .map(JobDetails::getStartTime)
          .filter(startTime -> startTime > 0)
          .map(Instant::ofEpochMilli);
    } catch (IOException e) {
      log.warn("Failed parsing Flink job details", e);
      return Optional.empty();
    }
  }

  private Optional<String> getApiResponse(String url, String description) {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      String json =
          httpClient.execute(
              new HttpGet(url), response -> EntityUtils.toString(response.getEntity()));
      return Optional.of(json);
    } catch (IOException e) {
      log.warn("Failed reading {} from {}", description, url, e);
      return Optional.empty();
    }
  }
}
