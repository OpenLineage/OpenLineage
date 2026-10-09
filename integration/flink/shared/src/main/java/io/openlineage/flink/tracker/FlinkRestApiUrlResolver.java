/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestOptions;

/**
 * Resolves the base URL of the Flink JobManager REST API ({@code .../jobs}) used by the checkpoint
 * tracker ({@link OpenLineageContinousJobTracker}) and {@link FlinkRestApiClient}.
 *
 * <p>By default this is derived from the statically configured {@link RestOptions#ADDRESS} and
 * {@link RestOptions#PORT}. However, the port Flink actually binds to at runtime can differ from
 * the configured {@code rest.port} - for example when {@code rest.port}/{@code rest.bind-port} is
 * set to {@code 0} (random free port) or a range, or when the configured port is already in use and
 * Flink falls back to another one. This is especially common in Kubernetes Application mode
 * deployments. Flink does not expose the effective bound port back through {@link ReadableConfig}
 * or through the {@code JobStatusChangedListenerFactory.Context} available to this plugin, so it
 * cannot be discovered automatically from here.
 *
 * <p>To handle that case, {@link #CHECKPOINT_TRACKING_REST_URL} lets users explicitly point
 * checkpoint tracking at the effective REST endpoint, overriding the value derived from {@code
 * rest.address}/{@code rest.port}.
 */
@Slf4j
public final class FlinkRestApiUrlResolver {

  public static final ConfigOption<String> CHECKPOINT_TRACKING_REST_URL =
      ConfigOptions.key("openlineage.flink.checkpointTrackingRestUrl")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Overrides the Flink REST API base URL (e.g. http://localhost:44193) used by the "
                  + "OpenLineage checkpoint tracker. Set this when the effective REST port bound "
                  + "at runtime differs from the configured rest.port (for example when rest.port "
                  + "/ rest.bind-port is 0, a range, or already in use), such as in Kubernetes "
                  + "Application mode deployments.");

  private FlinkRestApiUrlResolver() {}

  /**
   * Resolves the {@code .../jobs} base URL to poll for checkpoint statistics.
   *
   * @param config Flink configuration, potentially containing {@link #CHECKPOINT_TRACKING_REST_URL}
   * @return the resolved base URL, without a trailing slash, ending in {@code /jobs}
   */
  public static String resolveJobsApiUrl(ReadableConfig config) {
    Optional<String> override = Optional.ofNullable(config.get(CHECKPOINT_TRACKING_REST_URL));
    if (override.isPresent() && StringUtils.isNotBlank(override.get())) {
      String baseUrl = StringUtils.removeEnd(override.get().trim(), "/");
      String url = baseUrl + "/jobs";
      log.info(
          "Using {}={} override for checkpoint tracking REST API URL: {}",
          CHECKPOINT_TRACKING_REST_URL.key(),
          baseUrl,
          url);
      return url;
    }

    String url =
        String.format(
            "http://%s:%s/jobs",
            Optional.ofNullable(config.get(RestOptions.ADDRESS)).orElse("localhost"),
            config.get(RestOptions.PORT));
    log.debug(
        "{} is not set; resolved checkpoint tracking REST API URL from configured "
            + "rest.address/rest.port: {}. Note this may not match the effective bound port in "
            + "Flink Application mode - set {} to override it.",
        CHECKPOINT_TRACKING_REST_URL.key(),
        url,
        CHECKPOINT_TRACKING_REST_URL.key());
    return url;
  }
}
