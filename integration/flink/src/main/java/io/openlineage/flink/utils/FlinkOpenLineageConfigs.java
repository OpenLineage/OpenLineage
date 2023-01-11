/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

public enum FlinkOpenLineageConfigs {
  FLINK_CONF_URL_KEY("openlineage.url"),
  FLINK_CONF_HOST_KEY("openlineage.host"),
  FLINK_CONF_API_VERSION_KEY("openlineage.version"),
  FLINK_CONF_NAMESPACE_KEY("openlineage.namespace"),
  FLINK_CONF_JOB_NAME_KEY("openlineage.parentJobName"),
  FLINK_CONF_PARENT_RUN_ID_KEY("openlineage.parentRunId"),
  FLINK_CONF_API_KEY("openlineage.apiKey"),
  FLINK_CONF_URL_PARAM_PREFIX("openlineage.url.param");

  private final String configPath;

  FlinkOpenLineageConfigs(String configPath) {
    this.configPath = configPath;
  }

  public String getConfigPath() {
    return configPath;
  }
}
