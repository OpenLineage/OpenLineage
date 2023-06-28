/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.databricks;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@AllArgsConstructor
@Builder
public class CreateCluster {
  String cluster_name;
  String spark_version;
  String node_type_id;
  Long autotermination_minutes;
  Long num_workers;
  InitScript[] init_scripts;
  Map<String, String> spark_conf;
  ClusterLogConf cluster_log_conf;
}
