/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.hive.client.HiveOpenLineageConfig;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;

/**
 * Context holder with references to several required objects during construction of an OpenLineage
 * {@link OpenLineage.RunEvent}. An {@link OpenLineageContext} should be created once for every
 * detected Hive job execution.
 *
 * <p>This API is evolving and may change in future releases
 *
 * @apiNote This interface is evolving and may change in future releases
 */
@Value
@Builder
public class OpenLineageContext {
  UUID runUuid = UUID.randomUUID();

  /**
   * A non-null, preconfigured {@link OpenLineage} client instance for constructing OpenLineage
   * model objects
   */
  @NonNull OpenLineage openLineage;

  SemanticAnalyzer semanticAnalyzer;

  @NonNull @Getter HiveOpenLineageConfig openLineageConfig;

  @NonNull String openlineageHiveIntegrationVersion;

  @NonNull String queryString;

  @NonNull Configuration hadoopConf;

  @NonNull Set<ReadEntity> readEntities;

  @NonNull Set<WriteEntity> writeEntities;

  @NonNull ZonedDateTime eventTime;

  @NonNull OpenLineage.RunEvent.EventType eventType;
}
