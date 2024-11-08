/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
}
