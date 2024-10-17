/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.api.naming;

import com.google.common.collect.ImmutableList;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The ApplicationJobNameResolver class is responsible for resolving the application job name by
 * iterating over a list of ApplicationJobNameProvider instances. It uses the first provider that
 * can supply a job name for the given OpenLineageContext.
 */
@AllArgsConstructor
@Slf4j
class ApplicationJobNameResolver {
  private final List<ApplicationJobNameProvider> applicationJobNameProviders;

  /**
   * Retrieves the job name from the first provider that is able to define it for the given
   * OpenLineageContext. The name is then normalized to snake_case.
   *
   * @return the normalized job name.
   */
  public String getJobName(OpenLineageContext olContext) {
    return applicationJobNameProviders.stream()
        .filter(provider -> provider.isDefinedAt(olContext))
        .findFirst()
        .map(provider -> provider.getJobName(olContext))
        .map(ApplicationJobNameResolver::normalizeName)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "None of the job providers was able to provide the job name. The number of job providers is "
                        + applicationJobNameProviders.size()));
  }

  /**
   * Constructs and returns a list of ApplicationJobNameProvider instances. The list is ordered such
   * that a user-specified job name is prioritized, followed by environment-specific providers (such
   * as AWS Glue), and finally a universal provider that works in any Spark environment.
   */
  public static List<ApplicationJobNameProvider> buildProvidersList() {
    return ImmutableList.of(
        new OpenLineageAppNameApplicationJobNameProvider(),
        new AwsGlueApplicationJobNameProvider(),
        new SparkApplicationNameApplicationJobNameProvider());
  }

  /**
   * Normalizes a given job name by converting CamelCase to snake_case and replacing all
   * non-alphanumeric characters with underscores ('_').
   */
  private static String normalizeName(String name) {
    String normalizedName = NameNormalizer.normalize(name);
    log.debug("The application name [{}] has been normalized to [{}]", name, normalizedName);
    return normalizedName;
  }
}
