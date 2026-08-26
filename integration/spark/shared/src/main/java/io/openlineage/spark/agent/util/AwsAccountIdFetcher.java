/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.time.Duration;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

/**
 * Obtains and caches the account ID using the AWS SDK. The returned value is cached between
 * invocations. This could potentially cause problems when the application is using custom
 * credentials provider, but we don't support dynamic credentials providers anyway.
 */
@Slf4j
@UtilityClass
public class AwsAccountIdFetcher {
  private static final String ACCOUNT_ID_CACHE_KEY = "aws.sts.account-id";
  private static final Duration NEGATIVE_CACHE_TTL = Duration.ofMinutes(1);
  private static final ApplicationMetadataCache PROCESS_CACHE = new ApplicationMetadataCache();

  public static String getAccountId() {
    return PROCESS_CACHE.get(ACCOUNT_ID_CACHE_KEY, AwsAccountIdFetcher::fetchAccountId);
  }

  public static String getAccountId(SparkContext sparkContext) {
    return getAccountIdOptional(sparkContext).orElse(null);
  }

  public static Optional<String> getAccountIdOptional(SparkContext sparkContext) {
    return ApplicationMetadataCache.forSparkContext(sparkContext)
        .getOptional(
            ACCOUNT_ID_CACHE_KEY,
            () -> {
              try {
                return Optional.ofNullable(fetchAccountId());
              } catch (Exception e) {
                log.warn("Unable to retrieve AWS account ID.", e);
                return Optional.empty();
              }
            },
            NEGATIVE_CACHE_TTL);
  }

  private static String fetchAccountId() {
    log.info("Building STS client.");
    try (StsClient stsClient =
        StsClient.builder().httpClient(UrlConnectionHttpClient.builder().build()).build()) {
      GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().build();
      GetCallerIdentityResponse response = stsClient.getCallerIdentity(request);
      String accountId = response.account();
      log.info("Retrieved account ID [{}].", accountId);
      return accountId;
    }
  }
}
