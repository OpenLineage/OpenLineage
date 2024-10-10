/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import lombok.experimental.UtilityClass;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

/**
 * Obtains and caches the account ID using the AWS SDK. The returned value is cached between
 * invocations. This could potentially cause problems when the application is using custom
 * credentials provider, but we don't support dynamic credentials providers anyway.
 */
@UtilityClass
public class AwsAccountIdFetcher {
  private static String accountId;

  public static String getAccountId() {
    if (accountId == null) {
      try (StsClient stsClient = StsClient.builder().build()) {
        GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().build();
        GetCallerIdentityResponse response = stsClient.getCallerIdentity(request);
        accountId = response.account();
      }
    }
    return accountId;
  }
}
