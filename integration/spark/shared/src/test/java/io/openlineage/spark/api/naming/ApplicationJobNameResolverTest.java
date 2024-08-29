/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.api.naming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.openlineage.spark.api.OpenLineageContext;
import org.junit.jupiter.api.Test;

class ApplicationJobNameResolverTest {
  OpenLineageContext notUsedContext = mock(OpenLineageContext.class);

  @Test
  void getJobName() {
    // We expect the second provider to be used and the name should be normalized
    ApplicationJobNameResolver resolver1 =
        new ApplicationJobNameResolver(
            ImmutableList.of(
                new TestApplicationJobNameProvider(false, "Job name from provider 1"),
                new TestApplicationJobNameProvider(true, "Job name from provider 2"),
                new TestApplicationJobNameProvider(true, "Job name from provider 3")));
    assertThat(resolver1.getJobName(notUsedContext)).isEqualTo("job_name_from_provider_2");

    // Cross-check
    ApplicationJobNameResolver resolver2 =
        new ApplicationJobNameResolver(
            ImmutableList.of(
                new TestApplicationJobNameProvider(false, "Job name from provider 1 cross-check"),
                new TestApplicationJobNameProvider(false, "Job name from provider 2 cross-check"),
                new TestApplicationJobNameProvider(true, "Job name from provider 3 cross-check"),
                new TestApplicationJobNameProvider(true, "Job name from provider 4 cross-check")));
    assertThat(resolver2.getJobName(notUsedContext))
        .isEqualTo("job_name_from_provider_3_cross_check");
  }

  static class TestApplicationJobNameProvider implements ApplicationJobNameProvider {
    private final boolean isDefinedAtResult;
    private final String returnedJobName;

    TestApplicationJobNameProvider(boolean isDefinedAtResult, String returnedJobName) {
      this.isDefinedAtResult = isDefinedAtResult;
      this.returnedJobName = returnedJobName;
    }

    @Override
    public boolean isDefinedAt(OpenLineageContext openLineageContext) {
      return isDefinedAtResult;
    }

    @Override
    public String getJobName(OpenLineageContext openLineageContext) {
      return returnedJobName;
    }
  }
}
