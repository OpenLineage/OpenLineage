/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.RunEvent.EventType;
import org.apache.flink.api.common.JobStatus;
import org.junit.jupiter.api.Test;

/** Test class for {@link JobStatusUtil} */
class JobStatusUtilTest {

  @Test
  void testFromJobStatus() {
    assertThat(JobStatusUtil.fromJobStatus(JobStatus.INITIALIZING)).isEqualTo(EventType.START);

    assertThat(JobStatusUtil.fromJobStatus(JobStatus.CANCELED)).isEqualTo(EventType.FAIL);
    assertThat(JobStatusUtil.fromJobStatus(JobStatus.FAILED)).isEqualTo(EventType.FAIL);

    assertThat(JobStatusUtil.fromJobStatus(JobStatus.FINISHED)).isEqualTo(EventType.COMPLETE);

    assertThat(JobStatusUtil.fromJobStatus(JobStatus.CREATED)).isEqualTo(EventType.RUNNING);
  }
}
