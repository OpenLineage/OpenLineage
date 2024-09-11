/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.util;

import io.openlineage.client.OpenLineage.RunEvent.EventType;
import org.apache.flink.api.common.JobStatus;

public class JobStatusUtil {

  /**
   * Translates Flink's {@link JobStatus} into {@link EventType}
   *
   * @param jobStatus
   * @return
   */
  public static EventType fromJobStatus(JobStatus jobStatus) {
    if (jobStatus.isGloballyTerminalState()) {
      // termination status
      if (jobStatus.equals(JobStatus.FINISHED)) {
        return EventType.COMPLETE;
      }
      return EventType.FAIL;
    }

    if (jobStatus.equals(JobStatus.INITIALIZING)) {
      return EventType.START;
    }

    return EventType.RUNNING;
  }
}
