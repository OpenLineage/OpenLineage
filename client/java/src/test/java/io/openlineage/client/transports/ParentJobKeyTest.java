/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.runEvent;
import static io.openlineage.client.Events.runEventWithParentJobFacet;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ParentJobKeyTest {

  @Test
  void resolveReturnsNullWhenJobHasNoFacets() {
    assertThat(ParentJobKey.resolve(runEvent().getJob())).isNull();
  }

  @Test
  void resolveReturnsParentJobWhenThereIsNoRoot() {
    assertThat(ParentJobKey.resolve(runEventWithParentJobFacet(false).getJob()))
        .isEqualTo("run:parent-namespace/parent-job");
  }

  @Test
  void resolvePrefersRootJob() {
    assertThat(ParentJobKey.resolve(runEventWithParentJobFacet(true).getJob()))
        .isEqualTo("run:root-namespace/root-job");
  }
}
