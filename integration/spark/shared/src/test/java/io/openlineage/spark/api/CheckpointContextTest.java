/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CheckpointContextTest {

  @Test
  void testOldestCheckpointIsEvictedOnceMaxEntriesIsExceeded() {
    CheckpointContext context = new CheckpointContext(2);
    context.addCheckpoint(1, new Checkpoint(null, null));
    context.addCheckpoint(2, new Checkpoint(null, null));
    context.addCheckpoint(3, new Checkpoint(null, null));

    assertThat(context.getCheckpoint(1)).isEmpty();
    assertThat(context.getCheckpoint(2)).isPresent();
    assertThat(context.getCheckpoint(3)).isPresent();
  }

  @Test
  void testClaimIsKeptWhenCheckpointIsEvicted() {
    CheckpointContext context = new CheckpointContext(1);
    context.claim(1);
    context.addCheckpoint(1, new Checkpoint(null, null));
    context.claim(2);
    context.addCheckpoint(2, new Checkpoint(null, null));

    assertThat(context.getCheckpoint(1)).isEmpty();
    assertThat(context.isClaimed(1)).isTrue();
  }

  @Test
  void testRemoveRddDropsCheckpointAndClaim() {
    CheckpointContext context = new CheckpointContext();
    context.claim(1);
    context.addCheckpoint(1, new Checkpoint(null, null));

    context.removeRdd(1);

    assertThat(context.getCheckpoint(1)).isEmpty();
    assertThat(context.isClaimed(1)).isFalse();
  }
}
