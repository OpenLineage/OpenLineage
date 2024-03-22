/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineage.OwnershipJobFacetOwners;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

public class FlinkExecutionContextTest {

  Configuration config = new Configuration();

  @Test
  void testBuildEventForEventTypeWithJobOwnershipFacet() {
    ConfigOption transportTypeOption =
        ConfigOptions.key("openlineage.transport.type").mapType().noDefaultValue();

    config.setString(transportTypeOption, "console");
    config.setString("openlineage.job.owners.team", "MyTeam");
    config.setString("openlineage.job.owners.person", "John Smith");

    FlinkExecutionContext context =
        FlinkExecutionContextFactory.getContext(
            config,
            "jobNamespace",
            "jobName",
            mock(JobID.class),
            "streaming",
            Collections.emptyList());

    RunEvent runEvent = context.buildEventForEventType(EventType.COMPLETE).build();

    List<OwnershipJobFacetOwners> owners = runEvent.getJob().getFacets().getOwnership().getOwners();
    assertThat(owners).hasSize(2);
    assertThat(owners.stream().filter(o -> o.getType().equals("team")).findAny().get().getName())
        .isEqualTo("MyTeam");
    assertThat(owners.stream().filter(o -> o.getType().equals("person")).findAny().get().getName())
        .isEqualTo("John Smith");
  }

  @Test
  void testBuildEventForEventTypeWithNoJobOwnersConfig() {
    ConfigOption transportTypeOption =
        ConfigOptions.key("openlineage.transport.type").mapType().noDefaultValue();

    config.setString(transportTypeOption, "console");

    FlinkExecutionContext context =
        FlinkExecutionContextFactory.getContext(
            config,
            "jobNamespace",
            "jobName",
            mock(JobID.class),
            "streaming",
            Collections.emptyList());

    assertThat(
            context
                .buildEventForEventType(EventType.COMPLETE)
                .build()
                .getJob()
                .getFacets()
                .getOwnership())
        .isNull();
  }
}
