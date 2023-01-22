/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import lombok.*;

import javax.annotation.Nullable;
import java.net.URI;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@With
public final class OpenMetadataConfig implements TransportConfig {
    @Getter @Setter private URI url;
    @Getter @Setter private @Nullable Double timeout;
    @Getter @Setter private @Nullable TokenProvider auth;
    @Getter @Setter private String pipelineServiceName;
    @Getter @Setter private String pipelineName;
    @Getter @Setter private String airflowHost;
}