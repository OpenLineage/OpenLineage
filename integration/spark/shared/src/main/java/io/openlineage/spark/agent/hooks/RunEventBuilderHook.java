/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.hooks;

import io.openlineage.client.OpenLineage;

/**
 * Definition of a hook method called before running `build` method on {@link
 * OpenLineage.RunEventBuilder}.
 */
public interface RunEventBuilderHook {

  void preBuild(OpenLineage.RunEventBuilder runEventBuilder);
}
