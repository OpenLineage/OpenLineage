/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.spark.sql.delta;

import org.apache.spark.sql.sources.CreatableRelationProvider;

/** Test-only Delta-package provider that remains compatible across Spark API versions. */
public abstract class FakeDeltaProvider implements CreatableRelationProvider {}
