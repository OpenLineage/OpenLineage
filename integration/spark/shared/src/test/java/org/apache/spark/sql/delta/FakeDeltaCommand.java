/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.spark.sql.delta;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Stand-in for Delta command nodes (MergeIntoCommand, DeleteCommand, UpdateCommand, WriteIntoDelta,
 * ...) that live in the org.apache.spark.sql.delta package. Mockito subclass mocks are generated in
 * the mocked type's package, so mock(FakeDeltaCommand.class) exercises the delta-class-name
 * write-root check.
 */
public abstract class FakeDeltaCommand extends LogicalPlan {}
