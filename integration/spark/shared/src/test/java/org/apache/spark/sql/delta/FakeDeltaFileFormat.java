/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.spark.sql.delta;

import org.apache.spark.sql.execution.datasources.FileFormat;

/** Test-only Delta-package type that remains compatible across Spark FileFormat API versions. */
public abstract class FakeDeltaFileFormat implements FileFormat {}
