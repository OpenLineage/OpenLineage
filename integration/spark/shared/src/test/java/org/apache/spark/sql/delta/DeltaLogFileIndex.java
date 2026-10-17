/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.spark.sql.delta;

import org.apache.spark.sql.execution.datasources.FileIndex;

/** Test fixture for the optional Delta-owned index, without a Delta dependency in shared. */
public abstract class DeltaLogFileIndex implements FileIndex {}
