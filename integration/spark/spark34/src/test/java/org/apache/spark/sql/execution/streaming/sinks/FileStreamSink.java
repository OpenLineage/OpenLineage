/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.spark.sql.execution.streaming.sinks;

import org.apache.spark.sql.execution.streaming.Sink;

/** Stand-in for the Spark 4.1+ location of FileStreamSink, absent from the 3.4 classpath. */
public abstract class FileStreamSink implements Sink {}
