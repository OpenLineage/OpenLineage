/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.pojo;

import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;

/**
 * Pojo Class that is used in Cassandra Visitors to initialize the Cassandra Pojo input/output
 * format.
 */
@Table(keyspace = "flink", name = "sink_event")
public class Event implements Serializable {

  public void Event() {}
}
