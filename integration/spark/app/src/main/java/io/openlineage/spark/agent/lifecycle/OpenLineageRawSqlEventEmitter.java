/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.spark.agent.OpenLineageSparkListener;

public class OpenLineageRawSqlEventEmitter implements RawSqlEventEmitter {

  @Override
  public void sendSqlEvent(String sqlText) {
    OpenLineageSparkListener.sendSqlEvent(sqlText);
  }
}
