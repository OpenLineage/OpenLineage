/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.transports.TokenProvider;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
public class FakeTokenProvider implements TokenProvider {
  @Setter private String token;

  @Override
  public String getToken() {
    return token;
  }
}
