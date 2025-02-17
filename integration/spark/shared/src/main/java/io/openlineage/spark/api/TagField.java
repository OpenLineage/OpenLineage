/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class TagField {

  public TagField(String key) {
    this(key, "true", "CONFIG");
  }

  public TagField(String key, String value) {
    this(key, value, "CONFIG");
  }

  private String key;
  private String value;
  private String source;
}
