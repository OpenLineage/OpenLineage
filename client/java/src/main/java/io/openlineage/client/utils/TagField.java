/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
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
