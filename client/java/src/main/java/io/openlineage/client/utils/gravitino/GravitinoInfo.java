/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.utils.gravitino;

import java.util.Optional;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class GravitinoInfo {
  private Optional<String> metalake;
}
