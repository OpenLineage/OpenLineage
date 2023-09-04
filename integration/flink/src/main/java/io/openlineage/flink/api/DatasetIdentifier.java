/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.api;

import io.openlineage.utils.DatasetIdentifierUtils;
import java.net.URI;
import lombok.Value;

@Value
public class DatasetIdentifier {
  String name;
  String namespace;

  public static DatasetIdentifier fromUri(URI uri) {
    return new DatasetIdentifier(
        DatasetIdentifierUtils.nameFromURI(uri), DatasetIdentifierUtils.namespaceFromURI(uri));
  }
}
