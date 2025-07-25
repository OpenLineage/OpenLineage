/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.dataset.Naming;
import java.net.URI;

public interface FilesystemDatasetExtractor {
  boolean isDefinedAt(URI location);

  Naming.DatasetNaming extract(URI location);

  Naming.DatasetNaming extractWarehouseDatasetNaming(URI location, String rawName);
}
