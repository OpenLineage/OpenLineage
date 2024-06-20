/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;

public interface FilesystemDatasetExtractor {
  public abstract boolean isDefinedAt(URI location);

  public abstract DatasetIdentifier extract(URI location);

  public abstract DatasetIdentifier extract(URI location, String rawName);
}
