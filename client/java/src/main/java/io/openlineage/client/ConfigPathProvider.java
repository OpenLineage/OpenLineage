/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import java.nio.file.Path;
import java.util.List;

public interface ConfigPathProvider {
  List<Path> getPaths();
}
