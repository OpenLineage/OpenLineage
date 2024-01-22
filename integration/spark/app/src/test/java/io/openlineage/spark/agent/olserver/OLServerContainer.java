/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.olserver;

import lombok.Getter;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class OLServerContainer extends GenericContainer<OLServerContainer> {

  public OLServerContainer() {
    super(DEFAULT_IMAGE_NAME);
    this.imageName = DEFAULT_IMAGE_NAME;
  }

  static DockerImageName DEFAULT_IMAGE_NAME =
      DockerImageName.parse("quay.io/m_obuchowski/olserver");

  @Getter private DockerImageName imageName;

  @Getter private String tag = "latest";
  @Getter private int port = 80;
}
