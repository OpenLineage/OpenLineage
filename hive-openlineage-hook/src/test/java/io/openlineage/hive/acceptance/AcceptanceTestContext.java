/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.acceptance;

public class AcceptanceTestContext {
  final String testId;
  final String dataprocImageVersion;
  final String clusterId;
  final String testBaseGcsDir;
  final String connectorJarUri;
  final String connectorInitActionUri;
  final String project;
  final String bqDataset;

  public AcceptanceTestContext(
      String testId,
      String dataprocImageVersion,
      String clusterId,
      String testBaseGcsDir,
      String connectorJarUri,
      String connectorInitActionUri,
      String bqProject,
      String bqDataset) {
    this.testId = testId;
    this.dataprocImageVersion = dataprocImageVersion;
    this.clusterId = clusterId;
    this.testBaseGcsDir = testBaseGcsDir;
    this.connectorJarUri = connectorJarUri;
    this.connectorInitActionUri = connectorInitActionUri;
    this.project = bqProject;
    this.bqDataset = bqDataset;
  }

  public String getFileUri(String testName, String filename) {
    return testBaseGcsDir + "/" + testName + "/" + filename;
  }

  public String getOutputDirUri(String testName) {
    return testBaseGcsDir + "/" + testName + "/output";
  }

  @Override
  public String toString() {
    return String.join(
        "\n",
        "testId: " + testId,
        "dataprocImageVersion: " + dataprocImageVersion,
        "clusterId: " + clusterId,
        "testBaseGcsDir: " + testBaseGcsDir,
        "connectorJarUri: " + connectorJarUri,
        "connectorInitActionUri: " + connectorInitActionUri,
        "projectId: " + project,
        "bqDataset: " + bqDataset);
  }
}
