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

import com.google.common.base.Strings;

public class AcceptanceTestConstants {

  public static final String REGION = "us-west1";
  public static final String ZONE = REGION + "-a";
  public static final String DATAPROC_ENDPOINT = REGION + "-dataproc.googleapis.com:443";
  public static final String EVENTS_LOG_FILE = "/tmp/openlineage-events.log";
  public static final String CLUSTER_NAME_PREFIX = "hive-lineage-acceptance-";

  public static final boolean CLEAN_UP_CLUSTER =
      Strings.isNullOrEmpty(System.getenv("CLEAN_UP_CLUSTER"))
          ? true
          : Boolean.parseBoolean(System.getenv("CLEAN_UP_CLUSTER"));
  public static final boolean CLEAN_UP_BQ =
      Strings.isNullOrEmpty(System.getenv("CLEAN_UP_BQ"))
          ? true
          : Boolean.parseBoolean(System.getenv("CLEAN_UP_BQ"));
  public static final boolean CLEAN_UP_GCS =
      Strings.isNullOrEmpty(System.getenv("CLEAN_UP_GCS"))
          ? true
          : Boolean.parseBoolean(System.getenv("CLEAN_UP_GCS"));

  public static final String JAR_DIRECTORY = "build/libs";
  public static final String JAR_PREFIX = "hive-openlineage-hook-shaded";
  public static final String INIT_ACTION_SCRIPT = "install_jars.sh";
  public static final String INIT_ACTION_PATH = "/acceptance/" + INIT_ACTION_SCRIPT;
  protected static final long ACCEPTANCE_TEST_TIMEOUT_IN_SECONDS = 600;
}
