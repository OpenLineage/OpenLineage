/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * A local file system that answers to OneLake paths, {@code
 * abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/...}, so tests can run Spark against
 * paths that look like Fabric's. Files are kept under {@link #ROOT_CONF}, in a folder per
 * workspace. Like Fabric, relative paths are in the folder named by {@link #WORKING_DIR_CONF},
 * normally the default lakehouse's ID.
 */
public class FakeOneLakeFileSystem extends RawLocalFileSystem {
  public static final String ROOT_CONF = "fs.fakeonelake.root";
  public static final String WORKING_DIR_CONF = "fs.fakeonelake.workingDir";

  private URI uri;
  private File root;
  private Path workingDir;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    uri = URI.create(name.getScheme() + "://" + name.getAuthority());
    root = new File(conf.get(ROOT_CONF, "/tmp/fake_onelake"), name.getAuthority());
    workingDir = new Path(uri.toString() + "/" + conf.get(WORKING_DIR_CONF, ""));
  }

  @Override
  public String getScheme() {
    return "abfss";
  }

  @Override
  public URI getUri() {
    // RawLocalFileSystem's constructor asks before initialize() is called
    return uri == null ? super.getUri() : uri;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir == null ? super.getWorkingDirectory() : workingDir;
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    if (uri != null) {
      workingDir = makeQualified(dir);
    }
  }

  @Override
  public Path getHomeDirectory() {
    return getWorkingDirectory();
  }

  @Override
  public File pathToFile(Path path) {
    if (root == null) {
      return super.pathToFile(path);
    }
    Path absolute = path.isAbsolute() ? path : new Path(workingDir, path);
    return new File(root, absolute.toUri().getPath());
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    FileStatus status = super.getFileStatus(f);
    status.setPath(makeQualified(f));
    return status;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Path qualified = makeQualified(f);
    if (!pathToFile(qualified).exists()) {
      throw new FileNotFoundException(qualified.toString());
    }
    FileStatus[] statuses = super.listStatus(f);
    boolean isFile = pathToFile(qualified).isFile();
    for (FileStatus status : statuses) {
      status.setPath(isFile ? qualified : new Path(qualified, status.getPath().getName()));
    }
    return statuses;
  }
}
