/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.dataset.Naming;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;

public abstract class ObjectStorageDatasetExtractor implements FilesystemDatasetExtractor {
  private final String scheme;

  public ObjectStorageDatasetExtractor(String scheme) {
    this.scheme = scheme;
  }

  @Override
  public boolean isDefinedAt(URI location) {
    return location.getScheme().startsWith(scheme);
  }

  @Override
  @SneakyThrows
  public Naming.DatasetNaming extract(URI location) {
    URI namespace = new URI(location.getScheme(), location.getAuthority(), null, null, null);
    String name = location.getPath();
    return extract(namespace, name);
  }

  public String getBucket(URI location) {
    return FilesystemUriSanitizer.removeLastSlash(location.getAuthority());
  }

  public String getObject(String rawName) {
    return Optional.of(rawName)
        .map(FilesystemUriSanitizer::removeFirstSlash)
        .map(FilesystemUriSanitizer::removeLastSlash)
        .map(FilesystemUriSanitizer::nonEmptyPath)
        .get();
  }

  public static class S3NameExtractor extends ObjectStorageDatasetExtractor {

    public S3NameExtractor() {
      super("s3");
    }

    @Override
    public Naming.DatasetNaming extract(URI location, String rawName) {
      return new Naming.S3(getBucket(location), getObject(rawName));
    }
  }

  public static class GCSNameExtractor extends ObjectStorageDatasetExtractor {

    public GCSNameExtractor() {
      super("gs");
    }

    @Override
    public Naming.DatasetNaming extract(URI location, String rawName) {
      String path = location.getPath();

      if (rawName == null || rawName.isEmpty()) {
        if (path != null && !path.isEmpty() && !path.equals("/")) {
          return new Naming.GCS(getBucket(location), getObject(path));
        } else {
          return new Naming.GCS(getBucket(location), getObject(rawName));
        }
      }

      if (path != null && !path.isEmpty() && !path.equals("/")) {
        String locationPath = getObject(path);
        String processedName = getObject(rawName);
        return new Naming.GCS(getBucket(location), locationPath + "/" + processedName);
      }

      return new Naming.GCS(getBucket(location), getObject(rawName));
    }
  }

  public static class WasbsNameExtractor extends ObjectStorageDatasetExtractor {
    public WasbsNameExtractor() {
      super("wasbs");
    }

    @Override
    public Naming.DatasetNaming extract(URI location, String rawName) {
      String authority = location.getAuthority();
      String container = getContainer(authority);
      String storageAccount = getStorageAccount(authority);

      String path = location.getPath();
      String objectName;

      if (path != null
          && !path.isEmpty()
          && !path.equals("/")
          && rawName != null
          && !rawName.isEmpty()) {
        String locationPath = getObject(path);
        String processedName = getObject(rawName);
        objectName = locationPath + "/" + processedName;
      } else {
        objectName = getObject(rawName);
      }

      return new Naming.WASBS(container, storageAccount, objectName);
    }

    private String getContainer(String authority) {
      if (authority != null && authority.contains("@")) {
        return authority.split("@")[0];
      }
      return authority;
    }

    private String getStorageAccount(String authority) {
      if (authority != null && authority.contains("@")) {
        String storageAccountPart = authority.split("@")[1];
        if (storageAccountPart.contains(".")) {
          return storageAccountPart.split("\\.")[0];
        }
        return storageAccountPart;
      }
      if (authority != null && authority.contains(".")) {
        return authority.split("\\.")[0];
      }
      return authority;
    }
  }
}
