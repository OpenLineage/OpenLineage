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

  public abstract Naming.DatasetNaming extract(URI location, String rawName);

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

    @Override
    public Naming.DatasetNaming extractWarehouseDatasetNaming(URI location, String rawName) {
      String warehouseLocation = location.toString();
      // Normalize s3a/s3n to s3
      if (warehouseLocation.startsWith("s3a://") || warehouseLocation.startsWith("s3n://")) {
        warehouseLocation = "s3://" + warehouseLocation.substring(6);
      }
      return new Naming.WarehouseNaming(warehouseLocation, rawName);
    }
  }

  public static class GCSNameExtractor extends ObjectStorageDatasetExtractor {

    public GCSNameExtractor() {
      super("gs");
    }

    @Override
    public Naming.DatasetNaming extract(URI location, String rawName) {
      return new Naming.GCS(getBucket(location), getObject(rawName));
    }

    @Override
    public Naming.DatasetNaming extractWarehouseDatasetNaming(URI location, String rawName) {
      return new Naming.WarehouseNaming(location.toString(), rawName);
    }
  }

  public static class WasbsNameExtractor extends ObjectStorageDatasetExtractor {
    public WasbsNameExtractor() {
      super("wasbs");
    }

    @Override
    public Naming.DatasetNaming extract(URI location, String rawName) {
      return new Naming.WASBS(
          getContainer(location.getAuthority()),
          getStorageAccount(location.getAuthority()),
          getObject(rawName));
    }

    @Override
    public Naming.DatasetNaming extractWarehouseDatasetNaming(URI location, String rawName) {
      return new Naming.WarehouseNaming(location.toString(), rawName);
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
