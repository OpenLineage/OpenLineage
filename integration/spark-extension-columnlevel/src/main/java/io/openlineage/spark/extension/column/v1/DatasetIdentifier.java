/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension.column.v1;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class DatasetIdentifier {
  private final String name;
  private final String namespace;
  private final List<Symlink> symlinks;

  public enum SymlinkType {
    TABLE
  };

  public DatasetIdentifier(String name, String namespace) {
    this.name = name;
    this.namespace = namespace;
    this.symlinks = new LinkedList<>();
  }

  public DatasetIdentifier(String name, String namespace, List<Symlink> symlinks) {
    this.name = name;
    this.namespace = namespace;
    this.symlinks = symlinks;
  }

  public String getName() {
    return name;
  }

  public String getNamespace() {
    return namespace;
  }

  public List<Symlink> getSymlinks() {
    return symlinks;
  }

  public DatasetIdentifier withSymlink(String name, String namespace, SymlinkType type) {
    symlinks.add(new Symlink(name, namespace, type));
    return this;
  }

  public DatasetIdentifier withSymlink(Symlink symlink) {
    symlinks.add(symlink);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DatasetIdentifier that = (DatasetIdentifier) o;
    return Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(symlinks, that.symlinks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, namespace, symlinks);
  }

  public static class Symlink {
    private final String name;
    private final String namespace;
    private final SymlinkType type;

    public Symlink(String name, String namespace, SymlinkType type) {
      this.name = name;
      this.namespace = namespace;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getNamespace() {
      return namespace;
    }

    public SymlinkType getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Symlink symlink = (Symlink) o;
      return Objects.equals(name, symlink.name)
          && Objects.equals(namespace, symlink.namespace)
          && type == symlink.type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, namespace, type);
    }
  }
}
