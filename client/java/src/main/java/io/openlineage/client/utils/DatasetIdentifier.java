/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import io.openlineage.client.dataset.partition.trimmer.DatasetNameTrimmer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import lombok.Value;

@Value
public class DatasetIdentifier {
  String name;
  String namespace;
  List<Symlink> symlinks;

  public enum SymlinkType {
    TABLE,
    LOCATION
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

  public DatasetIdentifier withSymlink(String name, String namespace, SymlinkType type) {
    symlinks.add(new Symlink(name, namespace, type));
    return this;
  }

  public DatasetIdentifier withSymlink(Symlink symlink) {
    symlinks.add(symlink);
    return this;
  }

  public DatasetIdentifier withTrimmedName(Collection<DatasetNameTrimmer> trimmers) {
    String trimmedName = name;
    boolean continueTrimming = true;

    while (continueTrimming) {
      continueTrimming = false;
      for (DatasetNameTrimmer trimmer : trimmers) {
        if (trimmer.canTrim(trimmedName)) {
          trimmedName = trimmer.trim(trimmedName);
          continueTrimming = true;
        }
      }
    }

    if (name.equals(trimmedName)) {
      return this;
    } else {
      return new DatasetIdentifier(trimmedName, namespace, symlinks);
    }
  }

  @Value
  public static class Symlink {
    String name;
    String namespace;
    SymlinkType type;
  }
}
