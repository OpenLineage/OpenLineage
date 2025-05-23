/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Value;

@Value
public class DatasetIdentifier {
  String name;
  String namespace;
  List<Symlink> symlinks;

  public enum SymlinkType {
    TABLE,
    UNKNOWN
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

  public DatasetIdentifier merge(DatasetIdentifier ident) {
    Map<String, Symlink> current = toMap();
    Map<String, Symlink> other = ident.toMap();
    for (Map.Entry<String, Symlink> entry : other.entrySet()) {
      if (!current.containsKey(entry.getKey())) {
        symlinks.add(entry.getValue());
      }
    }
    return this;
  }

  @Value
  public static class Symlink {
    String name;
    String namespace;
    SymlinkType type;
  }

  private Map<String, Symlink> toMap() {
    Map<String, Symlink> map = new HashMap<>();
    map.put(namespace + "://" + name, new Symlink(name, namespace, SymlinkType.TABLE));
    for (Symlink symlink : symlinks) {
      map.put(symlink.getNamespace() + "://" + symlink.getName(), symlink);
    }
    return map;
  }
}
