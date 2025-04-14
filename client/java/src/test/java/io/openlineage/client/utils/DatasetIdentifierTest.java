/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

class DatasetIdentifierTest {

  @Test
  void testMerge() {
    DatasetIdentifier ident =
        new DatasetIdentifier(
            "name",
            "namespace",
            new ArrayList<>(
                Arrays.asList(
                    new DatasetIdentifier.Symlink(
                        "symlink", "namespace", DatasetIdentifier.SymlinkType.TABLE))));
    DatasetIdentifier other =
        new DatasetIdentifier(
            "otherName",
            "namespace",
            new ArrayList<>(
                Arrays.asList(
                    new DatasetIdentifier.Symlink(
                        "otherSymlink", "namespace", DatasetIdentifier.SymlinkType.TABLE))));

    ident.merge(other);

    assertEquals("namespace", ident.getNamespace());
    assertEquals("name", ident.getName());
    assertEquals(
        new HashSet<>(
            Arrays.asList(
                new DatasetIdentifier.Symlink(
                    "symlink", "namespace", DatasetIdentifier.SymlinkType.TABLE),
                new DatasetIdentifier.Symlink(
                    "otherName", "namespace", DatasetIdentifier.SymlinkType.TABLE),
                new DatasetIdentifier.Symlink(
                    "otherSymlink", "namespace", DatasetIdentifier.SymlinkType.TABLE))),
        new HashSet<>(ident.getSymlinks()));
  }

  @Test
  void testMergeSameIdentifiersNoSymlinks() {
    DatasetIdentifier ident = new DatasetIdentifier("name", "namespace");
    DatasetIdentifier other = new DatasetIdentifier("name", "namespace");

    ident.merge(other);

    assertEquals("namespace", ident.getNamespace());
    assertEquals("name", ident.getName());
    assertThat(ident.getSymlinks()).isEmpty();
  }

  @Test
  void testMergeDifferentNameSameSymlink() {
    DatasetIdentifier ident =
        new DatasetIdentifier(
            "name",
            "namespace",
            new ArrayList<>(
                Arrays.asList(
                    new DatasetIdentifier.Symlink(
                        "symlink", "namespace", DatasetIdentifier.SymlinkType.TABLE))));
    DatasetIdentifier other =
        new DatasetIdentifier(
            "otherName",
            "namespace",
            new ArrayList<>(
                Arrays.asList(
                    new DatasetIdentifier.Symlink(
                        "symlink", "namespace", DatasetIdentifier.SymlinkType.TABLE))));

    ident.merge(other);

    assertEquals("namespace", ident.getNamespace());
    assertEquals("name", ident.getName());
    assertEquals(
        new HashSet<>(
            Arrays.asList(
                new DatasetIdentifier.Symlink(
                    "symlink", "namespace", DatasetIdentifier.SymlinkType.TABLE),
                new DatasetIdentifier.Symlink(
                    "otherName", "namespace", DatasetIdentifier.SymlinkType.TABLE))),
        new HashSet<>(ident.getSymlinks()));
  }
}
