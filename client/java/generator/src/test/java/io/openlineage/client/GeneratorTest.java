/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GeneratorTest {

  @TempDir Path outputDirectory;

  @Test
  void generatesSingletonEnumDiscriminatedUnions() throws Exception {
    String source = generate();

    assertTrue(source.contains("public interface LineageEntry"));
    assertTrue(
        source.contains(
            "@JsonTypeInfo(\n"
                + "      use = JsonTypeInfo.Id.NAME,\n"
                + "      include = JsonTypeInfo.As.EXISTING_PROPERTY,\n"
                + "      property = \"type\",\n"
                + "      visible = true\n"
                + "  )"));
    assertTrue(
        source.contains(
            "@JsonSubTypes.Type(value = LineageDatasetEntry.class, name = \"DATASET\")"));
    assertTrue(
        source.contains(
            "@JsonSubTypes.Type(value = LineageJobEntry.class, name = \"JOB\")"));
    assertTrue(
        source.contains(
            "public static final class LineageDatasetEntry implements LineageEntry"));
    assertTrue(
        source.contains("public static final class LineageJobEntry implements LineageEntry"));
    assertTrue(source.contains("public interface LineageInput"));
  }

  @Test
  void attachesFacetsSharingANameToTheirBaseFacetContainers() throws Exception {
    String source = generate();

    assertTrue(
        source.contains("private final LineageDatasetFacet lineage;"));
    assertTrue(
        source.contains("private final LineageJobFacet lineage;"));
  }

  @Test
  void generatedUnionDeserializesToItsConcreteVariant() throws Exception {
    generate();
    Path generatedSource = outputDirectory.resolve("OpenLineage.java");
    Path compiledClasses = Files.createDirectory(outputDirectory.resolve("classes"));
    ByteArrayOutputStream compilerOutput = new ByteArrayOutputStream();
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

    assertNotNull(compiler);
    assertEquals(
        0,
        compiler.run(
            null,
            null,
            compilerOutput,
            "-classpath",
            System.getProperty("java.class.path"),
            "-d",
            compiledClasses.toString(),
            generatedSource.toString()),
        compilerOutput.toString("UTF-8"));

    try (URLClassLoader classLoader =
        new URLClassLoader(
            new URL[] {compiledClasses.toUri().toURL()}, getClass().getClassLoader())) {
      Class<?> unionType =
          classLoader.loadClass("io.openlineage.client.OpenLineage$LineageEntry");
      Object value =
          new ObjectMapper()
              .readValue("{\"type\":\"DATASET\",\"namespace\":\"warehouse\"}", unionType);

      assertEquals("LineageDatasetEntry", value.getClass().getSimpleName());
    }
  }

  @Test
  void preservesFirstBranchBehaviorForOtherUnions() throws Exception {
    Set<URL> schemas = schemas();
    TypeResolver resolver = new TypeResolver(schemas);
    TypeResolver.ObjectResolvedType root =
        (TypeResolver.ObjectResolvedType)
            resolver.getRootResolvedType(
                getClass().getResource("/discriminated-union/SharedFacet.json"));

    String source = generate(schemas);
    assertFalse(source.contains("interface LegacyChoice"));
    assertFalse(source.contains("interface PlainChoice"));
    assertEquals("LegacyFirst", resolvedObjectField(root, "legacy").getName());
    assertEquals("PlainFirst", resolvedObjectField(root, "plain").getName());
  }

  private String generate() throws Exception {
    return generate(schemas());
  }

  private String generate(Set<URL> schemas) throws Exception {
    Generator.generate(
        schemas, "io.openlineage.client", false, outputDirectory.toFile());

    File generatedSource = outputDirectory.resolve("OpenLineage.java").toFile();
    return new String(Files.readAllBytes(generatedSource.toPath()), "UTF-8");
  }

  private Set<URL> schemas() {
    URL coreSchema = getClass().getResource("/discriminated-union/OpenLineage.json");
    URL facetSchema = getClass().getResource("/discriminated-union/SharedFacet.json");
    Set<URL> schemas = new LinkedHashSet<>();
    schemas.add(coreSchema);
    schemas.add(facetSchema);
    return schemas;
  }

  private TypeResolver.ObjectResolvedType resolvedObjectField(
      TypeResolver.ObjectResolvedType type, String fieldName) {
    return (TypeResolver.ObjectResolvedType)
        type.getProperties().stream()
            .filter(field -> field.getName().equals(fieldName))
            .findFirst()
            .orElseThrow(AssertionError::new)
            .getType();
  }
}
