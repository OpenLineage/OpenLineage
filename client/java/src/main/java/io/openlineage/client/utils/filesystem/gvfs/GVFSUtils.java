/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.utils.filesystem.gvfs;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import io.openlineage.client.gravitino.GravitinoFacets;
import io.openlineage.client.gravitino.GravitinoFacets.LocationDatasetFacet;
import io.openlineage.client.utils.filesystem.GVFSFilesystemDatasetExtractor;
import java.net.URI;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Utility class for handling Gravitino Virtual File System (GVFS) paths and facets. GVFS paths
 * follow the format: gvfs://fileset/catalog/schema/fileset[/subpath]
 */
public class GVFSUtils {

  private static final String PATH_SEPARATOR = "/";

  /** Minimum number of path components required: catalog, schema, fileset */
  private static final int MIN_GVFS_PATH_PARTS = 3;

  public static boolean isGVFS(URI uri) {
    return GVFSFilesystemDatasetExtractor.SCHEME.equalsIgnoreCase(uri.getScheme());
  }

  public static void injectGVFSFacets(
      OpenLineage openLineage, DatasetCompositeFacetsBuilder builder, URI location) {
    // add GVFS data type
    builder.getFacets().datasetType(openLineage.newDatasetTypeDatasetFacet("Fileset", ""));
    // add location
    LocationDatasetFacet locationDatasetFacet =
        GravitinoFacets.newLocationDatasetFact(GVFSUtils.getGVFSLocation(location));
    builder.getFacets().put("fileset-location", locationDatasetFacet);
  }

  public static OutputDataset injectGVFSFacets(
      OpenLineage openLineage, OutputDataset outputDataset, URI location) {

    DatasetFacets datasetFacets = outputDataset.getFacets();
    DatasetCompositeFacetsBuilder builder = new DatasetCompositeFacetsBuilder(openLineage);
    builder
        .getFacets()
        .columnLineage(datasetFacets.getColumnLineage())
        .dataSource(datasetFacets.getDataSource())
        .tags(datasetFacets.getTags())
        .documentation(datasetFacets.getDocumentation())
        .lifecycleStateChange(datasetFacets.getLifecycleStateChange())
        .ownership(datasetFacets.getOwnership())
        .schema(datasetFacets.getSchema())
        .storage(datasetFacets.getStorage());
    outputDataset.getFacets().getAdditionalProperties();

    injectGVFSFacets(openLineage, builder, location);

    return openLineage
        .newOutputDatasetBuilder()
        .name(outputDataset.getName())
        .namespace(outputDataset.getNamespace())
        .facets(builder.getFacets().build())
        .outputFacets(outputDataset.getOutputFacets())
        .build();
  }

  /**
   * Extracts the dataset identifier name from a GVFS URI. The identifier is composed of the first
   * three path components (catalog.schema.fileset).
   *
   * @param uri GVFS URI in format: gvfs://fileset/catalog/schema/fileset[/subpath]
   * @return dataset identifier in format: catalog.schema.fileset
   * @throws IllegalArgumentException if the path doesn't contain required components
   */
  public static String getGVFSIdentifierName(URI uri) {
    String path = uri.getPath();
    if (path.startsWith(PATH_SEPARATOR)) {
      path = path.substring(1);
    }
    String[] parts = path.split(PATH_SEPARATOR);
    if (parts.length < MIN_GVFS_PATH_PARTS) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid GVFS path: '%s'. "
                  + "Expected format: gvfs://fileset/catalog/schema/fileset[/subpath]. "
                  + "Path must contain at least catalog, schema, and fileset components.",
              path));
    }
    return Arrays.stream(parts).limit(MIN_GVFS_PATH_PARTS).collect(Collectors.joining("."));
  }

  /**
   * Extracts the location path within a GVFS fileset. This is the subpath after the
   * catalog/schema/fileset components.
   *
   * @param uri GVFS URI in format: gvfs://fileset/catalog/schema/fileset[/subpath]
   * @return the location path within the fileset (e.g., "/data/2024/01/"), or "/" if no subpath
   * @throws IllegalArgumentException if the path doesn't contain required components
   */
  public static String getGVFSLocation(URI uri) {
    String path = uri.getPath();
    if (path.startsWith(PATH_SEPARATOR)) {
      path = path.substring(1);
    }
    String[] parts = path.split(PATH_SEPARATOR);
    if (parts.length < MIN_GVFS_PATH_PARTS) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid GVFS path: '%s'. "
                  + "Expected format: gvfs://fileset/catalog/schema/fileset[/subpath]. "
                  + "Path must contain at least catalog, schema, and fileset components.",
              path));
    } else if (parts.length == MIN_GVFS_PATH_PARTS) {
      return PATH_SEPARATOR; // Root of fileset
    }

    String end = "";
    if (path.endsWith(PATH_SEPARATOR)) {
      end = PATH_SEPARATOR;
    }
    return PATH_SEPARATOR
        + Arrays.stream(parts).skip(MIN_GVFS_PATH_PARTS).collect(Collectors.joining(PATH_SEPARATOR))
        + end;
  }
}
