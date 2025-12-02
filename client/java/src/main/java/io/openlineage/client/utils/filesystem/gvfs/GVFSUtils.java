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

public class GVFSUtils {

  private static final String PATH_SEPARATOR = "/";
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

  public static String getGVFSIdentifierName(URI uri) {
    String path = uri.getPath();
    if (path.startsWith(PATH_SEPARATOR)) {
      path = path.substring(1);
    }
    String[] parts = path.split(PATH_SEPARATOR);
    if (parts.length < MIN_GVFS_PATH_PARTS) {
      throw new IllegalArgumentException("Invalid GVFS path," + path);
    }
    return Arrays.stream(parts).limit(MIN_GVFS_PATH_PARTS).collect(Collectors.joining("."));
  }

  public static String getGVFSLocation(URI uri) {
    String path = uri.getPath();
    if (path.startsWith(PATH_SEPARATOR)) {
      path = path.substring(1);
    }
    String[] parts = path.split(PATH_SEPARATOR);
    if (parts.length < MIN_GVFS_PATH_PARTS) {
      throw new IllegalArgumentException("Invalid GVFS path," + path);
    } else if (parts.length == MIN_GVFS_PATH_PARTS) {
      return PATH_SEPARATOR;
    }

    String end = "";
    if (path.endsWith(PATH_SEPARATOR)) {
      end = PATH_SEPARATOR;
    }
    return PATH_SEPARATOR + Arrays.stream(parts).skip(MIN_GVFS_PATH_PARTS).collect(Collectors.joining(PATH_SEPARATOR)) + end;
  }
}
