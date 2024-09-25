/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.acceptance;

import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.openlineage.hive.TestsBase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class AcceptanceTestUtils {

  protected static class ClusterProperty {
    private String key;
    private String value;
    private String marker;

    private ClusterProperty(String key, String value, String marker) {
      this.key = key;
      this.value = value;
      this.marker = marker;
    }

    protected static ClusterProperty of(String key, String value, String marker) {
      return new ClusterProperty(key, value, marker);
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    public String getMarker() {
      return marker;
    }
  }

  // must be set in order to run the acceptance test
  static final String ACCEPTANCE_TEST_BUCKET_VAR = "ACCEPTANCE_TEST_BUCKET";
  private static final BigQuery bq = BigQueryOptions.getDefaultInstance().getService();

  static Storage storage =
      new StorageOptions.DefaultStorageFactory().create(StorageOptions.getDefaultInstance());

  public static Path getArtifact(Path jarDir, String prefix, String suffix) throws Exception {
    Predicate<Path> prefixSuffixChecker = prefixSuffixChecker(prefix, suffix);
    try {
      return Files.list(jarDir)
          .filter(Files::isRegularFile)
          .filter(prefixSuffixChecker)
          .max(Comparator.comparing(AcceptanceTestUtils::lastModifiedTime))
          .get();
    } catch (Exception e) {
      throw new Exception(
          String.format(
              "Failed to find artifact jarDir=%s, prefix=%s, suffix=%s", jarDir, prefix, suffix),
          e);
    }
  }

  private static Predicate<Path> prefixSuffixChecker(final String prefix, final String suffix) {
    return path -> {
      String name = path.toFile().getName();
      return name.startsWith(prefix) && name.endsWith(suffix) && name.indexOf("-javadoc") == -1;
    };
  }

  private static FileTime lastModifiedTime(Path path) {
    try {
      return Files.getLastModifiedTime(path);
    } catch (IOException e) {
      throw new UncheckedIOException(e.getMessage(), e);
    }
  }

  public static BlobId copyToGcs(Path source, String destinationUri, String contentType)
      throws Exception {
    File sourceFile = source.toFile();
    try (FileInputStream sourceInputStream = new FileInputStream(sourceFile)) {
      FileChannel sourceFileChannel = sourceInputStream.getChannel();
      MappedByteBuffer sourceContent =
          sourceFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, sourceFile.length());
      return uploadToGcs(sourceContent, destinationUri, contentType);
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to write '%s' to '%s'", source, destinationUri), e);
    }
  }

  public static BlobId uploadResourceToGcs(
      String resourcePath, String destinationUri, String contentType) throws Exception {
    return AcceptanceTestUtils.uploadToGcs(
        AcceptanceTestUtils.class.getResourceAsStream(resourcePath), destinationUri, contentType);
  }

  public static BlobId uploadToGcs(InputStream source, String destinationUri, String contentType)
      throws Exception {
    try {
      ByteBuffer sourceContent = ByteBuffer.wrap(ByteStreams.toByteArray(source));
      return uploadToGcs(sourceContent, destinationUri, contentType);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write to '%s'", destinationUri), e);
    }
  }

  public static BlobId uploadToGcs(ByteBuffer content, String destinationUri, String contentType)
      throws Exception {
    URI uri = new URI(destinationUri);
    BlobId blobId = BlobId.of(uri.getAuthority(), uri.getPath().substring(1));
    // Preserve POSIX file modified timestamp as GCS metadata, otherwise YARN localization might
    // throws exceptions of
    // "Resource file:/... changed on src filesystem" due to different timestamps on worker nodes.
    ImmutableMap<String, String> customMetadata =
        ImmutableMap.<String, String>of(
            "goog-reserved-file-mtime", String.valueOf(System.currentTimeMillis()));
    BlobInfo blobInfo =
        BlobInfo.newBuilder(blobId).setContentType(contentType).setMetadata(customMetadata).build();
    try (WriteChannel writer = storage.writer(blobInfo)) {
      writer.write(content);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write to '%s'", destinationUri), e);
    }
    return blobId;
  }

  public static String getAcceptanceTestBucket() {
    return System.getenv()
        .getOrDefault(ACCEPTANCE_TEST_BUCKET_VAR, TestsBase.getProject() + "-acceptance-tests");
  }

  public static String createTestBaseGcsDir(String testId) {
    return String.format("gs://%s/acceptance-tests/%s", getAcceptanceTestBucket(), testId);
  }

  public static Blob getBlob(String gcsDirUri, String fileSuffix) throws URISyntaxException {
    URI uri = new URI(gcsDirUri);
    return StreamSupport.stream(
            storage
                .list(uri.getAuthority(), Storage.BlobListOption.prefix(uri.getPath().substring(1)))
                .iterateAll()
                .spliterator(),
            false)
        .filter(b -> b.getName().endsWith(fileSuffix))
        .findFirst()
        .get();
  }

  public static String readGcsFile(String fileUri) throws Exception {
    String tmp = fileUri.replace("gs://", "");
    int index = tmp.indexOf("/");
    String bucketName = tmp.substring(0, index);
    String filePath = tmp.substring(index + 1);
    return new String(storage.readAllBytes(bucketName, filePath), StandardCharsets.UTF_8);
  }

  public static String readGcsFile(String outputDirUri, String fileSuffix) throws Exception {
    Blob blob = getBlob(outputDirUri, fileSuffix);
    return new String(storage.readAllBytes(blob.getBlobId()), StandardCharsets.UTF_8);
  }

  public static void deleteGcsDir(String testBaseGcsDir) throws Exception {
    System.out.println("Deleting " + testBaseGcsDir + " ...");
    URI uri = new URI(testBaseGcsDir);
    BlobId[] blobIds =
        StreamSupport.stream(
                storage
                    .list(
                        uri.getAuthority(),
                        Storage.BlobListOption.prefix(uri.getPath().substring(1)))
                    .iterateAll()
                    .spliterator(),
                false)
            .map(Blob::getBlobId)
            .toArray(BlobId[]::new);
    if (blobIds.length > 1) {
      storage.delete(blobIds);
    }
  }

  public static void createBqDataset(String projectId, String dataset) {
    DatasetId datasetId = DatasetId.of(projectId, dataset);
    bq.create(DatasetInfo.of(datasetId));
  }

  public static int getNumOfRowsOfBqTable(String dataset, String table) {
    return bq.getTable(dataset, table).getNumRows().intValue();
  }

  public static void runBqQuery(String query) throws Exception {
    bq.query(QueryJobConfiguration.of(query));
  }

  public static void deleteBqDatasetAndTables(String dataset) {
    System.out.println("Deleting BQ dataset " + dataset + " ...");
    bq.delete(DatasetId.of(dataset), DatasetDeleteOption.deleteContents());
  }

  static void uploadJar(String jarDir, String prefix, String jarUri) throws Exception {
    Path jarDirPath = Paths.get(jarDir);
    Path assemblyJar = AcceptanceTestUtils.getArtifact(jarDirPath, prefix, ".jar");
    AcceptanceTestUtils.copyToGcs(assemblyJar, jarUri, "application/java-archive");
  }

  static void uploadInitAction(String resourcePath, String gcsUri) throws Exception {
    AcceptanceTestUtils.uploadToGcs(
        DataprocAcceptanceTestBase.class.getResourceAsStream(resourcePath), gcsUri, "text/x-bash");
  }

  public static String generateTestId(String dataprocImageVersion) {
    String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    return String.format(
        "%s-%s%s", timestamp, dataprocImageVersion.charAt(0), dataprocImageVersion.charAt(2));
  }

  public static String generateClusterName(String testId) {
    return AcceptanceTestConstants.CLUSTER_NAME_PREFIX + testId;
  }

  public static String readFileFromVM(
      String project, String zone, String instanceName, String filePath)
      throws IOException, InterruptedException {
    ProcessBuilder pb =
        new ProcessBuilder(
            "gcloud",
            "compute",
            "ssh",
            instanceName,
            "--project",
            project,
            "--zone",
            zone,
            "--command",
            "cat " + filePath);
    Process process = pb.start();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    StringBuilder output = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      output.append(line).append("\n");
    }
    process.waitFor();
    return output.toString();
  }
}
