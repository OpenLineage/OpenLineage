/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

@Slf4j
@UtilityClass
class AwsUtils {

  public static final String OPEN_LINEAGE_JAR_LOCATION = "../build/libs/";
  public static final String S3_TRANSPORT_JAR_LOCATION =
      "../../../client/java/transports-s3/build/libs/";
  public static final String SNAPSHOT_JAR_SUFFIX = "-SNAPSHOT.jar";

  static String s3Url(String bucketName, String key) {
    return "s3://" + bucketName + "/" + key;
  }

  /** Fetches the newest jar file and uploads it to the specified directory */
  @SneakyThrows
  static String uploadOpenLineageJar(S3Client s3Client, String bucket, String prefix) {
    Path jarFile =
        findNewestFile(OPEN_LINEAGE_JAR_LOCATION, "openlineage-spark_", SNAPSHOT_JAR_SUFFIX)
            .orElseThrow(() -> new RuntimeException("openlineage-spark jar not found"));
    String uploadedFileKey = uploadFile(s3Client, jarFile, bucket, prefix);
    log.info("OpenLineage jar has been uploaded to [{}]", s3Url(bucket, uploadedFileKey));
    return uploadedFileKey;
  }

  /** Fetches the newest jar file and uploads it to the specified directory */
  @SneakyThrows
  static String uploadS3TransportJar(S3Client s3Client, String bucket, String prefix) {
    Path jarFile =
        findNewestFile(S3_TRANSPORT_JAR_LOCATION, "transports-s3", "-SNAPSHOT.jar")
            .orElseThrow(() -> new RuntimeException("S3 transport jar not found"));
    String uploadedFileKey = uploadFile(s3Client, jarFile, bucket, prefix);
    log.info("S3 transport jar has been uploaded to [{}]", s3Url(bucket, uploadedFileKey));
    return uploadedFileKey;
  }

  private static @NotNull Optional<Path> findNewestFile(String first, String prefix, String suffix)
      throws IOException {
    return Files.list(Paths.get(first))
        .filter(p -> p.getFileName().toString().startsWith(prefix))
        .filter(p -> p.getFileName().toString().endsWith(suffix))
        .max(Comparator.naturalOrder());
  }

  public void deleteFiles(S3Client s3Client, String bucket, String prefix) {}

  static List<OpenLineage.RunEvent> fetchEventsEmitted(
      S3Client s3Client, String bucketName, String location) {
    return readAllFilesInPath(s3Client, bucketName, location)
        .map(OpenLineageClientUtils::runEventFromJson)
        .collect(Collectors.toList());
  }

  private static Stream<String> readAllFilesInPath(
      S3Client s3Client, String bucketName, String directoryPath) {
    return s3Client
        .listObjectsV2(
            ListObjectsV2Request.builder().bucket(bucketName).prefix(directoryPath).build())
        .contents()
        .stream()
        .map(s3Object -> getS3ObjectContent(s3Client, bucketName, s3Object.key()));
  }

  private static String getS3ObjectContent(S3Client s3Client, String bucketName, String objectKey) {
    return s3Client
        .getObject(
            GetObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
            ResponseTransformer.toBytes())
        .asUtf8String();
  }

  /**
   * Uploads the file. If the prefix ends with forward slash, then the name of the file remains
   * unchanged. Otherwise, the last part of the prefix is used as the name of the file.
   *
   * @return The key of the stored file.
   */
  static String uploadFile(S3Client s3Client, Path sourceFile, String bucket, String prefix) {
    String key = (prefix.endsWith("/") ? prefix : (prefix + "/")) + sourceFile.getFileName();
    s3Client.putObject(
        PutObjectRequest.builder().bucket(bucket).key(key).build(),
        RequestBody.fromFile(sourceFile.toFile()));
    return key;
  }

  static void uploadFile(S3Client s3Client, String fileContent, String bucket, String key) {
    s3Client.putObject(
        PutObjectRequest.builder().bucket(bucket).key(key).build(),
        RequestBody.fromString(fileContent));
  }
}
