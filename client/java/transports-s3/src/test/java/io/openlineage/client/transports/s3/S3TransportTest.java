/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.s3;

import static org.junit.jupiter.api.Assertions.*;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.transports_s3.TestBuildConfig;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
@Testcontainers
class S3TransportTest {
  private static final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  @Container
  private final S3MockContainer s3Mock = new S3MockContainer(TestBuildConfig.S3_MOCK_VERSION);

  @Test
  void shouldWriteEventToS3() {
    mockAwsCredentials();
    long epochMilli = ZonedDateTime.now(clock).toInstant().toEpochMilli();
    testEventStored("test-bucket-1", null, String.format("%s.json", epochMilli));
    testEventStored("test-bucket-2", "event", String.format("event_%s.json", epochMilli));
    testEventStored(
        "test-bucket-3", "some/path/event", String.format("some/path/event_%s.json", epochMilli));
    testEventStored(
        "test-bucket-4", "another/path/", String.format("another/path/%s.json", epochMilli));
  }

  @Test
  void shouldThrowIfObjectAlreadyExists() {
    mockAwsCredentials();
    String bucketName = "test-5";
    try (S3Transport transport =
        new S3Transport(
            new S3TransportConfig(s3Mock.getHttpsEndpoint(), bucketName, "some_prefix"))) {
      S3Client s3Client = transport.getS3Client();
      ensureBucketExists(s3Client, bucketName);

      OpenLineageClient client = new OpenLineageClient(transport);
      OpenLineage.RunEvent event = runEvent(clock);
      client.emit(event);
      // We are using test clock. The time doesn't change, so we should emit file with the same name
      assertThrows(OpenLineageClientException.class, () -> client.emit(event));
    }
  }

  /**
   * The AWS credentials and region are not used, but the SDK still required them to satisfy
   * credentials provider chain.
   */
  private static void mockAwsCredentials() {
    System.setProperty("aws.region", "dummy");
    System.setProperty("aws.accessKeyId", "dummy");
    System.setProperty("aws.secretAccessKey", "dummy");
  }

  private static void ensureBucketExists(S3Client s3Client, String bucketName) {
    boolean bucketExists =
        s3Client.listBuckets().buckets().stream().anyMatch(b -> bucketName.equals(b.name()));
    if (!bucketExists) {
      s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }
  }

  /**
   * Creates an event and stores it in specified bucket under specified bucket. Then tests if the
   * file name matches the expected value
   */
  void testEventStored(String bucketName, @Nullable String prefix, String expectedFileName) {
    try (S3Transport transport =
        new S3Transport(new S3TransportConfig(s3Mock.getHttpsEndpoint(), bucketName, prefix))) {
      S3Client s3Client = transport.getS3Client();

      // Ensures bucket exists
      ensureBucketExists(s3Client, bucketName);

      OpenLineageClient client = new OpenLineageClient(transport);
      OpenLineage.RunEvent event = runEvent(clock);
      client.emit(event);

      ResponseBytes<GetObjectResponse> writtenObject =
          s3Client.getObjectAsBytes(
              GetObjectRequest.builder().bucket(bucketName).key(expectedFileName).build());
      assertNotNull(writtenObject);
      assertEquals(new String(writtenObject.asByteArray()), OpenLineageClientUtils.toJson(event));
    }
  }

  public static OpenLineage.RunEvent runEvent(Clock clock) {
    OpenLineage.Job job =
        new OpenLineage.JobBuilder().namespace("test-namespace").name("test-job").build();
    OpenLineage.Run run = new OpenLineage.RunBuilder().runId(UUID.randomUUID()).build();
    return new OpenLineage(URI.create("http://test.producer"))
        .newRunEventBuilder()
        .eventTime(ZonedDateTime.now(clock))
        .job(job)
        .run(run)
        .build();
  }
}
