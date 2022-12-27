package com.dkamakin.s3.stream.impl.integration;

import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers
abstract class MinioIntegrationTest {

    static final class Data {

        static final String ACCESS_KEY    = "accessKey";
        static final String SECRET_KEY    = "secretKey";
        static final String BUCKET        = "bucket";
        static final int    MINIO_PORT    = 9000;
        static final String MINIO_VERSION = "minio/minio:RELEASE.2022-05-08T23-50-31Z.hotfix.3d64b976d";
    }

    @Container
    private final static GenericContainer CONTAINER = new GenericContainer(Data.MINIO_VERSION)
        .withEnv("MINIO_ACCESS_KEY", Data.ACCESS_KEY)
        .withEnv("MINIO_SECRET_KEY", Data.SECRET_KEY)
        .withCommand("server /data")
        .withExposedPorts(Data.MINIO_PORT)
        .waitingFor(new HttpWaitStrategy()
                        .forPath("/minio/health/ready")
                        .forPort(Data.MINIO_PORT)
                        .withStartupTimeout(Duration.ofSeconds(10)));

    static S3Client s3Client;

    @BeforeAll
    static void setUp() {
        s3Client = S3Client.builder()
                           .endpointOverride(createUri())
                           .forcePathStyle(true)
                           .region(Region.US_WEST_1)
                           .credentialsProvider(createCredentials())
                           .build();

        createBucket();
    }

    @AfterAll
    static void shutDown() {
        s3Client.close();
    }

    private static AwsCredentialsProvider createCredentials() {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(Data.ACCESS_KEY, Data.SECRET_KEY));
    }

    private static URI createUri() {
        return URI.create(String.format("http://%s:%s", CONTAINER.getHost(), CONTAINER.getFirstMappedPort()));
    }

    private static void createBucket() {
        s3Client.createBucket(CreateBucketRequest.builder().bucket(Data.BUCKET).build());
    }

}
