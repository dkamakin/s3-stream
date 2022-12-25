package com.github.dkamakin.s3.stream.impl.integration;

import static com.github.dkamakin.s3.stream.impl.MultiPartOutputStreamBuilder.Constant.S3_MIN_PART_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.dkamakin.s3.stream.IMultiPartOutputStream;
import com.github.dkamakin.s3.stream.impl.MultiPartOutputStream;
import com.google.common.io.ByteStreams;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

class MultiPartOutputStreamIntegrationTest extends MinioIntegrationTest {

    byte[] download(String key, int length) {
        byte[] result = new byte[length];

        try (InputStream stream = download(key)) {
            ByteStreams.read(stream, result, 0, result.length);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    InputStream download(String key) {
        return s3Client.getObject(GetObjectRequest.builder()
                                                  .bucket(Data.BUCKET)
                                                  .key(key)
                                                  .build());
    }

    IMultiPartOutputStream createNewFile(String key) {
        return MultiPartOutputStream.builder()
                                    .forBucket(Data.BUCKET)
                                    .forKey(key)
                                    .withClient(s3Client)
                                    .build();
    }

    static Stream<byte[]> dataStream() {
        return Stream.of(
            "This is a simple string. Attempt to serialize and save text data".getBytes(StandardCharsets.UTF_8),
            RandomUtils.nextBytes(S3_MIN_PART_SIZE.toBytes() - 1),
            RandomUtils.nextBytes(S3_MIN_PART_SIZE.toBytes()),
            RandomUtils.nextBytes(S3_MIN_PART_SIZE.toBytes() + 1),
            RandomUtils.nextBytes(S3_MIN_PART_SIZE.toBytes() * 2)
        );
    }

    @ParameterizedTest
    @MethodSource("dataStream")
    void write_SingleString_SerializeThenReceiveCorrectBytes(byte[] expected) {
        String key = UUID.randomUUID().toString();

        System.out.println("Writing data, length: " + expected.length);

        try (IMultiPartOutputStream stream = createNewFile(key)) {
            stream.write(expected);
        }

        byte[] actual = download(key, expected.length);

        assertThat(actual).hasSize(expected.length).isEqualTo(expected);
    }

}
