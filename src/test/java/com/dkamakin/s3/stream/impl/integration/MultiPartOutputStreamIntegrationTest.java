package com.dkamakin.s3.stream.impl.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.dkamakin.s3.stream.impl.MultiPartOutputStream;
import com.dkamakin.s3.stream.impl.MultiPartOutputStreamBuilder.Constant;
import com.dkamakin.s3.stream.util.impl.Bytes;
import com.dkamakin.s3.stream.util.impl.RedirectableOutputStream;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

@Disabled("manual")
class MultiPartOutputStreamIntegrationTest extends MinioIntegrationTest {

    static class ChunkSizeArgument {

        Bytes length;
        Bytes chunkSize;

        ChunkSizeArgument(Bytes length, Bytes chunkSize) {
            this.length    = length;
            this.chunkSize = chunkSize;
        }
    }

    boolean isExists(String key) {
        try {
            s3Client.headObject(HeadObjectRequest.builder()
                                                 .bucket(Data.BUCKET)
                                                 .key(key)
                                                 .build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

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

    MultiPartOutputStream stream(String key) {
        return MultiPartOutputStream.builder()
                                    .bucket(Data.BUCKET)
                                    .key(key)
                                    .client(s3Client)
                                    .build();
    }

    static Stream<byte[]> dataStream() {
        return Stream.of(
            "This is a simple string. Attempt to serialize and save text data".getBytes(StandardCharsets.UTF_8),
            RandomUtils.nextBytes(Constant.S3_MIN_PART_SIZE.toBytes() - 1),
            RandomUtils.nextBytes(Constant.S3_MIN_PART_SIZE.toBytes()),
            RandomUtils.nextBytes(Constant.S3_MIN_PART_SIZE.toBytes() + 1),
            RandomUtils.nextBytes(Constant.S3_MIN_PART_SIZE.toBytes() * 2)
        );
    }

    static Stream<ChunkSizeArgument> chunkSizeArgumentStream() {
        return Stream.of(
            new ChunkSizeArgument(Bytes.fromKb(300), Bytes.fromKb(100)),
            new ChunkSizeArgument(Bytes.fromKb(900), Bytes.fromKb(300)),
            new ChunkSizeArgument(Bytes.fromMb(6), Bytes.fromMb(1)),
            new ChunkSizeArgument(Bytes.fromMb(9), Bytes.fromMb(3)),
            new ChunkSizeArgument(Bytes.fromMb(15), Bytes.fromMb(3))
        );
    }

    private void assertData(Consumer<MultiPartOutputStream> streamAction, Supplier<byte[]> expectedProvider) {
        String key = UUID.randomUUID().toString();

        try (MultiPartOutputStream stream = stream(key)) {
            streamAction.accept(stream);
        }

        byte[] expected = expectedProvider.get();

        System.out.println("Expected length: " + expected.length);

        byte[] actual = download(key, expected.length);

        assertThat(actual).hasSize(expected.length).isEqualTo(expected);
    }

    void writeRandomData(MultiPartOutputStream stream, RedirectableOutputStream buffer, ChunkSizeArgument argument) {
        int length    = argument.length.toBytes();
        int chunkSize = argument.chunkSize.toBytes();

        try {
            for (int i = 0; i < length; i += chunkSize) {
                byte[] chunk = RandomUtils.nextBytes(chunkSize);
                buffer.write(chunk);
                stream.write(chunk);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @MethodSource("dataStream")
    void write_SingleWrite_SerializeThenReceiveCorrectBytes(byte[] expected) {
        assertData(stream -> stream.write(expected), () -> expected);
    }

    @ParameterizedTest
    @MethodSource("chunkSizeArgumentStream")
    void write_MultipleWriteDifferentSizes_WriteDataOnClose(ChunkSizeArgument argument) {
        RedirectableOutputStream expected = new RedirectableOutputStream(argument.length);

        assertData(stream -> writeRandomData(stream, expected, argument), expected::toByteArray);
    }

    @Test
    void write_NoData_FileDoesNotExists() {
        String key = UUID.randomUUID().toString();

        try (MultiPartOutputStream stream = stream(key)) {
            stream.flush();
        }

        assertThat(isExists(key)).isFalse();
    }

}
