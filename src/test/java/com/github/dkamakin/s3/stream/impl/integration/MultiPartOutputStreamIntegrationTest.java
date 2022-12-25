package com.github.dkamakin.s3.stream.impl.integration;

import static com.github.dkamakin.s3.stream.impl.MultiPartOutputStreamBuilder.Constant.S3_MIN_PART_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.dkamakin.s3.stream.IMultiPartOutputStream;
import com.github.dkamakin.s3.stream.impl.MultiPartOutputStream;
import com.github.dkamakin.s3.stream.util.impl.Bytes;
import com.github.dkamakin.s3.stream.util.impl.RedirectableOutputStream;
import com.google.common.io.ByteStreams;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

class MultiPartOutputStreamIntegrationTest extends MinioIntegrationTest {

    static class ChunkSizeArgument {

        Bytes length;
        Bytes chunkSize;

        ChunkSizeArgument(Bytes length, Bytes chunkSize) {
            this.length    = length;
            this.chunkSize = chunkSize;
        }
    }

    long size(String key) {
        return s3Client.headObject(HeadObjectRequest.builder()
                                                    .bucket(Data.BUCKET)
                                                    .key(key)
                                                    .build()).contentLength();
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

    static Stream<ChunkSizeArgument> chunkSizeArgumentStream() {
        return Stream.of(
            new ChunkSizeArgument(Bytes.fromKb(300), Bytes.fromKb(100)),
            new ChunkSizeArgument(Bytes.fromKb(900), Bytes.fromKb(300)),
            new ChunkSizeArgument(Bytes.fromMb(6), Bytes.fromMb(1)),
            new ChunkSizeArgument(Bytes.fromMb(9), Bytes.fromMb(3)),
            new ChunkSizeArgument(Bytes.fromMb(15), Bytes.fromMb(3))
        );
    }

    private void assertData(Consumer<IMultiPartOutputStream> streamAction, byte[] expected) {
        String key = UUID.randomUUID().toString();

        try (IMultiPartOutputStream stream = createNewFile(key)) {
            streamAction.accept(stream);
        }

        System.out.println("Expected length: " + expected.length);

        byte[] actual = download(key, expected.length);

        assertThat(actual).hasSize(expected.length).isEqualTo(expected);
    }

    void writeRandomData(IMultiPartOutputStream stream, RedirectableOutputStream buffer, ChunkSizeArgument argument) {
        int length    = argument.length.toBytes();
        int chunkSize = argument.chunkSize.toBytes();

        for (int i = 0; i < length; i += chunkSize) {
            byte[] chunk = RandomUtils.nextBytes(chunkSize);
            buffer.write(chunk);
            stream.write(chunk);
        }
    }

    @ParameterizedTest
    @MethodSource("dataStream")
    void write_SingleWrite_SerializeThenReceiveCorrectBytes(byte[] expected) {
        assertData(stream -> stream.write(expected), expected);
    }

    @ParameterizedTest
    @MethodSource("chunkSizeArgumentStream")
    void write_MultipleWriteDifferentSizes_WriteDataOnClose(ChunkSizeArgument argument) {
        RedirectableOutputStream expected = new RedirectableOutputStream(argument.length);

        assertData(stream -> writeRandomData(stream, expected, argument), expected.toByteArray());
    }

    @Test
    void write_NoData_AboutRequest() {
        String key = UUID.randomUUID().toString();

        try (IMultiPartOutputStream stream = createNewFile(key)) {
            stream.flush();
        }

        assertThat(size(key)).isZero();
    }

}
