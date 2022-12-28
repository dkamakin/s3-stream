package com.dkamakin.s3.stream.impl.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dkamakin.s3.stream.impl.MultiPartInputStream;
import com.dkamakin.s3.stream.util.impl.Bytes;
import com.dkamakin.s3.stream.util.impl.RedirectableOutputStream;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Disabled("manual")
class MultiPartInputStreamIntegrationTest extends MinioIntegrationTest {

    void create(String key, RequestBody body) {
        s3Client.putObject(PutObjectRequest.builder().key(key).bucket(Data.BUCKET).build(), body);
    }

    void create(String key, byte[] data) {
        create(key, RequestBody.fromBytes(data));
    }

    MultiPartInputStream stream(String key) {
        return MultiPartInputStream.builder()
                                   .bucket(Data.BUCKET)
                                   .key(key)
                                   .client(s3Client)
                                   .build();
    }

    @Test
    void read_EmptyFile_EmptyResult() {
        String key = UUID.randomUUID().toString();

        create(key, new byte[0]);

        int actual;

        try (MultiPartInputStream stream = stream(key)) {
            actual = stream.read(new byte[10]);
        }

        assertThat(actual).isNegative();
    }

    @Test
    void read_FileDoesNotExist_NoSuchKEeyException() {
        MultiPartInputStream stream = stream(UUID.randomUUID().toString());
        byte[]               data   = new byte[10];

        assertThatThrownBy(() -> stream.read(data)).isInstanceOf(NoSuchKeyException.class);
    }

    @Test
    void read_FileWithContent_ReadRequestedContent() throws IOException {
        String                   key       = UUID.randomUUID().toString();
        Bytes                    chunkSize = Bytes.fromKb(512);
        RedirectableOutputStream expected  = new RedirectableOutputStream(chunkSize);

        expected.write(RandomUtils.nextBytes(chunkSize.toBytes()));

        create(key, expected.toByteArray());

        byte[] actual = new byte[expected.size()];
        int    read;
        int    secondRead;

        try (MultiPartInputStream stream = stream(key)) {
            read       = stream.read(actual);
            secondRead = stream.read(actual);
        }

        assertThat(read).isEqualTo(chunkSize.toBytes());
        assertThat(secondRead).isNegative();
        assertThat(actual).isEqualTo(expected.toByteArray());
    }

    @Test
    void read_ReadFileInWhileCycle_GetEOS() throws IOException {
        String                   key       = UUID.randomUUID().toString();
        Bytes                    chunkSize = Bytes.fromKb(512);
        RedirectableOutputStream expected  = new RedirectableOutputStream(chunkSize);

        expected.write(RandomUtils.nextBytes(chunkSize.toBytes()));

        create(key, expected.toByteArray());

        int    cycles         = 8;
        int    cycleSize      = expected.size() / cycles;
        byte[] buffer         = new byte[cycleSize];
        byte[] expectedBuffer = new byte[cycleSize];
        int    actual         = 0;

        try (MultiPartInputStream stream = stream(key)) {
            int read;

            while ((read = stream.read(buffer)) > 0) {
                expected.redirect(actual * read, read).read(expectedBuffer);

                assertThat(buffer).isEqualTo(expectedBuffer);

                actual++;
            }
        }

        assertThat(actual).isEqualTo(cycles);
    }

    @Test
    void read_ExceedsFileSize_NoException() throws IOException {
        String                   key       = UUID.randomUUID().toString();
        Bytes                    chunkSize = Bytes.fromMb(1);
        RedirectableOutputStream expected  = new RedirectableOutputStream(chunkSize);

        expected.write(RandomUtils.nextBytes(chunkSize.toBytes()));

        create(key, expected.toByteArray());

        byte[] actual = new byte[expected.size() * 2];
        int    read;
        int    secondRead;

        try (MultiPartInputStream stream = stream(key)) {
            read       = stream.read(actual);
            secondRead = stream.read(actual);
        }

        assertThat(read).isEqualTo(chunkSize.toBytes());
        assertThat(secondRead).isNegative();
    }

}
