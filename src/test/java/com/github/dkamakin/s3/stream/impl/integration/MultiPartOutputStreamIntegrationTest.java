package com.github.dkamakin.s3.stream.impl.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.dkamakin.s3.stream.IMultiPartOutputStream;
import com.github.dkamakin.s3.stream.impl.MultiPartOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MultiPartOutputStreamIntegrationTest extends MinioIntegrationTest {

    IMultiPartOutputStream createNewFile() {
        return MultiPartOutputStream.builder()
                                    .forBucket(Data.BUCKET)
                                    .forKey(UUID.randomUUID().toString())
                                    .withClient(s3Client)
                                    .build();
    }

    @Test
    void write_SingleString_SerializeThenReceiveCorrectBytes() {
        String original = "This is a simple test attempting to serialize/deserialize a string";
        byte[] expected = original.getBytes(StandardCharsets.UTF_8);
        String key;

        try (IMultiPartOutputStream stream = createNewFile()) {
            stream.write(expected);

            key = stream.fileDescriptor().key();
        }

        assertThat(key).isNotBlank();

        byte[] actual = download(key, expected.length);

        assertThat(actual).hasSize(expected.length).isEqualTo(expected);
        assertThat(new String(actual)).isEqualTo(original);
    }

}
