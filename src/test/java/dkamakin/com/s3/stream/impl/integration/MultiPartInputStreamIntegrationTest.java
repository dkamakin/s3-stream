package dkamakin.com.s3.stream.impl.integration;

import static org.assertj.core.api.Assertions.assertThat;

import dkamakin.com.s3.stream.IMultiPartInputStream;
import dkamakin.com.s3.stream.impl.MultiPartInputStream;
import dkamakin.com.s3.stream.util.impl.Bytes;
import dkamakin.com.s3.stream.util.impl.RedirectableOutputStream;
import java.util.UUID;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Disabled("manual")
class MultiPartInputStreamIntegrationTest extends MinioIntegrationTest {

    void create(String key, RequestBody body) {
        s3Client.putObject(PutObjectRequest.builder().key(key).bucket(Data.BUCKET).build(),
                           body);
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

        try (IMultiPartInputStream stream = stream(key)) {
            actual = stream.read(new byte[10]);
        }

        assertThat(actual).isNegative();
    }

    @Test
    void read_FileWithContent_ReadRequestedContent() {
        String                   key       = UUID.randomUUID().toString();
        Bytes                    chunkSize = Bytes.fromKb(512);
        RedirectableOutputStream expected  = new RedirectableOutputStream(chunkSize);

        expected.write(RandomUtils.nextBytes(chunkSize.toBytes()));

        create(key, expected.toByteArray());

        byte[] actual = new byte[chunkSize.toBytes()];
        int    read;

        try (MultiPartInputStream stream = stream(key)) {
            read = stream.read(actual);
        }

        assertThat(read).isEqualTo(chunkSize.toBytes());
        assertThat(actual).isEqualTo(expected.toByteArray());
    }

}
