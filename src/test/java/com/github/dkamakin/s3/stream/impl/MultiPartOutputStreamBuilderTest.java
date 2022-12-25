package com.github.dkamakin.s3.stream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.github.dkamakin.s3.stream.IMultiPartOutputStreamBuilder;
import com.github.dkamakin.s3.stream.impl.MultiPartOutputStreamBuilder.Constant;
import com.github.dkamakin.s3.stream.util.impl.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;

@ExtendWith(MockitoExtension.class)
class MultiPartOutputStreamBuilderTest {

    static class Data {

        static final String                        KEY       = "file.txt";
        static final String                        BUCKET    = "storage";
        static final String                        UPLOAD_ID = "some_id";
        static final CreateMultipartUploadResponse RESPONSE  = CreateMultipartUploadResponse.builder()
                                                                                            .uploadId(UPLOAD_ID)
                                                                                            .build();
    }

    @Mock S3Client s3Client;

    void whenNeedToCreateMultipartUpload(CreateMultipartUploadResponse response) {
        when(s3Client.createMultipartUpload((CreateMultipartUploadRequest) any())).thenReturn(response);
    }

    IMultiPartOutputStreamBuilder defaultBuilder() {
        return MultiPartOutputStream.builder()
                                    .forBucket(Data.BUCKET)
                                    .forKey(Data.BUCKET)
                                    .withClient(s3Client);
    }

    @Test
    void build_MinimumConfiguration_ReturnNewStream() {
        whenNeedToCreateMultipartUpload(Data.RESPONSE);

        assertThatCode(() -> MultiPartOutputStream.builder()
                                                  .forKey(Data.KEY)
                                                  .forBucket(Data.BUCKET)
                                                  .withClient(s3Client)
                                                  .build())
            .doesNotThrowAnyException();
    }

    @Test
    void build_ExtendedConfiguration_ReturnNewStream() {
        whenNeedToCreateMultipartUpload(Data.RESPONSE);

        Bytes minPartSize = Bytes.fromMb(6);

        assertThatCode(() -> MultiPartOutputStream.builder()
                                                  .forKey(Data.KEY)
                                                  .forBucket(Data.BUCKET)
                                                  .withClient(s3Client)
                                                  .setMinPartSize(minPartSize)
                                                  .build())
            .doesNotThrowAnyException();
    }

    @Test
    void build_BucketNotPresent_IllegalArgumentException() {
        assertThatThrownBy(() -> MultiPartOutputStream.builder()
                                                      .forKey(Data.KEY)
                                                      .withClient(s3Client)
                                                      .build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void build_ClientNotPresent_IllegalArgumentException() {
        assertThatThrownBy(() -> MultiPartOutputStream.builder()
                                                      .forBucket(Data.BUCKET)
                                                      .forKey(Data.KEY)
                                                      .build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void build_KeyNotPresent_IllegalArgumentException() {
        assertThatThrownBy(() -> MultiPartOutputStream.builder()
                                                      .forBucket(Data.BUCKET)
                                                      .withClient(s3Client)
                                                      .build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void build_MinPartSizeNotPresent_UseDefault() {
        whenNeedToCreateMultipartUpload(Data.RESPONSE);

        Bytes expected = Constant.S3_MIN_PART_SIZE;
        Bytes actual   = defaultBuilder().build().minPartSize();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void build_MinPartSizeLessThanAmazonRequirements_IllegalArgumentException() {
        assertThatThrownBy(() -> defaultBuilder().setMinPartSize(Bytes.fromBytes(1024 * 1024 * 5 - 1))
                                                 .build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void build_MinPartSizeGreaterThanAmazonRequirements_NoException() {
        whenNeedToCreateMultipartUpload(Data.RESPONSE);

        Bytes expected = Bytes.fromMb(6);
        Bytes actual   = defaultBuilder().setMinPartSize(expected).build().minPartSize();

        assertThat(actual).isEqualTo(expected);
    }
}
