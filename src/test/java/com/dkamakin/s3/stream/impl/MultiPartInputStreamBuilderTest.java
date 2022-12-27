package com.dkamakin.s3.stream.impl;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;

@ExtendWith(MockitoExtension.class)
class MultiPartInputStreamBuilderTest {

    static class Data {

        static final String KEY    = "file.txt";
        static final String BUCKET = "storage";
    }

    @Mock S3Client s3Client;

    @Test
    void build_CorrectConfiguration_NoException() {
        assertThatCode(() -> MultiPartInputStream.builder()
                                                 .bucket(Data.BUCKET)
                                                 .key(Data.KEY)
                                                 .client(s3Client)
                                                 .build())
            .doesNotThrowAnyException();

        verifyNoMoreInteractions(s3Client);
    }

}
