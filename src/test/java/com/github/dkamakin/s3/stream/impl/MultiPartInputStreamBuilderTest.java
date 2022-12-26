package com.github.dkamakin.s3.stream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

@ExtendWith(MockitoExtension.class)
class MultiPartInputStreamBuilderTest {

    static class Data {

        static final String KEY       = "file.txt";
        static final String BUCKET    = "storage";
        static final long   FILE_SIZE = 100L;
    }

    @Mock S3Client s3Client;

    void whenNeedToGetSize(HeadObjectResponse response) {
        when(s3Client.headObject((HeadObjectRequest) any())).thenReturn(response);
    }

    @Test
    void build_CorrectConfiguration_NoException() {
        assertThatCode(() -> MultiPartInputStream.builder()
                                                 .bucket(Data.BUCKET)
                                                 .key(Data.KEY)
                                                 .fileSize(Data.FILE_SIZE)
                                                 .client(s3Client)
                                                 .build())
            .doesNotThrowAnyException();

        verifyNoMoreInteractions(s3Client);
    }

    @Test
    void build_FileSizeNotPresent_PerformRequest() {
        whenNeedToGetSize(HeadObjectResponse.builder().contentLength(Data.FILE_SIZE).build());

        MultiPartInputStream.builder()
                            .bucket(Data.BUCKET)
                            .key(Data.KEY)
                            .client(s3Client)
                            .build();

        ArgumentCaptor<HeadObjectRequest> captor = ArgumentCaptor.forClass(HeadObjectRequest.class);

        verify(s3Client).headObject(captor.capture());

        assertThat(captor.getValue()).satisfies(head -> assertThat(head.bucket()).isEqualTo(Data.BUCKET))
                                     .satisfies(head -> assertThat(head.key()).isEqualTo(Data.KEY));
    }

}
