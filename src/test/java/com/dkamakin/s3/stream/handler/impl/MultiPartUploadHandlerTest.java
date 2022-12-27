package com.dkamakin.s3.stream.handler.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dkamakin.s3.stream.handler.IMultiPartUploadHandler;
import com.dkamakin.s3.stream.exception.PartNumberExceedLimitException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

@ExtendWith(MockitoExtension.class)
class MultiPartUploadHandlerTest {

    static class Data {

        static final String                        KEY                     = "file.txt";
        static final String                        BUCKET                  = "storage";
        static final String                        E_TAG                   = "eTag";
        static final String                        UPLOAD_ID               = "some id";
        static final String                        ANOTHER_UPLOAD_ID       = "another id";
        static final CreateMultipartUploadResponse CREATE_RESPONSE         = CreateMultipartUploadResponse.builder()
                                                                                                          .uploadId(
                                                                                                              UPLOAD_ID)
                                                                                                          .build();
        static final CreateMultipartUploadResponse ANOTHER_CREATE_RESPONSE = CreateMultipartUploadResponse.builder()
                                                                                                          .uploadId(
                                                                                                              ANOTHER_UPLOAD_ID)
                                                                                                          .build();
    }

    @Mock S3Client s3Client;

    IMultiPartUploadHandler target;

    @BeforeEach
    void setUp() {
        whenNeedToCreateMultipartUpload(Data.CREATE_RESPONSE);

        target = new MultiPartUploadHandler(new S3FileDescriptor(Data.BUCKET, Data.KEY, s3Client));
    }

    void whenNeedToCreateMultipartUpload(CreateMultipartUploadResponse response) {
        when(s3Client.createMultipartUpload((CreateMultipartUploadRequest) any())).thenReturn(response);
    }

    void whenNeedToUploadPart(UploadPartResponse response, UploadPartResponse... other) {
        when(s3Client.uploadPart((UploadPartRequest) any(), (RequestBody) any())).thenReturn(response, other);
    }

    UploadPartResponse createDefaultUploadPartResponse() {
        return UploadPartResponse.builder().eTag(Data.E_TAG).build();
    }

    void exceedLimit() {
        for (int i = 0; i < 10002; i++) {
            target.upload(RequestBody.empty());
        }
    }

    @Test
    void equals_DifferentHandlers_NotEquals() {
        whenNeedToCreateMultipartUpload(Data.ANOTHER_CREATE_RESPONSE);

        IMultiPartUploadHandler another = new MultiPartUploadHandler(new S3FileDescriptor(Data.BUCKET, Data.KEY,
                                                                                          s3Client));

        System.out.println("Another: " + another);
        System.out.println("Descriptor: " + another.fileDescriptor());

        assertThat(target).isNotEqualTo(another).doesNotHaveSameHashCodeAs(another);
    }

    @Test
    void equals_SameHandlers_Equals() {
        whenNeedToCreateMultipartUpload(Data.CREATE_RESPONSE);

        IMultiPartUploadHandler another = new MultiPartUploadHandler(new S3FileDescriptor(Data.BUCKET, Data.KEY,
                                                                                          s3Client));

        System.out.println("Another: " + another);

        assertThat(target).isEqualTo(another).hasSameHashCodeAs(another);
    }

    @Test
    void close_NoDataProvided_AbortMultiPartUpload() {
        target.close();

        ArgumentCaptor<AbortMultipartUploadRequest> captor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);

        verify(s3Client).abortMultipartUpload(captor.capture());

        assertThat(captor.getValue()).satisfies(abort -> assertThat(abort.key()).isEqualTo(Data.KEY))
                                     .satisfies(abort -> assertThat(abort.bucket()).isEqualTo(Data.BUCKET));
    }

    @Test
    void close_DataProvided_CompleteUploadRequest() {
        whenNeedToUploadPart(createDefaultUploadPartResponse());

        target.upload(RequestBody.empty());
        target.close();

        ArgumentCaptor<CompleteMultipartUploadRequest> captor = ArgumentCaptor.forClass(
            CompleteMultipartUploadRequest.class);

        verify(s3Client).completeMultipartUpload(captor.capture());

        assertThat(captor.getValue()).satisfies(complete -> assertThat(complete.key()).isEqualTo(Data.KEY))
                                     .satisfies(complete -> assertThat(complete.bucket()).isEqualTo(Data.BUCKET))
                                     .satisfies(complete -> assertThat(complete.uploadId()).isEqualTo(Data.UPLOAD_ID))
                                     .satisfies(complete -> assertThat(complete.multipartUpload().parts())
                                         .map(CompletedPart::eTag)
                                         .contains(Data.E_TAG));

    }

    @Test
    void upload_FirstCall_UploadFirstPartNumber() {
        whenNeedToUploadPart(createDefaultUploadPartResponse());

        RequestBody expected = RequestBody.empty();

        target.upload(expected);

        ArgumentCaptor<UploadPartRequest> captor = ArgumentCaptor.forClass(UploadPartRequest.class);

        verify(s3Client).uploadPart(captor.capture(), eq(expected));

        assertThat(captor.getValue()).satisfies(upload -> assertThat(upload.partNumber()).isEqualTo(1))
                                     .satisfies(upload -> assertThat(upload.key()).isEqualTo(Data.KEY))
                                     .satisfies(upload -> assertThat(upload.bucket()).isEqualTo(Data.BUCKET))
                                     .satisfies(upload -> assertThat(upload.uploadId()).isEqualTo(Data.UPLOAD_ID));
    }

    @Test
    void upload_MultipleCalls_IncreasePartNumber() {
        whenNeedToUploadPart(createDefaultUploadPartResponse(),
                             createDefaultUploadPartResponse(),
                             createDefaultUploadPartResponse());

        target.upload(RequestBody.empty());
        target.upload(RequestBody.empty());
        target.upload(RequestBody.empty());

        ArgumentCaptor<UploadPartRequest> captor = ArgumentCaptor.forClass(UploadPartRequest.class);

        verify(s3Client, times(3)).uploadPart(captor.capture(), (RequestBody) any());

        assertThat(captor.getAllValues()).extracting(UploadPartRequest::partNumber)
                                         .containsOnlyOnce(1)
                                         .containsOnlyOnce(2)
                                         .containsOnlyOnce(3);
    }

    @Test
    void upload_ExceedsMaxPartNumber_PartNumberExceedLimitException() {
        whenNeedToUploadPart(createDefaultUploadPartResponse());

        assertThatThrownBy(this::exceedLimit).isInstanceOf(PartNumberExceedLimitException.class);
    }
}
