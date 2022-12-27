package com.dkamakin.s3.stream.handler.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dkamakin.s3.stream.exception.ReadException;
import com.dkamakin.s3.stream.handler.IFileDescriptorHolder;
import com.dkamakin.s3.stream.handler.IMultiPartDownloadHandler;
import com.dkamakin.s3.stream.util.impl.ByteRange;
import com.dkamakin.s3.stream.util.impl.RetryableStreamReader;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@ExtendWith(MockitoExtension.class)
class MultiPartDownloadHandlerTest {

    static class Data {

        static final String KEY    = "key";
        static final String BUCKET = "bucket";
    }

    @Mock S3Client              s3Client;
    @Mock RetryableStreamReader streamReader;

    IMultiPartDownloadHandler target;

    @BeforeEach
    void setUp() {
        target = new MultiPartDownloadHandler(new S3FileDescriptor(Data.BUCKET, Data.KEY, s3Client),
                                              this::streamReaderProvider);
    }

    RetryableStreamReader streamReaderProvider(InputStream stream) {
        return streamReader;
    }

    void whenNeedToGetObject(ResponseInputStream<GetObjectResponse> stream) {
        when(s3Client.getObject((GetObjectRequest) any())).thenReturn(stream);
    }

    void whenNeedToThrowOnRead(Throwable throwable) throws IOException {
        when(streamReader.read(any(), anyInt(), anyInt())).thenThrow(throwable);
    }

    @Test
    void equals_SameDescriptors_Equals() {
        IMultiPartDownloadHandler another = new MultiPartDownloadHandler(new S3FileDescriptor(Data.BUCKET, Data.KEY,
                                                                                              s3Client),
                                                                         this::streamReaderProvider);

        assertThat(target).isEqualTo(another).hasSameHashCodeAs(another);
    }

    @Test
    void equals_DifferentDescriptors_NotEquals() {
        IMultiPartDownloadHandler another = new MultiPartDownloadHandler(new S3FileDescriptor(Data.BUCKET, "other file",
                                                                                              s3Client),
                                                                         this::streamReaderProvider);

        System.out.println("Another: " + another);

        assertThat(target).isNotEqualTo(another).doesNotHaveSameHashCodeAs(another)
                          .extracting(IFileDescriptorHolder::fileDescriptor).isNotEqualTo(another.fileDescriptor());
    }

    @Test
    void getPart_RequestRange_GetCorrectPartAndReadWithRetries() throws IOException {
        long        from           = 10;
        int         length         = 10;
        ByteRange   range          = new ByteRange(10, from + length);
        byte[]      data           = new byte[length];
        InputStream expectedStream = mock(InputStream.class);

        whenNeedToGetObject(new ResponseInputStream<>(GetObjectResponse.builder().build(),
                                                      AbortableInputStream.create(expectedStream)));

        target.getPart(range, data, 0, length);

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);

        verify(streamReader).read(data, 0, length);
        verify(s3Client).getObject(captor.capture());
        verify(expectedStream).close();

        assertThat(captor.getValue()).satisfies(get -> assertThat(get.key()).isEqualTo(Data.KEY))
                                     .satisfies(get -> assertThat(get.bucket()).isEqualTo(Data.BUCKET))
                                     .satisfies(get -> assertThat(get.range()).isEqualTo(range.toString()));
    }

    @Test
    void getPart_ExceptionWhenReading_ReadException() throws IOException {
        whenNeedToThrowOnRead(new IOException());

        long        from           = 10;
        int         length         = 10;
        ByteRange   range          = new ByteRange(10, from + length);
        byte[]      data           = new byte[length];
        InputStream expectedStream = mock(InputStream.class);

        whenNeedToGetObject(new ResponseInputStream<>(GetObjectResponse.builder().build(),
                                                      AbortableInputStream.create(expectedStream)));

        assertThatThrownBy(() -> target.getPart(range, data, 0, length)).isInstanceOf(ReadException.class);

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);

        verify(streamReader).read(data, 0, length);
        verify(s3Client).getObject(captor.capture());
        verify(expectedStream).close();
    }

}
