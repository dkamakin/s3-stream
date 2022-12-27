package com.dkamakin.s3.stream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.dkamakin.s3.stream.handler.IMultiPartUploadHandler;
import com.dkamakin.s3.stream.util.impl.Bytes;
import com.google.common.base.MoreObjects;
import com.dkamakin.s3.stream.impl.MultiPartOutputStreamTest.Data.WriteArguments;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MultiPartOutputStreamTest {

    static class Data {

        static class WriteArguments {

            byte[] data;
            int    off;
            int    len;

            WriteArguments(byte[] data, int off, int len) {
                this.data = data;
                this.off  = off;
                this.len  = len;
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
                                  .add("data", data)
                                  .add("off", off)
                                  .add("len", len)
                                  .toString();
            }
        }

        static final int   MIN_PART_SIZE = 10;
        static final Bytes BUFFER_SIZE   = Bytes.fromBytes(MIN_PART_SIZE);
    }

    @Mock IMultiPartUploadHandler uploadHandler;

    MultiPartOutputStream target;

    @BeforeEach
    public void setUp() {
        target = new MultiPartOutputStream(Data.BUFFER_SIZE, uploadHandler);
    }

    static Stream<byte[]> streamArraysOverflowsBufferSize() {
        return Stream.of(
            new byte[Data.MIN_PART_SIZE],
            new byte[Data.MIN_PART_SIZE + 1]
        );
    }

    static Stream<WriteArguments> streamIllegalWriteArguments() {
        return Stream.of(
            new WriteArguments(new byte[1], 0, 2),
            new WriteArguments(new byte[1], 1, 1),
            new WriteArguments(new byte[0], 0, 1),
            new WriteArguments(new byte[2], 2, 1),
            new WriteArguments(new byte[10], 5, 10),
            new WriteArguments(new byte[1], -1, 1),
            new WriteArguments(new byte[1], 0, -1)
        );
    }

    @Test
    void equals_DifferentStreams_NotEquals() {
        MultiPartOutputStream another = new MultiPartOutputStream(Data.BUFFER_SIZE,
                                                                  mock(IMultiPartUploadHandler.class));

        assertThat(target).isNotEqualTo(another).doesNotHaveSameHashCodeAs(another);
    }

    @Test
    void equals_SameStreams_NotEquals() {
        MultiPartOutputStream another = new MultiPartOutputStream(Data.BUFFER_SIZE, uploadHandler);

        assertThat(target).isEqualTo(another).hasSameHashCodeAs(another);
    }

    @Test
    void fileDescriptor_HandlerPresent_ExtractDescriptor() {
        target.fileDescriptor();

        verify(uploadHandler).fileDescriptor();
    }

    @Test
    void write_SingleByte_DataIsBuffered() {
        target.write(1);

        verifyNoMoreInteractions(uploadHandler);

        assertThat(target.size()).isEqualTo(1);
    }

    @Test
    void close_ArrayIsEmpty_NoFlushHandlerIsClosed() {
        target.close();

        verify(uploadHandler).close();
        verify(uploadHandler, never()).upload(any());
    }

    @Test
    void flush_EmptyBuffer_NoAction() {
        target.flush();

        verifyNoInteractions(uploadHandler);
    }

    @Test
    void flush_BufferIsNotEmpty_FlushAllData() {
        target.write(new byte[1]);
        target.flush();

        verify(uploadHandler).upload(any());
    }

    @Test
    void close_ArrayIsNotEmptyAndSizeLessThanMinPartSize_FlushThenCloseHandler() {
        int size = Data.MIN_PART_SIZE - 1;

        target.write(new byte[size]);
        target.close();

        int actual = target.size();

        verify(uploadHandler).close();
        verify(uploadHandler).upload(any());

        assertThat(actual).isEqualTo(size);
    }

    @Test
    void write_ArrayLessThanMinPartSize_DataIsBufferedNothingFlushed() {
        int expected = Data.MIN_PART_SIZE - 1;

        target.write(new byte[expected]);

        int actual = target.size();

        verify(uploadHandler, never()).upload(any());

        assertThat(expected).isEqualTo(actual);
    }

    @ParameterizedTest
    @MethodSource("streamArraysOverflowsBufferSize")
    void write_ArraySizeEqualToOrGreaterThanMinPartSize_FlushData(byte[] data) {
        target.write(data);

        verify(uploadHandler).upload(any());

        assertThat(target).extracting(MultiPartOutputStream::size).isEqualTo(0);
    }

    @Test
    void write_MultipleArrays_FlushOnlyWhenDataExceedsMinPartSize() {
        int expected = 2;

        target.write(new byte[Data.MIN_PART_SIZE]);
        target.write(new byte[Data.MIN_PART_SIZE - 1]);
        target.write(new byte[Data.MIN_PART_SIZE]);
        target.write(new byte[expected]);

        int actual = target.size();

        assertThat(actual).isEqualTo(expected);

        verify(uploadHandler, times(2)).upload(any());
    }

    @ParameterizedTest
    @MethodSource("streamIllegalWriteArguments")
    void write_InvalidArguments_IllegalArgumentException(WriteArguments arguments) {
        assertThatThrownBy(() -> target.write(arguments.data, arguments.off, arguments.len))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

}
