package dkamakin.com.s3.stream.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import dkamakin.com.s3.stream.handler.IMultiPartDownloadHandler;
import dkamakin.com.s3.stream.util.impl.ByteRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MultiPartInputStreamTest {

    static final class Data {

        static final int FILE_SIZE = 100;
    }

    @Mock
    IMultiPartDownloadHandler downloadHandler;

    MultiPartInputStream target;

    @BeforeEach
    void setUp() {
        target = new MultiPartInputStream((long) Data.FILE_SIZE, downloadHandler);
    }

    void whenNeedToRead(Integer first, Integer... others) {
        when(downloadHandler.getPart(any(), any(), anyInt(), anyInt())).thenReturn(first, others);
    }

    void whenNeedToGetSize(long size) {
        when(downloadHandler.size()).thenReturn(size);
    }

    @Test
    void equals_DifferentStreams_NotEquals() {
        MultiPartInputStream another = new MultiPartInputStream((long) Data.FILE_SIZE,
                                                                mock(IMultiPartDownloadHandler.class));

        System.out.println("Another: " + another);

        assertThat(target).isNotEqualTo(another).doesNotHaveSameHashCodeAs(another);
    }

    @Test
    void equals_SameStreams_NotEquals() {
        MultiPartInputStream another = new MultiPartInputStream((long) Data.FILE_SIZE,
                                                                downloadHandler);

        assertThat(target).isEqualTo(another).hasSameHashCodeAs(another);
    }

    @Test
    void ctor_FileSizeIsNull_GetFromDownloadHandler() {
        long expected = 100L;

        whenNeedToGetSize(expected);

        MultiPartInputStream target = new MultiPartInputStream(null, downloadHandler);
        long                 actual = target.fileSize();

        verify(downloadHandler).size();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void ctor_FileSizeProvided_UseProvidedSize() {
        verifyNoInteractions(downloadHandler);
    }

    @Test
    void close_NoAction_NoException() {
        target.close();

        verifyNoInteractions(downloadHandler);
    }

    @Test
    void read_SingleByte_UnsupportedOperationException() {
        assertThatThrownBy(target::read).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void fileDescriptor_HandlerPresent_ExtractDescriptor() {
        target.fileDescriptor();

        verify(downloadHandler).fileDescriptor();
    }

    @Test
    void read_OverflowFileSize_EOS() {
        whenNeedToRead(Data.FILE_SIZE);

        target.read(new byte[Data.FILE_SIZE]);

        int actual = target.read(new byte[10]);

        assertThat(actual).isNegative();
        assertThat(target.readLength()).isEqualTo(Data.FILE_SIZE);
    }

    @Test
    void read_FirstReadArrayNoOffset_CorrectRange() {
        int    length = 10;
        byte[] data   = new byte[length];

        whenNeedToRead(length);

        target.read(data);

        verify(downloadHandler).getPart(new ByteRange(0, length), data, 0, data.length);
        assertThat(target.readLength()).isEqualTo(length);
    }

    @Test
    void read_MultipleRead_MoveRange() {
        int    length = 10;
        byte[] data   = new byte[length];

        whenNeedToRead(length, length);

        target.read(data);
        target.read(data);

        ArgumentCaptor<ByteRange> captor = ArgumentCaptor.forClass(ByteRange.class);

        verify(downloadHandler, times(2)).getPart(captor.capture(), eq(data), anyInt(), anyInt());

        assertThat(captor.getAllValues()).contains(new ByteRange(0, length), new ByteRange(length, length * 2));
        assertThat(target.readLength()).isEqualTo(length * 2);
    }

    @Test
    void read_ProvideOffset_ReadBasedOnProvidedOffset() {
        int    length = 10;
        int    offset = 5;
        byte[] data   = new byte[length + offset];

        whenNeedToRead(length);

        target.read(data, offset, length);

        verify(downloadHandler).getPart(new ByteRange(0, length), data, offset, length);
        assertThat(target.readLength()).isEqualTo(length);
    }

    @Test
    void read_ProvideOffsetMultipleTimes_ReadBasedOnProvidedOffset() {
        int    length       = 10;
        int    offset       = 5;
        int    secondOffset = 10;
        byte[] data         = new byte[length + offset + secondOffset];

        whenNeedToRead(length, length);

        target.read(data, offset, length);
        target.read(data, secondOffset, length);

        ArgumentCaptor<ByteRange> captor = ArgumentCaptor.forClass(ByteRange.class);

        verify(downloadHandler, times(2)).getPart(captor.capture(), eq(data), anyInt(), anyInt());

        assertThat(captor.getAllValues()).contains(new ByteRange(0, length), new ByteRange(length, length * 2));
        assertThat(target.readLength()).isEqualTo(length * 2);
    }
}
