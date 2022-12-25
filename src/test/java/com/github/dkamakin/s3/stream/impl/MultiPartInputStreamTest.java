package com.github.dkamakin.s3.stream.impl;

import com.github.dkamakin.s3.stream.IMultiPartInputStream;
import com.github.dkamakin.s3.stream.handler.IMultiPartDownloadHandler;
import com.github.dkamakin.s3.stream.util.impl.ByteRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MultiPartInputStreamTest {

    static final class Data {

        static final int FILE_SIZE = 100;
    }

    @Mock
    IMultiPartDownloadHandler downloadHandler;

    IMultiPartInputStream target;

    @BeforeEach
    void setUp() {
        target = new MultiPartInputStream((long) Data.FILE_SIZE, downloadHandler);
    }

    void whenNeedToRead(Integer first, Integer... others) {
        when(downloadHandler.getPart(any(), any(), anyInt(), anyInt())).thenReturn(first, others);
    }

    @Test
    void ctor_FileSizeIsNull_GetFromDownloadHandler() {
        new MultiPartInputStream(null, downloadHandler);

        verify(downloadHandler).size();
    }

    @Test
    void ctor_FileSizeProvided_UseProvidedSize() {
        verifyNoInteractions(downloadHandler);
    }

    @Test
    void read_OverflowFileSize_EOS() {
        whenNeedToRead(Data.FILE_SIZE);

        target.read(new byte[Data.FILE_SIZE]);

        int actual = target.read(new byte[10]);

        assertThat(actual).isNegative();
    }

    @Test
    void read_FirstReadArrayNoOffset_CorrectRange() {
        int    length = 10;
        byte[] data   = new byte[length];

        target.read(data);

        verify(downloadHandler).getPart(new ByteRange(0, length), data, 0, data.length);
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
    }

    @Test
    void read_ProvideOffset_ReadBasedOnProvidedOffset() {
        int    length = 10;
        int    offset = 5;
        byte[] data   = new byte[length + offset];

        whenNeedToRead(length, length);

        target.read(data, offset, length);

        verify(downloadHandler).getPart(new ByteRange(0, length), data, offset, length);
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
    }
}
