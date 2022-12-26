package dkamakin.com.s3.stream.impl;

import dkamakin.com.s3.stream.IMultiPartInputStream;
import dkamakin.com.s3.stream.IMultiPartInputStreamBuilder;
import dkamakin.com.s3.stream.handler.IMultiPartDownloadHandler;
import dkamakin.com.s3.stream.handler.impl.S3FileDescriptor;
import dkamakin.com.s3.stream.util.impl.ByteRange;
import com.google.common.base.MoreObjects;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class MultiPartInputStream extends InputStream implements IMultiPartInputStream {

    private final IMultiPartDownloadHandler downloadHandler;
    private final long                      fileSize;
    private       long                      readLength;

    protected MultiPartInputStream(Long fileSze, IMultiPartDownloadHandler downloadHandler) {
        this.downloadHandler = downloadHandler;
        this.fileSize        = Optional.ofNullable(fileSze).orElseGet(downloadHandler::size);
    }

    @Override
    public int read() {
        throw new UnsupportedOperationException("Attempt to read a single byte from S3");
    }

    @Override
    public int read(byte[] data) {
        return read(data, 0, data.length);
    }

    @Override
    public int read(byte[] data, int offset, int length) {
        if (readLength >= fileSize) {
            return -1;
        }

        int result = downloadHandler.getPart(getRange(length), data, offset, length);

        readLength += result;

        return result;
    }

    @Override
    public long fileSize() {
        return fileSize;
    }

    @Override
    public long readLength() {
        return readLength;
    }

    @Override
    public S3FileDescriptor fileDescriptor() {
        return downloadHandler.fileDescriptor();
    }

    @Override
    public void close() {
        // --Nothing to do
    }

    private ByteRange getRange(int requestedLength) {
        return new ByteRange(readLength, readLength + requestedLength);
    }

    public static IMultiPartInputStreamBuilder builder() {
        return new MultiPartInputStreamBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MultiPartInputStream that = (MultiPartInputStream) o;
        return downloadHandler.equals(that.downloadHandler);
    }

    @Override
    public int hashCode() {
        return Objects.hash(downloadHandler);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("downloadHandler", downloadHandler)
                          .add("fileSize", fileSize)
                          .add("readLength", readLength)
                          .toString();
    }
}
