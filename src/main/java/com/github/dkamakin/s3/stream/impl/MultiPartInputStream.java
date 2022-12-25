package com.github.dkamakin.s3.stream.impl;

import static com.google.common.base.Preconditions.checkPositionIndexes;

import com.github.dkamakin.s3.stream.IMultiPartInputStream;
import com.github.dkamakin.s3.stream.handler.IMultiPartDownloadHandler;
import com.github.dkamakin.s3.stream.handler.impl.S3FileDescriptor;
import com.github.dkamakin.s3.stream.util.impl.ByteRange;
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
    public int read(byte[] b) {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (readLength >= fileSize) {
            return -1;
        }

        checkPositionIndexes(off, off + len, b.length);

        int result = downloadHandler.getPart(getRange(len), b, off, len);

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
