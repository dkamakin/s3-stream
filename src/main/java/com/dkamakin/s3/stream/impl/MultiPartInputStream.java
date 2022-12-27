package com.dkamakin.s3.stream.impl;

import com.dkamakin.s3.stream.IMultiPartInputStreamBuilder;
import com.dkamakin.s3.stream.handler.IMultiPartDownloadHandler;
import com.dkamakin.s3.stream.handler.impl.S3FileDescriptor;
import com.dkamakin.s3.stream.util.impl.ByteRange;
import com.google.common.base.MoreObjects;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.IntSupplier;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Allows you to download files from S3 without the need to know file size, caching or storing the entire object in the
 * file system. The stream downloads each part using {@link GetObjectRequest#range()} option. This class remembers the
 * amount of read bytes and moves the pointer {@link ByteRange}. EOS considered as a 416 HTTP code got from S3. A new
 * instance can be built using {@link MultiPartInputStream#builder()}. See also {@link IMultiPartInputStreamBuilder}
 */
@NotThreadSafe
public class MultiPartInputStream extends InputStream {

    static class Constant {

        static final int RANGE_NOT_SATISFIABLE = 416;
        static final int EOS                   = -1;
    }

    private static final Logger LOG = LoggerFactory.getLogger(MultiPartInputStream.class);

    private final IMultiPartDownloadHandler downloadHandler;
    private       long                      readLength;
    private       boolean                   isEnded;

    protected MultiPartInputStream(IMultiPartDownloadHandler downloadHandler) {
        this.downloadHandler = downloadHandler;
    }

    /**
     * Reading a single byte from S3 is considered bad practice because it can be used as a byte-by-byte read in a loop,
     * which would significantly affect performance.
     *
     * @throws UnsupportedOperationException the function is not needed
     */
    @Override
    public int read() {
        throw new UnsupportedOperationException("Attempt to read a single byte from S3");
    }

    /**
     * Tries to read requested array length from file
     *
     * @param data the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or -1 if there is no more data because the end of the
     * stream has been reached.
     */
    @Override
    public int read(byte[] data) {
        return read(data, 0, data.length);
    }

    /**
     * Reads up to {@code len} bytes of data from the file into an array of bytes.  An attempt is made to read as many
     * as {@code len} bytes, but a smaller number may be read. The number of bytes actually read is returned as an
     * integer.
     *
     * @param data   the buffer into which the data is read.
     * @param offset the start offset in array {@code b} at which the data is written.
     * @param length the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or -1 if there is no more data because the end of the *
     * stream has been reached.
     */
    @Override
    public int read(byte[] data, int offset, int length) {
        if (isEnded) {
            return Constant.EOS;
        }

        int read;

        try {
            read = logReadLength(() -> downloadHandler.getPart(getRange(length), data, offset, length));
        } catch (S3Exception e) {
            read = handle(e);
        }

        return read;
    }

    /**
     * Does nothing
     */
    @Override
    public void close() {
        // --Nothing to do
    }

    /**
     * Get current amount of bytes read from file
     *
     * @return read length
     */
    public long readLength() {
        return readLength;
    }

    /**
     * Get a file descriptor on which stream based
     *
     * @return file descriptor
     */
    public S3FileDescriptor fileDescriptor() {
        return downloadHandler.fileDescriptor();
    }

    public static IMultiPartInputStreamBuilder builder() {
        return new MultiPartInputStreamBuilder();
    }

    private int handle(S3Exception exception) {
        if (exception.statusCode() == Constant.RANGE_NOT_SATISFIABLE) {
            LOG.info("Got 416 from S3, treat like an EOS");
            isEnded = true;
            return Constant.EOS;
        } else {
            LOG.error(exception.getMessage(), exception);
            throw exception;
        }
    }

    private ByteRange getRange(int requestedLength) {
        return new ByteRange(readLength, readLength + requestedLength);
    }

    private int logReadLength(IntSupplier action) {
        int read = action.getAsInt();

        readLength += read;

        LOG.debug("Read: {}, total read: {}", read, readLength);

        return read;
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
                          .add("readLength", readLength)
                          .toString();
    }
}
