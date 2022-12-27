package com.dkamakin.s3.stream.impl;

import com.dkamakin.s3.stream.IMultiPartOutputStreamBuilder;
import com.dkamakin.s3.stream.handler.IMultiPartUploadHandler;
import com.dkamakin.s3.stream.handler.impl.S3FileDescriptor;
import com.dkamakin.s3.stream.util.impl.Bytes;
import com.dkamakin.s3.stream.util.impl.RedirectableOutputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;

/**
 * Allows you to upload files to S3 without the need to cache or save entire objects in the file system, as suggested by
 * the official API. The class is based on multipart upload, which implies loading in parts, the maximum number of which
 * is 10000. Keep in mind that only one part can be less than 5 MB. This class tries to buffer data until the required
 * limit of 5 MB is reached. if more information is provided at one time, a {@link MultiPartOutputStream#flush()} will
 * occur, which will upload everything that is available in the stream. The value of 5 MB can be increased, see
 * {@link IMultiPartOutputStreamBuilder#minPartSize(Bytes)}. Anyway, {@link MultiPartOutputStream#close()} tries to
 * flush the stored data. A new instance can be built using {@link MultiPartOutputStream#builder()}. See also
 * {@link IMultiPartOutputStreamBuilder}
 */
@NotThreadSafe
public class MultiPartOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(MultiPartOutputStream.class);

    private final IMultiPartUploadHandler  uploadHandler;
    private final Bytes                    minPartSize;
    private       RedirectableOutputStream buffer;

    protected MultiPartOutputStream(Bytes minPartSize, IMultiPartUploadHandler uploadHandler) {
        this.uploadHandler = uploadHandler;
        this.buffer        = new RedirectableOutputStream(minPartSize);
        this.minPartSize   = minPartSize;
    }

    /**
     * Buffering provided byte until {@link MultiPartOutputStream#size()} is less than
     * {@link MultiPartOutputStream#minPartSize()} or {@link MultiPartOutputStream#close()} is called.
     *
     * @param data the {@code byte}.
     */
    @Override
    public void write(int data) {
        flushable(stream -> stream.write(data));
    }

    /**
     * Append data to buffer until {@link MultiPartOutputStream#size()} is less than
     * {@link MultiPartOutputStream#minPartSize()} or {@link MultiPartOutputStream#close()} is called.
     *
     * @param data the data.
     */
    @Override
    public void write(byte[] data) {
        write(data, 0, data.length);
    }

    /**
     * Buffering provided data until {@link MultiPartOutputStream#size()} is less than
     * {@link MultiPartOutputStream#minPartSize()} or {@link MultiPartOutputStream#close()} is called.
     *
     * @param data   the data.
     * @param offset the start offset in the data.
     * @param length the number of bytes to write.
     */
    @Override
    public void write(byte[] data, int offset, int length) {
        flushable(stream -> stream.write(data, offset, length));
    }

    /**
     * Flushes the stored data and sends multipart upload complete or abort request depends on whether data was
     * provided
     */
    @Override
    public void close() {
        upload();
        uploadHandler.close();
    }

    /**
     * Upload the stored data as a new part within multipart upload request. Allocates a new buffer with
     * {@link MultiPartOutputStream#minPartSize()} size
     */
    @Override
    public void flush() {
        upload();
        resetBuffer();
    }

    /**
     * Get a minimum part size to be uploaded
     *
     * @return minimum part size
     */
    public Bytes minPartSize() {
        return minPartSize;
    }

    /**
     * Get a file descriptor on which stream based
     *
     * @return file descriptor
     */
    public S3FileDescriptor fileDescriptor() {
        return uploadHandler.fileDescriptor();
    }

    /**
     * Get current buffer size
     *
     * @return buffer size
     */
    public int size() {
        return buffer.size();
    }

    private void flushable(Consumer<RedirectableOutputStream> streamAction) {
        streamAction.accept(buffer);

        if (isBufferExceedsMinPartSize()) {
            flush();
        }
    }

    private boolean isBufferExceedsMinPartSize() {
        return buffer.size() >= minPartSize.toBytes();
    }

    private void upload() {
        if (isNotEmpty()) {
            LOG.info("Uploading data, size: {}", size());
            uploadHandler.upload(RequestBody.fromInputStream(buffer.redirect(), buffer.size()));
        }
    }

    private void resetBuffer() {
        buffer = new RedirectableOutputStream(minPartSize);
    }

    public static IMultiPartOutputStreamBuilder builder() {
        return new MultiPartOutputStreamBuilder();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean isNotEmpty() {
        return !isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MultiPartOutputStream that = (MultiPartOutputStream) o;
        return Objects.equals(uploadHandler, that.uploadHandler);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uploadHandler);
    }
}
