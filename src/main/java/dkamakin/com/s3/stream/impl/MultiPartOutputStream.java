package dkamakin.com.s3.stream.impl;

import dkamakin.com.s3.stream.IMultiPartOutputStreamBuilder;
import dkamakin.com.s3.stream.handler.IMultiPartUploadHandler;
import dkamakin.com.s3.stream.handler.impl.S3FileDescriptor;
import dkamakin.com.s3.stream.util.impl.Bytes;
import dkamakin.com.s3.stream.util.impl.RedirectableOutputStream;
import java.io.OutputStream;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;

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

    @Override
    public void write(int b) {
        buffer.write(b);
    }

    @Override
    public void write(byte[] data) {
        write(data, 0, data.length);
    }

    @Override
    public void write(byte[] data, int offset, int length) {
        buffer.write(data, offset, length);

        if (isBufferExceedsMinPartSize()) {
            flush();
        }
    }

    @Override
    public void close() {
        upload();
        uploadHandler.close();
    }

    @Override
    public void flush() {
        upload();
        resetBuffer();
    }

    public Bytes minPartSize() {
        return minPartSize;
    }

    public S3FileDescriptor fileDescriptor() {
        return uploadHandler.fileDescriptor();
    }

    public int size() {
        return buffer.size();
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
