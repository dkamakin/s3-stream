package com.github.dkamakin.s3.stream.impl;

import com.github.dkamakin.s3.stream.IMultiPartOutputStream;
import com.github.dkamakin.s3.stream.IMultiPartOutputStreamBuilder;
import com.github.dkamakin.s3.stream.handler.IMultiPartUploadHandler;
import com.github.dkamakin.s3.stream.handler.impl.S3FileDescriptor;
import com.github.dkamakin.s3.stream.util.impl.Bytes;
import com.github.dkamakin.s3.stream.util.impl.RedirectableOutputStream;
import java.io.OutputStream;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;

@NotThreadSafe
public class MultiPartOutputStream extends OutputStream implements IMultiPartOutputStream {

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
    public Bytes minPartSize() {
        return minPartSize;
    }

    @Override
    public S3FileDescriptor fileDescriptor() {
        return uploadHandler.fileDescriptor();
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public void write(int b) {
        throw new UnsupportedOperationException("Attempt to write a single byte to S3");
    }

    @Override
    public void write(byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        buffer.write(b, off, len);

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
