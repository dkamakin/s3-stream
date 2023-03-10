package com.dkamakin.s3.stream.handler.impl;

import com.dkamakin.s3.stream.exception.ReadException;
import com.dkamakin.s3.stream.handler.IMultiPartDownloadHandler;
import com.dkamakin.s3.stream.util.impl.ByteRange;
import com.dkamakin.s3.stream.util.impl.Constant;
import com.dkamakin.s3.stream.util.impl.RetryableStreamReader;
import com.dkamakin.s3.stream.util.impl.S3HttpCodes;
import com.dkamakin.s3.stream.util.impl.Validator;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class MultiPartDownloadHandler implements IMultiPartDownloadHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MultiPartDownloadHandler.class);

    private final S3FileDescriptor                             fileDescriptor;
    private final Function<InputStream, RetryableStreamReader> wrapper;

    public MultiPartDownloadHandler(S3FileDescriptor fileDescriptor,
                                    Function<InputStream, RetryableStreamReader> wrapper) {
        Validator.nonNull(fileDescriptor, "fileDescriptor");
        Validator.nonNull(wrapper, "wrapper");

        this.fileDescriptor = fileDescriptor;
        this.wrapper        = wrapper;
    }

    @Override
    public int getPart(ByteRange range, byte[] target, int off, int len) {
        try (InputStream stream = getObject(range.toString())) {
            return wrapper.apply(stream).read(target, off, len);
        } catch (IOException e) {
            throw new ReadException(e);
        } catch (S3Exception e) {
            return handle(e);
        }
    }

    @Override
    public S3FileDescriptor fileDescriptor() {
        return fileDescriptor;
    }

    private InputStream getObject(String range) {
        LOG.info("Downloading from {}, range: {}", this, range);

        return fileDescriptor.s3Client().getObject(GetObjectRequest.builder()
                                                                   .bucket(fileDescriptor.bucketName())
                                                                   .key(fileDescriptor.key())
                                                                   .range(range)
                                                                   .build());
    }

    private int handle(S3Exception exception) {
        if (exception.statusCode() == S3HttpCodes.RANGE_NOT_SATISFIABLE.code()) {
            LOG.info("Got 416 from S3, treat like an EOS");

            return Constant.EOS;
        } else {
            LOG.error(exception.getMessage(), exception);

            throw exception;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("fileDescriptor", fileDescriptor)
                          .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MultiPartDownloadHandler that = (MultiPartDownloadHandler) o;
        return fileDescriptor.equals(that.fileDescriptor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileDescriptor);
    }
}
