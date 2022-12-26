package com.github.dkamakin.s3.stream.impl;

import static com.github.dkamakin.s3.stream.impl.MultiPartOutputStreamBuilder.Constant.S3_MIN_PART_SIZE;

import com.github.dkamakin.s3.stream.IMultiPartOutputStreamBuilder;
import com.github.dkamakin.s3.stream.handler.impl.MultiPartUploadHandler;
import com.github.dkamakin.s3.stream.handler.impl.S3FileDescriptor;
import com.github.dkamakin.s3.stream.util.impl.Bytes;
import com.github.dkamakin.s3.stream.util.impl.Validator;
import java.util.Optional;
import software.amazon.awssdk.services.s3.S3Client;

public class MultiPartOutputStreamBuilder implements IMultiPartOutputStreamBuilder {

    public static class Constant {

        public static final Bytes S3_MIN_PART_SIZE = Bytes.fromMb(5);
    }

    private S3Client s3Client;
    private String   bucketName;
    private String   key;
    private Bytes    minPartSize;

    @Override
    public IMultiPartOutputStreamBuilder client(S3Client s3Client) {
        this.s3Client = s3Client;
        return this;
    }

    @Override
    public IMultiPartOutputStreamBuilder bucket(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    @Override
    public IMultiPartOutputStreamBuilder key(String path) {
        this.key = path;
        return this;
    }

    @Override
    public IMultiPartOutputStreamBuilder minPartSize(Bytes minPartSize) {
        this.minPartSize = minPartSize;
        return this;
    }

    @Override
    public MultiPartOutputStream build() {
        minPartSize = Optional.ofNullable(minPartSize).map(this::validate).orElse(S3_MIN_PART_SIZE);

        return new MultiPartOutputStream(minPartSize,
                                         new MultiPartUploadHandler(new S3FileDescriptor(bucketName, key, s3Client)));
    }

    private Bytes validate(Bytes minPartSize) {
        Validator.ifValue(minPartSize).lessThan(S3_MIN_PART_SIZE).thenThrow(this::illegalPartSize);
        return minPartSize;
    }

    private IllegalArgumentException illegalPartSize() {
        return new IllegalArgumentException(String.format("Part size must be at least %s mb", S3_MIN_PART_SIZE.toMb()));
    }
}