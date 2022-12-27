package com.dkamakin.s3.stream.impl;

import static com.dkamakin.s3.stream.impl.MultiPartOutputStreamBuilder.Constant.S3_MIN_PART_SIZE;

import com.dkamakin.s3.stream.IMultiPartOutputStreamBuilder;
import com.dkamakin.s3.stream.handler.impl.MultiPartUploadHandler;
import com.dkamakin.s3.stream.util.impl.Bytes;
import com.dkamakin.s3.stream.util.impl.Validator;
import java.util.Optional;

public class MultiPartOutputStreamBuilder extends FileDescriptorBuilder<IMultiPartOutputStreamBuilder>
    implements IMultiPartOutputStreamBuilder {

    public static class Constant {

        public static final Bytes S3_MIN_PART_SIZE = Bytes.fromMb(5);
    }

    private Bytes minPartSize;

    @Override
    protected IMultiPartOutputStreamBuilder getThis() {
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

        return new MultiPartOutputStream(minPartSize, new MultiPartUploadHandler(buildDescriptor()));
    }

    private Bytes validate(Bytes minPartSize) {
        Validator.ifValue(minPartSize).lessThan(S3_MIN_PART_SIZE).thenThrow(this::illegalPartSize);
        return minPartSize;
    }

    private IllegalArgumentException illegalPartSize() {
        return new IllegalArgumentException(String.format("Part size must be at least %s mb", S3_MIN_PART_SIZE.toMb()));
    }
}