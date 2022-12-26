package com.github.dkamakin.s3.stream;

import com.github.dkamakin.s3.stream.handler.impl.S3FileDescriptor;
import com.github.dkamakin.s3.stream.util.ICloseable;
import com.github.dkamakin.s3.stream.util.IFlushable;
import com.github.dkamakin.s3.stream.util.impl.Bytes;

public interface IMultiPartOutputStream extends IFlushable, ICloseable {

    void write(int data);

    void write(byte[] data);

    void write(byte[] data, int offset, int length);

    int size();

    Bytes minPartSize();

    S3FileDescriptor fileDescriptor();

    default boolean isEmpty() {
        return size() == 0;
    }

    default boolean isNotEmpty() {
        return !isEmpty();
    }

}
