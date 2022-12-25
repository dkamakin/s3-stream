package com.github.dkamakin.s3.stream;

import com.github.dkamakin.s3.stream.handler.impl.S3FileDescriptor;
import com.github.dkamakin.s3.stream.util.ICloseable;

public interface IMultiPartInputStream extends ICloseable {

    int read(byte[] b);

    int read(byte[] b, int off, int len);

    long fileSize();

    long readLength();

    S3FileDescriptor fileDescriptor();

}
