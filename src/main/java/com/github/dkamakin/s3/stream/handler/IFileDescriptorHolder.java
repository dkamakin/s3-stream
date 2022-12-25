package com.github.dkamakin.s3.stream.handler;

import com.github.dkamakin.s3.stream.handler.impl.S3FileDescriptor;

public interface IFileDescriptorHolder {

    S3FileDescriptor fileDescriptor();

}
