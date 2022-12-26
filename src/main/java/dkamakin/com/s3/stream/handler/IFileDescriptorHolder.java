package dkamakin.com.s3.stream.handler;

import dkamakin.com.s3.stream.handler.impl.S3FileDescriptor;

public interface IFileDescriptorHolder {

    S3FileDescriptor fileDescriptor();

}
