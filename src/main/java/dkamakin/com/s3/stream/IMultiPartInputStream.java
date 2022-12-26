package dkamakin.com.s3.stream;

import dkamakin.com.s3.stream.handler.impl.S3FileDescriptor;
import dkamakin.com.s3.stream.util.ICloseable;

public interface IMultiPartInputStream extends ICloseable {

    int read(byte[] data);

    int read(byte[] data, int offset, int length);

    long fileSize();

    long readLength();

    S3FileDescriptor fileDescriptor();

}