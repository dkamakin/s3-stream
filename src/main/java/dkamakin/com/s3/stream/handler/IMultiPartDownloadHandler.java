package dkamakin.com.s3.stream.handler;

import dkamakin.com.s3.stream.util.impl.ByteRange;

public interface IMultiPartDownloadHandler extends IFileDescriptorHolder {

    int getPart(ByteRange range, byte[] target, int off, int len);

    long size();

}