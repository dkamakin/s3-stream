package com.dkamakin.s3.stream.impl;

import com.dkamakin.s3.stream.handler.impl.MultiPartDownloadHandler;
import com.dkamakin.s3.stream.handler.impl.S3FileDescriptor;
import com.dkamakin.s3.stream.util.impl.RetryableStreamReader;
import com.dkamakin.s3.stream.IMultiPartInputStreamBuilder;
import software.amazon.awssdk.services.s3.S3Client;

public class MultiPartInputStreamBuilder implements IMultiPartInputStreamBuilder {

    private S3Client s3Client;
    private String   bucketName;
    private String   key;
    private Long     fileSize;

    @Override
    public IMultiPartInputStreamBuilder client(S3Client s3Client) {
        this.s3Client = s3Client;
        return this;
    }

    @Override
    public IMultiPartInputStreamBuilder bucket(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    @Override
    public IMultiPartInputStreamBuilder key(String path) {
        this.key = path;
        return this;
    }

    @Override
    public IMultiPartInputStreamBuilder fileSize(Long fileSize) {
        this.fileSize = fileSize;
        return this;
    }

    @Override
    public MultiPartInputStream build() {
        return new MultiPartInputStream(fileSize,
                                        new MultiPartDownloadHandler(new S3FileDescriptor(bucketName, key, s3Client),
                                                                     RetryableStreamReader::new));
    }
}
