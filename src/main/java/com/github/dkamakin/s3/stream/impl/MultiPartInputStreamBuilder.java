package com.github.dkamakin.s3.stream.impl;

import com.github.dkamakin.s3.stream.IMultiPartInputStream;
import com.github.dkamakin.s3.stream.IMultiPartInputStreamBuilder;
import com.github.dkamakin.s3.stream.handler.impl.MultiPartDownloadHandler;
import com.github.dkamakin.s3.stream.handler.impl.S3FileDescriptor;
import software.amazon.awssdk.services.s3.S3Client;

public class MultiPartInputStreamBuilder implements IMultiPartInputStreamBuilder {

    private S3Client s3Client;
    private String   bucketName;
    private String   key;
    private Long     fileSize;

    @Override
    public IMultiPartInputStreamBuilder withClient(S3Client s3Client) {
        this.s3Client = s3Client;
        return this;
    }

    @Override
    public IMultiPartInputStreamBuilder forBucket(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    @Override
    public IMultiPartInputStreamBuilder forKey(String path) {
        this.key = path;
        return this;
    }

    @Override
    public IMultiPartInputStreamBuilder fileSize(Long fileSize) {
        this.fileSize = fileSize;
        return this;
    }

    @Override
    public IMultiPartInputStream build() {
        return new MultiPartInputStream(fileSize,
                                        new MultiPartDownloadHandler(new S3FileDescriptor(bucketName, key, s3Client)));
    }
}
