package com.dkamakin.s3.stream.impl;

import com.dkamakin.s3.stream.handler.impl.S3FileDescriptor;
import software.amazon.awssdk.services.s3.S3Client;

public abstract class FileDescriptorBuilder<T> {

    protected S3Client s3Client;
    protected String   bucketName;
    protected String   key;

    protected abstract T getThis();

    public T client(S3Client s3Client) {
        this.s3Client = s3Client;
        return getThis();
    }

    public T bucket(String bucketName) {
        this.bucketName = bucketName;
        return getThis();
    }

    public T key(String key) {
        this.key = key;
        return getThis();
    }

    protected S3FileDescriptor buildDescriptor() {
        return new S3FileDescriptor(bucketName, key, s3Client);
    }

}
