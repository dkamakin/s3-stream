package com.github.dkamakin.s3.stream;

import software.amazon.awssdk.services.s3.S3Client;

public interface IMultiPartInputStreamBuilder {

    IMultiPartInputStreamBuilder withClient(S3Client s3Client);

    IMultiPartInputStreamBuilder forBucket(String bucketName);

    IMultiPartInputStreamBuilder forKey(String path);

    IMultiPartInputStreamBuilder fileSize(Long fileSize);

    IMultiPartInputStream build();

}
