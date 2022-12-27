package com.dkamakin.s3.stream.handler.impl;

import com.dkamakin.s3.stream.util.impl.Validator;
import com.google.common.base.MoreObjects;
import java.util.Objects;
import software.amazon.awssdk.services.s3.S3Client;

public class S3FileDescriptor {

    private final String   bucketName;
    private final String   key;
    private final S3Client s3Client;

    public S3FileDescriptor(String bucketName, String key, S3Client s3Client) {
        Validator.isNotEmpty(bucketName, "bucketName");
        Validator.isNotEmpty(key, "key");
        Validator.nonNull(s3Client, "s3Client");

        this.bucketName = bucketName;
        this.key        = key;
        this.s3Client   = s3Client;
    }

    public String bucketName() {
        return bucketName;
    }

    public String key() {
        return key;
    }

    public S3Client s3Client() {
        return s3Client;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        S3FileDescriptor that = (S3FileDescriptor) o;
        return bucketName.equals(that.bucketName) &&
               key.equals(that.key) &&
               s3Client.equals(that.s3Client);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketName, key, s3Client);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("bucketName", bucketName)
                          .add("key", key)
                          .add("s3Client", s3Client)
                          .toString();
    }
}
