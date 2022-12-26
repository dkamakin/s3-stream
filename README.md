# S3 Stream

# What is this?

This is a wrapper over the AWS SDK v2 based on Java 8, which allows you to upload and download files using S3 without having to cache or
save entire objects in the file system, as the official API suggests. The library offers InputStream and OutputStream
with well-known methods for convenient work with your files.


# Examples

1. Creating an OutputStream

```
MultiPartOutputStream stream(String key, String bucketName, S3Client s3Client) {
    return MultiPartOutputStream.builder()
                                .bucket(bucketName)
                                .key(key)
                                .client(s3Client)
                                .build();
}
```


2. Creating an InputStream

```
MultiPartInputStream stream(String key, String bucketName, S3Client s3Client) {
    return MultiPartInputStream.builder()
                                .bucket(bucketName)
                                .key(key)
                                .client(s3Client)
                                .build();
}
```

# Install

*Maven Central is coming soon*