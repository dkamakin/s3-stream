# S3 Stream

# What is this?

This is a wrapper over the AWS SDK v2 based on Java 8, which allows you to upload and download files using S3 without
having to cache or
save entire objects in the file system, as the official API suggests. The library offers InputStream and OutputStream
with well-known methods for convenient work with your files. Read more
in [JavaDoc](https://dkamakin.github.io/s3-stream/javadoc/apidocs/com/dkamakin/s3/stream/impl/package-summary.html)

# Examples

1. Creating an [OutputStream](https://dkamakin.github.io/s3-stream/javadoc/apidocs/com/dkamakin/s3/stream/impl/MultiPartOutputStream.html)

```
MultiPartOutputStream stream(String key, String bucketName, S3Client s3Client) {
    return MultiPartOutputStream.builder()
                                .bucket(bucketName)
                                .key(key)
                                .client(s3Client)
                                .build();
}
```

2. Creating an [InputStream](https://dkamakin.github.io/s3-stream/javadoc/apidocs/com/dkamakin/s3/stream/impl/MultiPartInputStream.html)

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

```
<dependency>
  <groupId>com.dkamakin</groupId>
  <artifactId>s3-stream</artifactId>
  <version>1.0.1</version>
</dependency>
```