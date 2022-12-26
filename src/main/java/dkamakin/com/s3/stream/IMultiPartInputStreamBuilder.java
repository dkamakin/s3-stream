package dkamakin.com.s3.stream;

import dkamakin.com.s3.stream.impl.MultiPartInputStream;
import software.amazon.awssdk.services.s3.S3Client;

public interface IMultiPartInputStreamBuilder {

    IMultiPartInputStreamBuilder client(S3Client s3Client);

    IMultiPartInputStreamBuilder bucket(String bucketName);

    IMultiPartInputStreamBuilder key(String path);

    IMultiPartInputStreamBuilder fileSize(Long fileSize);

    MultiPartInputStream build();

}
