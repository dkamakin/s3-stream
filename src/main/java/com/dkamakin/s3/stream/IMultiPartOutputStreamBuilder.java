package com.dkamakin.s3.stream;

import com.dkamakin.s3.stream.impl.MultiPartOutputStream;
import com.dkamakin.s3.stream.util.impl.Bytes;
import software.amazon.awssdk.services.s3.S3Client;

public interface IMultiPartOutputStreamBuilder {

    IMultiPartOutputStreamBuilder client(S3Client s3Client);

    IMultiPartOutputStreamBuilder bucket(String bucketName);

    IMultiPartOutputStreamBuilder key(String path);

    IMultiPartOutputStreamBuilder minPartSize(Bytes minPartSize);

    MultiPartOutputStream build();

}
