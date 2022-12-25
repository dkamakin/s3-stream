package com.github.dkamakin.s3.stream;

import com.github.dkamakin.s3.stream.util.impl.Bytes;
import com.github.dkamakin.s3.stream.util.impl.RedirectableOutputStream;
import java.util.function.Supplier;
import software.amazon.awssdk.services.s3.S3Client;

public interface IMultiPartOutputStreamBuilder {

    IMultiPartOutputStreamBuilder withClient(S3Client s3Client);

    IMultiPartOutputStreamBuilder forBucket(String bucketName);

    IMultiPartOutputStreamBuilder forKey(String path);

    IMultiPartOutputStreamBuilder setMinPartSize(Bytes minPartSize);

    IMultiPartOutputStreamBuilder withBuffer(Supplier<RedirectableOutputStream> streamSupplier);

    IMultiPartOutputStream build();

}
