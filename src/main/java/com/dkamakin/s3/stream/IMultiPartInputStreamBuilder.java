package com.dkamakin.s3.stream;

import com.dkamakin.s3.stream.impl.MultiPartInputStream;
import software.amazon.awssdk.services.s3.S3Client;

public interface IMultiPartInputStreamBuilder {

    /**
     * Required. Specify a client to communicate with s3
     *
     * @param s3Client client to use
     * @return builder instance
     */
    IMultiPartInputStreamBuilder client(S3Client s3Client);

    /**
     * Required. Specify a bucket to search {@link IMultiPartInputStreamBuilder#key(String)} in
     *
     * @param bucketName bucket
     * @return builder instance
     */
    IMultiPartInputStreamBuilder bucket(String bucketName);

    /**
     * Required. Specify a key in {@link IMultiPartInputStreamBuilder#bucket(String)} to read from
     *
     * @param key file name
     * @return builder instance
     */
    IMultiPartInputStreamBuilder key(String key);

    /**
     * Builds an input stream with specified values
     *
     * @return built input stream
     * @throws IllegalArgumentException required arguments weren't specified
     */
    MultiPartInputStream build();

}
