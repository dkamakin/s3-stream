package com.dkamakin.s3.stream;

import com.dkamakin.s3.stream.impl.MultiPartOutputStream;
import com.dkamakin.s3.stream.util.impl.Bytes;
import software.amazon.awssdk.services.s3.S3Client;

public interface IMultiPartOutputStreamBuilder {

    /**
     * Required. Specify a client to communicate with s3
     *
     * @param s3Client client to use
     * @return builder instance
     */
    IMultiPartOutputStreamBuilder client(S3Client s3Client);

    /**
     * Required. Specify a bucket to search {@link IMultiPartOutputStreamBuilder#key(String)} in
     *
     * @param bucketName bucket
     * @return builder instance
     */
    IMultiPartOutputStreamBuilder bucket(String bucketName);

    /**
     * Required. Specify a key in {@link IMultiPartOutputStreamBuilder#bucket(String)} to upload
     *
     * @param key file name
     * @return builder instance
     */
    IMultiPartOutputStreamBuilder key(String key);

    /**
     * Optional. Multipart upload API does not allow the uploading of more than one part smaller than 5 MB. Here it's
     * possible to increase the limit specifying minPartSize
     *
     * @param minPartSize increased min part value
     * @return builder instance
     */
    IMultiPartOutputStreamBuilder minPartSize(Bytes minPartSize);

    /**
     * Builds an output stream with specified values
     *
     * @return built input stream
     * @throws IllegalArgumentException required arguments weren't specified
     */
    MultiPartOutputStream build();

}
