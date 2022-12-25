package com.github.dkamakin.s3.stream.handler.impl;

import com.github.dkamakin.s3.stream.handler.IMultiPartUploadHandler;
import com.github.dkamakin.s3.stream.util.impl.Validator;
import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class MultiPartUploadHandler implements IMultiPartUploadHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MultiPartUploadHandler.class);

    private final List<CompletedPart> partETags;
    private final String              uploadId;
    private final S3FileDescriptor    fileDescriptor;
    private       int                 partNumber;

    public MultiPartUploadHandler(S3FileDescriptor fileDescriptor) {
        Validator.nonNull(fileDescriptor, "fileDescriptor");

        this.partETags      = new ArrayList<>();
        this.fileDescriptor = fileDescriptor;
        this.uploadId       = createMultipartUpload().uploadId();
        this.partNumber     = 1;

        LOG.info("Upload handler initialized: {}", this);
    }

    @Override
    public void close() {
        LOG.info("Close: {}", this);

        if (partETags.isEmpty()) {
            handleEmptyComplete();
        } else {
            sendCompleteRequest();
        }
    }

    @Override
    public void upload(RequestBody body) {
        LOG.debug("Uploading part {}", partNumber);

        partETags.add(CompletedPart.builder()
                                   .partNumber(partNumber)
                                   .eTag(uploadPart(body).eTag())
                                   .build());

        partNumber++;
    }

    @Override
    public S3FileDescriptor fileDescriptor() {
        return fileDescriptor;
    }

    public void abort() {
        LOG.info("Aborting multipart upload");

        fileDescriptor.s3Client()
                      .abortMultipartUpload(AbortMultipartUploadRequest.builder()
                                                                       .bucket(fileDescriptor.bucketName())
                                                                       .key(fileDescriptor.key())
                                                                       .uploadId(uploadId)
                                                                       .build());
    }

    private void handleEmptyComplete() {
        abort();
        uploadEmpty();
    }

    private void uploadEmpty() {
        LOG.info("Uploading empty data");

        fileDescriptor.s3Client()
                      .putObject(PutObjectRequest.builder()
                                                 .bucket(fileDescriptor.bucketName())
                                                 .key(fileDescriptor.key())
                                                 .contentLength(0L)
                                                 .build(),
                                 RequestBody.empty());
    }

    private void sendCompleteRequest() {
        fileDescriptor.s3Client()
                      .completeMultipartUpload(CompleteMultipartUploadRequest
                                                   .builder()
                                                   .bucket(fileDescriptor.bucketName())
                                                   .key(fileDescriptor.key())
                                                   .uploadId(uploadId)
                                                   .multipartUpload(builder -> builder.parts(partETags))
                                                   .build());
    }

    private UploadPartResponse uploadPart(RequestBody body) {
        return fileDescriptor.s3Client()
                             .uploadPart(UploadPartRequest.builder()
                                                          .bucket(fileDescriptor.bucketName())
                                                          .key(fileDescriptor.key())
                                                          .uploadId(uploadId)
                                                          .partNumber(partNumber)
                                                          .build(),
                                         body);
    }

    private CreateMultipartUploadResponse createMultipartUpload() {
        return fileDescriptor.s3Client()
                             .createMultipartUpload(CreateMultipartUploadRequest.builder()
                                                                                .bucket(fileDescriptor.bucketName())
                                                                                .key(fileDescriptor.key())
                                                                                .build());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MultiPartUploadHandler that = (MultiPartUploadHandler) o;
        return Objects.equals(uploadId, that.uploadId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uploadId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("partETags", partETags)
                          .add("uploadId", uploadId)
                          .add("fileDescriptor", fileDescriptor)
                          .add("partNumber", partNumber)
                          .toString();
    }
}

