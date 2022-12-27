package com.dkamakin.s3.stream.handler;

import com.dkamakin.s3.stream.util.ICloseable;
import software.amazon.awssdk.core.sync.RequestBody;

public interface IMultiPartUploadHandler extends ICloseable, IFileDescriptorHolder {

    void upload(RequestBody body);

}
