package com.dkamakin.s3.stream.impl;

import com.dkamakin.s3.stream.IMultiPartInputStreamBuilder;
import com.dkamakin.s3.stream.handler.impl.MultiPartDownloadHandler;
import com.dkamakin.s3.stream.util.impl.RetryableStreamReader;

public class MultiPartInputStreamBuilder extends FileDescriptorBuilder<IMultiPartInputStreamBuilder>
    implements IMultiPartInputStreamBuilder {

    @Override
    protected IMultiPartInputStreamBuilder getThis() {
        return this;
    }

    @Override
    public MultiPartInputStream build() {
        return new MultiPartInputStream(new MultiPartDownloadHandler(buildDescriptor(), RetryableStreamReader::new));
    }

}
