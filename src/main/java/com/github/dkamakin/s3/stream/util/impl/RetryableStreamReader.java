package com.github.dkamakin.s3.stream.util.impl;

import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;

public class RetryableStreamReader {

    private final InputStream stream;

    public RetryableStreamReader(InputStream stream) {
        this.stream = stream;
    }

    public int read(byte[] data, int offset, int length) throws IOException {
        return ByteStreams.read(stream, data, offset, length);
    }
}
