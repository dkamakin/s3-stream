package com.github.dkamakin.s3.stream.util.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class RedirectableOutputStream extends ByteArrayOutputStream {

    public RedirectableOutputStream(Bytes bytes) {
        super(bytes.toBytes());
    }

    @Override
    public void write(int b) {
        throw new UnsupportedOperationException("Attempt to write a single byte to stream");
    }

    @Override
    public int size() {
        return count;
    }

    public InputStream redirect() {
        return new ByteArrayInputStream(buf, 0, count);
    }
}
