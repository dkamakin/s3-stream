package com.dkamakin.s3.stream.util.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class RedirectableOutputStream extends ByteArrayOutputStream {

    public RedirectableOutputStream(Bytes bytes) {
        super(bytes.toBytes());
    }

    @Override
    public void write(byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public synchronized byte[] toByteArray() {
        return buf;
    }

    public ByteArrayInputStream redirect() {
        return new ByteArrayInputStream(buf, 0, count);
    }
}
