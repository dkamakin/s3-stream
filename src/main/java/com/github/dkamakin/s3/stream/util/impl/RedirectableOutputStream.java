package com.github.dkamakin.s3.stream.util.impl;

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
        super.write(b, 0, b.length);
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public byte[] toByteArray() {
        return buf;
    }

    public ByteArrayInputStream redirect() {
        return new ByteArrayInputStream(buf, 0, count);
    }
}
