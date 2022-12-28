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
    public synchronized byte[] toByteArray() {
        return buf;
    }

    public ByteArrayInputStream redirect() {
        return redirect(0, count);
    }

    public ByteArrayInputStream redirect(int offset, int count) {
        return new ByteArrayInputStream(buf, offset, count);
    }
}
