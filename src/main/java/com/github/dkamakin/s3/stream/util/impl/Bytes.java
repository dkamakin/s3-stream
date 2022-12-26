package com.github.dkamakin.s3.stream.util.impl;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class Bytes implements Comparable<Bytes> {

    static class Constant {

        static final int KB = 1024;
        static final int MB = 1024 * 1024;
    }

    private final int byteCount;

    private Bytes(int byteCount) {
        this.byteCount = byteCount;
    }

    public static Bytes fromMb(int byteCount) {
        return new Bytes(byteCount * Constant.MB);
    }

    public static Bytes fromKb(int kbCount) {
        return new Bytes(kbCount * Constant.KB);
    }

    public static Bytes fromBytes(int bytes) {
        return new Bytes(bytes);
    }

    public int toBytes() {
        return byteCount;
    }

    public int toMb() {
        return byteCount / Constant.MB;
    }

    public int toKb() {
        return byteCount / Constant.KB;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("bytes", toBytes())
                          .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Bytes bytes = (Bytes) o;
        return byteCount == bytes.byteCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(byteCount);
    }

    @Override
    public int compareTo(Bytes other) {
        return Integer.compare(toBytes(), other.toBytes());
    }
}
