package com.github.dkamakin.s3.stream.util.impl;


import java.util.Objects;

public class ByteRange {

    static class Token {

        static final String HTTP_GET_RANGE_HEADER = "bytes=";
    }

    private final long from;
    private final long to;

    public ByteRange(long from, long to) {
        this.from = from;
        this.to   = to;
    }

    @Override
    public String toString() {
        return Token.HTTP_GET_RANGE_HEADER + from + "-" + to;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ByteRange byteRange = (ByteRange) o;
        return from == byteRange.from && to == byteRange.to;
    }

}
