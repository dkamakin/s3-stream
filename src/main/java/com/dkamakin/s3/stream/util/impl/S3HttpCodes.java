package com.dkamakin.s3.stream.util.impl;

public enum S3HttpCodes {

    RANGE_NOT_SATISFIABLE(416);

    final int code;

    S3HttpCodes(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
