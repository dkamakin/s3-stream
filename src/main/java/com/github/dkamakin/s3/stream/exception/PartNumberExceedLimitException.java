package com.github.dkamakin.s3.stream.exception;

public class PartNumberExceedLimitException extends RuntimeException {

    public PartNumberExceedLimitException() {
        super("Part number exceeds its limit");
    }
}
