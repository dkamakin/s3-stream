package com.dkamakin.s3.stream.util;

@FunctionalInterface
public interface ObjIntFunction<T> {

    int apply(T argument);

}
