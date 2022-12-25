package com.github.dkamakin.s3.stream.util.impl;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Validator {

    public static <T> void check(T argument, Predicate<T> predicate, Supplier<RuntimeException> exceptionSupplier) {
        if (!predicate.test(argument)) {
            throw exceptionSupplier.get();
        }
    }

    public static <T extends Comparable<T>> Comparator<T> ifValue(T value) {
        return new Comparator<>(value);
    }

    public static <T> void nonNull(T argument, String name) {
        check(argument, Objects::nonNull,
                () -> new IllegalArgumentException(String.format("Argument '%s' must be present", name)));
    }

    public static void isNotEmpty(String argument, String name) {
        check(argument, StringUtils::isNotEmpty,
                () -> new IllegalArgumentException(String.format("Argument '%s' must be non-empty", name)));
    }

    public static class Comparator<T extends Comparable<T>> {

        private final T value;

        public Comparator(T value) {
            this.value = value;
        }

        private IExamine examine(Predicate<T> predicate) {
            if (predicate.test(value)) {
                return new PassedExamine();
            } else {
                return new FailedExamine();
            }
        }

        public IExamine lessThan(T other) {
            return examine(first -> first.compareTo(other) < 0);
        }
    }

    public interface IExamine {

        <E extends RuntimeException> void thenThrow(Supplier<E> exceptionSupplier);

    }

    public static class PassedExamine implements IExamine {

        public <E extends RuntimeException> void thenThrow(Supplier<E> exceptionSupplier) {
            throw exceptionSupplier.get();
        }

    }

    public static class FailedExamine implements IExamine {

        @Override
        public <E extends RuntimeException> void thenThrow(Supplier<E> exceptionSupplier) {

        }

    }

}
