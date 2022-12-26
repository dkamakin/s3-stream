package com.github.dkamakin.s3.stream.util.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.MoreObjects;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class BytesTest {

    static class Argument {

        Function<Integer, Bytes> bytesFactory;
        Function<Bytes, Integer> extractor;
        int                      factoryArgument;
        int                      expected;

        public Argument(Function<Integer, Bytes> bytesFactory,
                        Function<Bytes, Integer> extractor,
                        int factoryArgument,
                        int expected) {
            this.bytesFactory    = bytesFactory;
            this.extractor       = extractor;
            this.factoryArgument = factoryArgument;
            this.expected        = expected;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                              .add("factoryArgument", factoryArgument)
                              .add("expected", expected)
                              .toString();
        }
    }

    static Stream<Argument> argumentStream() {
        return Stream.of(
            new Argument(Bytes::fromBytes, Bytes::toBytes, 10, 10),
            new Argument(Bytes::fromBytes, Bytes::toKb, 1024, 1),
            new Argument(Bytes::fromBytes, Bytes::toKb, 1000, 0),
            new Argument(Bytes::fromBytes, Bytes::toMb, 1024 * 1024, 1),
            new Argument(Bytes::fromBytes, Bytes::toMb, 1024, 0),
            new Argument(Bytes::fromKb, Bytes::toBytes, 1, 1024),
            new Argument(Bytes::fromKb, Bytes::toBytes, 0, 0),
            new Argument(Bytes::fromKb, Bytes::toMb, 1, 0),
            new Argument(Bytes::fromKb, Bytes::toMb, 1024, 1),
            new Argument(Bytes::fromKb, Bytes::toBytes, 1, 1024),
            new Argument(Bytes::fromMb, Bytes::toBytes, 1, 1024 * 1024),
            new Argument(Bytes::fromMb, Bytes::toKb, 1, 1024),
            new Argument(Bytes::fromMb, Bytes::toBytes, 0, 0)
        );
    }

    @Test
    void equals_NotSameCount_NotEquals() {
        Bytes first  = Bytes.fromKb(1);
        Bytes second = Bytes.fromKb(2);

        assertThat(first).isNotEqualTo(second).doesNotHaveSameHashCodeAs(second);
    }

    @Test
    void equals_SameCount_Equals() {
        Bytes first  = Bytes.fromMb(2);
        Bytes second = Bytes.fromMb(2);

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
    }

    @Test
    void fromMb_CorrectNumberProvided_CorrectConversion() {
        int   expected = 10;
        Bytes actual   = Bytes.fromMb(expected);

        assertThat(actual).extracting(Bytes::toBytes).isEqualTo(expected * 1024 * 1024);
    }

    @Test
    void fromKb_CorrectNumberProvided_CorrectConversion() {
        int   expected = 10;
        Bytes actual   = Bytes.fromKb(expected);

        assertThat(actual).extracting(Bytes::toBytes).isEqualTo(expected * 1024);
    }

    @Test
    void fromBytes_CorrectNumberProvided_CorrectConversion() {
        int   expected = 10132123;
        Bytes actual   = Bytes.fromBytes(expected);

        assertThat(actual).extracting(Bytes::toBytes).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("argumentStream")
    void conversion_Argument_CorrectConversion(Argument argument) {
        Bytes actual = argument.bytesFactory.apply(argument.factoryArgument);

        System.out.println("Actual: " + actual);

        assertThat(actual).extracting(argument.extractor).isEqualTo(argument.expected);
    }

}
