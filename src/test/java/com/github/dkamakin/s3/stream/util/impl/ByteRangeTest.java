package com.github.dkamakin.s3.stream.util.impl;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ByteRangeTest {

    @Test
    void toString_FromToPresent_ConstructCorrectString() {
        assertThat(new ByteRange(10, 20)).hasToString("bytes=10-20");
    }

    @Test
    void equals_DifferentRanges_NotEquals() {
        ByteRange first  = new ByteRange(10, 20);
        ByteRange second = new ByteRange(11, 20);

        assertThat(first).isNotEqualTo(second).doesNotHaveSameHashCodeAs(second);
    }

    @Test
    void equals_SameRanges_Equals() {
        ByteRange first  = new ByteRange(10, 20);
        ByteRange second = new ByteRange(10, 20);

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
    }

}
