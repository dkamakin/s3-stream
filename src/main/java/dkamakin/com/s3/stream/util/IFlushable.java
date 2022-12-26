package dkamakin.com.s3.stream.util;

import java.io.Flushable;

public interface IFlushable extends Flushable {

    @Override
    void flush();

}
