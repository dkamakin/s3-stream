package dkamakin.com.s3.stream.util;

import java.io.Closeable;

public interface ICloseable extends Closeable {

    @Override
    void close();

}
