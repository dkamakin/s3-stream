package dkamakin.com.s3.stream.exception;

public class ReadException extends RuntimeException {

    private static final long serialVersionUID = 3401677347809660211L;

    public ReadException(Throwable cause) {
        super("Failed to read data", cause);
    }
}
