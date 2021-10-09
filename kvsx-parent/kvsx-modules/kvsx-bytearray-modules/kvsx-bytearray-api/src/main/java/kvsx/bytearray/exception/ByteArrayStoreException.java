package kvsx.bytearray.exception;

public class ByteArrayStoreException extends Exception {

	private static final long serialVersionUID = 1L;

	public ByteArrayStoreException() {
		super();
	}

	public ByteArrayStoreException(String message, Throwable cause) {
		super(message, cause);
	}

	public ByteArrayStoreException(String message) {
		super(message);
	}

	public ByteArrayStoreException(Throwable cause) {
		super(cause);
	}
}
