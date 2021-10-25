package kvsx.exception;

public class KvsxException extends Exception {

	private static final long serialVersionUID = 1L;

	public KvsxException() {
		super();
	}

	public KvsxException(String message, Throwable cause) {
		super(message, cause);
	}

	public KvsxException(String message) {
		super(message);
	}

	public KvsxException(Throwable cause) {
		super(cause);
	}
	
	public static void wrapException(Exception e) throws KvsxException {
		if (e instanceof KvsxException) {
			throw (KvsxException)e;
		} else {
			throw new KvsxException(e);
		}
	}
}
