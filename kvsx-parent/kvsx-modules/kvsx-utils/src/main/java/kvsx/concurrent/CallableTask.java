package kvsx.concurrent;

@FunctionalInterface
public interface CallableTask<OutputType, ExceptionType extends Exception> {
	OutputType call() throws ExceptionType;
}
