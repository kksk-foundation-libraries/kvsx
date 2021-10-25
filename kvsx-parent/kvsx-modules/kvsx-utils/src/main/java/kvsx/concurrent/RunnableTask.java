package kvsx.concurrent;

@FunctionalInterface
public interface RunnableTask<ExceptionType extends Exception> {
	void run() throws ExceptionType;
}
