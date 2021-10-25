package kvsx.concurrent;

import java.util.concurrent.locks.Lock;

import kvsx.serde.Bin;

public class KeyedLockProcessor {
	public static <OutputType, ExceptionType extends Exception> OutputType call(Lock lock, CallableTask<OutputType, ExceptionType> callable) throws ExceptionType {
		try {
			lock.lock();
			return callable.call();
		} finally {
			lock.unlock();
		}
	}

	public static <ExceptionType extends Exception> void run(Lock lock, RunnableTask<ExceptionType> runnable) throws ExceptionType {
		try {
			lock.lock();
			runnable.run();
		} finally {
			lock.unlock();
		}
	}

	private final KeyedLock keyedLock = new KeyedLock();

	public <OutputType, ExceptionType extends Exception> OutputType call(byte[] key, CallableTask<OutputType, ExceptionType> callable) throws ExceptionType {
		return call(new Bin(key), callable);
	}

	public <OutputType, ExceptionType extends Exception> OutputType call(long key, CallableTask<OutputType, ExceptionType> callable) throws ExceptionType {
		return call(Long.valueOf(key), callable);
	}

	public <OutputType, ExceptionType extends Exception> OutputType call(Object key, CallableTask<OutputType, ExceptionType> callable) throws ExceptionType {
		Lock lock = keyedLock.get(key);
		return call(lock, callable);
	}

	public <ExceptionType extends Exception> void run(byte[] key, RunnableTask<ExceptionType> runnable) throws ExceptionType {
		run(new Bin(key), runnable);
	}

	public <ExceptionType extends Exception> void run(long key, RunnableTask<ExceptionType> runnable) throws ExceptionType {
		run(Long.valueOf(key), runnable);
	}

	public <ExceptionType extends Exception> void run(Object key, RunnableTask<ExceptionType> runnable) throws ExceptionType {
		Lock lock = keyedLock.get(key);
		run(lock, runnable);
	}

}
