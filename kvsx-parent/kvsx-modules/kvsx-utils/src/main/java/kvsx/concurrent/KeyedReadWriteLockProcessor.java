package kvsx.concurrent;

import java.util.concurrent.locks.Lock;

import kvsx.serde.Bin;

public class KeyedReadWriteLockProcessor {
	public final KeyedReadWriteLock keyedReadWriteLock = new KeyedReadWriteLock();

	public <OutputType, ExceptionType extends Exception> OutputType callR(byte[] key, CallableTask<OutputType, ExceptionType> callable) throws ExceptionType {
		return callR(new Bin(key), callable);
	}

	public <OutputType, ExceptionType extends Exception> OutputType callR(long key, CallableTask<OutputType, ExceptionType> callable) throws ExceptionType {
		return callR(Long.valueOf(key), callable);
	}

	public <OutputType, ExceptionType extends Exception> OutputType callR(Object key, CallableTask<OutputType, ExceptionType> callable) throws ExceptionType {
		Lock lock = keyedReadWriteLock.readLock(key);
		return KeyedLockProcessor.call(lock, callable);
	}

	public <OutputType, ExceptionType extends Exception> OutputType callW(byte[] key, CallableTask<OutputType, ExceptionType> callable) throws ExceptionType {
		return callW(new Bin(key), callable);
	}

	public <OutputType, ExceptionType extends Exception> OutputType callW(long key, CallableTask<OutputType, ExceptionType> callable) throws ExceptionType {
		return callW(Long.valueOf(key), callable);
	}

	public <OutputType, ExceptionType extends Exception> OutputType callW(Object key, CallableTask<OutputType, ExceptionType> callable) throws ExceptionType {
		Lock lock = keyedReadWriteLock.writeLock(key);
		return KeyedLockProcessor.call(lock, callable);
	}

	public <ExceptionType extends Exception> void runR(byte[] key, RunnableTask<ExceptionType> runnable) throws ExceptionType {
		runR(new Bin(key), runnable);
	}

	public <ExceptionType extends Exception> void runR(long key, RunnableTask<ExceptionType> runnable) throws ExceptionType {
		runR(Long.valueOf(key), runnable);
	}

	public <ExceptionType extends Exception> void runR(Object key, RunnableTask<ExceptionType> runnable) throws ExceptionType {
		Lock lock = keyedReadWriteLock.readLock(key);
		KeyedLockProcessor.run(lock, runnable);
	}

	public <ExceptionType extends Exception> void runW(byte[] key, RunnableTask<ExceptionType> runnable) throws ExceptionType {
		runW(new Bin(key), runnable);
	}

	public <ExceptionType extends Exception> void runW(long key, RunnableTask<ExceptionType> runnable) throws ExceptionType {
		runW(Long.valueOf(key), runnable);
	}

	public <ExceptionType extends Exception> void runW(Object key, RunnableTask<ExceptionType> runnable) throws ExceptionType {
		Lock lock = keyedReadWriteLock.writeLock(key);
		KeyedLockProcessor.run(lock, runnable);
	}
}
