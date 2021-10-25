package kvsx.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import kvsx.serde.Bin;

public class KeyedReadWriteLock {
	final ConcurrentMap<Object, CountDownReadWriteLock> locks = new ConcurrentHashMap<>();
	final ConcurrentMap<Object, CountDownLatch> inProc = new ConcurrentHashMap<>();

	public Lock readLock(byte[] key) {
		return readLock(new Bin(key));
	}

	public Lock readLock(long key) {
		return readLock(Long.valueOf(key));
	}

	public Lock readLock(Object key) {
		return run(key, 1, () -> {
			Lock lock = get(key, () -> new CountDownReadWriteLock(this, key)).readLock();
			return lock;
		});
	}

	public Lock writeLock(byte[] key) {
		return writeLock(new Bin(key));
	}

	public Lock writeLock(long key) {
		return writeLock(Long.valueOf(key));
	}

	public Lock writeLock(Object key) {
		return run(key, 1, () -> {
			Lock lock = get(key, () -> new CountDownReadWriteLock(this, key)).writeLock();
			return lock;
		});
	}

	CountDownReadWriteLock get(Object key, Supplier<CountDownReadWriteLock> supplier) {
		CountDownReadWriteLock existing = locks.get(key);
		if (existing != null) {
			return existing;
		}
		CountDownReadWriteLock created = supplier.get();
		existing = locks.putIfAbsent(key, created);
		return existing != null ? existing : created;
	}

	<T> T run(Object key, int flag, Callable<T> runnable) {
		try {
			if (flag != 2) {
				CountDownLatch owned = new CountDownLatch(1);
				while (true) {
					CountDownLatch last = inProc.putIfAbsent(key, owned);
					if (last == null) {
						break;
					}
					try {
						last.await();
					} catch (InterruptedException e) {
					}
				}
			}
			return runnable.call();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			if (flag != 1) {
				CountDownLatch unlock = inProc.remove(key);
				unlock.countDown();
			}
		}
	}

	private static class CountDownReadWriteLock implements ReadWriteLock {
		private final AtomicInteger count = new AtomicInteger();
		private final ReadWriteLock delegate = new ReentrantReadWriteLock();
		private final Lock readLock;
		private final Lock writeLock;

		public CountDownReadWriteLock(KeyedReadWriteLock context, Object key) {
			this.readLock = new CountDownLock(context, this, key, count, delegate.readLock());
			this.writeLock = new CountDownLock(context, this, key, count, delegate.writeLock());
		}

		@Override
		public Lock readLock() {
			return readLock;
		}

		@Override
		public Lock writeLock() {
			return writeLock;
		}

		@Override
		public String toString() {
			return delegate.toString();
		}
	}

	private static class CountDownLock implements Lock {
		private final KeyedReadWriteLock context;
		private final CountDownReadWriteLock parent;
		private final Object key;
		private final AtomicInteger count;
		private final Lock delegate;

		public CountDownLock(KeyedReadWriteLock context, CountDownReadWriteLock parent, Object key, AtomicInteger count, Lock delegate) {
			this.context = context;
			this.parent = parent;
			this.key = key;
			this.count = count;
			this.delegate = delegate;
		}

		private void add(int delta) {
			if (delta > 0) {
				context.run(key, 2, () -> {
					count.incrementAndGet();
					return null;
				});
			} else if (delta < 0) {
				context.run(key, 0, () -> {
					if (count.decrementAndGet() == 0) {
						context.locks.remove(key);
					}
					return null;
				});
			}
		}

		@Override
		public void lock() {
			add(1);
			delegate.lock();
		}

		@Override
		public void lockInterruptibly() throws InterruptedException {
			try {
				add(1);
				delegate.lockInterruptibly();
			} catch (InterruptedException e) {
				add(-1);
			}
		}

		@Override
		public boolean tryLock() {
			add(1);
			boolean result = delegate.tryLock();
			if (!result) {
				add(-1);
			}
			return result;
		}

		@Override
		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
			add(1);
			try {
				boolean result = delegate.tryLock(time, unit);
				if (!result) {
					add(-1);
				}
				return result;
			} catch (InterruptedException e) {
				add(-1);
				throw e;
			}
		}

		@Override
		public void unlock() {
			delegate.unlock();
			add(-1);
		}

		@Override
		public Condition newCondition() {
			return delegate.newCondition();
		}

		@Override
		public String toString() {
			return parent.toString();
		}
	}
}
