package kvsx.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import kvsx.serde.Bin;

public class KeyedLock {
	final ConcurrentMap<Object, CountDownLock> locks = new ConcurrentHashMap<>();
	final ConcurrentMap<Object, CountDownLatch> inProc = new ConcurrentHashMap<>();

	public Lock get(byte[] key) {
		return get(new Bin(key));
	}

	public Lock get(long key) {
		return get(Long.valueOf(key));
	}

	public Lock get(Object key) {
		return run(key, 1, () -> {
			Lock lock = get(key, () -> new CountDownLock(this, key));
			return lock;
		});
	}

	CountDownLock get(Object key, Supplier<CountDownLock> supplier) {
		CountDownLock existing = locks.get(key);
		if (existing != null) {
			return existing;
		}
		CountDownLock created = supplier.get();
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

	private static class CountDownLock implements Lock {
		private final KeyedLock context;
		private final Object key;
		private final AtomicInteger count = new AtomicInteger();
		private final Lock delegate = new ReentrantLock();

		public CountDownLock(KeyedLock context, Object key) {
			this.context = context;
			this.key = key;
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
	}
}
