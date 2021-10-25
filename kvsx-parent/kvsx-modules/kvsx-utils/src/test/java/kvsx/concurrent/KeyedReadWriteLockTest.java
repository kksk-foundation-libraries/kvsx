package kvsx.concurrent;

import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;

import org.junit.Test;
import org.junit.runner.OrderWith;
import org.junit.runner.manipulation.Alphanumeric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@OrderWith(Alphanumeric.class)
public class KeyedReadWriteLockTest {
	static {
		System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
		System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy/MM/dd HH:mm:ss.SSS");
		System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
	}
	private static Logger LOG = LoggerFactory.getLogger(KeyedReadWriteLockTest.class);

	@Test
	public void test001() {
		final int THREADS = 10;
		final int LOOP_COUNT = 100;
		final CountDownLatch latch = new CountDownLatch(THREADS);
		final KeyedReadWriteLock keyedReadWriteLock = new KeyedReadWriteLock();
		final long start = System.currentTimeMillis() + 1_000L;
		int arr[] = new int[LOOP_COUNT];
		for (int i = 0; i < LOOP_COUNT; i++) {
			arr[i] = 0;
		}
		final List<String> list = new LinkedList<>();
		for (int t = 0; t < THREADS; t++) {
			final int threadNo = t;
			new Thread(() -> {
				for (int l = 0; l < LOOP_COUNT; l++) {
					long wait = start + (l * 20) - System.currentTimeMillis();
					if (wait > 0) {
						try {
							Thread.sleep(wait);
						} catch (InterruptedException e) {
						}
					}
					final boolean write = threadNo % 2 != 0;
					final int loopNo = l;
					Lock lock = (!write ? keyedReadWriteLock.readLock(loopNo) : keyedReadWriteLock.writeLock(loopNo));
					try {
						lock.lock();
						if (write) {
							arr[loopNo]++;
						}
						String msg = String.format("write:%s, threadNo:%d, loopNo:%d, run same key:%d, ts:%d, lock:%s", Boolean.toString(write), threadNo, loopNo, arr[loopNo], System.currentTimeMillis(), lock.toString());
						LOG.debug(msg);
						if (arr[loopNo] != 1 && write) {
							list.add(msg);
						}
						if (write) {
							arr[loopNo]--;
						}
					} finally {
						lock.unlock();
					}
				}
				latch.countDown();
			}).start();
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
		}

		if (!list.isEmpty()) {
			list.forEach(LOG::error);
		}
		LOG.debug("size:{}", keyedReadWriteLock.locks.size());
		assertTrue("error must be none.", list.size() == 0);
		assertTrue("rest data must be none.", keyedReadWriteLock.locks.size() == 0);
	}

}
