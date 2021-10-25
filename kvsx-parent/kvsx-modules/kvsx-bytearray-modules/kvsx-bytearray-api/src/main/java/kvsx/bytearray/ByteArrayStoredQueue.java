package kvsx.bytearray;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import kvsx.exception.KvsxException;
import kvsx.serde.Bits;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Builder(builderClassName = "Builder")
public class ByteArrayStoredQueue {
	public static class Builder {
		public ByteArrayStoredQueue build() throws KvsxException {
			return new ByteArrayStoredQueue(byteArrayStore, queueDataStoreId, queueMetaStoreId);
		}
	}

	private static final byte[] LAST_OFFSET_KEY = { 0 };
	private static final byte[] FIRST_OFFSET_KEY = { 1 };

	private final AtomicLong firstOffset = new AtomicLong();
	private final AtomicLong lastOffset = new AtomicLong();

	private final Lock firstOffsetLock = new ReentrantLock();
	private final Lock lastOffsetLock = new ReentrantLock();

	private final ByteArrayStore byteArrayStore;
	private final byte queueDataStoreId;
	private final byte queueMetaStoreId;

	private final ExecutorService tp = Executors.newFixedThreadPool(Integer.parseInt(System.getProperty("remove.threads", "1")), new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setDaemon(true);
			return t;
		}
	});

	public ByteArrayStoredQueue(ByteArrayStore byteArrayStore, byte queueDataStoreId, byte queueMetaStoreId) throws KvsxException {
		this.byteArrayStore = byteArrayStore;
		this.queueDataStoreId = queueDataStoreId;
		this.queueMetaStoreId = queueMetaStoreId;
		byte[] buf;
		buf = this.byteArrayStore.get(this.queueMetaStoreId, LAST_OFFSET_KEY);
		if (buf != null) {
			lastOffset.set(Bits.getLong(buf));
			buf = this.byteArrayStore.get(this.queueMetaStoreId, FIRST_OFFSET_KEY);
			if (buf != null) {
				firstOffset.set(Bits.getLong(buf));
			}
		}
	}

	public void offer(byte[] value) throws KvsxException {
		try {
			lastOffsetLock.lock();
			long nextOffset = lastOffset.incrementAndGet();
			byte[] nextOffsetBin = Bits.toByteArray(nextOffset);
			byteArrayStore.put(queueDataStoreId, nextOffsetBin, value);
			byteArrayStore.put(queueMetaStoreId, LAST_OFFSET_KEY, nextOffsetBin);
		} finally {
			lastOffsetLock.unlock();
		}
	}

	public byte[] pollAndCommit() throws KvsxException {
		Entry result = poll();
		if (result != null) {
			commit(result);
			return result.value();
		}
		return null;
	}

	public Entry poll() throws KvsxException {
		long nextOffset = firstOffset.get();
		while (true) {
			nextOffset++;
			if (nextOffset >= lastOffset.get()) {
				return null;
			}
			byte[] nextOffsetBin = Bits.toByteArray(nextOffset);
			byte[] value = byteArrayStore.get(queueDataStoreId, nextOffsetBin);
			if (value != null) {
				return new Entry(this).value(value).offset(nextOffset);
			}
		}
	}

	private Entry next(Entry entry) throws KvsxException {
		long nextOffset = entry.offset();
		nextOffset++;
		while (true) {
			if (nextOffset >= lastOffset.get()) {
				return null;
			}
			byte[] nextOffsetBin = Bits.toByteArray(nextOffset);
			byte[] value = byteArrayStore.get(queueDataStoreId, nextOffsetBin);
			if (value != null) {
				return new Entry(this).value(value).offset(nextOffset);
			}
		}
	}

	private void commit(Entry entry) throws KvsxException {
		try {
			firstOffsetLock.lock();
			long nextOffset = entry.offset();
			byte[] nextOffsetBin = Bits.toByteArray(nextOffset);
			byteArrayStore.put(queueMetaStoreId, FIRST_OFFSET_KEY, nextOffsetBin);
			long start = firstOffset.get() + 1L;
			while (true) {
				long offset = start++;
				final byte[] offsetBin = Bits.toByteArray(offset);
				tp.submit(() -> {
					try {
						byteArrayStore.remove(queueDataStoreId, offsetBin);
					} catch (KvsxException ignore) {
					}
				});
				if (offset == entry.offset()) {
					firstOffset.set(offset);
					break;
				}
			}
		} finally {
			firstOffsetLock.unlock();
		}
	}

	public boolean isEmpty() {
		return lastOffset.get() <= firstOffset.get();
	}

	public int size() {
		return (int) ((-1) & (lastOffset.get() - firstOffset.get()));
	}

	public void dump(Consumer<byte[]> consumer) {
		for (long i = firstOffset.get(); i <= lastOffset.get(); i++) {
			byte[] offsetBin = Bits.toByteArray(i);
			byte[] value;
			try {
				value = byteArrayStore.get(queueDataStoreId, offsetBin);
				if (value != null) {
					consumer.accept(value);
				}
			} catch (KvsxException e) {
			}
		}
	}

	@Accessors(fluent = true)
	public static class Entry {
		private final ByteArrayStoredQueue queue;
		@Setter
		@Getter
		private byte[] value;
		@Setter
		@Getter(value = AccessLevel.PRIVATE)
		private long offset;

		private Entry(ByteArrayStoredQueue queue) {
			this.queue = queue;
		}

		public Entry next() throws KvsxException {
			return queue.next(this);
		}

		public void commit() throws KvsxException {
			queue.commit(this);
		}
	}
}
