package kvsx.bytearray;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import kvsx.bytearray.exception.ByteArrayStoreException;
import lombok.Builder;

@Builder(builderClassName = "Builder")
public class ByteArrayStoredQueue {
	public static class Builder {
		public ByteArrayStoredQueue build() throws ByteArrayStoreException {
			return new ByteArrayStoredQueue(byteArrayStore, queueDataStoreId, queueLastOffsetStoreId, queueFirstOffsetStoreId);
		}
	}

	private final AtomicLong firstOffset = new AtomicLong();
	private final AtomicLong lastOffset = new AtomicLong();

	private final ByteArrayStore byteArrayStore;
	private final byte queueDataStoreId;
	private final byte queueLastOffsetStoreId;
	private final byte queueFirstOffsetStoreId;

	public ByteArrayStoredQueue(ByteArrayStore byteArrayStore, byte queueDataStoreId, byte queueLastOffsetStoreId, byte queueFirstOffsetStoreId) throws ByteArrayStoreException {
		this.byteArrayStore = byteArrayStore;
		this.queueDataStoreId = queueDataStoreId;
		this.queueLastOffsetStoreId = queueLastOffsetStoreId;
		this.queueFirstOffsetStoreId = queueFirstOffsetStoreId;
		byte[] buf;
		buf = this.byteArrayStore.get(this.queueLastOffsetStoreId, Bits.MIN_KEY);
		if (buf != null) {
			lastOffset.set(Bits.getLong(buf));
			buf = this.byteArrayStore.get(this.queueFirstOffsetStoreId, Bits.MIN_KEY);
			if (buf != null) {
				firstOffset.set(Bits.getLong(buf));
			}
		}
	}

	public void offer(byte[] value) throws ByteArrayStoreException {
		synchronized (lastOffset) {
			long nextOffset = lastOffset.get();
			nextOffset++;
			byte[] nextOffsetBin = Bits.toByteArray(nextOffset);
			byteArrayStore.put(queueDataStoreId, nextOffsetBin, value);
			byteArrayStore.put(queueLastOffsetStoreId, Bits.MIN_KEY, nextOffsetBin);
			lastOffset.incrementAndGet();
		}
	}

	public byte[] poll() throws ByteArrayStoreException {
		synchronized (firstOffset) {
			while (true) {
				if (firstOffset.get() >= lastOffset.get()) {
					return null;
				}
				try {
					long nextOffset = firstOffset.get();
					nextOffset++;
					byte[] nextOffsetBin = Bits.toByteArray(nextOffset);
					byte[] value = byteArrayStore.get(queueDataStoreId, nextOffsetBin);
					if (value != null) {
						byteArrayStore.put(queueFirstOffsetStoreId, Bits.MIN_KEY, nextOffsetBin);
						byteArrayStore.remove(queueDataStoreId, nextOffsetBin);
						return value;
					}
				} finally {
					firstOffset.incrementAndGet();
				}
			}
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
			} catch (ByteArrayStoreException e) {
			}
		}
	}
}
