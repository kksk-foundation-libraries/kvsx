package kvsx.bytearray;

import java.util.Objects;

import kvsx.exception.KvsxException;
import kvsx.serde.Bits;

public interface ByteArrayStore extends AutoCloseable {
	default void put(byte storeId, byte[] key, byte[] value) throws KvsxException {
		put(physicalKey(storeId, key), value);
	}

	default void remove(byte storeId, byte[] key) throws KvsxException {
		remove(physicalKey(storeId, key));
	}

	default boolean putIfAbsent(byte storeId, byte[] key, byte[] value) throws KvsxException {
		return putIfAbsent(physicalKey(storeId, key), value);
	}

	default byte[] get(byte storeId, byte[] key) throws KvsxException {
		return get(physicalKey(storeId, key));
	}

	void put(byte[] physicalKey, byte[] value) throws KvsxException;

	default void merge(byte storeId, byte[] key, byte[] value) throws KvsxException {
		put(storeId, key, value);
	}

	default void merge(byte[] physicalKey, byte[] value) throws KvsxException {
		put(physicalKey, value);
	}

	void remove(byte[] physicalKey) throws KvsxException;

	boolean putIfAbsent(byte[] physicalKey, byte[] value) throws KvsxException;

	byte[] get(byte[] physicalKey) throws KvsxException;

	default byte[] physicalKey(byte storeId, byte[] key) {
		Objects.requireNonNull(key, "key must not be null.");
		return join(Bits.toByteArray(storeId), key);
	}

	void clear() throws KvsxException;

	default byte[] join(byte[]... bs) {
		int length = 0;
		for (byte[] b : bs) {
			length += b.length;
		}
		byte[] join = new byte[length];
		int pos = 0;
		for (byte[] b : bs) {
			System.arraycopy(b, 0, join, pos, b.length);
			pos += b.length;
		}
		return join;
	}
}
