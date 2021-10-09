package kvsx.bytearray;

import java.util.Objects;

import kvsx.bytearray.exception.ByteArrayStoreException;

public interface ByteArrayStore {
	default void put(byte storeId, byte[] key, byte[] value) throws ByteArrayStoreException {
		put(physicalKey(storeId, key), value);
	}

	default void remove(byte storeId, byte[] key) throws ByteArrayStoreException {
		remove(physicalKey(storeId, key));
	}

	default boolean putIfAbsent(byte storeId, byte[] key, byte[] value) throws ByteArrayStoreException {
		return putIfAbsent(physicalKey(storeId, key), value);
	}

	default byte[] get(byte storeId, byte[] key) throws ByteArrayStoreException {
		return get(physicalKey(storeId, key));
	}

	void put(byte[] physicalKey, byte[] value) throws ByteArrayStoreException;

	void remove(byte[] physicalKey) throws ByteArrayStoreException;

	boolean putIfAbsent(byte[] physicalKey, byte[] value) throws ByteArrayStoreException;

	byte[] get(byte[] physicalKey) throws ByteArrayStoreException;

	default byte[] physicalKey(byte storeId, byte[] key) {
		Objects.requireNonNull(key, "key must not be null.");
		return join(Bits.toByteArray(storeId), key);
	}

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
