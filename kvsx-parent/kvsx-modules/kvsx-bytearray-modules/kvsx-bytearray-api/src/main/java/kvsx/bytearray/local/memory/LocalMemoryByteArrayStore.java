package kvsx.bytearray.local.memory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kvsx.bytearray.ByteArrayStore;
import kvsx.bytearray.exception.ByteArrayStoreException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;

@Builder(builderClassName = "Builder")
public class LocalMemoryByteArrayStore implements ByteArrayStore {
	private final ConcurrentMap<Bin, byte[]> internalStore = new ConcurrentHashMap<>();

	private LocalMemoryByteArrayStore() {
	}

	@Override
	public void put(byte[] physicalKey, byte[] value) throws ByteArrayStoreException {
		internalStore.put(new Bin(physicalKey), value);
	}

	@Override
	public void remove(byte[] physicalKey) throws ByteArrayStoreException {
		internalStore.remove(new Bin(physicalKey));
	}

	@Override
	public boolean putIfAbsent(byte[] physicalKey, byte[] value) throws ByteArrayStoreException {
		return internalStore.putIfAbsent(new Bin(physicalKey), value) == null;
	}

	@Override
	public byte[] get(byte[] physicalKey) throws ByteArrayStoreException {
		return internalStore.get(new Bin(physicalKey));
	}

	@Override
	public void clear() throws ByteArrayStoreException {
		internalStore.clear();
	}

	@Override
	public void close() throws Exception {
		internalStore.clear();
	}

	@AllArgsConstructor
	@EqualsAndHashCode
	private static class Bin {
		private byte[] data;
	}
}
