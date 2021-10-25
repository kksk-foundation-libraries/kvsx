package kvsx.bytearray.local.memory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kvsx.bytearray.ByteArrayStore;
import kvsx.exception.KvsxException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;

@Builder(builderClassName = "Builder")
public class LocalMemoryByteArrayStore implements ByteArrayStore {
	private final ConcurrentMap<Bin, byte[]> internalStore = new ConcurrentHashMap<>();

	public static class Builder {
		public LocalMemoryByteArrayStore build() throws KvsxException {
			return new LocalMemoryByteArrayStore();
		}
	}

	private LocalMemoryByteArrayStore() throws KvsxException {
	}

	@Override
	public void put(byte[] physicalKey, byte[] value) throws KvsxException {
		internalStore.put(new Bin(physicalKey), value);
	}

	@Override
	public void remove(byte[] physicalKey) throws KvsxException {
		internalStore.remove(new Bin(physicalKey));
	}

	@Override
	public boolean putIfAbsent(byte[] physicalKey, byte[] value) throws KvsxException {
		return internalStore.putIfAbsent(new Bin(physicalKey), value) == null;
	}

	@Override
	public byte[] get(byte[] physicalKey) throws KvsxException {
		return internalStore.get(new Bin(physicalKey));
	}

	@Override
	public void clear() throws KvsxException {
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
