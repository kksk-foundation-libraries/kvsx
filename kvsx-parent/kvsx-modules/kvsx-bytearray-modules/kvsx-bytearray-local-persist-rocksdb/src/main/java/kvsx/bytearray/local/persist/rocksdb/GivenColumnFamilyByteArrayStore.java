package kvsx.bytearray.local.persist.rocksdb;

import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.util.concurrent.Striped;

import kvsx.bytearray.ByteArrayStore;
import kvsx.exception.KvsxException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;

@Builder(builderClassName = "Builder")
public class GivenColumnFamilyByteArrayStore implements ByteArrayStore {
	private RocksDB rocksDB;
	private ConcurrentMap<Byte, ColumnFamilyHandle> handles;

	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Striped<Lock> stripedUpdateLock = Striped.lock(8_191);

	private GivenColumnFamilyByteArrayStore(RocksDB rocksDB, ConcurrentMap<Byte, ColumnFamilyHandle> handles) throws KvsxException {
		this.rocksDB = rocksDB;
		this.handles = handles;
	}

	public static class Builder {
		public GivenColumnFamilyByteArrayStore build() throws KvsxException {
			return new GivenColumnFamilyByteArrayStore(rocksDB, handles);
		}
	}

	@Override
	public void put(byte storeId, byte[] key, byte[] value) throws KvsxException {
		Bin lockKey = new Bin(physicalKey(storeId, key));
		Lock lock = readWriteLock.readLock();
		Lock updateLock = stripedUpdateLock.get(lockKey);
		try {
			lock.lock();
			updateLock.lock();
			rocksDB.put(handles.get(storeId), key, value);
		} catch (RocksDBException e) {
			throw new KvsxException(e);
		} finally {
			updateLock.unlock();
			lock.unlock();
		}
	}

	@Override
	public void put(byte[] physicalKey, byte[] value) throws KvsxException {
		throw new UnsupportedOperationException("please use put(byte storeId, byte[] key, byte[] value)");

	}

	@Override
	public void remove(byte storeId, byte[] key) throws KvsxException {
		Bin lockKey = new Bin(physicalKey(storeId, key));
		Lock lock = readWriteLock.readLock();
		Lock updateLock = stripedUpdateLock.get(lockKey);
		try {
			lock.lock();
			updateLock.lock();
			rocksDB.delete(handles.get(storeId), key);
		} catch (RocksDBException e) {
			throw new KvsxException(e);
		} finally {
			updateLock.unlock();
			lock.unlock();
		}
	}

	@Override
	public void remove(byte[] physicalKey) throws KvsxException {
		throw new UnsupportedOperationException("please use remove(byte storeId, byte[] key)");
	}

	@Override
	public boolean putIfAbsent(byte storeId, byte[] key, byte[] value) throws KvsxException {
		Bin lockKey = new Bin(physicalKey(storeId, key));
		Lock lock = readWriteLock.readLock();
		Lock updateLock = stripedUpdateLock.get(lockKey);
		try {
			lock.lock();
			updateLock.lock();
			byte[] old = rocksDB.get(handles.get(storeId), key);
			if (old == null) {
				rocksDB.put(handles.get(storeId), key, value);
				return true;
			}
			return false;
		} catch (RocksDBException e) {
			throw new KvsxException(e);
		} finally {
			updateLock.unlock();
			lock.unlock();
		}
	}

	@Override
	public boolean putIfAbsent(byte[] physicalKey, byte[] value) throws KvsxException {
		throw new UnsupportedOperationException("please use putIfAbsent(byte storeId, byte[] key, byte[] value)");
	}

	@Override
	public byte[] get(byte storeId, byte[] key) throws KvsxException {
		Lock lock = readWriteLock.readLock();
		try {
			lock.lock();
			return rocksDB.get(handles.get(storeId), key);
		} catch (RocksDBException e) {
			throw new KvsxException(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public byte[] get(byte[] physicalKey) throws KvsxException {
		throw new UnsupportedOperationException("please use get(byte storeId, byte[] key)");
	}

	private static final byte[] MIN_KEY = { 0 };
	private static final byte[] MAX_KEY = new byte[256];

	static {
		Arrays.fill(MAX_KEY, (byte) 0xFF);
	}

	@Override
	public void clear() throws KvsxException {
		Lock lock = readWriteLock.writeLock();
		try {
			lock.lock();
			final AtomicReference<RocksDBException> err = new AtomicReference<>();
			handles.values().forEach(handle -> {
				try {
					rocksDB.deleteRange(handle, MIN_KEY, MAX_KEY);
				} catch (RocksDBException e) {
					err.compareAndSet(null, e);
				}
			});
			if (err.get() != null) {
				throw err.get();
			}
		} catch (RocksDBException e) {
			throw new KvsxException(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void close() throws Exception {
	}

	@AllArgsConstructor
	@EqualsAndHashCode
	private static class Bin {
		private byte[] data;
	}
}
