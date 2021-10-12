package kvsx.bytearray.local.persist.rocksdb;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.util.concurrent.Striped;

import kvsx.bytearray.ByteArrayStore;
import kvsx.bytearray.exception.ByteArrayStoreException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;

@Builder(builderClassName = "Builder")
public class GivenRocksDBByteArrayStore implements ByteArrayStore {
	private RocksDB rocksDB;
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Striped<Lock> stripedUpdateLock = Striped.lock(8_191);

	private GivenRocksDBByteArrayStore(RocksDB rocksDB) throws ByteArrayStoreException {
		this.rocksDB = rocksDB;
	}

	public static class Builder {
		public GivenRocksDBByteArrayStore build() throws ByteArrayStoreException {
			return new GivenRocksDBByteArrayStore(rocksDB);
		}
	}

	@Override
	public void put(byte[] physicalKey, byte[] value) throws ByteArrayStoreException {
		Bin lockKey = new Bin(physicalKey);
		Lock lock = readWriteLock.readLock();
		Lock updateLock = stripedUpdateLock.get(lockKey);
		try {
			lock.lock();
			updateLock.lock();
			rocksDB.put(physicalKey, value);
		} catch (RocksDBException e) {
			throw new ByteArrayStoreException(e);
		} finally {
			updateLock.unlock();
			lock.unlock();
		}
	}

	@Override
	public void remove(byte[] physicalKey) throws ByteArrayStoreException {
		Bin lockKey = new Bin(physicalKey);
		Lock lock = readWriteLock.readLock();
		Lock updateLock = stripedUpdateLock.get(lockKey);
		try {
			lock.lock();
			updateLock.lock();
			rocksDB.delete(physicalKey);
		} catch (RocksDBException e) {
			throw new ByteArrayStoreException(e);
		} finally {
			updateLock.unlock();
			lock.unlock();
		}
	}

	@Override
	public boolean putIfAbsent(byte[] physicalKey, byte[] value) throws ByteArrayStoreException {
		Bin lockKey = new Bin(physicalKey);
		Lock lock = readWriteLock.readLock();
		Lock updateLock = stripedUpdateLock.get(lockKey);
		try {
			lock.lock();
			updateLock.lock();
			byte[] old = rocksDB.get(physicalKey);
			if (old == null) {
				rocksDB.put(physicalKey, value);
				return true;
			}
			return false;
		} catch (RocksDBException e) {
			throw new ByteArrayStoreException(e);
		} finally {
			updateLock.unlock();
			lock.unlock();
		}
	}

	@Override
	public byte[] get(byte[] physicalKey) throws ByteArrayStoreException {
		Lock lock = readWriteLock.readLock();
		try {
			lock.lock();
			return rocksDB.get(physicalKey);
		} catch (RocksDBException e) {
			throw new ByteArrayStoreException(e);
		} finally {
			lock.unlock();
		}
	}

	private static final byte[] MIN_KEY = { 0 };
	private static final byte[] MAX_KEY = new byte[256];

	static {
		Arrays.fill(MAX_KEY, (byte) 0xFF);
	}

	@Override
	public void clear() throws ByteArrayStoreException {
		Lock lock = readWriteLock.writeLock();
		try {
			lock.lock();
			rocksDB.deleteRange(MIN_KEY, MAX_KEY);
		} catch (RocksDBException e) {
			throw new ByteArrayStoreException(e);
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
