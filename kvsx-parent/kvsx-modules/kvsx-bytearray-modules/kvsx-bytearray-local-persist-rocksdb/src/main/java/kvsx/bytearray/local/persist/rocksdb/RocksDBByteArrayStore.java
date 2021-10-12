package kvsx.bytearray.local.persist.rocksdb;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.util.concurrent.Striped;

import kvsx.bytearray.ByteArrayStore;
import kvsx.bytearray.exception.ByteArrayStoreException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;

@Builder(builderClassName = "Builder")
public class RocksDBByteArrayStore implements ByteArrayStore {
	private String path;
	@Default
	private Options options = null;

	private final RocksDB rocksDB;
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Striped<Lock> stripedUpdateLock = Striped.lock(8_191);

	private RocksDBByteArrayStore(String path, Options options) throws ByteArrayStoreException {
		this.path = path;
		this.options = options;

		try {
			rocksDB = RocksDB.open(this.options, this.path);
		} catch (RocksDBException e) {
			throw new ByteArrayStoreException(e);
		} finally {
			this.options.close();
			this.options = null;
			this.path = null;
		}
	}

	public static class Builder {
		@SuppressWarnings("unused")
		private Builder rocksDB(RocksDB rocksDB) {
			return this;
		}

		public RocksDBByteArrayStore build() throws ByteArrayStoreException {
			return new RocksDBByteArrayStore(path, options$set ? options$value : new Options());
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
		Lock lock = readWriteLock.writeLock();
		try {
			lock.lock();
			rocksDB.close();
		} finally {
			lock.unlock();
		}
	}

	@AllArgsConstructor
	@EqualsAndHashCode
	private static class Bin {
		private byte[] data;
	}
}
