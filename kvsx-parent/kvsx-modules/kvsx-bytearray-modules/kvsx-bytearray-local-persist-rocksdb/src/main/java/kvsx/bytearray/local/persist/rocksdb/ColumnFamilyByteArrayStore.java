package kvsx.bytearray.local.persist.rocksdb;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.util.concurrent.Striped;

import kvsx.bytearray.ByteArrayStore;
import kvsx.exception.KvsxException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;

@Builder(builderClassName = "Builder")
public class ColumnFamilyByteArrayStore implements ByteArrayStore {
	private String path;
	@Default
	private Options options = null;
	@Default
	private DBOptions dbOptions = null;
	private String[] columnFamilyNames;

	private final RocksDB rocksDB;
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Striped<Lock> stripedUpdateLock = Striped.lock(8_191);
	private final ConcurrentMap<Byte, ColumnFamilyHandle> handles = new ConcurrentHashMap<>();
	private final List<ColumnFamilyHandle> allHandles = new ArrayList<>();

	private ColumnFamilyByteArrayStore(String path, Options options, DBOptions dbOptions, String[] columnFamilyNames) throws KvsxException {
		this.path = path;
		this.options = options;
		this.dbOptions = dbOptions;
		this.columnFamilyNames = columnFamilyNames;

		Map<String, Byte> _columnFamilyNamesMap = new HashMap<>();
		for (int i = 0; i < this.columnFamilyNames.length; i++) {
			Byte b = Byte.valueOf((byte) i);
			_columnFamilyNamesMap.put(this.columnFamilyNames[i], b);
		}
		try {
			File dir = new File(this.path);
			if (dir.exists()) {
				Options _opt = new Options();
				List<byte[]> descNames = RocksDB.listColumnFamilies(_opt, path);
				_opt.close();
				List<ColumnFamilyDescriptor> descs = descNames.stream().map(ColumnFamilyDescriptor::new).collect(Collectors.toList());
				List<ColumnFamilyHandle> hands = new ArrayList<>();
				rocksDB = RocksDB.open(this.dbOptions, path, descs, hands);

				String[] descsArr = (String[]) descNames.stream().map(String::new).collect(Collectors.toList()).toArray();
				ColumnFamilyHandle[] handsArr = (ColumnFamilyHandle[]) hands.toArray();
				allHandles.addAll(hands);
				for (int i = 0; i < descsArr.length; i++) {
					Byte b = _columnFamilyNamesMap.remove(descsArr[i]);
					if (b != null) {
						handles.put(b, handsArr[i]);
					}
				}
				final AtomicReference<RocksDBException> err = new AtomicReference<>();
				_columnFamilyNamesMap.forEach((columnFamilyName, b) -> {
					try {
						ColumnFamilyHandle handle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(columnFamilyName.getBytes()));
						handles.put(b, handle);
						allHandles.add(handle);
					} catch (RocksDBException e) {
						err.compareAndSet(null, e);
					}
				});
				if (err.get() != null) {
					throw err.get();
				}
			} else {
				rocksDB = RocksDB.open(this.options, this.path);
				final AtomicReference<RocksDBException> err = new AtomicReference<>();
				_columnFamilyNamesMap.forEach((columnFamilyName, b) -> {
					try {
						ColumnFamilyHandle handle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(columnFamilyName.getBytes()));
						handles.put(b, handle);
						allHandles.add(handle);
					} catch (RocksDBException e) {
						err.compareAndSet(null, e);
					}
				});
				if (err.get() != null) {
					throw err.get();
				}
			}
		} catch (RocksDBException e) {
			throw new KvsxException(e);
		} finally {
			this.options.close();
			this.options = null;
			this.dbOptions.close();
			this.dbOptions = null;
			this.path = null;
		}
	}

	public static class Builder {
		@SuppressWarnings("unused")
		private Builder rocksDB(RocksDB rocksDB) {
			return this;
		}

		public ColumnFamilyByteArrayStore build() throws KvsxException {
			return new ColumnFamilyByteArrayStore(path, options$set ? options$value : new Options(), dbOptions$set ? dbOptions$value : new DBOptions(), columnFamilyNames);
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
	public void merge(byte storeId, byte[] key, byte[] value) throws KvsxException {
		Bin lockKey = new Bin(physicalKey(storeId, key));
		Lock lock = readWriteLock.readLock();
		Lock updateLock = stripedUpdateLock.get(lockKey);
		try {
			lock.lock();
			updateLock.lock();
			rocksDB.merge(handles.get(storeId), key, value);
		} catch (RocksDBException e) {
			throw new KvsxException(e);
		} finally {
			updateLock.unlock();
			lock.unlock();
		}
	}

	@Override
	public void merge(byte[] physicalKey, byte[] value) throws KvsxException {
		throw new UnsupportedOperationException("please use merge(byte storeId, byte[] key, byte[] value)");
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
		Lock lock = readWriteLock.writeLock();
		try {
			lock.lock();
			allHandles.forEach(handle -> handle.close());
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
