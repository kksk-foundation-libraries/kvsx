package kvsx.bytearray;

class Bits {

	/*
	 * Methods for unpacking primitive values from byte arrays starting at
	 * given offsets.
	 */

	static boolean getBoolean(byte[] b) {
		return getBoolean(b, 0);
	}

	static boolean getBoolean(byte[] b, int off) {
		return b[off] != 0;
	}

	static char getChar(byte[] b) {
		return getChar(b, 0);
	}

	static char getChar(byte[] b, int off) {
		return (char) ((b[off + 1] & 0xFF) + (b[off] << 8));
	}

	static short getShort(byte[] b) {
		return getShort(b, 0);
	}

	static short getShort(byte[] b, int off) {
		return (short) ((b[off + 1] & 0xFF) + (b[off] << 8));
	}

	static int getInt(byte[] b) {
		return getInt(b, 0);
	}

	static int getInt(byte[] b, int off) {
		return ((b[off + 3] & 0xFF)) + ((b[off + 2] & 0xFF) << 8) + ((b[off + 1] & 0xFF) << 16) + ((b[off]) << 24);
	}

	static float getFloat(byte[] b) {
		return getFloat(b, 0);
	}

	static float getFloat(byte[] b, int off) {
		return Float.intBitsToFloat(getInt(b, off));
	}

	static long getLong(byte[] b) {
		return getLong(b, 0);
	}

	static long getLong(byte[] b, int off) {
		return ((b[off + 7] & 0xFFL)) + ((b[off + 6] & 0xFFL) << 8) + ((b[off + 5] & 0xFFL) << 16) + ((b[off + 4] & 0xFFL) << 24) + ((b[off + 3] & 0xFFL) << 32) + ((b[off + 2] & 0xFFL) << 40) + ((b[off + 1] & 0xFFL) << 48) + (((long) b[off]) << 56);
	}

	static double getDouble(byte[] b) {
		return getDouble(b, 0);
	}

	static double getDouble(byte[] b, int off) {
		return Double.longBitsToDouble(getLong(b, off));
	}

	/*
	 * Methods for packing primitive values into byte arrays starting at given
	 * offsets.
	 */

	static void putBoolean(byte[] b, boolean val) {
		putBoolean(b, 0, val);
	}

	static void putBoolean(byte[] b, int off, boolean val) {
		b[off] = (byte) (val ? 1 : 0);
	}

	static void putChar(byte[] b, char val) {
		putChar(b, 0, val);
	}

	static void putChar(byte[] b, int off, char val) {
		b[off + 1] = (byte) (val);
		b[off] = (byte) (val >>> 8);
	}

	static void putShort(byte[] b, short val) {
		putShort(b, 0, val);
	}

	static void putShort(byte[] b, int off, short val) {
		b[off + 1] = (byte) (val);
		b[off] = (byte) (val >>> 8);
	}

	static void putInt(byte[] b, int val) {
		putInt(b, 0, val);
	}

	static void putInt(byte[] b, int off, int val) {
		b[off + 3] = (byte) (val);
		b[off + 2] = (byte) (val >>> 8);
		b[off + 1] = (byte) (val >>> 16);
		b[off] = (byte) (val >>> 24);
	}

	static void putFloat(byte[] b, float val) {
		putFloat(b, 0, val);
	}

	static void putFloat(byte[] b, int off, float val) {
		putInt(b, off, Float.floatToIntBits(val));
	}

	static void putLong(byte[] b, long val) {
		putLong(b, 0, val);
	}

	static void putLong(byte[] b, int off, long val) {
		b[off + 7] = (byte) (val);
		b[off + 6] = (byte) (val >>> 8);
		b[off + 5] = (byte) (val >>> 16);
		b[off + 4] = (byte) (val >>> 24);
		b[off + 3] = (byte) (val >>> 32);
		b[off + 2] = (byte) (val >>> 40);
		b[off + 1] = (byte) (val >>> 48);
		b[off] = (byte) (val >>> 56);
	}

	static void putDouble(byte[] b, double val) {
		putDouble(b, 0, val);
	}

	static void putDouble(byte[] b, int off, double val) {
		putLong(b, off, Double.doubleToLongBits(val));
	}

	static byte[] newByteArray(int length) {
		return new byte[length];
	}

	static byte[] toByteArray(boolean val) {
		byte[] toByteArray = newByteArray(1);
		putBoolean(toByteArray, val);
		return toByteArray;
	}

	static byte[] toByteArray(byte val) {
		byte[] toByteArray = newByteArray(1);
		toByteArray[0] = val;
		return toByteArray;
	}

	static byte[] toByteArray(char val) {
		byte[] toByteArray = newByteArray(1);
		putChar(toByteArray, val);
		return toByteArray;
	}

	static byte[] toByteArray(short val) {
		byte[] toByteArray = newByteArray(1);
		putShort(toByteArray, val);
		return toByteArray;
	}

	static byte[] toByteArray(int val) {
		byte[] toByteArray = newByteArray(1);
		putInt(toByteArray, val);
		return toByteArray;
	}

	static byte[] toByteArray(float val) {
		byte[] toByteArray = newByteArray(1);
		putFloat(toByteArray, val);
		return toByteArray;
	}

	static byte[] toByteArray(long val) {
		byte[] toByteArray = newByteArray(1);
		putLong(toByteArray, val);
		return toByteArray;
	}

	static byte[] toByteArray(double val) {
		byte[] toByteArray = newByteArray(1);
		putDouble(toByteArray, val);
		return toByteArray;
	}

}
