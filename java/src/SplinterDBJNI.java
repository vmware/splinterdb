package org.splinterdb;

import java.util.concurrent.TimeUnit;

public class SplinterDBJNI {


	static {
		System.loadLibrary("splinterdbjni");
	}


	public native synchronized int createOrOpen(String filename, long cacheSize, long diskSize,
				int maxKeySize, int valueSize, int open_existing);

	public native synchronized int insert(int dbid, byte[] key, byte[] value);

	public  native synchronized byte[] lookup(int dbid, byte[] key);

	public  native synchronized int delete(int dbid, byte[] key);

	public  native synchronized int close(int dbid);

	public  native synchronized String version();

	public void myString() {
		System.out.println("hello");
	}

	public static void main (String args[])
	{
		try {
	
			SplinterDBJNI splinter = new SplinterDBJNI();

			System.out.println(splinter.version());

			int id = splinter.createOrOpen("mydb", 64, 1024, 100, 5, 0);

			String keyStr = "key1";
			byte[] keyBytes = keyStr.getBytes();

			String valueStr = "value1";
			byte[] valueBytes = valueStr.getBytes();

			splinter.insert(id, keyBytes, valueBytes);

			byte[] value = splinter.lookup(id, keyBytes);

			String outputValueStr = new String(value);

			System.out.println("My value is:" + outputValueStr);

			splinter.delete(id, keyBytes);

			splinter.close(id);

		} catch (Exception ex) {
			System.out.println(ex.toString());
		}
	}
} 
