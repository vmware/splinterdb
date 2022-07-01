import java.util.concurrent.TimeUnit;

public class SplinterDBJNI {


	static {
		System.loadLibrary("splinterdbjni");
	}


	private native synchronized int createOrOpen(String filename, long cacheSize, long diskSize,
				int maxKeySize, int valueSize, int open_existing);

	private native synchronized int insert(int dbid, byte[] key, byte[] value);

	private native synchronized byte[] lookup(int dbid, byte[] key);

	private native synchronized int delete(int dbid, byte[] key);

	private native synchronized int close(int dbid);

	private native synchronized String version();

	public static void main (String args[])
	{
		try {
	
			SplinterDBJNI splinter = new SplinterDBJNI();

			System.out.println(splinter.version());

			int id = splinter.createOrOpen("mydb", 64, 1024, 100, 5, 1);

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
