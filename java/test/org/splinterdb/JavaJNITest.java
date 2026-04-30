package org.splinterdb;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.splinterdb.SplinterDBJNI;

public class JavaJNITest {

	@Test
	public void testSplinterDB() {
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

			assertTrue(false);
                }
		assertTrue(true);

	}	
}
