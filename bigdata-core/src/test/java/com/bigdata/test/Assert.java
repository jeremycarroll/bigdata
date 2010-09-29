/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package com.bigdata.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

/**
 * Provide extensions to junit.
 */
public class Assert extends org.junit.Assert {

    // *** Don't provide a real logger! Calling classes should get their
    // *** own! This only exists to minimize code changes.
    protected static FakeLogger log = new FakeLogger();

    /**
     * Assert that an iterator returns a set of objects equal to
     * those in the given array and in the same order.
     * @param i1 the object array
     * @param i2the oterator
     */
    public static void assertSameIterator(Object[] i1, Iterator i2) {
        assertSameIterator("", i1, i2);
    }

    /**
     * Assert that an iterator returns a set of objects equal to
     * those in the given array and in the same order.
     * @param msg a failure message
     * @param i1 the object array
     * @param i2 the iterator
     */
    public static void assertSameIterator(String msg,
                                          byte[][] i1, Iterator i2) {
        assertSameByteArrayIterator(msg, Arrays.asList(i1).iterator(), i2);
    }

    /**
     * Assert that an iterator returns a set of objects equal to
     * those in the given array and in the same order.
     * @param msg a failure message
     * @param i1 the object array
     * @param i2 the iterator
     */
    public static void assertSameIterator(String msg,
                                          Object[] i1, Iterator i2) {
        assertSameIterator(msg, Arrays.asList(i1).iterator(), i2);
    }

    /**
     * Assert that an iterable returns a set of objects equal to
     * those in the given array and in the same order.
     * @param i1 the object array
     * @param i2the oterator
     */
    public static void assertSameIterator(Object[] i1, Iterable i2) {
        assertSameIterator(Arrays.asList(i1).iterator(), i2.iterator());
    }

    /**
     * Assert that two iterables return an equal set of objects in the
     * same order.
     * @param i1 the object array
     * @param i2the oterator
     */
    public static void assertSameIterator(Iterable i1, Iterable i2) {
        assertSameIterator(i1.iterator(), i2.iterator());
    }

    /**
     * Assert that two iterators return an equal set of objects in the
     * same order.
     * @param i1 the object array
     * @param i2the oterator
     */
    public static void assertSameIterator(Iterator i1, Iterator i2) {
        assertSameIterator("", i1, i2);
    }

    /**
     * Assert that two iterators return an equal set of objects in the
     * same order.
     * @param msg a failure message
     * @param i1 the object array
     * @param i2the oterator
     */
    public static void assertSameByteArrayIterator(String msg,
                                          Iterator i1, Iterator i2) {
        while (true) {
            if (i1.hasNext()) {
                if (i2.hasNext()) {
                    Object o1 = i1.next();
                    Object o2 = i2.next();
                    assertTrue(o1 instanceof byte[]);
                    assertTrue(o2 instanceof byte[]);
                    assertArrayEquals(msg, (byte[])o1, (byte[])o2);
                } else {
                    fail(msg + " : first iterator has more elements.");
                }
            } else {
                if (i2.hasNext()) {
                    fail(msg + " : second iterator has more elements.");
                } else {
                    break;
                }
            }
        }
    }

    /*********************************************************************
     * NOTE: These methods override methods of the same name in 
     * org.junit.Assert which have terrible performance characteristics.
     * (e.g. spending > 80% of CPU time in Array.get() for some tests)
     *********************************************************************/

    public static void assertArrayEquals(byte[] expecteds, byte[] actuals) {
        assertArrayEquals(null, expecteds, actuals);
    }

    public static void assertArrayEquals(char[] expecteds, char[] actuals) {
        assertArrayEquals(null, expecteds, actuals);
    }

    public static void assertArrayEquals(double[] expecteds, double[] actuals, double delta) {
        assertArrayEquals(null, expecteds, actuals, delta);
    }

    public static void assertArrayEquals(float[] expecteds, float[] actuals, float delta) {
        assertArrayEquals(null, expecteds, actuals, delta);
    }

    public static void assertArrayEquals(int[] expecteds, int[] actuals) {
        assertArrayEquals(null, expecteds, actuals);
    }

    public static void assertArrayEquals(long[] expecteds, long[] actuals) {
        assertArrayEquals(null, expecteds, actuals);
    }

    public static void assertArrayEquals(Object[] expecteds, Object[] actuals) {
        assertArrayEquals(null, expecteds, actuals);
    }

    public static void assertArrayEquals(short[] expecteds, short[] actuals) {
        assertArrayEquals(null, expecteds, actuals);
    }

    public static void assertArrayEquals(String message, byte[] expecteds, byte[] actuals) {
        assertTrue(message, Arrays.equals(expecteds, actuals));
    }

    public static void assertArrayEquals(String message, char[] expecteds, char[] actuals) {
        assertTrue(message, Arrays.equals(expecteds, actuals));
    }

    public static void assertArrayEquals(String message, double[] expecteds, double[] actuals, double delta) {
        if (expecteds == null && actuals == null) {
            return;
        }
        assertNotNull(message, expecteds);
        assertNotNull(message, actuals);
        assertEquals(message, expecteds.length, actuals.length);
        for (int i = 0; i < expecteds.length; ++i) {
            assertEquals(message, expecteds[i], actuals[i], delta);
        }
    }

    public static void assertArrayEquals(String message, float[] expecteds, float[] actuals, float delta) {
        if (expecteds == null && actuals == null) {
            return;
        }
        assertNotNull(message, expecteds);
        assertNotNull(message, actuals);
        assertEquals(message, expecteds.length, actuals.length);
        for (int i = 0; i < expecteds.length; ++i) {
            assertEquals(message, expecteds[i], actuals[i], delta);
        }
    }

    public static void assertArrayEquals(String message, int[] expecteds, int[] actuals) {
        assertTrue(message, Arrays.equals(expecteds, actuals));
    }

    public static void assertArrayEquals(String message, long[] expecteds, long[] actuals) {
        assertTrue(message, Arrays.equals(expecteds, actuals));
    }

    public static void assertArrayEquals(String message, Object[] expecteds, Object[] actuals) {
        assertTrue(message, Arrays.equals(expecteds, actuals));
    }

    public static void assertArrayEquals(String message, short[] expecteds, short[] actuals) {
        assertTrue(message, Arrays.equals(expecteds, actuals));
    }

    /**
     * Assert that two iterators return an equal set of objects in the
     * same order.
     * @param msg a failure message
     * @param i1 the object array
     * @param i2the oterator
     */
    public static void assertSameIterator(String msg,
                                          Iterator i1, Iterator i2) {
        while (true) {
            if (i1.hasNext()) {
                if (i2.hasNext()) {
                    Object o1 = i1.next();
                    Object o2 = i2.next();
                    assertEquals(msg, o1, o2);
                } else {
                    fail(msg + " : first iterator has more elements.");
                }
            } else {
                if (i2.hasNext()) {
                    fail(msg + " : second iterator has more elements.");
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Assert that an iterator returns a set of objects equal to
     * those in the given array but not necessarily in the same order.
     * @param msg a failure message
     * @param i1 the object array
     * @param i2 the iterator
     */
    public static void assertSameIteratorAnyOrder(Object[] i1, Iterator i2) {
        LinkedList list = new LinkedList();
        list.addAll(Arrays.asList(i1));
        while (i2.hasNext()) {
            assertTrue(list.remove(i2.next()));
        }
        assertTrue(list.isEmpty());
    }

    /**
     * Create and return a set of properties derived from (1) a
     * properties file in the classpath with the same name as the test
     * class but with the suffix ".class" replaced with ".properties",
     * and (2) the system properties.
     *
     * Note that previous versions of this mechanism also looked in a
     * build.properties file in the user's home directory. This has been
     * removed because unit tests need to be run the same way everywhere.
     *
     * @return a properties object for a test
     */
    public Properties getProperties() {
        Properties result = new Properties();
        try {
            String name = this.getClass().getName();
            String resourceName = name.replace('.','/') + ".properties";
            ClassLoader cl = getClass().getClassLoader();
            InputStream is = cl.getResourceAsStream(resourceName);
            if (is != null) {
                result.load(is);
            }
            Properties sysProps = System.getProperties();
            for (String propName : sysProps.stringPropertyNames()) {
                result.put(propName, sysProps.get(propName));
            }
        } catch( IOException ex ) {
            throw new RuntimeException("Could not load properties.", ex);
        }
	return result;
    }

    public InputStream getTestInputStream(String rsrcName) {
        ClassLoader cl = getClass().getClassLoader();
        InputStream is = cl.getResourceAsStream(rsrcName);
        assertNotNull(is);
        return is;
    }

    public String getTestResource(String name) {
        throw new UnsupportedOperationException();
    }

    public static void fail(String msg, Throwable t) {
        fail(msg);
    }

    public static Throwable getInnerCause(Throwable t, Class clazz) {
        Throwable cause = t.getCause();
        assertNotNull(t);
        assertTrue(cause.getClass() == clazz);
        return cause;
    }

    public static boolean isInnerCause(Throwable t, Class clazz) {
        Throwable cause = t.getCause();
        if (cause != null) {
            return cause.getClass() == clazz;
        }
        return false;
    }

    /**
     * Return an array of integers in the range [0:n-1] in random order.
     * @param n number of integers to return.
     * @return the array.
     */
    public static int[] getRandomOrder(int n) {
        // Initialize the result array with its indices in order.
        int[] result = new int[n];
        for (int i = 0; i < n; ++i) {
            result[i] = i;
        }
        // For each element i in the array, starting with element 0, randomly
        // choose an element in the range [i:n-1] and exchange the contents
        // of element i with that element.
        for (int i = 0; i < (n - 1); ++i) {
            int r = (int)((n - i) * Math.random()) + i;
            int t = result[r];
            result[r] = result[i];
            result[i] = t;
        }
        return result;
    }

    /**
     * Return a random string of random length (in the range [0:max_len-1]),
     * with the id appended on the end for uniqueness.
     */
    public static String getRandomString(int maxlen, int id) {
        int len = (int)(Math.random() * maxlen);
        char[] result = new char[len];
        for (int i = 0; i < len; ++i) {
            result[i] = charList[(int)(Math.random() * charList.length)];
        }
        return new String(result) + "_" + Integer.toString(id);
    }
    private static final char[] charList =
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
            .toCharArray();


    public static PrintStream getNullPrintStream() {
        return new PrintStream(new NullOutputStream());
    }

    private static class NullOutputStream extends OutputStream {
        @Override
        public void write(int b) throws IOException {
            // Do nothing.
        }
    }

    public static class FakeLogger {
        public boolean isInfoEnabled() {
            return false;
        }
        public void info(Object ... msg) {
            // Do nothing! Classes doing logging should get their own
            // logger!
        }
        public boolean isDebugEnabled() {
            return false;
        }
        public void debug(Object ... msg) {
            // Do nothing! Classes doing logging should get their own
            // logger!
        }
        public void warn(Object ... msg) {
            // Do nothing! Classes doing logging should get their own
            // logger!
        }
        public void error(Object ... msg) {
            // Do nothing! Classes doing logging should get their own
            // logger!
            System.err.println("ERROR: " + msg[0]);
        }
    }
}
