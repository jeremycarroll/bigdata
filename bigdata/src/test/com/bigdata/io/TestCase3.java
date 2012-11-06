/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
/*
 * Created on Apr 27, 2010
 */

package com.bigdata.io;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.BytesUtil;
import com.bigdata.journal.TestHelper;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import junit.framework.TestCase2;

/**
 * Base class for some <code>assertEquals</code> methods not covered by
 * {@link TestCase} or {@link TestCase2}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCase3 extends TestCase2 {

    /**
     * 
     */
    public TestCase3() {
     
    }

    /**
     * @param name
     */
    public TestCase3(String name) {
        super(name);
     
    }

    protected void tearDown() throws Exception {

        super.tearDown();

        TestHelper.checkJournalsClosed(this);
        
    }
    
    /**
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
     * 
     * @param expected
     *            Non-null byte[].
     * @param actual
     *            Buffer.
     */
    public static void assertEquals(ByteBuffer expectedBuffer,
            final ByteBuffer actual) {

        if (expectedBuffer == null)
            throw new IllegalArgumentException();

        if (actual == null)
            fail("actual is null");

        if (expectedBuffer.hasArray() && expectedBuffer.arrayOffset() == 0
                && expectedBuffer.position() == 0
                && expectedBuffer.limit() == expectedBuffer.capacity()) {

            // evaluate byte[] against actual.
            assertEquals(expectedBuffer.array(), actual);

            return;

        }
        
        /*
         * Copy the expected data into a byte[] using a read-only view on the
         * buffer so that we do not mess with its position, mark, or limit.
         */
        final byte[] expected;
        {

            expectedBuffer = expectedBuffer.asReadOnlyBuffer();

            final int len = expectedBuffer.remaining();

            expected = new byte[len];

            expectedBuffer.get(expected);

        }

        // evaluate byte[] against actual.
        assertEquals(expected, actual);

    }

    /**
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
     * 
     * @param expected
     *            Non-null byte[].
     * @param actual
     *            Buffer.
     */
    public static void assertEquals(final byte[] expected, ByteBuffer actual) {

        if (expected == null)
            throw new IllegalArgumentException();

        if (actual == null)
            fail("actual is null");

        if (actual.hasArray() && actual.arrayOffset() == 0
                && actual.position() == 0
                && actual.limit() == actual.capacity()) {

            assertEquals(expected, actual.array());

            return;

        }

        /*
         * Create a read-only view on the buffer so that we do not mess with its
         * position, mark, or limit.
         */
        actual = actual.asReadOnlyBuffer();

        final int len = actual.remaining();

        final byte[] actual2 = new byte[len];

        actual.get(actual2);

        // compare byte[]s.
        assertEquals(expected, actual2);

    }

	/**
	 * Return the data in the buffer.
	 */
	public static byte[] getBytes(final ByteBuffer buf) {

	    return BytesUtil.getBytes(buf);

	}

    
    /**
     * Wait up to a timeout until some condition succeeds.
     * 
     * @param cond
     *            The condition, which must throw an
     *            {@link AssertionFailedError} if it does not succeed.
     * @param timeout
     *            The timeout.
     * @param unit
     * 
     * @throws AssertionFailedError
     *             if the condition does not succeed within the timeout.
     */
    static public void assertCondition(final Runnable cond,
            final long timeout, final TimeUnit units) {
        final long begin = System.nanoTime();
        final long nanos = units.toNanos(timeout);
        long remaining = nanos;
        // remaining = nanos - (now - begin) [aka elapsed]
        remaining = nanos - (System.nanoTime() - begin);
        while (true) {
            try {
                // try the condition
                cond.run();
                // success.
                return;
            } catch (final AssertionFailedError e) {
                remaining = nanos - (System.nanoTime() - begin);
                if (remaining < 0) {
                    // Timeout - rethrow the failed assertion.
                    throw e;
                }
                // Sleep up to 10ms or the remaining nanos, which ever is less.
                final int millis = (int) Math.min(
                        TimeUnit.NANOSECONDS.toMillis(remaining), 10);
                if (millis > 0) {
                    // sleep and retry.
                    try {
                        Thread.sleep(millis);
                    } catch (InterruptedException e1) {
                        // propagate the interrupt.
                        Thread.currentThread().interrupt();
                        return;
                    }
                    remaining = nanos - (System.nanoTime() - begin);
                    if (remaining < 0) {
                        // Timeout - rethrow the failed assertion.
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * Waits up to 5 seconds for the condition to succeed.
     * 
     * @param cond
     *            The condition, which must throw an
     *            {@link AssertionFailedError} if it does not succeed.
     * 
     * @throws AssertionFailedError
     *             if the condition does not succeed within the timeout.
     * 
     * @see #assertCondition(Runnable, long, TimeUnit)
     */
    static public void assertCondition(final Runnable cond) {
        
        assertCondition(cond, 5, TimeUnit.SECONDS);
        
    }

}
