/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on May 16, 2008
 */

package com.bigdata.io;

import java.nio.ByteBuffer;

import junit.framework.TestCase2;

/**
 * Test suite for {@link DirectBufferPool}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Unit test to verify that a pool will reject a buffer not
 *          acquired from that pool.
 * 
 *          TODO Write a unit test to verify that buffers (up to the size of the
 *          pool, but not more than 10) are recycled when they are released
 *          (that is, they are made available again to subsequent acquires).
 * 
 *          <pre>
 * final int limit = Math.min(10, DirectBufferPool.INSTANCE.getPoolCapacity());
 * </pre>
 */
public class TestDirectBufferPool extends TestCase2 {

    /**
     * 
     */
    public TestDirectBufferPool() {
    }

    /**
     * @param arg0
     */
    public TestDirectBufferPool(String arg0) {
        super(arg0);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        DirectBufferPoolTestHelper.checkBufferPools(this);
    }

    @Override
    protected void tearDown() throws Exception {
        DirectBufferPoolTestHelper.checkBufferPools(this);
        super.tearDown();
    }

    public void test_allocateRelease() throws InterruptedException {

        final int poolSizeBefore = DirectBufferPool.INSTANCE.getPoolSize();
        final int poolAcquiredBefore = DirectBufferPool.INSTANCE
                .getAcquiredBufferCount();

        final int poolSizeDuring;
        final int poolAcquiredDuring;
        {
            ByteBuffer b = null;
            try {
                b = DirectBufferPool.INSTANCE.acquire();

                poolSizeDuring = DirectBufferPool.INSTANCE.getPoolSize();
                poolAcquiredDuring = DirectBufferPool.INSTANCE
                        .getAcquiredBufferCount();

                assertEquals(poolSizeBefore + 1, poolSizeDuring);
                assertEquals(poolAcquiredBefore + 1, poolAcquiredDuring);

            } finally {
                if (b != null)
                    DirectBufferPool.INSTANCE.release(b);
            }
        }

        final int poolSizeAfter = DirectBufferPool.INSTANCE.getPoolSize();
        final int poolAcquiredAfter = DirectBufferPool.INSTANCE
                .getAcquiredBufferCount();

        // the pool size does not decrease.
        assertEquals(poolSizeBefore + 1, poolSizeAfter);

        // the #of acquired buffers does decrease.
        assertEquals(poolAcquiredBefore, poolAcquiredAfter);

    }

    /**
     * Test verifies that a pool will not allocate a new buffer when it can
     * recycle one instead.
     * 
     * @throws InterruptedException
     */
    public void test_buffersRecycled() throws InterruptedException {

        final int poolSizeBefore = DirectBufferPool.INSTANCE.getPoolSize();
        for (int i = 0; i < 10; i++) {
            ByteBuffer b = null;
            try {
                b = DirectBufferPool.INSTANCE.acquire();
                // pool size remains constant.
                assertEquals(poolSizeBefore, DirectBufferPool.INSTANCE
                        .getPoolSize());
            } finally {
                if (b != null)
                    DirectBufferPool.INSTANCE.release(b);
            }
        }

    }

    /**
     * Unit test to verify that a pool will reject an attempt to
     * "double release" a buffer (only currently acquired buffers can be
     * released).
     * 
     * @throws InterruptedException
     */
    public void test_doubleRelease() throws InterruptedException {

        ByteBuffer b = null;
        try {
            b = DirectBufferPool.INSTANCE.acquire();
        } finally {
            if (b != null)
                DirectBufferPool.INSTANCE.release(b);
        }

        if (b != null) {
            try {
                // Attempt to double-release the buffer.
                DirectBufferPool.INSTANCE.release(b);
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }
        }

    }

}
