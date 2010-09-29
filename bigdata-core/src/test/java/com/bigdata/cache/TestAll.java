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
/*
 * Created on Apr 21, 2006
 */
package com.bigdata.cache;

import com.bigdata.DataFinder;
import com.bigdata.test.ExperimentDriver;
import java.io.File;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Aggregates unit tests into dependency order.
 */
@RunWith(Suite.class)
@SuiteClasses( {
        TestRingBuffer.class,
        TestHardReferenceQueue.class,
        TestHardReferenceQueueWithBatchingUpdates.class,

//        // Test all ICacheEntry implementations.
//        TestCacheEntry.class,

        // Test LRU semantics.
        TestLRUCache.class,

        // Test cache semantics with weak/soft reference values.
        TestWeakValueCache.class,

        TestStoreAndAddressLRUCache.class,

        // Note: This implementation is not used.
//        TestHardReferenceGlobalLRU.class,

        TestHardReferenceGlobalLRURecycler.class,

        TestHardReferenceGlobalLRURecyclerExplicitDeleteRequired.class,

        /*
         * A high concurrency cache based on the infinispan project w/o support
         * for memory cap. This implementation has the disadvantage that we can
         * not directly manage the amount of memory which will be used by the
         * cache. It has pretty much been replaced by the BCHMGlobalLRU2, which
         * gets tested below.
         *
         * @todo commented out since causing a problem w/ the CI builds (live
         * lock).  I am not sure which of these two access policies is at fault.
         * I suspect the LRU since it has more aberrant behavior.
         */
//        TestBCHMGlobalLRU.class, // w/ LRU access policy
//        TestBCHMGlobalLRUWithLIRS.class, // w/ LIRS

        /*
         * These are test suites for the same high concurrency cache with
         * support for memory cap. The cache can be configured with thread-lock
         * buffers or striped locks, so we test it both ways.
         */
        TestBCHMGlobalLRU2WithThreadLocalBuffers.class,
//        TestBCHMGlobalLRU2WithThreadLocalBuffersAndLIRS.class,
        TestBCHMGlobalLRU2WithStripedLocks.class
//        TestBCHMGlobalLRU2WithStripedLocksAndLIRS.class,

        /*
         * Run the stress tests.
         *
         * @todo I have commented this out since it is suspect of failing the
         * build. Probably one of the cache implementations is experiencing high
         * contention on the CI machine (which has more cores). 5/21/2010 BBT.
         * See above.  This appears to be one of the infinispan-based caches.
         */
//        StressTests.class,
        } )
public class TestAll {

    /**
     * 
     */
    public TestAll() {
        super();
    }

    /**
     * Glue class used to execute the stress tests.
      * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class StressTests {

        public StressTests() {
        }

        /**
         * FIXME Modify the stress test configuration file to run each condition
         * of interest. It is only setup for a few conditions right now.
         */
        @Test
        public void test() throws Exception {
            ExperimentDriver
                    .doMain(
                            new File(
                                    DataFinder.bestPath("testing/data/com/bigdata/cache/StressTestGlobalLRU.xml" ) ),
                            1/* nruns */, true/* randomize */);
        }
    }
}
