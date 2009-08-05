/*

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
 * Created on Nov 29, 2007
 */

package com.bigdata.service;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Test suite for embedded services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
        
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {

        super(arg0);
        
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     * 
     * FIXME Make the unit tests in this package into proxy unit tests and run
     * all of the tests in this suite against both the
     * {@link LocalDataServiceClient} and the {@link EmbeddedClient}. Ideally
     * the jini module can then re-run the same unit tests.
     */
    public static Test suite() {

        final TestSuite suite = new TestSuite("bigdata services");

        // event handling
        suite.addTestSuite(TestEventParser.class);
        suite.addTestSuite(TestEventReceiver.class);
        
        // tests of the round-robin aspects of the LBS (isolated behaviors).
        suite.addTestSuite(TestLoadBalancerRoundRobin.class);

        // test utility to read index segments using NIO.
        suite.addTestSuite(TestResourceService.class);
        
        // tests of the metadata index.
        suite.addTestSuite(TestMetadataIndex.class);

        // client basics, including static partitioning of indices.
        suite.addTestSuite(TestEmbeddedClient.class);

        // test basic index operations.
        suite.addTestSuite(TestBasicIndexStuff.class);

        // test range iterators (within and across index partitions).
        suite.addTestSuite(TestRangeQuery.class);

        // test ability to re-open an embedded federation.
        suite.addTestSuite(TestRestartSafe.class);

        // unit tests for the streaming index write API.
        suite.addTest(com.bigdata.service.ndx.TestAll.suite());

        // unit tests for the distributed transaction service's snapshots.
        suite.addTestSuite(TestSnapshotHelper.class);

        // unit tests of the commit time index for the dist. transaction service.
        suite.addTestSuite(TestDistributedTransactionServiceRestart.class);
        
        // unit tests of single-phase and distributed tx commit protocol.
        suite.addTestSuite(TestDistributedTransactionService.class);
        
        // test journal overflow scenarios (split/join).
        suite.addTestSuite( TestOverflow.class );
        
        // test journal overflow scenarios (move) 
        suite.addTestSuite( TestIndexPartitionMove.class );
        
        /*
         * Stress test of concurrent clients writing on a single data service.
         */
        suite.addTestSuite( StressTestConcurrent.class );

        return suite;
        
    }
    
}
