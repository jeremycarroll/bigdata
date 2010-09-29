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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Test suite for embedded services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
        // event handling
        TestEventParser.class,
        TestEventReceiver.class,

        // tests of the round-robin aspects of the LBS (isolated behaviors).
        TestLoadBalancerRoundRobin.class,

        // test utility to read index segments using NIO.
        TestResourceService.class,

        // tests of the metadata index.
        TestMetadataIndex.class,
        TestMetadataIndexRemote.class,

        // tests of the client's view of a scale-out index.
        com.bigdata.service.ndx.TestAll.class,

        // test ability to re-open an embedded federation.
        TestRestartSafe.class,
        TestRestartSafeRemote.class,

        // unit tests for the distributed transaction service's snapshots.
        TestSnapshotHelper.class,

        // unit tests of the commit time index for the dist. transaction service.
        TestDistributedTransactionServiceRestart.class,

        // unit tests of single-phase and distributed tx commit protocol.
        TestDistributedTransactionService.class,
        TestDistributedTransactionServiceRemote.class,

        // test basic journal overflow scenario.
        TestOverflow.class,
        TestOverflowRemote.class,

        // test split/join (inserts eventually split; deletes eventually join).
        TestSplitJoin.class,
        TestSplitJoinRemote.class,

        // test scatter splits with 2DS.
        TestScatterSplit.class,
        TestScatterSplitRemote.class,

        // test journal overflow scenarios (move)
        TestMove.class,
        TestMoveRemote.class,

        /*
         * Stress test of concurrent clients writing on a single data service.
         */
        StressTestConcurrent.class,
        StressTestConcurrentRemote.class
        } )
public class TestAll {

    /**
     * 
     */
    public TestAll() {
        
    }
}
