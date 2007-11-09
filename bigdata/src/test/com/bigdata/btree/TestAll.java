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
package com.bigdata.btree;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
 *
 * FIXME write a test that verifies that spurious errors are not reported
 * when the log level for the com.bigdata.btree package is DEBUG.
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
     */
    public static Test suite()
    {

        TestSuite suite = new TestSuite("BTree");
        
        /*
         * test key encoding and comparison support.
         */
        // test methods that compute the successor for various data types.
        suite.addTestSuite( TestSuccessorUtil.class );
        // test low level variable length byte[] operations.
        suite.addTestSuite( TestBytesUtil.class );
        // test key encoding operations.
        suite.addTestSuite(TestKeyBuilder.class);
        suite.addTestSuite(TestUnicodeKeyBuilder.class);
        // test mutable key buffer.
        suite.addTestSuite(TestMutableKeyBuffer.class);
        // test immutable key buffer.
        suite.addTestSuite(TestImmutableKeyBuffer.class);
        // test key search routines on the key buffer implementations.
        suite.addTestSuite( TestKeyBufferSearch.class );
        // test key buffer (de-)serialization.
        suite.addTestSuite( TestKeyBufferSerializer.class );
        
        /*
         * test value serializers and compression methods.
         */
        // test serialization of byte[] values.
        suite.addTestSuite( TestByteArrayValueSerializer.class );
        
        /*
         * test record compression support.
         */
        // test bulk data compression.
        suite.addTestSuite( TestRecordCompressor.class );

        /*
         * test btree fundementals.
         */
        // test static and instance utility methods on AbstractNode and ArrayType.
        suite.addTestSuite( TestUtilMethods.class );
        // test assertions that test for node/leaf invariants.
        suite.addTestSuite( TestInvariants.class );
        // test finding a child of a node by its key.
        suite.addTestSuite( TestFindChild.class );
        // test insert, lookup, and remove for root leaf w/o splitting it.
        suite.addTestSuite( TestInsertLookupRemoveKeysInRootLeaf.class );
        // test insert, lookup, and remove for root leaf w/o splitting it using the batch api.
        suite.addTestSuite( TestInsertLookupRemoveOnRootLeafWithBatchApi.class );
        // test splitting the root leaf.
        suite.addTestSuite( TestSplitRootLeaf.class );
        // test splitting and joining the root leaf (no more than two levels).
        suite.addTestSuite( TestSplitJoinRootLeaf.class );
        // test splitting and joining with more than two levels.
        suite.addTestSuite( TestSplitJoinThreeLevels.class );
        // test indexOf, keyAt, valueAt.
        suite.addTestSuite( TestLinearListMethods.class );
        // test getCounter()
        suite.addTestSuite( TestIndexCounter.class );
        // test iterator semantics.
        suite.addTestSuite( TestIterators.class );
        // test delete semantics (also see the isolation package).
        suite.addTestSuite( TestRemoveAll.class );
        // test contract for BTree#touch(node) w/o IO.
        suite.addTestSuite( TestTouch.class );
        // stress test basic tree operations w/o IO.
        suite.addTestSuite( TestBTree.class );
        // test fused view operations on ordered list of trees.
        suite.addTestSuite( TestFusedView.class );
        // test of user-defined functions.
        suite.addTestSuite( TestUserDefinedFunction.class );
        // test child address serialization.
        suite.addTestSuite( TestAddressSerializer.class );
        suite.addTestSuite( TestPackedAddressSerializer.class );
        // test node/leaf serialization.
        suite.addTestSuite( TestNodeSerializer.class );
        // test iterator semantics for visiting only "dirty" nodes or leaves.
        suite.addTestSuite( TestDirtyIterators.class );
        // test incremental write of leaves and nodes.
        suite.addTestSuite( TestIncrementalWrite.class );
        // test copy-on-write scenarios.
        suite.addTestSuite( TestCopyOnWrite.class );
        
        /*
         * test persistence protocols. 
         */
        // test the commit protocol.
        suite.addTestSuite( TestCommit.class );
        // test the dirty event protocol.
        suite.addTestSuite(TestDirtyListener.class);
        // test the close/reopen protocol for releasing index buffers.
        suite.addTestSuite( TestReopen.class );

        /*
         * index rebuilding.
         */
        // test static methods for the index builder.
        suite.addTestSuite( TestIndexSegmentPlan.class );
        // test encoding and decoding of child node/leaf addresses.
        suite.addTestSuite( TestIndexSegmentAddressSerializer.class );
        // test write and read back of the index segment metadata record.
        suite.addTestSuite( TestIndexSegmentMetadata.class );
        // test with small known examples in detail.
        suite.addTestSuite( TestIndexSegmentBuilderWithSmallTree.class );
        // stress test with larger random input trees and a variety of branching factors.
        suite.addTestSuite( TestIndexSegmentBuilderWithLargeTrees.class );
        // test of the bloom filter integration.
        suite.addTestSuite( TestIndexSegmentWithBloomFilter.class );
        // test of the fast forward and reverse leaf scans.
        suite.addTestSuite( TestIndexSegmentFastLeafScan.class );
        // test compacting merge of two index segments.
        suite.addTestSuite( TestIndexSegmentMerger.class );
        
        return suite;
        
    }
    
}
