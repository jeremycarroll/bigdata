package com.bigdata.objndx;

import com.bigdata.util.TestChecksumUtility;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
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
        // test mutable key buffer.
        suite.addTestSuite(TestMutableKeyBuffer.class);
        // test immutable key buffer.
        suite.addTestSuite(TestImmutableKeyBuffer.class);
        // test key search routines on the key buffer implementations.
        suite.addTestSuite( TestKeyBufferSearch.class );
        // test key buffer (de-)serialization.
        suite.addTestSuite( TestKeyBufferSerializer.class );
        
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
        // test iterator semantics.
        suite.addTestSuite( TestIterators.class );
        // test contract for BTree#touch(node) w/o IO.
        suite.addTestSuite( TestTouch.class );
        // stress test basic tree operations w/o IO.
        suite.addTestSuite( TestBTree.class );
        // test fused view operations on ordered list of trees.
        suite.addTestSuite( TestFusedView.class );
        // test of user-defined functions.
        suite.addTestSuite( TestUserDefinedFunction.class );
        // test node/leaf serialization.
        suite.addTestSuite( TestNodeSerializer.class );
        // test iterator semantics for visiting only "dirty" nodes or leaves.
        suite.addTestSuite( TestDirtyIterators.class );
        // test incremental write of leaves and nodes.
        suite.addTestSuite( TestIncrementalWrite.class );
        // test copy-on-write scenarios.
        suite.addTestSuite( TestCopyOnWrite.class );
        // stress test using journal as the backing store.
        suite.addTestSuite( TestBTreeWithJournal.class );
        
        /*
         * test atomic commit
         * 
         * @todo test btree may be reloaded from its metadata record.
         * 
         * @todo that failure to commit results in effective rollback of
         * btree(s) on a journal (writes are ignored since the btree metadata
         * index is not updated).
         * 
         * @todo test atomic commit of btree(s) on a journal.
         * 
         * @todo test journal restart semantics w/o shutdown (uncomitted changes
         * are ignored).
         * 
         * @todo test journal transaction isolation using the new object index.
         */
        // test the commit protocol. @todo expand tests.
        suite.addTestSuite( TestCommit.class );
        // verify that a store may be re-opened and the data in the btree(s) is preserved.
        suite.addTestSuite( TestRestartSafe.class );

        /*
         * use of btree to support transactional isolation.
         *
         * @todo verify that null is allowed to represent a delted value.
         * 
         * @todo test of double-delete.
         * 
         * @todo test as simple object store (persistent identifiers) by
         * refactoring the journal test suites.
         * 
         * @todo test on partitioned index.
         */

        /*
         * use of btree to support column store.
         * 
         * @todo handle column names as part of the key?
         * 
         * @todo test version expiration based on age
         * 
         * @todo test version expiration based on #of versions.
         * 
         * @todo test on paritioned index. 
         */
        
        /*
         * index rebuilding.
         */
        // test static methods for the index builder.
        suite.addTestSuite( TestIndexSegmentPlan.class );
        // test encoding and decoding of child node/leaf addresses.
        suite.addTestSuite( TestIndexSegmentAddressSerializer.class );
        // test with small known examples in detail.
        suite.addTestSuite( TestIndexSegmentBuilderWithSmallTree.class );
        // stress test with larger random input trees and a variety of branching factors.
        suite.addTestSuite( TestIndexSegmentBuilderWithLargeTrees.class );
        // test of the bloom filter integration.
        suite.addTestSuite( TestIndexSegmentWithBloomFilter.class );
        // @todo test compacting merge of two index segments.
        suite.addTestSuite( TestIndexSegmentMerger.class );
        // @todo test merge that results in enough data to warrent a split.

        /*
         * journal overflow
         * 
         * @todo test overflow triggers near journal capacity
         * 
         * @todo test overflow will abort transactions if necessary, e.g., after
         * a grace period and possible journal extension.
         */
        
        /*
         * FIXME partitioned indices.
         * 
         * @todo test leaf search rule. The leaf search rule for the index
         * partitions is the first entry having a key less than or equal to the
         * search key. This search rule lets us locate the corresponding
         * partition much as the node search rule lets us direct search to
         * correct node or leaf in the the next level down of the btree.
         * 
         * @todo test overflow resulting in parition merge or split.
         * 
         * @todo test DistributedBTree (reads through to active index segments
         * if miss on BTree in the journal). there is a lot to test here
         * including all of the transactional semantics.
         * 
         * @todo test metadata management for index segments.
         */
       
        return suite;
        
    }
    
}
