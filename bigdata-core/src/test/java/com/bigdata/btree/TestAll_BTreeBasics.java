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
 * Created on Jan 31, 2009
 */

package com.bigdata.btree;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Aggregates the unit tests for the core B+Tree operations, all of which are in
 * the same package as the {@link BTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
       /*
        * test btree fundamentals.
        */
       // test static and instance utility methods on AbstractNode and ArrayType.
       TestUtilMethods.class,
       // test finding a child of a node by its key.
       TestFindChild.class,
       // test insert, lookup, and remove for root leaf w/o splitting it.
       TestInsertLookupRemoveKeysInRootLeaf.class,
       // test splitting the root leaf.
       TestSplitRootLeaf.class,
       // test splitting and joining the root leaf (no more than two levels).
       TestSplitJoinRootLeaf.class,
       // test splitting and joining with more than two levels.
       TestSplitJoinThreeLevels.class,
       // test edge cases in finding the shortest separator key for a leaf.
       TestLeafSplitShortestSeparatorKey.class,
       // test indexOf, keyAt, valueAt.
       TestLinearListMethods.class,
       // test getCounter()
       TestIndexCounter.class,

       // test imposing constraint on a fromKey or toKey based on an index
       // partition's boundaries.
       TestConstrainKeys.class,

       // test iterator semantics.
       TestAll_Iterators.class,

       // test delete semantics (also see the isolation package).
       TestRemoveAll.class,
       // test contract for BTree#touch(node) w/o IO.
       TestTouch.class,
       // stress test basic tree operations w/o IO.
       TestBTree.class,
       // test node/leaf serialization.
       //TestNodeSerializer.class,

       // test iterator semantics for visiting only "dirty" nodes or leaves.
       TestDirtyIterators.class,

       // test incremental write of leaves and nodes.
       TestIncrementalWrite.class,
       // test copy-on-write scenarios.
       TestCopyOnWrite.class,

       /*
        * test with delete markers.
        *
        * Note: tests with timestamps and delete markers are done in the
        * isolation package.
        *
        * FIXME We should verify correct maintenance of the min/max and per
        * tuple version timestamps here. The raba coder tests already verify
        * correct coding and decoding IFF the data are being correctly
        * maintained.
        */
       TestDeleteMarkers.class,

       /*
        * test persistence protocols.
        */
       // test the commit protocol.
       TestCommit.class,
       // test the dirty event protocol.
       TestDirtyListener.class,
       // test the close/reopen protocol for releasing index buffers.
       TestReopen.class,
       // test of storing null values under a key with persistence.
       TestNullValues.class,

       /*
        * test of transient BTree's (no backing store).
        */
       TestTransientBTree.class,

       /*
        * Test bloom filters for a BTree (vs an IndexSegment, which is handled
        * below).
        */
       TestBloomFilter.class,
       TestBTreeWithBloomFilter.class
        } )
public class TestAll_BTreeBasics {

    public TestAll_BTreeBasics() {
    }
}
