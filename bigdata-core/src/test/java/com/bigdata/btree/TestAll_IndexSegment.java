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
 * Aggregates the unit tests for the {@link IndexSegment} and its related
 * classes, all of which are in the same package as the {@link BTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
        //basic unit tests for IndexSegment.
        TestIndexSegment.class,
       // test static methods for the index builder.
       TestIndexSegmentPlan.class,
       // test encoding and decoding of child node/leaf addresses.
       TestIndexSegmentAddressManager.class,
       // test write and read back of the index segment metadata record.
       TestIndexSegmentCheckpoint.class,
       // test with small known examples in detail.
       TestIndexSegmentBuilderWithSmallTree.class,
       // test fence posts for incremental builds with deleted index entries.
       TestIndexSegmentBuilderWithIncrementalBuild.class,
       // test fence posts for compacting merges with deleted index entries.
       TestIndexSegmentBuilderWithCompactingMerge.class,
       // test multi-block iterators for the index segment.
       TestIndexSegmentMultiBlockIterators.class,
       // test iterators for the index segment.
       TestIndexSegmentCursors.class,
       // stress test with larger random input trees and a variety of branching
       // factors.
       TestIndexSegmentBuilderWithLargeTrees.class,
       // test of the bloom filter integration.
       TestIndexSegmentWithBloomFilter.class
        } )
public class TestAll_IndexSegment {

    public TestAll_IndexSegment() {
    }
}
