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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Aggregates test suites into increasing dependency order.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Suite.class)
@SuiteClasses( {
       // unsigned byte[] key encoding and decoding.
       com.bigdata.btree.keys.TestAll.class,
       // test rabas implementations, including key search and coded data.
       com.bigdata.btree.raba.TestAll.class,
       // test suite for the B+Tree node and leaf data records.
       com.bigdata.btree.data.TestAll.class,
       // core B+Tree API tests, including w/ and w/o persistence.
       TestAll_BTreeBasics.class,
       // pick up the index segment test suites.
       TestAll_IndexSegment.class,
       // Test the Map and Set implementations.
       TestBigdataMap.class,
       TestBigdataSet.class,
       // Test fused views, including iterators for the fused view.
       com.bigdata.btree.view.TestAll.class,
       // Test transactional isolation support, including iterators and
       // iterator#remove() for the isolated index.
       com.bigdata.btree.isolation.TestAll.class,
       // test index procedures.
       com.bigdata.btree.proc.TestAll.class
        } )
public class TestAll {

    /**
     * 
     */
    public TestAll() {
    }
}
