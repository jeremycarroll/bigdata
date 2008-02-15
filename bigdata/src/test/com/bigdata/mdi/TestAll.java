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
package com.bigdata.mdi;

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

        TestSuite suite = new TestSuite("scaleup");

        /*
         * journal overflow
         * 
         * FIXME write unit tests (or refactor the old ones to run).
         * 
         * @todo make sure that the index counters are correctly propagated to
         * the new btree on overflow.
         * 
         * @todo look at overflow handling for for the sparse row store vs an
         * index with just delete markers vs an index with transactional
         * isolation. how history is retained and purged is different for each
         * of these cases.
         * 
         * @todo test overflow when fused view must read from an old journal
         * resource.
         * 
         * @todo test correct eventual delete of old resources (journal and
         * index segments) based on the kind of index (sparse, scale-out,
         * scale-out isolated).
         * 
         * @todo test overflow triggers near journal capacity
         * 
         * @todo test overflow will abort transactions if necessary, e.g., after
         * a grace period and possible journal extension.
         */
        
        /*
         * management of partitioned indices.
         * 
         * @todo test overflow resulting in parition merge or split.
         * 
         * @todo test metadata management for index segments.
         */
//        suite.addTestSuite(TestMetadataIndex.class);
//        suite.addTestSuite(TestPartitionedIndex.class);
//        suite.addTestSuite(TestPartitionedJournal.class);
       
        return suite;
        
    }
    
}
