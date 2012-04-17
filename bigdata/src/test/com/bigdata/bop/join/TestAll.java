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
package com.bigdata.bop.join;

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

        final TestSuite suite = new TestSuite("join operators");

        // Test suite for pipeline join.
        suite.addTestSuite(TestPipelineJoin.class);

        // Test suite for the guts of the JVM hash join logic.
        suite.addTestSuite(TestJVMHashJoinUtility.class);

        // Test suite for the guts of the HTree hash join logic.
        suite.addTestSuite(TestHTreeHashJoinUtility.class);
        
        // Test suite for a hash join with an access path.
        suite.addTestSuite(TestJVMHashJoinOp.class); // JVM
        suite.addTestSuite(TestHTreeHashJoinOp.class); // HTree
        
        // Test suite for building a hash index from solutions and joining that
        // hash index back into the pipeline.
        suite.addTestSuite(TestHTreeHashIndexOp.class);
        suite.addTestSuite(TestHTreeSolutionSetHashJoin.class);
        
		/*
		 * Test suite for join against a named solution set when the left
		 * cardinality is low.
		 */
		suite.addTestSuite(TestNamedSolutionSetScanOp.class);

        return suite;
        
    }
    
}
