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
 * Created on Nov 7, 2007
 */

package com.bigdata.rdf.sail.sparql;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
        super();
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    public static Test suite() {

        final TestSuite suite = new TestSuite(TestAll.class.getPackage()
                .getName());

        // Test suite for building up value expressions.
        suite.addTestSuite(TestValueExprBuilder.class);

        /*
         * Test suite for building up triple patterns, including those which are
         * covered by the property paths extension in SPARQL 1.1 (a triple
         * pattern which a constant in the predicate position is treated by the
         * sesame SPARQL grammar as a degenerate case of a property path.)
         */
        suite.addTestSuite(TestTriplePatternBuilder.class);
        
        /*
         * Test suite for group graph patterns (join groups, unions, optional,
         * etc.) and filters in those graph patterns. Subquery is tested in a
         * separate test suite.
         */
        suite.addTestSuite(TestGroupGraphPatternBuilder.class);

        /*
         * Test suite for various kinds of subquery patterns.
         */
        suite.addTestSuite(TestSubqueryPatterns.class);
        
        /*
         * Test suite for basic query types (SELECT|ASK|DESCRIBE|CONSTRUCT),
         * including DISTINCT/REDUCED, GROUP BY, HAVING, ORDER BY, and
         * LIMIT/OFFSET.
         */
        suite.addTestSuite(TestBigdataExprBuilder.class);

        // TODO Test suite for property paths.

        return suite;

    }

}