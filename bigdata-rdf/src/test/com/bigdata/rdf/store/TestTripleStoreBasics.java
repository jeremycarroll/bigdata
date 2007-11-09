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
 * Created on Oct 18, 2007
 */

package com.bigdata.rdf.store;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase2;
import junit.framework.TestSuite;


/**
 * Aggregates test that are run for each {@link ITripleStore} implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTripleStoreBasics extends TestCase2 {

    /**
     * Aggregates the test suites into something approximating increasing
     * dependency. This is designed to run as a <em>proxy test suite</em> in
     * which all tests are run using a common configuration and a delegatation
     * mechanism. You MUST add the returned {@link Test} into a properly
     * configured {@link ProxyTestSuite}.
     * 
     * @see ProxyTestSuite
     */
    public static Test suite() {

        TestSuite suite = new TestSuite("Triple store basics");

        // test adding terms and statements.
        suite.addTestSuite(TestTripleStore.class);

        // test adding terms and statements is restart safe.
        suite.addTestSuite(TestRestartSafe.class);

        // test suite for the access path api.
        suite.addTestSuite(TestAccessPath.class);

        // somewhat dated test of sustained insert rate on synthetic data.
        suite.addTestSuite(TestInsertRate.class);

        // test suite for the rio parser and data loading integration.
        suite.addTest(com.bigdata.rdf.rio.TestAll.suite());

        // test suite for the inference engine.
        suite.addTest(com.bigdata.rdf.inf.TestAll.suite());

        return suite;
        
    }
    
}
