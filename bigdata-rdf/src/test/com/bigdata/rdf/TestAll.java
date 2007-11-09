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
package com.bigdata.rdf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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

        /*
         * log4j defaults to DEBUG which will produce simply huge amounts of
         * logging information when running the unit tests. Therefore we
         * explicitly set the default logging level to WARN. If you are using a
         * log4j configuration file then this is unlikely to interact with your
         * configuration, and in any case you can override specific loggers.
         */
        {

            Logger log = Logger.getRootLogger();

            if (log.getLevel().equals(Level.DEBUG)) {

                log.setLevel(Level.WARN);

                log.warn("Defaulting debugging level to WARN for the unit tests");

            }
            
        }
        
        TestSuite suite = new TestSuite("RDF");

        // test utility classes.
        suite.addTest( com.bigdata.rdf.util.TestAll.suite() );
     
        // test RDF Value and Statement object model (Sesame compliant).
        suite.addTest( com.bigdata.rdf.model.TestAll.suite() );

        // test low-level statement model using long term identifiers.
        suite.addTest( com.bigdata.rdf.spo.TestAll.suite() );

        // test various RDF database implementations.
        suite.addTest( com.bigdata.rdf.store.TestAll.suite() );

        if (Boolean.parseBoolean(System.getProperty("maven.test.services.skip",
                "false"))) {

            /*
             * Test scale-out RDF database.
             * 
             * Note: This test suite sets up a local bigdata federation for each
             * test. See the test suite for more information about required Java
             * properties.
             */

            suite.addTest(com.bigdata.rdf.scaleout.TestAll.suite());

        }

        return suite;
        
    }
    
}
