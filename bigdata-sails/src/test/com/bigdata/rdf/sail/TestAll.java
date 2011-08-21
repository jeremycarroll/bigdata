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

package com.bigdata.rdf.sail;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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

        /*
         * log4j defaults to DEBUG which will produce simply huge amounts of
         * logging information when running the unit tests. Therefore we
         * explicitly set the default logging level to WARN unless it has
         * already been set to another value. If you are using a log4j
         * configuration file then this is unlikely to interact with your
         * configuration, and in any case you can override specific loggers.
         */
        {

            Logger log = Logger.getRootLogger();

            if (log.getLevel().equals(Level.DEBUG)) {

                log.setLevel(Level.WARN);

                log.warn("Defaulting debugging level to WARN for the unit tests");

            }
            
        }
        
        final TestSuite suite = new TestSuite("Sesame 2.x integration");

        suite.addTest(com.bigdata.rdf.sail.sparql.TestAll.suite());
        
        // test suite for extracting query hints from a SPARQL query.
        suite.addTestSuite(TestQueryHints.class);
     
        // test suite for utility to extract the type of a SPARQL query.
        suite.addTestSuite(TestQueryType.class);

        // bootstrap tests for the BigdataSail
        suite.addTestSuite(TestBootstrapBigdataSail.class);

        // run the test suite with statement identifiers enabled.
        suite.addTest(TestBigdataSailWithSids.suite());
        
        // run the test suite without statement identifiers enabled.
        suite.addTest(TestBigdataSailWithoutSids.suite());
        
        // quad store test suite w/ pipeline joins.
        suite.addTest(TestBigdataSailWithQuads.suite());

        // NanoSparqlServer
        suite.addTest(com.bigdata.rdf.sail.webapp.TestAll.suite());

        // quad store in scale-out.
        suite.addTest(TestBigdataSailEmbeddedFederationWithQuads.suite());
        
        return suite;

    }

}
