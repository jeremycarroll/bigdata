/*
 * Copyright SYSTAP, LLC 2006-2008.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Sep 4, 2008
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail.Options;

/**
 * Test suite for the {@link BigdataSail} with quads enabled. The provenance
 * mode is disabled. Inference is disabled.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBigdataSailWithQuads extends AbstractBigdataSailTestCase {

    /**
     * 
     */
    public TestBigdataSailWithQuads() {
    }

    public TestBigdataSailWithQuads(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        final TestBigdataSailWithQuads delegate = new TestBigdataSailWithQuads(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate, "SAIL with Quads");

        // misc named graph API stuff.
        suite.addTestSuite(TestQuadsAPI.class);
        
        // SPARQL named graphs tests.
        suite.addTestSuite(TestNamedGraphs.class);
        
        // test of the search magic predicate
        suite.addTestSuite(TestSearchQuery.class);
        
        // high-level query tests.
        suite.addTestSuite(TestQuery.class);

        // test of high-level query on a graph with statements about statements.
        suite.addTestSuite(TestProvenanceQuery.class);

        // unit tests for custom evaluation of high-level query
        suite.addTestSuite(TestBigdataSailEvaluationStrategyImpl.class);

        // The Sesame TCK, including the SPARQL test suite.
        suite.addTest(com.bigdata.rdf.sail.tck.TestAll.suite());
        
        return suite;
        
    }
    
    @Override
    protected BigdataSail getSail(final Properties properties) {
        
        return new BigdataSail(properties);
        
    }

    public Properties getProperties() {

        final Properties properties = new Properties(super.getProperties());

        properties.setProperty(Options.STATEMENT_IDENTIFIERS, "false");

        properties.setProperty(Options.QUADS, "true");

        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());

        return properties;
        
    }
    
    @Override
    protected BigdataSail reopenSail(BigdataSail sail) {

        final Properties properties = sail.database.getProperties();

        if (sail.isOpen()) {

            try {

                sail.shutDown();

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }
        
        return getSail(properties);
        
    }

}
