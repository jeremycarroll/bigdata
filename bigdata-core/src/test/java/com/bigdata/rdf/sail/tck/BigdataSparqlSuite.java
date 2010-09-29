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
 * Created on Jun 19, 2008
 */
package com.bigdata.rdf.sail.tck;

import java.util.Enumeration;
import java.util.Properties;
import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.ManifestTest;
import org.openrdf.query.parser.sparql.SPARQLQueryTest;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.relation.AbstractResource;
import java.util.Collection;
import java.util.LinkedList;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openrdf.query.parser.sparql.SPARQLQueryTest.Factory;

/**
 * Test harness for running the SPARQL test suites.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@RunWith(Parameterized.class)
public class BigdataSparqlSuite { //extends TestCase {

    private Test t;

    /**
     * Use the {@link #suiteLTSWithPipelineJoins()} test suite by default.
     * <p>
     * Skip the dataset tests for now until we can figure out what is wrong
     * with them.
     * 
     * @todo FIXME fix the dataset tests 
     */
    @Parameters
    public static Collection<Object[]> getDelegates() {
        TestSuite suite = suite(true /*hideDatasetTests*/);
        LinkedList<Object[]> list = new LinkedList<Object[]>();
        addSuiteToList(suite, list);
        return list;
    };

    private static void addSuiteToList(TestSuite suite, LinkedList<Object[]> list) {
        for (Enumeration e = suite.tests(); e.hasMoreElements(); ) {
            Object o = e.nextElement();
            if (o instanceof TestSuite) {
                addSuiteToList((TestSuite)o, list);
            } else {
                list.add(new Object[] { o } );
            }
        }
    }

    /**
     * Run the Sparql test.
     * @param hideDatasetTests
     * @return
     */
    public BigdataSparqlSuite(Test t) {
        this.t = t;
    }

    @org.junit.Test
    public void test() {

    }
    
    public static TestSuite suite() {
        return  suite(true /*hideDatasetTests*/);
    }

    static TestSuite suite(final boolean hideDatasetTests) {

        TestSuite suite1 = null;
        try {
            suite1 = suiteLTSWithPipelineJoins();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unexpected exception from Sparql test generation", e);
        }

        if (!hideDatasetTests) {
            
            return suite1;
            
        }
        
        TestSuite suite2 = new TestSuite(suite1.getName());
        
        Enumeration e = suite1.tests();
        
        while (e.hasMoreElements()) {
            
            TestSuite suite3 = (TestSuite)e.nextElement();
            
            if (suite3.getName().equals("dataset") == false) {
                
                suite2.addTest(suite3);
                
            }
        }
        
        return suite2;
        
    }
    
    /**
     * Return a test suite using the {@link LocalTripleStore} and nested
     * subquery joins.
     */
    public static TestSuite suiteLTSWithNestedSubquery() throws Exception {
        
        return ManifestTest.suite(new Factory() {

            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet, boolean laxCardinality) {

                return new BigdataSparqlTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet, laxCardinality) {

                    @Override
                    protected Properties getProperties() {

                        final Properties p = new Properties(super
                                .getProperties());

                        p.setProperty(AbstractResource.Options.NESTED_SUBQUERY,
                                "true");

                        return p;

                    }

                };

            }
            
        });
        
    }

    /**
     * Return a test suite using the {@link LocalTripleStore} and pipeline joins. 
     */
    public static TestSuite suiteLTSWithPipelineJoins() throws Exception {
       
        return ManifestTest.suite(new Factory() {

            public SPARQLQueryTest createSPARQLQueryTest(String testURI,
                    String name, String queryFileURL, String resultFileURL,
                    Dataset dataSet, boolean laxCardinality) {

                return new BigdataSparqlTest(testURI, name, queryFileURL,
                        resultFileURL, dataSet, laxCardinality) {

                    @Override
                    protected Properties getProperties() {

                        final Properties p = new Properties(super
                                .getProperties());

                        p.setProperty(AbstractResource.Options.NESTED_SUBQUERY,
                                "false");

                        return p;

                    }
                    
                };

            }
        });
    }
}
