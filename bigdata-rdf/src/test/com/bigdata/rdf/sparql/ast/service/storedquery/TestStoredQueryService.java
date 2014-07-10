/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Sep 4, 2011
 */
package com.bigdata.rdf.sparql.ast.service.storedquery;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;

/**
 * Test suite for stored query evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/989">Stored Query Service</a>
 */
public class TestStoredQueryService extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestStoredQueryService() {
    }

    /**
     * @param name
     */
    public TestStoredQueryService(final String name) {
        super(name);
    }

    /**
     * Simple stored query test. Note that this test also verifies that the
     * BINDINGS flow into the stored query.
     * 
     * <pre>
     * PREFIX bsq:  <http://www.bigdata.com/rdf/stored-query#>
     * PREFIX : <http://example.org/book/>
     * 
     * SELECT ?book ?title ?price
     * {
     *    SERVICE <http://www.bigdata.com/rdf/stored-query#test_stored_query_001> {
     *    }
     * } 
     * BINDINGS ?book {
     *       (:book1)
     * }
     * </pre>
     * 
     * @throws Exception
     */
    public void test_stored_query_001() throws Exception {
        
        class MyStoredQueryService extends StoredQueryService {

            @Override
            public String getQuery(final ServiceCallCreateParams createParams,
                    final ServiceParams serviceParams) {
                final StringBuilder sb = new StringBuilder();
                sb.append("PREFIX dc:   <http://purl.org/dc/elements/1.1/> \n");
                sb.append("PREFIX :     <http://example.org/book/> \n");
                sb.append("PREFIX ns:   <http://example.org/ns#> \n");
                sb.append("SELECT ?book ?title ?price { \n");
                sb.append("  ?book dc:title ?title ; \n");
                sb.append("  ns:price ?price . \n");
                sb.append("} \n");
                return sb.toString();
            }
            
        }
        
        final URI serviceURI = new URIImpl(StoredQueryService.Options.NAMESPACE + getName());
        try {

            // register the service.
            ServiceRegistry.getInstance().add(serviceURI,
                    new MyStoredQueryService());

            final TestHelper h = new TestHelper("stored-query-001", // testURI,
                    "stored-query-001.rq",// queryFileURL
                    "stored-query-001.ttl",// dataFileURL
                    "stored-query-001.srx" // resultFileURL,
                    // false, // laxCardinality
                    // true // checkOrder
            );

            h.runTest();

        } finally {
            // unregister the service.
            ServiceRegistry.getInstance().remove(serviceURI);
        }

    }

    /**
     * Simple stored query test verifies that the stored query has access to the
     * {@link ServiceParams}.
     * 
     * <pre>
     * PREFIX bsq:  <http://www.bigdata.com/rdf/stored-query#>
     * PREFIX : <http://example.org/book/>
     * 
     * SELECT ?book ?title ?price
     * {
     *    SERVICE <http://www.bigdata.com/rdf/stored-query#test_stored_query_002> {
     *        bd:serviceParam :book :book1
     *    }
     * } 
     * </pre>
     * 
     * @throws Exception
     */
    public void test_stored_query_002() throws Exception {
        
        class MyStoredQueryService extends StoredQueryService {

            @Override
            public String getQuery(final ServiceCallCreateParams createParams,
                    final ServiceParams serviceParams) {
                
                final URI val = serviceParams.getAsURI(new URIImpl(
                        "http://example.org/book/book"));

                final StringBuilder sb = new StringBuilder();
                sb.append("PREFIX dc:   <http://purl.org/dc/elements/1.1/> \n");
                sb.append("PREFIX :     <http://example.org/book/> \n");
                sb.append("PREFIX ns:   <http://example.org/ns#> \n");
                sb.append("SELECT ?book ?title ?price { \n");
                sb.append("  BIND( <"+val.stringValue()+"> as ?book ) . \n");
                sb.append("  ?book dc:title ?title ; \n");
                sb.append("  ns:price ?price . \n");
                sb.append("} \n");
                return sb.toString();
            }
            
        }
        
        final URI serviceURI = new URIImpl(StoredQueryService.Options.NAMESPACE + getName());
        try {

            // register the service.
            ServiceRegistry.getInstance().add(serviceURI,
                    new MyStoredQueryService());

            final TestHelper h = new TestHelper("stored-query-002", // testURI,
                    "stored-query-002.rq",// queryFileURL
                    "stored-query-001.ttl",// dataFileURL
                    "stored-query-001.srx" // resultFileURL,
                    // false, // laxCardinality
                    // true // checkOrder
            );

            h.runTest();

        } finally {
            // unregister the service.
            ServiceRegistry.getInstance().remove(serviceURI);
        }

    }

}
