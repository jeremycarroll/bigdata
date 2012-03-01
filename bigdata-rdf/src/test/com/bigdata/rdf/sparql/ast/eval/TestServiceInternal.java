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

package com.bigdata.rdf.sparql.ast.eval;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.ServiceFactory;
import com.bigdata.rdf.sparql.ast.ServiceRegistry;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Data driven test suite for SPARQL 1.1 Federated Query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestServiceInternal.java 6053 2012-02-29 18:47:54Z thompsonbry
 *          $
 */
public class TestServiceInternal extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestServiceInternal() {
    }

    /**
     * @param name
     */
    public TestServiceInternal(String name) {
        super(name);
    }

    /**
     * A simple SERVICE query against an INTERNAL service. The service adds in a
     * single solution which restricts the set of solutions for the overall
     * query.
     */
    public void test_service_001() throws Exception {
        
        /*
         * Note: This IV needs to be resolved in order to join against data that
         * loaded into the database. Since we have not yet loaded the data, the
         * RDF Value is being inserted into the store now. That way, when the
         * data are loaded, the data will use the same IV and the join will
         * succeed.
         */
        final IV<?, ?> book1 = store.addTerm(store.getValueFactory().createURI(
                "http://example.org/book/book1"));
        
//        final IV<?,?> fourtyTwo;
////      fourtyTwo = makeIV(store.getValueFactory().createLiteral("42", XSD.INTEGER));
//        fourtyTwo = new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(42));

        final List<IBindingSet> serviceSolutions = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(Var.var("book"), new Constant<IV>(book1));
            serviceSolutions.add(bset);
        }
   
        final URI serviceURI = new URIImpl(
                "http://www.bigdata.com/mockService/" + getName());

        ServiceRegistry.add(serviceURI,
                new MockServiceFactory(serviceSolutions));

        try {

            new TestHelper(//
                    "sparql11-service-001", // testURI
                    "sparql11-service-001.rq",// queryFileURL
                    "sparql11-service-001.ttl",// dataFileURL
                    "sparql11-service-001.srx"// resultFileURL
            ).runTest();
            
        } finally {
            
            ServiceRegistry.remove(serviceURI);
            
        }
        
    }
    
    /**
     * A simple SERVICE query against an INTERNAL service. The service provides
     * three solutions, two of which join with the remainder of the query.
     * <p>
     * Note: Since the SERVICE is not actually doing joins, we wind up with
     * duplicate solutions.
     */
    public void test_service_002() throws Exception {
        
        /*
         * Note: This IV needs to be resolved in order to join against data that
         * loaded into the database. Since we have not yet loaded the data, the
         * RDF Value is being inserted into the store now. That way, when the
         * data are loaded, the data will use the same IV and the join will
         * succeed.
         */
        final IV<?, ?> book1 = store.addTerm(store.getValueFactory().createURI(
                "http://example.org/book/book1"));
        final IV<?, ?> book2 = store.addTerm(store.getValueFactory().createURI(
                "http://example.org/book/book2"));

        final List<IBindingSet> serviceSolutions = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(Var.var("book"), new Constant<IV>(book1));
            serviceSolutions.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(Var.var("book"), new Constant<IV>(book2));
            serviceSolutions.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            serviceSolutions.add(bset);
        }
   
        final URI serviceURI = new URIImpl(
                "http://www.bigdata.com/mockService/" + getName());

        ServiceRegistry.add(serviceURI,
                new MockServiceFactory(serviceSolutions));

        try {

            new TestHelper(//
                    "sparql11-service-002", // testURI
                    "sparql11-service-002.rq",// queryFileURL
                    "sparql11-service-002.ttl",// dataFileURL
                    "sparql11-service-002.srx"// resultFileURL
            ).runTest();
            
        } finally {
            
            ServiceRegistry.remove(serviceURI);
            
        }
        
    }
    
    /**
     * FIXME Write test which vectors multiple source solutions into the
     * service.
     */
    public void test_service_004() {
        fail("write test");
    }
    
    /**
     * FIXME Write test which uses a variable for the service reference, but
     * where the service always resolves to a known service. Verify evaluation
     * with an empty solution in. Maybe write an alternative test which does the
     * same thing with multiple source solutions in (e.g., using BINDINGS in the
     * SPARQL query).
     * <p>
     * The join variables SHOULD be a non-empty set for this case unless the
     * incoming variables are only MAYBE bound due to an OPTIONAL construct.
     * Write a test for that case too and write tests of the static analysis of
     * the join and projected variables for a SERVICE call.
     */
    public void test_service_005() {
        fail("write test");
    }
    
    /**
     * TODO Test queries where the evaluation order does not place the service
     * first (or does not lift it out into a named subquery).
     */
    public void test_service_006() {
        fail("write test");
    }
    
    /**
     * Mock service reports the solutions provided in the constructor.
     * <p>
     * Note: This can not be used to test complex queries because the caller
     * needs to know the order in which the query will be evaluated in order to
     * know the correct response for the mock service.
     */
    private static class MockServiceFactory implements ServiceFactory
    {

        private final List<IBindingSet> serviceSolutions;
        
        public MockServiceFactory(final List<IBindingSet> serviceSolutions) {

            this.serviceSolutions = serviceSolutions;
            
        }
        
        @Override
        public BigdataServiceCall create(final AbstractTripleStore store,
                final IGroupNode<IGroupMemberNode> groupNode,
                final URI serviceURI, final String exprImage,
                final Map<String, String> prefixDecls) {

            assertNotNull(store);
            
            assertNotNull(groupNode);

            return new MockBigdataServiceCall();
            
        }
        
        private class MockBigdataServiceCall implements BigdataServiceCall {

            @Override
            public ICloseableIterator<IBindingSet> call(
                    final IBindingSet[] bindingSets) {

                assertNotNull(bindingSets);

//                System.err.println("ServiceCall: in="+Arrays.toString(bindingSets));
//                
//                System.err.println("ServiceCall: out="+serviceSolutions);
                
                return new CloseableIteratorWrapper<IBindingSet>(
                        serviceSolutions.iterator());

            }
            
        }
        
    }

}
