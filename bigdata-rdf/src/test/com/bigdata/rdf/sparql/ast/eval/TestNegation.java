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
 * Created on Nov 22, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.bop.join.IHashJoinUtility;

/**
 * Test suite for SPARQL negation (EXISTS and MINUS).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNegation extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestNegation() {
    }

    /**
     * @param name
     */
    public TestNegation(String name) {
        super(name);
    }

    /**
     * Unit test for an query with an EXISTS filter. The EXISTS filter is
     * modeled as an ASK sub-query which projects an anonymous variable and a
     * simple test of the truth state of that anonymous variable.
     * 
     * <pre>
     * PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX foaf: <http://xmlns.com/foaf/0.1/>
     * SELECT DISTINCT ?x
     * WHERE {
     *   ?x ?p ?o .
     *   FILTER ( EXISTS {?x rdf:type foaf:Person} ) 
     * }
     * </pre>
     */
    public void test_exists_1() throws Exception {

        new TestHelper(
                "exists-1", // testURI,
                "exists-1.rq",// queryFileURL
                "exists-1.trig",// dataFileURL
                "exists-1.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * Sesame Unit <code>sparql11-exists-05</code>.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * WHERE {
     *     ?a :p ?n
     *     FILTER NOT EXISTS {
     *         ?a :q ?n .
     *     }
     * }
     * </pre>
     */
    public void test_sparql11_exists_05() throws Exception {

        new TestHelper(
                "sparql11-exists-05", // testURI,
                "sparql11-exists-05.rq",// queryFileURL
                "sparql11-exists-05.ttl",// dataFileURL
                "sparql11-exists-05.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }
    
    /**
     * Sesame Unit <code>sparql11-exists-06</code>.
     * 
     * <pre>
     * PREFIX : <http://example/>
     * SELECT * WHERE {
     *     ?a :p ?n
     *     FILTER NOT EXISTS {
     *         ?a :q ?m .
     *         FILTER(?n = ?m)
     *     }
     * }
     * </pre>
     * 
     * FIXME This test is failing because we are not projecting the variables in
     * the parent's context into the EXISTS subquery. EXISTS is not really the
     * same as ASK because bindings (including both definitely and maybe bound
     * variables) need to be projected IN, but not projected OUT.
     * <p>
     * I believe that what is needed is a variant join in which we bind output
     * the left solution IFF there is a JOIN, but we DO NOT output the
     * (left+right) solution from that JOIN. This means a variant method (or
     * constructor) on the {@link IHashJoinUtility}. It might be worth while to
     * handle MINUS while we are at it.
     */
    public void test_sparql11_exists_06() throws Exception {

        new TestHelper(
                "sparql11-exists-06", // testURI,
                "sparql11-exists-06.rq",// queryFileURL
                "sparql11-exists-06.ttl",// dataFileURL
                "sparql11-exists-06.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     */
    public void test_sparql11_minus_01() throws Exception {

        new TestHelper(
                "sparql11-minus-01", // testURI,
                "sparql11-minus-01.rq",// queryFileURL
                "sparql11-minus-01.ttl",// dataFileURL
                "sparql11-minus-01.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     */
    public void test_sparql11_minus_02() throws Exception {

        new TestHelper(
                "sparql11-minus-02", // testURI,
                "sparql11-minus-02.rq",// queryFileURL
                "sparql11-minus-02.ttl",// dataFileURL
                "sparql11-minus-02.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     */
    public void test_sparql11_minus_05() throws Exception {

        new TestHelper(
                "sparql11-minus-05", // testURI,
                "sparql11-minus-05.rq",// queryFileURL
                "sparql11-minus-05.ttl",// dataFileURL
                "sparql11-minus-05.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     */
    public void test_sparql11_minus_06() throws Exception {

        new TestHelper(
                "sparql11-minus-06", // testURI,
                "sparql11-minus-06.rq",// queryFileURL
                "sparql11-minus-06.ttl",// dataFileURL
                "sparql11-minus-06.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

    /**
     * A Sesame test.
     */
    public void test_sparql11_minus_07() throws Exception {

        new TestHelper(
                "sparql11-minus-07", // testURI,
                "sparql11-minus-07.rq",// queryFileURL
                "sparql11-minus-07.ttl",// dataFileURL
                "sparql11-minus-07.srx" // resultFileURL,
//                false, // laxCardinality
//                true // checkOrder
                ).runTest();

    }

}
