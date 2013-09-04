/**

Copyright (C) SYSTAP, LLC 2013.  All rights reserved.

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

package com.bigdata.rdf.sparql.ast.eval;



/**
 * Tests concerning:
 * 
SELECT ?A
WHERE {
    ?A rdf:type  / rdfs:subClassOf * <os:ClassA> ;
       rdf:value ?B .
    ?B rdf:type  / rdfs:subClassOf *  <os:ClassB> .
}
 */
public class TestTwoPropertyPaths734 extends AbstractDataDrivenSPARQLTestCase {

    /**
     *
     */
    public TestTwoPropertyPaths734() {
    }

    /**
     * @param name
     */
    public TestTwoPropertyPaths734(String name) {
        super(name);
    }

    private void property_path_test(String name) throws Exception {

        new TestHelper(
                "property-path-734-" + name,        // testURI,
                "property-path-734-" + name + ".rq",// queryFileURL
                "property-path-734.ttl",// dataFileURL
                "property-path-734.srx" // resultFileURL,
                ).runTest();
    }

    public void test_no_property_paths() throws Exception {
        property_path_test("none");
    }
    public void test_first_property_path() throws Exception {
        property_path_test("first");
    }
    public void test_second_property_path() throws Exception {
        property_path_test("second");
    }
    public void test_both_property_paths() throws Exception {
        property_path_test("both");
    }
}
