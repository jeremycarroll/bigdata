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
 * Test for trac725
 * <pre>
#select *
#where {
#{
SELECT ( COUNT(?narrow) as ?countNarrow ) ?scheme
WHERE
{ ?narrow skos:inScheme ?scheme .
FILTER EXISTS { ?narrow skos:broader ?b }
}
GROUP BY ?scheme
#}
#}
</pre>
 * 
 */
public class TestSubSelectFilterExist725 extends AbstractDataDrivenSPARQLTestCase {

    public TestSubSelectFilterExist725() {
    }

    public TestSubSelectFilterExist725(String name) {
        super(name);
    }

    public void test_without_subselect() throws Exception {

        new TestHelper(
                "filter-exist-725-no-sub-select",// testURI
                "filter-exist-725-no-sub-select.rq", // queryURI
                "filter-exist-725.ttl", // dataURI
                "filter-exist-725.srx" // resultURI
                ).runTest();
        
    }

    public void test_with_subselect() throws Exception {

        new TestHelper(
                "filter-exist-725-sub-select",// testURI
                "filter-exist-725-sub-select.rq", // queryURI
                "filter-exist-725.ttl", // dataURI
                "filter-exist-725.srx" // resultURI
                ).runTest();
        
    } 
    
}
