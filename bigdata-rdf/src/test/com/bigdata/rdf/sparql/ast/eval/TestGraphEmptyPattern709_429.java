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
 * Tests concerning "SELECT GRAPH XXXX {}" with XXXX and the dataset varying.
 *
 */
public class TestGraphEmptyPattern709_429 extends AbstractDataDrivenSPARQLTestCase {

    /**
     *
     */
    public TestGraphEmptyPattern709_429() {
    }

    /**
     * @param name
     */
    public TestGraphEmptyPattern709_429(String name) {
        super(name);
    }

    public void test_graph_var() throws Exception {

        new TestHelper("trac709").runTest();

    }

    public void test_empty_graph_matches() throws Exception {

        new TestHelper("trac709empty").runTest();

    }
    public void test_graph_uri() throws Exception {

        new TestHelper("trac429").runTest();

    }
    public void test_work_around_graph_var() throws Exception {

        new TestHelper("trac709workaround", "trac709workaround.rq", "trac709.trig", "trac709.srx").runTest();

    }

}
