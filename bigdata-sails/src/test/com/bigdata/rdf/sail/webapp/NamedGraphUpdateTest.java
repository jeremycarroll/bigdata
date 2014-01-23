/**
Copyright (C) SYSTAP, LLC 2014.  All rights reserved.

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

package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import junit.framework.Test;


/**
 * This class is concernign the issues raised in trac 804
 * https://sourceforge.net/apps/trac/bigdata/ticket/804
 * @author jeremycarroll
 *
 */
public class NamedGraphUpdateTest extends AbstractProtocolTest {


	public NamedGraphUpdateTest(String name)  {
		super(name);
	}
	
	
	private String atomicMoveNamedGraph(boolean useHint) {
		// Atomic update of uploaded graph - moving eg:tmp to eg:a (deleting old contents of eg:a)
		return 
		"DELETE {\n" +   
	    "  GRAPH <eg:a> {\n" +
	    "    ?olds ?oldp ?oldo\n" +
	    "  }\n" +       
	    "  GRAPH <eg:tmp> {\n" +
	    "    ?news ?newp ?newo\n" +
	    "  }\n" +       
	    "}\n" +     
	    "INSERT {\n" +
	    "  GRAPH <eg:a> {\n" +
	    "    ?news ?newp ?newo\n" +
	    "  }\n" +       
	    "}\n" +     
	    "WHERE {\n" +
	    (useHint?"  hint:Query hint:chunkSize 2 .\n":"") +
	    "  {\n" +       
	    "    GRAPH <eg:a> {\n" +
	    "      ?olds ?oldp ?oldo\n" +
	    "    }\n" +         
	    "  } UNION {\n" +
	    "    GRAPH <eg:tmp> {\n" +
	    "      ?news ?newp ?newo\n" +
	    "    }\n" +         
	    "  }\n" +       
	    "}";
	}
	
	private String insertData = 
			        "prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
				    "INSERT DATA\n" +
				    "{ \n" +
				    " GRAPH <eg:a> {\n" +
				    "   [ a \"Blank\" ] .\n" +
				    "   <eg:b> rdf:type <eg:c> ; rdf:value [] .\n" +
				    "   [ rdf:value [] ]\n" +
				    " }\n" +
				    " GRAPH <eg:tmp> {\n" +
				    "   [ a \"Blankx\" ] .\n" +
				    "   <eg:B> rdf:type <eg:C> ; rdf:value [] .\n" +
				    "   [ rdf:value [] ]\n" +
				    " }\n" +
				    "}\n";
	
	
	private void makeUpdate(String update) throws IOException {
		setMethodisPostUrlEncodedData();
		serviceRequest("update", update);
	}
	
	private void assertQuad(String graph, String triple) throws IOException {
		assertQuad("true", graph, triple);
	}

	private void assertNotQuad(String graph, String triple) throws IOException {
		assertQuad("false", graph, triple);
	}

	void assertQuad(String expected, String graph, String triple) throws IOException {
		String result = serviceRequest("query", "ASK { GRAPH " + graph + " { " + triple + "} }" );
		assertTrue(result.contains(expected));
	}
	
	private void updateAFewTimes(boolean useHint, int numberOfTimes, int numberOfUpdatesPerTime) throws IOException {
		for (int i=0; i<numberOfTimes; i++) {
			for (int j=0; j<numberOfUpdatesPerTime;j++) {
				makeUpdate(insertData);
			}
			makeUpdate( atomicMoveNamedGraph(useHint) );
			assertNotQuad("<eg:tmp>", " ?s ?p ?o ");
		}
	}

	public void test_t_20_1() throws  IOException {
		updateAFewTimes(true, 20, 1);
	}
	public void test_t_20_2() throws  IOException {
		updateAFewTimes(true, 20, 2);
	}
	public void test_t_20_3() throws  IOException {
		updateAFewTimes(true, 20, 3);
	}
	public void test_t_20_5() throws  IOException {
		updateAFewTimes(true, 20, 5);
	}
	public void test_f_20_1() throws  IOException {
		updateAFewTimes(false, 20, 1);
	}
	public void test_f_20_2() throws  IOException {
		updateAFewTimes(false, 20, 2);
	}
	public void test_f_20_3() throws  IOException {
		updateAFewTimes(false, 20, 3);
	}
	public void test_f_20_5() throws  IOException {
		updateAFewTimes(false, 20, 5);
	}
	public void test_double_triple_delete() throws  IOException {
		setMethodisPostUrlEncodedData();
		makeUpdate("prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
			    "INSERT DATA\n" +
			    "{ \n" +
			    " GRAPH <eg:a> {\n" +
			    "   <eg:b> rdf:type <eg:c> \n" +
			    " }\n" +
			    " GRAPH <eg:tmp> {\n" +
			    "   <eg:b> rdf:type <eg:c> \n" +
			    " }\n" +
			    "}\n");
		makeUpdate( "DELETE {\n" +   
			    "  GRAPH <eg:a> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "  GRAPH <eg:tmp> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "}\n" +   
			    "WHERE {\n" +
			    "    GRAPH <eg:a> {\n" +
			    "      ?olds ?oldp ?oldo\n" +
			    "    }\n" +     
			    "}");
		assertNotQuad("?g","?s ?p ?o");
		
	}

	public void test_double_triple_insert() throws  IOException {
		makeUpdate( "prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
			    "INSERT DATA\n" +
			    "{ \n" +
			    " GRAPH <eg:tmp> {\n" +
			    "   <eg:b> rdf:type <eg:c> .\n" +
			    "   <eg:x> rdf:type _:foo \n" +
			    " }\n" +
			    "}\n");
		makeUpdate( "INSERT {\n" +   
			    "  GRAPH <eg:A> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "  GRAPH <eg:B> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "}\n" +   
			    "WHERE {\n" +
			    "    GRAPH <eg:tmp> {\n" +
			    "      ?olds ?oldp ?oldo\n" +
			    "    }\n" +     
			    "}");
		assertQuad("<eg:A>","<eg:b> rdf:type <eg:c> ");
		assertQuad("<eg:B>","<eg:b> rdf:type <eg:c> ");
		assertQuad("<eg:A>","<eg:x> rdf:type ?x");
		assertQuad("<eg:B>","<eg:x> rdf:type ?x ");
	}
	public void test_double_triple_delete_insert() throws  IOException {
		makeUpdate( "prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
			    "INSERT DATA\n" +
			    "{ \n" +
			    " GRAPH <eg:tmp> {\n" +
			    "   <eg:A> <eg:moveTo> <eg:AA> .\n" +
			    "   <eg:B> <eg:moveTo> <eg:BB> \n" +
			    " }\n" +
			    "}\n");
		makeUpdate( "INSERT {\n" +   
			    "  GRAPH <eg:A> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +      
			    "}\n" +   
			    "WHERE {\n" +
			    "    GRAPH <eg:tmp> {\n" +
			    "      ?olds ?oldp ?oldo\n" +
			    "    }\n" +     
			    "}");
		makeUpdate( "INSERT {\n" +  
			    "  GRAPH <eg:B> {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "}\n" +   
			    "WHERE {\n" +
			    "    GRAPH <eg:tmp> {\n" +
			    "      ?olds ?oldp ?oldo\n" +
			    "    }\n" +     
			    "}");
		assertQuad("<eg:A>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertQuad("<eg:B>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertQuad("<eg:A>","<eg:B> <eg:moveTo> <eg:BB>");
		assertQuad("<eg:B>","<eg:B> <eg:moveTo> <eg:BB> ");
		makeUpdate( "DELETE {\n" + 
			    "  GRAPH ?oldg {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "}\n" +   
				"INSERT {\n" +  
			    "  GRAPH ?newg {\n" +
			    "    ?olds ?oldp ?oldo\n" +
			    "  }\n" +       
			    "}\n" +   
			    "WHERE {\n" +
			    "    GRAPH <eg:tmp> {\n" +
			    "      ?oldg <eg:moveTo> ?newg\n" +
			    "    }\n" +     
			    "    GRAPH ?oldg {\n" +
			    "       ?olds ?oldp ?oldo\n" +
			    "    }\n" +     
			    "}");
		assertNotQuad("<eg:A>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertNotQuad("<eg:B>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertNotQuad("<eg:A>","<eg:B> <eg:moveTo> <eg:BB>");
		assertNotQuad("<eg:B>","<eg:B> <eg:moveTo> <eg:BB> ");
		assertQuad("<eg:AA>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertQuad("<eg:BB>","<eg:A> <eg:moveTo> <eg:AA> ");
		assertQuad("<eg:AA>","<eg:B> <eg:moveTo> <eg:BB>");
		assertQuad("<eg:BB>","<eg:B> <eg:moveTo> <eg:BB> ");
	}
	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(NamedGraphUpdateTest.class,"test.*", TestMode.quads);
	}

}
