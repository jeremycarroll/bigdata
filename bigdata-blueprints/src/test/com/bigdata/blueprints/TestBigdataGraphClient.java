/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.blueprints;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.client.BigdataSailNSSWrapper;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;

/**
 */
public class TestBigdataGraphClient extends AbstractTestBigdataGraph {

    protected static final transient Logger log = Logger.getLogger(TestBigdataGraphClient.class);
    
    /**
     * 
     */
    public TestBigdataGraphClient() {
    }

    /**
     * @param name
     */
    public TestBigdataGraphClient(String name) {
        super(name);
    }

    protected GraphTest newBigdataGraphTest() {
        return new BigdataGraphTest();
    }
    
    private class BigdataGraphTest extends GraphTest {

		@Override
		public void doTestSuite(TestSuite testSuite) throws Exception {
	        for (Method method : testSuite.getClass().getDeclaredMethods()) {
	            if (method.getName().startsWith("test")) {
	                System.out.println("Testing " + method.getName() + "...");
	                try {
		                method.invoke(testSuite);
	                } catch (Exception ex) {
	                	ex.getCause().printStackTrace();
	                	throw ex;
	                } finally {
		                shutdown();
	                }
	            }
	        }
		}
		
		private Map<String,BigdataSailNSSWrapper> testSails = new LinkedHashMap<String, BigdataSailNSSWrapper>();

		@Override
		public Graph generateGraph(final String key) {
			
			try {
	            if (testSails.containsKey(key) == false) {
	                final BigdataSail testSail = getSail();
	                testSail.initialize();
	                final BigdataSailNSSWrapper nss = new BigdataSailNSSWrapper(testSail);
	                nss.init();
	                testSails.put(key, nss);
	            }
	            
				final BigdataSailNSSWrapper nss = testSails.get(key);
				final BigdataGraph graph = new BigdataGraphClient(nss.m_repo);
				
				return graph;
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
			
		}

		@Override
		public Graph generateGraph() {
			
			return generateGraph(null);
		}
		
		public void shutdown() {
		    for (BigdataSailNSSWrapper wrapper : testSails.values()) {
		        try {
    		        wrapper.shutdown();
    		        wrapper.sail.__tearDownUnitTest();
		        } catch (Exception ex) {
		            throw new RuntimeException(ex);
		        }
		    }
		    testSails.clear();
		}
		
    	
    }

    public static final void main(final String[] args) throws Exception {
        
        final String url = "http://localhost:9999/bigdata/";
        
        final BigdataGraph graph = BigdataGraphFactory.connect(url);
        
        for (Vertex v : graph.getVertices()) {
            
            System.err.println(v);
            
        }
        
        for (Edge e : graph.getEdges()) {
            
            System.err.println(e);
            
        }
        
    }

}
