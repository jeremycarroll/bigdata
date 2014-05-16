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

import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraphTestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 */
public class TestBigdataGraphEmbedded extends AbstractTestBigdataGraph {

    protected static final transient Logger log = Logger.getLogger(TestBigdataGraphEmbedded.class);
    
    /**
     * 
     */
    public TestBigdataGraphEmbedded() {
    }

    /**
     * @param name
     */
    public TestBigdataGraphEmbedded(String name) {
        super(name);
    }

    public void testTransactionalGraphTestSuite() throws Exception {
        final GraphTest test = newBigdataGraphTest();
        test.stopWatch();
        test.doTestSuite(new TransactionalGraphTestSuite(test));
        GraphTest.printTestPerformance("TransactionalGraphTestSuite",
                test.stopWatch());
    }
    
//  public void testGraphSuite() throws Exception {
//  final GraphTest test = newBigdataGraphTest();
//  test.stopWatch();
//    test.doTestSuite(new GraphTestSuite(test));
//    GraphTest.printTestPerformance("GraphTestSuite", test.stopWatch());
//}


//    public void testGetEdgesByLabel() throws Exception {
//        final BigdataGraphTest test = new BigdataGraphTest();
//        test.stopWatch();
//        final BigdataTestSuite testSuite = new BigdataTestSuite(test);
//        try {
//            testSuite.testGetEdgesByLabel();
//        } finally {
//            test.shutdown();
//        }
//        
//    }
    
    private static class BigdataTestSuite extends TestSuite {
        
        public BigdataTestSuite(final BigdataGraphTest graphTest) {
            super(graphTest);
        }
        
        public void testGetEdgesByLabel() {
            Graph graph = graphTest.generateGraph();
            if (graph.getFeatures().supportsEdgeIteration) {
              Vertex v1 = graph.addVertex(null);
              Vertex v2 = graph.addVertex(null);
              Vertex v3 = graph.addVertex(null);
      
              Edge e1 = graph.addEdge(null, v1, v2, graphTest.convertLabel("test1"));
              Edge e2 = graph.addEdge(null, v2, v3, graphTest.convertLabel("test2"));
              Edge e3 = graph.addEdge(null, v3, v1, graphTest.convertLabel("test3"));
      
              assertEquals(e1, getOnlyElement(graph.query().has("label", graphTest.convertLabel("test1")).edges()));
              assertEquals(e2, getOnlyElement(graph.query().has("label", graphTest.convertLabel("test2")).edges()));
              assertEquals(e3, getOnlyElement(graph.query().has("label", graphTest.convertLabel("test3")).edges()));
      
              assertEquals(e1, getOnlyElement(graph.getEdges("label", graphTest.convertLabel("test1"))));
              assertEquals(e2, getOnlyElement(graph.getEdges("label", graphTest.convertLabel("test2"))));
              assertEquals(e3, getOnlyElement(graph.getEdges("label", graphTest.convertLabel("test3"))));
            }
        }


    }

    
    protected GraphTest newBigdataGraphTest() {
        return new BigdataGraphTest();
    }
    
    private class BigdataGraphTest extends GraphTest {

        private List<String> exclude = Arrays.asList(new String[] {
           // this one creates a deadlock, no way around it
           "testTransactionIsolationCommitCheck"  
        });
        
		@Override
		public void doTestSuite(TestSuite testSuite) throws Exception {
	        for (Method method : testSuite.getClass().getDeclaredMethods()) {
	            if (method.getName().startsWith("test")) {
	                if (exclude.contains(method.getName())) {
	                    System.out.println("Skipping test " + method.getName() + ".");
	                } else {
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
		}
		
		private Map<String,BigdataGraphEmbedded> testGraphs = new LinkedHashMap<String, BigdataGraphEmbedded>();

		@Override
		public Graph generateGraph(final String key) {
			
			try {
	            if (testGraphs.containsKey(key) == false) {
	                final BigdataSail testSail = getSail();
	                testSail.initialize();
	                final BigdataSailRepository repo = new BigdataSailRepository(testSail);
	                final BigdataGraphEmbedded graph = new BigdataGraphEmbedded(repo) {
	    
	                    /**
	                     * Test cases have weird semantics for shutdown.
	                     */
	                    @Override
	                    public void shutdown() {
	                        try {
//	                          if (cxn != null) {
//	                              cxn.commit();
//	                              cxn.close();
//	                              cxn = null;
//	                          }
//	                          commit();
//	                          super.shutdown();
	                        } catch (Exception ex) {
	                            throw new RuntimeException(ex);
	                        }
	                    }
	                    
	                };
                   testGraphs.put(key, graph);
	            }
	            
				BigdataGraphEmbedded graph = testGraphs.get(key); //testSail; //getSail();
				
//				if (!graph.repo.getSail().isOpen()) {
//				    
//				    final BigdataSail sail = reopenSail(graph.repo.getSail());
//				    sail.initialize();
//                    final BigdataSailRepository repo = new BigdataSailRepository(sail);
//                    graph = new BigdataGraphEmbedded(repo);// {
//                    testGraphs.put(key, graph);
//				    
//				}
				
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
		    for (BigdataGraphEmbedded sail : testGraphs.values()) {
		        sail.repo.getSail().__tearDownUnitTest();
		    }
		    testGraphs.clear();
		}
		
    	
    }

    public static final void main(final String[] args) throws Exception {

        { // create an in-memory instance
            
            final BigdataGraph graph = BigdataGraphFactory.create();
            
            GraphMLReader.inputGraph(graph, TestBigdataGraphEmbedded.class.getResourceAsStream("graph-example-1.xml"));
            
            System.err.println("data loaded (in-memory).");
            System.err.println("graph:");
            
            for (Vertex v : graph.getVertices()) {
                
                System.err.println(v);
                
            }
            
            for (Edge e : graph.getEdges()) {
                
                System.err.println(e);
                
            }
            
            graph.shutdown();
            
        }
        
        final File jnl = File.createTempFile("bigdata", ".jnl");
        
        { // create a persistent instance
            
            final BigdataGraph graph = BigdataGraphFactory.create(jnl.getAbsolutePath());
            
            GraphMLReader.inputGraph(graph, TestBigdataGraphEmbedded.class.getResourceAsStream("graph-example-1.xml"));
            
            System.err.println("data loaded (persistent).");
            System.err.println("graph:");
            
            for (Vertex v : graph.getVertices()) {
                
                System.err.println(v);
                
            }
            
            for (Edge e : graph.getEdges()) {
                
                System.err.println(e);
                
            }
            
            graph.shutdown();
            
        }
        
        { // re-open the persistent instance
            
            final BigdataGraph graph = BigdataGraphFactory.open(jnl.getAbsolutePath());
            
            System.err.println("persistent graph re-opened.");
            System.err.println("graph:");
            
            for (Vertex v : graph.getVertices()) {
                
                System.err.println(v);
                
            }
            
            for (Edge e : graph.getEdges()) {
                
                System.err.println(e);
                
            }
            
            graph.shutdown();
            
        }
        
        jnl.delete();
        
    }


}
