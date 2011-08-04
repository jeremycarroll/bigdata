/**
Copyright (C) SYSTAP, LLC 2011.  All rights reserved.

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

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.sail.SailException;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;

/**
 * Unit test template for use in submission of bugs.
 * <p>
 * This test case will delegate to an underlying backing store. You can specify
 * this store via a JVM property as follows:
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code>
 * <p>
 * There are three possible configurations for the testClass:
 * <ul>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithQuads (quads mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithoutSids (triples mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithSids (SIDs mode)</li>
 * </ul>
 * <p>
 * The default for triples and SIDs mode is for inference with truth maintenance
 * to be on. If you would like to turn off inference, make sure to do so in
 * {@link #getProperties()}.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestTicket361 extends QuadsTestCase {
    
    private final static Logger log = Logger.getLogger(TestTicket361.class);
    
    public TestTicket361() {
    }

    public TestTicket361(String arg0) {
        super(arg0);
    }

    public void testEvaluate() throws Exception {
        final BigdataSail sail = getSail();
        try {
            sail.initialize();
            final BigdataSailConnection conn = sail.getConnection();
            try {
                conn.addStatement(new URIImpl("s:1"), new URIImpl("p:1"), new LiteralImpl("l1"));
                conn.addStatement(new URIImpl("s:2"), new URIImpl("p:2"), new LiteralImpl("l1"));
                conn.addStatement(new URIImpl("s:3"), new URIImpl("p:3"), new LiteralImpl("l3"));
                CloseableIteration<? extends BindingSet, QueryEvaluationException> results = null;
                try {
                    // submit query
                    results = query(conn);
                } finally {
                    if (results != null) {
                        // immediately close the query result iteration.
                        log.info("Closing query result iteration.");
                        results.close();
                    }
                }
            } finally {
                log.info("Closing connection.");
                conn.close();
            }
        } finally {
            final QueryEngine queryEngine = QueryEngineFactory
                    .getExistingQueryController(sail.getDatabase()
                            .getIndexManager());
            if (queryEngine != null) {
                /*
                 * Note: The query engine should shutdown automatically once it
                 * is finalized. This protects against a shutdown when there are
                 * concurrent users, e.g., different sails against the same
                 * Journal instance. However, if there are any queries still
                 * running on the QueryEngine when the backing IIndexManager
                 * shuts down its ExecutorService, then a
                 * RejectedExecutionException will be logged. In general, this
                 * can be safely ignored.
                 */
                final UUID[] uuids = queryEngine.getRunningQueries();
                assertEquals("Query not terminated: " + Arrays.toString(uuids),
                        new UUID[0], uuids);
//                log.info("Shutting down QueryEngine");
//                queryEngine.shutdown();
            }
            log.info("Shutting down sail");
            sail.shutDown();
            log.info("Tear down");
            sail.__tearDownUnitTest();
        }
    }
    
    private CloseableIteration<? extends BindingSet, QueryEvaluationException> query(
            final BigdataSailConnection conn) throws SailException,
            QueryEvaluationException {

        final ProjectionElemList elemList = new ProjectionElemList(
                new ProjectionElem("z"));

        final TupleExpr query = new Projection(new StatementPattern(
                new Var("s"), new Var("p"), new Var("o")), elemList);

        final QueryBindingSet bindings = mb("o", "l1", "o1", "l2", "o2", "l3");

        return conn.evaluate(query, null, new QueryBindingSet(), new Iter(
                bindings), false, null);

    }

    /**
     * Makes a binding set by taking each pair of values and using the first
     * value as name and the second as value. Creates an URI for a value with a
     * ':' in it, or a Literal for a value without a ':'.
     */
    private QueryBindingSet mb(final String... nameValuePairs) {
        final QueryBindingSet bs = new QueryBindingSet();
        for (int i = 0; i < nameValuePairs.length; i += 2)
            bs.addBinding(nameValuePairs[i],
                    nameValuePairs[i + 1].indexOf(':') > 0 ? new URIImpl(
                            nameValuePairs[i + 1]) : new LiteralImpl(
                            nameValuePairs[i + 1]));
        return bs;
    }
    
    /**
     * Iterates over the given bindings.
     */
    private static class Iter implements
            CloseableIteration<BindingSet, QueryEvaluationException> {

        final private Iterator<BindingSet> iter;
        private volatile boolean open = true;
        
        private Iter(final Collection<BindingSet> bindings) {
        
            this.iter = bindings.iterator();
            
        }
        
        private Iter(final BindingSet... bindings) {

            this(Arrays.asList(bindings));
            
        }
//        private int ncalls = 0;
        public boolean hasNext() throws QueryEvaluationException {
//            log.error("Callers: ",new RuntimeException("caller#"+(++ncalls)));
            try {
                /*
                 * Note: hasNext() is called ~ 6 times during the test, so this
                 * timeout gets multiplied.
                 */
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
           
            if (!open)
                return false;
            
            return iter.hasNext();
            
        }

        public BindingSet next() throws QueryEvaluationException {
            
            if (!hasNext())
                throw new NoSuchElementException();

            return iter.next();
            
        }

        public void remove() throws QueryEvaluationException {

            if (!open)
                throw new IllegalStateException();

            iter.remove();
            
        }

        public void close() throws QueryEvaluationException {

            if (open) {

                open = false;
                
                log.info("Source iteration closed");
                
            }

        }

    }

}
