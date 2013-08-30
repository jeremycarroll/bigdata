/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.graph.impl;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.AbstractGraphTestCase;
import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.impl.bd.BigdataGASEngine;
import com.bigdata.rdf.graph.impl.bd.BigdataGASEngine.BigdataGraphAccessor;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;

import cutthecrap.utils.striterators.IStriterator;

/**
 * Test class for GATHER.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("rawtypes")
public class TestGather extends AbstractGraphTestCase {

    public TestGather() {
        
    }
    
    public TestGather(String name) {
        super(name);
    }

    /**
     * Mock gather class uses a UNION for SUM to test the GATHER semantics.
     * The gathered edge set is then APPLYed to the vertex and becomes the
     * state of that vertex.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static class MockGASProgram extends
            BaseGASProgram<Set<Statement>, Set<Statement>, Set<Statement>> {

        private final EdgesEnum gatherEdges;
        
        MockGASProgram(final EdgesEnum gatherEdges) {
            
            this.gatherEdges = gatherEdges;
            
        }
        
        @Override
        public EdgesEnum getGatherEdges() {
            return gatherEdges;
        }

        @Override
        public EdgesEnum getScatterEdges() {
            return EdgesEnum.NoEdges;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to only visit the edges of the graph.
         */
        @Override
        public IStriterator constrainFilter(
                final IGASContext<Set<Statement>, Set<Statement>, Set<Statement>> ctx,
                final IStriterator itr) {

            return itr.addFilter(getEdgeOnlyFilter(ctx));

        }

        @Override
        public Factory<Value, Set<Statement>> getVertexStateFactory() {
            return new Factory<Value, Set<Statement>>() {
                @Override
                public Set<Statement> initialValue(Value value) {
                    return new LinkedHashSet<Statement>();
                }
            };
        }

        @Override
        public Factory<Statement, Set<Statement>> getEdgeStateFactory() {

            return null;
            
        }

        @Override
        public void init(IGASState<Set<Statement>, Set<Statement>, Set<Statement>> ctx, Value u) {

            // NOP
            
        }

        /**
         * Return the edge as a singleton set.
         */
        @Override
        public Set<Statement> gather(
                final IGASState<Set<Statement>, Set<Statement>, Set<Statement>> state,
                final Value u, final Statement e) {

            return Collections.singleton(e);

        }

        /**
         * Set UNION over the GATHERed edges.
         */
        @Override
        public Set<Statement> sum(Set<Statement> left, Set<Statement> right) {
            
            /*
             * Note: This happens to preserve the visitation order. That is not
             * essential, but it is nice.
             */
            final Set<Statement> tmp = new LinkedHashSet<Statement>(left);
            
            tmp.addAll(right);
            
            return tmp;
            
        }

        /**
         * UNION the gathered edges with those already decorating the vertex.
         */
        @Override
        public Set<Statement> apply(
                final IGASState<Set<Statement>, Set<Statement>, Set<Statement>> state,
                final Value u, final Set<Statement> sum) {
 
            if (sum != null) {

                // Get the state for that vertex.
                final Set<Statement> us = state.getState(u);

                // UNION with the accumulant.
                us.addAll(sum);

                return us;

            }

            // No change.
            return null;

        }

        @Override
        public boolean isChanged(
                IGASState<Set<Statement>, Set<Statement>, Set<Statement>> state, Value u) {

            return true;
            
        }

        @Override
        public void scatter(IGASState<Set<Statement>, Set<Statement>, Set<Statement>> state,
                final IGASScheduler sch, Value u, Statement e) {

            throw new UnsupportedOperationException();
            
        }

        @Override
        public boolean nextRound(IGASContext ctx) {

            return true;
            
        }

    };
    
    public void testGather_inEdges() throws Exception {

        final SmallGraphProblem p = setupSmallGraphProblem();

        // gather no-edges for :mike
        {
            
            final Set<Statement> expected = set();

            doGatherTest(EdgesEnum.NoEdges, expected, p.mike.getIV()/* startingVertex */);
        
        }

        // gather in-edges for :mike
        {
            
            final Set<Statement> expected = set(//
            (Statement) new SPO(p.bryan.getIV(), p.foafKnows.getIV(), p.mike.getIV(),
                    StatementEnum.Explicit)//
            );

            doGatherTest(EdgesEnum.InEdges, expected, p.mike.getIV()/* startingVertex */);

        }

        // gather out-edges for :mike
        {
            
            final Set<Statement> expected = set(//
                    (Statement) new SPO(p.mike.getIV(), p.rdfType.getIV(), p.foafPerson.getIV(),
                            StatementEnum.Explicit),//
                    (Statement) new SPO(p.mike.getIV(), p.foafKnows.getIV(), p.bryan.getIV(),
                            StatementEnum.Explicit)//
            );

            doGatherTest(EdgesEnum.OutEdges, expected, p.mike.getIV()/* startingVertex */);
        
        }

        // gather all-edges for :mike 
        {
            
            final Set<Statement> expected = set(//
                    (Statement) new SPO(p.bryan.getIV(), p.foafKnows.getIV(), p.mike.getIV(),
                            StatementEnum.Explicit),//
                    (Statement) new SPO(p.mike.getIV(), p.rdfType.getIV(), p.foafPerson.getIV(),
                            StatementEnum.Explicit),//
                    (Statement) new SPO(p.mike.getIV(), p.foafKnows.getIV(), p.bryan.getIV(),
                            StatementEnum.Explicit)//
            );

            doGatherTest(EdgesEnum.AllEdges, expected, p.mike.getIV()/* startingVertex */);

        }

    }

    /**
     * Start on a known vertex. Do one iteration. Verify that the GATHER
     * populated the data structures on the mock object with the appropriate
     * collections.
     * @throws Exception 
     */
    protected void doGatherTest(final EdgesEnum gatherEdges,
            final Set<Statement> expected, final IV startingVertex) throws Exception {

        final IGASEngine gasEngine = new BigdataGASEngine(sail.getDatabase()
                .getIndexManager(), 1/* nthreads */);

        try {

            final BigdataGraphAccessor graphAccessor = ((BigdataGASEngine) gasEngine)
                    .newGraphAccessor(sail.getDatabase().getNamespace(), sail
                            .getDatabase().getIndexManager()
                            .getLastCommitTime());

            final IGASContext<Set<Statement>, Set<Statement>, Set<Statement>> gasContext = gasEngine
                    .newGASContext(graphAccessor, new MockGASProgram(
                            gatherEdges));

            final IGASState<Set<Statement>, Set<Statement>, Set<Statement>> gasState = gasContext
                    .getGASState();
            
            // Initialize the froniter.
            gasState.init(startingVertex);

            // Do one round.
            gasContext.doRound(new GASStats());

            /*
             * Lookup the state for the starting vertex (this should be the only
             * vertex whose state was modified since we did only one round).
             */
            final Set<Statement> actual = gasState.getState(startingVertex);

            // Verify against the expected state.
            assertSameEdges(expected, actual);

        } finally {

            gasEngine.shutdownNow();

        }

    }

}
