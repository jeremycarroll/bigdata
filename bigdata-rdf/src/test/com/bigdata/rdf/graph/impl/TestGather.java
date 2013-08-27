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
import com.bigdata.rdf.spo.ISPO;
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
            BaseGASProgram<Set<ISPO>, Set<ISPO>, Set<ISPO>> {

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
        public IStriterator constrainFilter(IStriterator itr) {

            return itr.addFilter(edgeOnlyFilter);
            
        }

        @Override
        public Factory<IV, Set<ISPO>> getVertexStateFactory() {
            return new Factory<IV, Set<ISPO>>() {
                @Override
                public Set<ISPO> initialValue(IV value) {
                    return new LinkedHashSet<ISPO>();
                }
            };
        }

        @Override
        public Factory<ISPO, Set<ISPO>> getEdgeStateFactory() {

            return null;
            
        }

        @Override
        public void init(IGASState<Set<ISPO>, Set<ISPO>, Set<ISPO>> ctx, IV u) {

            // NOP
            
        }

        /**
         * Return the edge as a singleton set.
         */
        @Override
        public Set<ISPO> gather(
                final IGASState<Set<ISPO>, Set<ISPO>, Set<ISPO>> state,
                final IV u, final ISPO e) {

            return Collections.singleton(e);

        }

        /**
         * Set UNION over the GATHERed edges.
         */
        @Override
        public Set<ISPO> sum(Set<ISPO> left, Set<ISPO> right) {
            
            /*
             * Note: This happens to preserve the visitation order. That is not
             * essential, but it is nice.
             */
            final Set<ISPO> tmp = new LinkedHashSet<ISPO>(left);
            
            tmp.addAll(right);
            
            return tmp;
            
        }

        /**
         * UNION the gathered edges with those already decorating the vertex.
         */
        @Override
        public Set<ISPO> apply(
                final IGASState<Set<ISPO>, Set<ISPO>, Set<ISPO>> state,
                final IV u, final Set<ISPO> sum) {
 
            if (sum != null) {

                // Get the state for that vertex.
                final Set<ISPO> us = state.getState(u);

                // UNION with the accumulant.
                us.addAll(sum);

                return us;

            }

            // No change.
            return null;

        }

        @Override
        public boolean isChanged(
                IGASState<Set<ISPO>, Set<ISPO>, Set<ISPO>> state, IV u) {

            return true;
            
        }

        @Override
        public void scatter(IGASState<Set<ISPO>, Set<ISPO>, Set<ISPO>> state,
                final IGASScheduler sch, IV u, ISPO e) {

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
            
            final Set<ISPO> expected = set();

            doGatherTest(EdgesEnum.NoEdges, expected, p.mike.getIV()/* startingVertex */);
        
        }

        // gather in-edges for :mike
        {
            
            final Set<ISPO> expected = set(//
            (ISPO) new SPO(p.bryan.getIV(), p.foafKnows.getIV(), p.mike.getIV(),
                    StatementEnum.Explicit)//
            );

            doGatherTest(EdgesEnum.InEdges, expected, p.mike.getIV()/* startingVertex */);

        }

        // gather out-edges for :mike
        {
            
            final Set<ISPO> expected = set(//
                    (ISPO) new SPO(p.mike.getIV(), p.rdfType.getIV(), p.foafPerson.getIV(),
                            StatementEnum.Explicit),//
                    (ISPO) new SPO(p.mike.getIV(), p.foafKnows.getIV(), p.bryan.getIV(),
                            StatementEnum.Explicit)//
            );

            doGatherTest(EdgesEnum.OutEdges, expected, p.mike.getIV()/* startingVertex */);
        
        }

        // gather all-edges for :mike 
        {
            
            final Set<ISPO> expected = set(//
                    (ISPO) new SPO(p.bryan.getIV(), p.foafKnows.getIV(), p.mike.getIV(),
                            StatementEnum.Explicit),//
                    (ISPO) new SPO(p.mike.getIV(), p.rdfType.getIV(), p.foafPerson.getIV(),
                            StatementEnum.Explicit),//
                    (ISPO) new SPO(p.mike.getIV(), p.foafKnows.getIV(), p.bryan.getIV(),
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
            final Set<ISPO> expected, final IV startingVertex) throws Exception {

        final IGASEngine gasEngine = new BigdataGASEngine(sail.getDatabase()
                .getIndexManager(), 1/* nthreads */);

        try {

            final BigdataGraphAccessor graphAccessor = ((BigdataGASEngine) gasEngine)
                    .newGraphAccessor(sail.getDatabase().getNamespace(), sail
                            .getDatabase().getIndexManager()
                            .getLastCommitTime());

            final IGASContext<Set<ISPO>, Set<ISPO>, Set<ISPO>> gasContext = gasEngine
                    .newGASContext(graphAccessor, new MockGASProgram(
                            gatherEdges));

            final IGASState<Set<ISPO>, Set<ISPO>, Set<ISPO>> gasState = gasContext
                    .getGASState();
            
            // Initialize the froniter.
            gasState.init(startingVertex);

            // Do one round.
            gasContext.doRound(new GASStats());

            /*
             * Lookup the state for the starting vertex (this should be the only
             * vertex whose state was modified since we did only one round).
             */
            final Set<ISPO> actual = gasState.getState(startingVertex);

            // Verify against the expected state.
            assertSameEdges(expected, actual);

        } finally {

            gasEngine.shutdownNow();

        }

    }

}
