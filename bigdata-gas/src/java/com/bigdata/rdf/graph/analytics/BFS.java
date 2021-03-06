/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.analytics;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.FrontierEnum;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IReducer;
import com.bigdata.rdf.graph.impl.BaseGASProgram;

import cutthecrap.utils.striterators.IStriterator;

/**
 * Breadth First Search (BFS) is an iterative graph traversal primitive. The
 * frontier is expanded iteratively until no new vertices are discovered. Each
 * visited vertex is marked with the round (origin ZERO) in which it was
 * visited. This is its distance from the initial frontier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BFS extends BaseGASProgram<BFS.VS, BFS.ES, Void> {

//    private static final Logger log = Logger.getLogger(BFS.class);
    
    public static class VS {

        /**
         * <code>-1</code> until visited. When visited, set to the current round
         * in order to assign each vertex its traversal depth.
         * <p>
         * Note: It is possible that the same vertex may be visited multiple
         * times in a given expansion (from one or more source vertices that all
         * target the same destination vertex). However, in this case the same
         * value will be assigned by each visitor. Thus, synchronization is only
         * required for visibility of the update within the round. As long as
         * one thread reports that it modified the depth, the vertex will be
         * scheduled.
         */
        private final AtomicInteger depth = new AtomicInteger(-1);

        /**
         * The depth at which this vertex was first visited (origin ZERO) and
         * <code>-1</code> if the vertex has not been visited.
         */
        public int depth() {
//            synchronized (this) {
                return depth.get();
//            }
        }

        /**
         * Note: This marks the vertex at the current traversal depth.
         * 
         * @return <code>true</code> if the vertex was visited for the first
         *         time in this round and the calling thread is the thread that
         *         first visited the vertex (this helps to avoid multiple
         *         scheduling of a vertex).
         */
        public boolean visit(final int depth) {
            if (this.depth.compareAndSet(-1/* expect */, depth/* newValue */)) {
                // Scheduled by this thread.
                return true;
            }
            return false;
//            synchronized (this) {
//                if (this.depth == -1) {
//                    this.depth = depth;
//                    return true;
//                }
//                return false;
//            }
        }

        @Override
        public String toString() {
            return "{depth=" + depth() + "}";
        }

    }// class VS

    /**
     * Edge state is not used.
     */
    public static class ES {

    }

    private static final Factory<Value, BFS.VS> vertexStateFactory = new Factory<Value, BFS.VS>() {

        @Override
        public BFS.VS initialValue(final Value value) {

            return new VS();

        }

    };

    @Override
    public Factory<Value, BFS.VS> getVertexStateFactory() {

        return vertexStateFactory;

    }

    @Override
    public Factory<Statement, BFS.ES> getEdgeStateFactory() {

        return null;

    }

    @Override
    public FrontierEnum getInitialFrontierEnum() {

        return FrontierEnum.SingleVertex;
        
    }
    
    @Override
    public EdgesEnum getGatherEdges() {

        return EdgesEnum.NoEdges;

    }

    @Override
    public EdgesEnum getScatterEdges() {

        return EdgesEnum.OutEdges;

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to only visit the edges of the graph.
     */
    @Override
    public IStriterator constrainFilter(
            final IGASContext<BFS.VS, BFS.ES, Void> ctx, final IStriterator itr) {

        return itr.addFilter(getEdgeOnlyFilter(ctx));

    }

    /**
     * Not used.
     */
    @Override
    public void initVertex(final IGASContext<BFS.VS, BFS.ES, Void> ctx,
            final IGASState<BFS.VS, BFS.ES, Void> state, final Value u) {

        state.getState(u).visit(0);
        
    }
    
    /**
     * Not used.
     */
    @Override
    public Void gather(IGASState<BFS.VS, BFS.ES, Void> state, Value u, Statement e) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Not used.
     */
    @Override
    public Void sum(final IGASState<BFS.VS, BFS.ES, Void> state,
            final Void left, final Void right) {

        throw new UnsupportedOperationException();
        
    }

    /**
     * NOP
     */
    @Override
    public BFS.VS apply(final IGASState<BFS.VS, BFS.ES, Void> state, final Value u, 
            final Void sum) {

        return null;
        
    }

    /**
     * Returns <code>true</code>.
     */
    @Override
    public boolean isChanged(IGASState<VS, ES, Void> state, Value u) {

        return true;
        
    }

    /**
     * The remote vertex is scheduled for activation unless it has already been
     * visited.
     * <p>
     * Note: We are scattering to out-edges. Therefore, this vertex is
     * {@link Statement#getSubject()}. The remote vertex is
     * {@link Statement#getObject()}.
     */
    @Override
    public void scatter(final IGASState<BFS.VS, BFS.ES, Void> state,
            final IGASScheduler sch, final Value u, final Statement e) {

        // remote vertex state.
        final VS otherState = state.getState(e.getObject());

        // visit.
        if (otherState.visit(state.round() + 1)) {

            /*
             * This is the first visit for the remote vertex. Add it to the
             * schedule for the next iteration.
             */

            sch.schedule(e.getObject());

        }

    }

    @Override
    public boolean nextRound(final IGASContext<BFS.VS, BFS.ES, Void> ctx) {

        return true;
        
    }

    /**
     * Reduce the active vertex stat, returning a histogram reporting the #of
     * vertices at each distance from the starting vertex. There will always be
     * one vertex at depth zero - this is the starting vertex. For each
     * successive depth, the #of vertices that were labeled at that depth is
     * reported. This is essentially the same as reporting the size of the
     * frontier in each round of the traversal, but the histograph is reported
     * based on the vertex state.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     *         TODO Do another reducer that reports the actual BFS tree rather
     *         than a histogram. For each depth, it needs to have the set of
     *         vertices that are at that number of hops from the starting
     *         vertex. So, there is an outer map from depth to set. The inner
     *         set should also be concurrent if we allow concurrent reduction of
     *         the activated vertex state.
     */
    protected static class HistogramReducer implements
            IReducer<VS, ES, Void, Map<Integer, AtomicLong>> {

        private final ConcurrentHashMap<Integer, AtomicLong> values = new ConcurrentHashMap<Integer, AtomicLong>();

        @Override
        public void visit(final IGASState<VS, ES, Void> state, final Value u) {

            final VS us = state.getState(u);

            if (us != null) {

                final Integer depth = Integer.valueOf(us.depth());

                AtomicLong newval = values.get(depth);

                if (newval == null) {

                    final AtomicLong oldval = values.putIfAbsent(depth,
                            newval = new AtomicLong());

                    if (oldval != null) {

                        // lost data race.
                        newval = oldval;

                    }

                }

                newval.incrementAndGet();

            }

        }

        @Override
        public Map<Integer, AtomicLong> get() {

            return Collections.unmodifiableMap(values);
            
        }
        
    }
    
    @Override
    public void after(final IGASContext<BFS.VS, BFS.ES, Void> ctx) {

        final HistogramReducer r = new HistogramReducer();
        
        ctx.getGASState().reduce(r);

        class NV implements Comparable<NV> {
            public final int n;
            public final long v;
            public NV(final int n, final long v) {
                this.n = n;
                this.v = v;
            }
            @Override
            public int compareTo(final NV o) {
                if (o.n > this.n)
                    return -1;
                if (o.n < this.n)
                    return 1;
                return 0;
            }
        }

        final Map<Integer, AtomicLong> h = r.get();

        final NV[] a = new NV[h.size()];

        int i = 0;

        for (Map.Entry<Integer, AtomicLong> e : h.entrySet()) {

            a[i++] = new NV(e.getKey().intValue(), e.getValue().get());

        }

        Arrays.sort(a);

        System.out.println("distance, frontierSize, sumFrontierSize");
        long sum = 0L;
        for (NV t : a) {

            System.out.println(t.n + ", " + t.v + ", " + sum);

            sum += t.v;

        }

    }

}
