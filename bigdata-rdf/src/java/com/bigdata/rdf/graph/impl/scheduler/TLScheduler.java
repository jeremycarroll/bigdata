package com.bigdata.rdf.graph.impl.scheduler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.rdf.graph.IGASScheduler;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.graph.impl.util.GASImplUtil;
import com.bigdata.rdf.graph.impl.util.IArraySlice;
import com.bigdata.rdf.graph.impl.util.ManagedArray;
import com.bigdata.rdf.graph.impl.util.MergeSortIterator;
import com.bigdata.rdf.internal.IV;

/**
 * This scheduler uses thread-local buffers ({@link LinkedHashSet}) to track the
 * distinct vertices scheduled by each execution thread. After the computation
 * round, those per-thread segments of the frontier are combined into a single
 * global, compact, and ordered frontier. To maximize the parallel activity, the
 * per-thread frontiers are sorted using N threads (one per segment). Finally,
 * the frontier segments are combined using a {@link MergeSortIterator} - this
 * is a sequential step with a linear cost in the size of the frontier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("rawtypes")
public class TLScheduler implements IGASSchedulerImpl {

    /**
     * Class bundles a reusable, extensible array for sorting the thread-local
     * frontier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class MySTScheduler extends STScheduler {

        /**
         * This is used to sort the thread-local frontier (that is, the frontier
         * for a single thread). The backing array will grow as necessary and is
         * reused in each round.
         */
        private final ManagedArray<IV> tmp;

        public MySTScheduler(final GASEngine gasEngine) {

            super(gasEngine);

            tmp = new ManagedArray<IV>(IV.class, 64);

        }
        
    } // class MySTScheduler
    
    private final GASEngine gasEngine;
    private final int nthreads;
    private final ConcurrentHashMap<Long/* threadId */, MySTScheduler> map;

    public TLScheduler(final GASEngine gasEngine) {

        this.gasEngine = gasEngine;

        this.nthreads = gasEngine.getNThreads();

        this.map = new ConcurrentHashMap<Long, MySTScheduler>(
                nthreads/* initialCapacity */, .75f/* loadFactor */, nthreads);

    }

    private IGASScheduler threadLocalScheduler() {

        final Long id = Thread.currentThread().getId();

        MySTScheduler s = map.get(id);

        if (s == null) {

            final IGASScheduler old = map.putIfAbsent(id, s = new MySTScheduler(
                    gasEngine));

            if (old != null) {

                /*
                 * We should not have a key collision since this is based on the
                 * threadId.
                 */

                throw new AssertionError();

            }

        }

        return s;

    }

    @Override
    public void schedule(final IV v) {

        threadLocalScheduler().schedule(v);

    }

    @Override
    public void clear() {

        /*
         * Clear the per-thread maps, but do not discard. They will be reused in
         * the next round.
         */
        for (STScheduler s : map.values()) {

            s.clear();

        }

    }

    @Override
    public void compactFrontier(final IStaticFrontier frontier) {

        /*
         * Extract a sorted, compact frontier from each thread local frontier.
         */
        @SuppressWarnings("unchecked")
        final IArraySlice<IV>[] frontiers = new IArraySlice[nthreads];

        int nsources = 0;
        int nvertices = 0;
        {
            final List<Callable<IArraySlice<IV>>> tasks = new ArrayList<Callable<IArraySlice<IV>>>(
                    nthreads);

            for (MySTScheduler s : map.values()) {
                final MySTScheduler t = s;
                tasks.add(new Callable<IArraySlice<IV>>() {
                    @Override
                    public IArraySlice<IV> call() throws Exception {
                        return GASImplUtil.compactAndSort(t.vertices, t.tmp);
                    }
                });

            }
            // invokeAll() - futures will be done() before it returns.
            final List<Future<IArraySlice<IV>>> futures;
            try {
                futures = gasEngine.getGASThreadPool().invokeAll(tasks);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            for (Future<IArraySlice<IV>> f : futures) {

                try {
                    final IArraySlice<IV> b = frontiers[nsources] = f.get();
                    nvertices += b.len();
                    nsources++;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }

            }
        }

        // // Clear the new frontier.
        // frontier.clear();

        if (nsources == 0) {

            /*
             * The new frontier is empty.
             */

            frontier.resetFrontier(0/* minCapacity */, true/* ordered */,
                    GASImplUtil.EMPTY_VERTICES_ITERATOR);

            return;

        }

        if (nsources > nthreads) {

            /*
             * nsources could be LT nthreads if we have a very small frontier,
             * but it should never be GTE nthreads.
             */

            throw new AssertionError("nsources=" + nsources + ", nthreads="
                    + nthreads);

        }

        /*
         * Now merge sort those arrays and populate the new frontier.
         */
        mergeSortSourcesAndSetFrontier(nsources, nvertices, frontiers, frontier);

    }

    /**
     * Now merge sort the ordered frontier segments and populate the new
     * frontier.
     * 
     * @param nsources
     *            The #of frontier segments.
     * @param nvertices
     *            The total #of vertice across those segments (may double-count
     *            across segments).
     * @param frontiers
     *            The ordered, compact frontier segments
     * @param frontier
     *            The new frontier to be populated.
     */
    private void mergeSortSourcesAndSetFrontier(final int nsources,
            final int nvertices, final IArraySlice<IV>[] frontiers,
            final IStaticFrontier frontier) {

        // wrap IVs[] as Iterators.
        @SuppressWarnings("unchecked")
        final Iterator<IV>[] itrs = new Iterator[nsources];

        for (int i = 0; i < nsources; i++) {

            itrs[i] = frontiers[i].iterator();

        }

        // merge sort of those iterators.
        final Iterator<IV> itr = new MergeSortIterator(itrs);

        frontier.resetFrontier(nvertices/* minCapacity */, true/* ordered */,
                itr);

    }

}