/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Sep 25, 2009
 */

package com.bigdata.rdf.spo;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.concurrent.MappedTaskExecutor;

/**
 * Solution expander provides an efficient merged access path for the graphs in
 * the SPARQL default graph. This expander should only be used when a join is
 * run against the default graph and the default graph is non-empty (e.g., one
 * or more FROM clauses). The expander operates by merging the ordered results
 * from the access path iterator for each graph included in the set of graphs
 * comprising the SPARQL default graph. The context position of the visited
 * {@link ISPO}s is discarded (set to null). Duplicate triples are discarded.
 * The result is the distinct union of the access paths and hence provides a
 * view of the graphs in the default graph as if they had been merged according
 * to <a href="http://www.w3.org/TR/rdf-mt/#graphdefs>RDF Semantics</a>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultGraphSolutionExpander implements ISolutionExpander<ISPO> {

    protected static transient Logger log = Logger
            .getLogger(DefaultGraphSolutionExpander.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 3092400550324170339L;

    private final IJoinNexus joinNexus;
    
    /**
     * The set of graphs in the SPARQL DATASET's default graph. The {@link URI}
     * MUST have been resolved against the appropriate {@link LexiconRelation}
     * such that their term identifiers (when the exist) are known. If any term
     * identifier is {@link IRawTripleStore#NULL}, then the corresponding graph
     * does not exist and no access path will be queried for that graph.
     * However, a non-{@link IRawTripleStore#NULL} term identifier may also
     * identify a graph which does not exist, in which case an access path will
     * be created for that {@link URI}s but will no visit any data.
     */
    final Iterable<BigdataURI> defaultGraphs;

    /**
     * The caller SHOULD NOT use this expander when the default graph is known
     * to be empty as it will only impose unnecessary overhead. However, using
     * the expander makes sense even when there is a single graph in the default
     * graph since the expander will strip the context information from the
     * materialized {@link ISPO}s. If the caller can identify that some graph
     * URIs are not known to the database, then they may be safely removed from
     * the defaultGraphs. If this leaves an empty set, then no query against the
     * default graph can yield any data.
     * 
     * @param defaultGraphs
     *            The set of default graphs in the SPARQL DATASET.
     * 
     * @throws IllegalArgumentException
     *             if <i>defaultGraphs</i> is <code>null</code>.
     */
//    * @throws IllegalArgumentException
//    *             if <i>defaultGraphs</i> is empty (the caller should optimize
//    *             this expander out when the default graph is known to be
//    *             empty).
    public DefaultGraphSolutionExpander(final IJoinNexus joinNexus,
            final Iterable<BigdataURI> defaultGraphs) {

        if (joinNexus == null) {

            throw new IllegalArgumentException();
            
        }
        
        if (defaultGraphs == null) {

            /*
             * No data set?
             */

            throw new IllegalArgumentException();

        }

//        if (!defaultGraphs.iterator().hasNext()) {
//
//            /*
//             * The default graph is an empty graph. The caller should optimize
//             * out this expander when the default graph is known to be empty.
//             */
//
//            throw new IllegalArgumentException();
//
//        }

        this.joinNexus = joinNexus;
        
        this.defaultGraphs = defaultGraphs;
        
    }
    
    public boolean backchain() {
        
        return true;
        
    }

    public boolean runFirst() {

        return false;
        
    }

    /**
     * @throws IllegalArgumentException
     *             if the context position is bound.
     * 
     * 
     *             FIXME For scale-out this could place us onto a different
     *             shard and hence a different data service with the consequence
     *             that we wind up doing RMI for the access path. In order to
     *             avoid that we either need to rewrite the rule to use a nested
     *             query along the lines of:
     * 
     *             <pre>
     * DISTINCT (s,p,o)
     *  UNION
     *      SELECT s,p,o FROM g1
     *      SELECT s,p,o FROM g2
     *      ...
     *      SELECT s,p,o FROM gn
     * </pre>
     * 
     *             The alternative approach for scale-out is to add a filter so
     *             that only the specific context is accepted. This filter MUST
     *             applied for ALL possible contexts (or all on that shard) so
     *             we only run the access path once rather than once per
     *             context.
     */
    public IAccessPath<ISPO> getAccessPath(final IAccessPath<ISPO> accessPath) {

        if (accessPath == null)
            throw new IllegalArgumentException();

        if(!accessPath.getPredicate().get(3).isVar()) {

            // the context position should not be bound.
            throw new IllegalArgumentException();
            
        }
        
        if(!(accessPath instanceof SPOAccessPath)) {
            
            // The logic relies on wrapping an SPOAccessPath, at least for now.
            throw new IllegalArgumentException();
            
        }

        // @todo should we check accessPath.isEmpty() here?

        return new DefaultGraphAccessPath((SPOAccessPath) accessPath);

        /*
         * FIXME scale-out or high-volumn visitation alternative : filter all
         * contexts at once.
         * 
         * Constrain the access path by adding a filter on the context position.
         * 
         * Note: One advantage of this approach is that it does not require us
         * to use a remote read on a different shard. This approach may also
         * perform better when reading against a significant fraction of the KB,
         * e.g., 20% or better.
         * 
         * Note: When using this code path, rangeCount(false) will overestimate
         * the count because it is not using the iterator and applying the
         * filter and therefore will see all contexts, not just the one
         * specified by [c].
         */
//        final Set<Long> contextSet = ....; // set of contexts we will accept.
//        final SPOPredicate p = ((SPOPredicate) getPredicate())
//                .setConstraint(new SPOFilter() {
//                    private static final long serialVersionUID = 1L;
//                    public boolean accept(final ISPO spo) {
//                        return contextSet.contains(spo.c());
//                    }
//                });
        /*
         * This will wind up assigning the same index since we have not
         * changed the bindings on the predicate, only made the filter
         * associated with the predicate more restrictive.
         */
//        return (SPOAccessPath) accessPath.getRelation().getAccessPath(p);

    }

    /**
     * Inner class evaluates the access path for each context specified in the
     * {@link DefaultGraphSolutionExpander#defaultGraphs}, discarding the
     * context argument for each {@link ISPO}, and filtering out duplicate
     * triples based on their (s,p,o) term identifiers. This implementation
     * evaluates the access paths sequentially and uses an internal hash set to
     * filter out duplicates.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private final class DefaultGraphAccessPath implements IAccessPath<ISPO> {

        private final long timeout = Long.MAX_VALUE;

        private final TimeUnit unit = TimeUnit.SECONDS;

        private final int maxParallel;

        private final MappedTaskExecutor executor;

        private final SPOAccessPath accessPath;

        public String toString() {

            return super.toString() + "{baseAccessPath="
                    + accessPath.toString() + "}";

        }
        
        /**
         * @param accessPath
         */
        public DefaultGraphAccessPath(final SPOAccessPath accessPath) {

            this.accessPath = accessPath;

            this.executor = new MappedTaskExecutor(joinNexus.getIndexManager()
                    .getExecutorService());

            /*
             * FIXME maxParallelSubqueries is zero for pipeline joins, but we
             * want to use a non-zero value here anyway. Rather than declaring a
             * new parameter, change the pipeline joins so that the ignore this
             * parameter. Also note that a parallelism limitation is placed on
             * the ClientIndexView through the IBigdataClient.Options.
             */

            this.maxParallel = joinNexus.getMaxParallelSubqueries();

        }

        public IIndex getIndex() {
 
            return accessPath.getIndex();
            
        }

        public IKeyOrder<ISPO> getKeyOrder() {

            return accessPath.getKeyOrder();
            
        }

        public IPredicate<ISPO> getPredicate() {

            return accessPath.getPredicate();
            
        }

        public boolean isEmpty() {

            final IChunkedOrderedIterator<ISPO> itr = iterator(0L/* offset */,
                    1/* limit */, 1/* capacity */);            
            
            try {
                
                return !itr.hasNext();
                
            } finally {
            
                itr.close();
                
            }
            
        }

        public ITupleIterator<ISPO> rangeIterator() {

            return null;
            
        }

        public long removeAll() {

            throw new UnsupportedOperationException();
            
        }
        
        public IChunkedOrderedIterator<ISPO> iterator() {
            
            return iterator(0L/* offset */, 0L/* limit */, 0/* capacity */);
            
        }

        public IChunkedOrderedIterator<ISPO> iterator(final int limit,
                final int capacity) {

            return iterator(0L/* offset */, limit, capacity);

        }

        /**
         * This is the common entry point for all iterator implementations.
         * 
         * @todo Alternative implementation using limited parallel evaluation of
         *       the access paths to reduce latency and a hash set to filter out
         *       the duplicates.
         * 
         * @todo initialCapacity for the hash sets?
         * 
         * @todo Alternative implementations based on filtering the distinct
         *       triples using a {@link BTree} with an (s,p,o) key to filter
         *       duplicates since that can spill onto the disk and handle larger
         *       result sets (e.g., a {@link TempTripleStore} with only the SPO
         *       index and no lexicon).
         * 
         * @todo Alternative implementation using fully parallel evaluation of
         *       the access paths and a merge sort to combine chunks drawn from
         *       each access path, and then an iterator which skips over
         *       duplicates by considering the last returned (s,p,o). We need
         *       to: (a) allocate a buffer each time we draw from the current
         *       chunks based on the total size of the current chunks; and (b)
         *       we can only draw keys from the current chunks up to the
         *       min(nextKey) for each chunk. The min(nextKey) constraint is
         *       necessary to ensure that a merge sort will get rid of
         *       duplicates. Without that constraint it is possible that a
         *       latter chunk from some access path will report an (s,p,o) that
         *       has already be visited. (The constraint allows us to use a
         *       closed world assumption to filter duplicates after the merge
         *       sort.)
         * 
         * @todo Performance comparisons among these implementations and logic
         *       to choose the right implementation for the combination of the
         *       deployment, the cardinality of the default graph set size, and
         *       the scale of the data.
         */
        @SuppressWarnings("unchecked")
        public IChunkedOrderedIterator<ISPO> iterator(final long offset,
                final long limit, final int capacity) {

            final ICloseableIterator<ISPO> src = new InnerIterator1(offset,
                    limit, capacity);

            if(src instanceof IChunkedOrderedIterator) {
                
                return (IChunkedOrderedIterator<ISPO>)src;

            }
            
            return new ChunkedWrappedIterator<ISPO>(src);
            
        }

        /**
         * Iterator implementation based on limited parallelism over the
         * iterators for the {@link IAccessPath} associated with each graph in
         * the default graphs set and using a hash map to filter out duplicate
         * (s,p,o) tuples.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        private class InnerIterator1 implements ICloseableIterator<ISPO> {

            private final long offset;

            private final long limit;

            private final int capacity;

            /**
             * @todo buffer chunks of {@link #ISPO}s for more efficiency and
             *       better alignment with the chunked source iterators? The
             *       only issue is that {@link #hasNext()} will have to maintain
             *       a chunk of known distinct tuples to be visited.
             */
            private final BlockingBuffer<ISPO> buffer;
            
            /**
             * The source iterator.
             */
            private final IAsynchronousIterator<ISPO> src;
            
            /**
             * The set of distinct {@link ISPO}s that have been accepted by
             * {@link #hasNext()}, which is responsible for pulling the {@link #next()}
             * {@link ISPO} from the #src iterator.
             */
            private final Set<ISPO> set;

            /**
             * The next element to be visited or <code>null</code> if we need to
             * scan ahead.
             */
            private ISPO next = null;

            /**
             * <code>true</code> iff the iterator has been proven to be
             * exhausted.
             */
            private boolean exhausted = false;

            /**
             * @param offset
             * @param limit
             * @param capacity
             */
            public InnerIterator1(final long offset, final long limit,
                    final int capacity) {

                this.offset = offset;

                this.limit = limit;
                
                this.capacity = capacity;

                this.set = new HashSet<ISPO>();

                this.buffer = new BlockingBuffer<ISPO>(joinNexus
                        .getChunkCapacity());

                Future<Void> future = null;
                try {

                    /*
                     * Note: We do NOT get() this Future. This task will run
                     * asynchronously.
                     * 
                     * The Future is canceled IF (hopefully WHEN) the iterator
                     * is closed.
                     * 
                     * If the task itself throws an error, then it will use
                     * buffer#abort(cause) to notify the buffer of the cause (it
                     * will be passed along to the iterator) and to close the
                     * buffer (the iterator will notice that the buffer has been
                     * closed as well as that the cause was set on the buffer).
                     */

                    // run the task.
                    future = joinNexus.getIndexManager().getExecutorService()
                            .submit(newRunIteratorsTask(buffer));

                    // set the future on the BlockingBuffer.
                    buffer.setFuture(future);

                    // save reference to the asynchronous iterator.
                    src = buffer.iterator();

                } catch (Throwable ex) {

                    try {

                        buffer.close();

                        if (future != null) {

                            future.cancel(true/* mayInterruptIfRunning */);

                        }

                    } catch (Throwable t) {

                        log.error(t, t);

                    }

                    throw new RuntimeException(ex);
                    
                }

            }

            public void close() {

                /*
                 * Close the iterator, interrupting the running task if
                 * necessary.
                 */
                
                src.close();
                
            }

            /**
             * Returns immediately if there is an element waiting. Otherwise,
             * scans ahead until it finds an element which has not already been
             * visited. It then add the element to the set of elements already
             * seen and saves a reference to that element to be returned by
             * {@link #next()}.
             */
            public boolean hasNext() {

                if (exhausted)
                    return false;
                
                if (next != null)
                    return true;
                
                while (next == null && src.hasNext()) {

                    ISPO tmp = src.next();

                    /*
                     * Strip off the context position.
                     * 
                     * Note: distinct is enforced on (s,p,o). By stripping off
                     * the context and statement type information first, we
                     * ensure that (s,p,o) duplicates will be recognized as
                     * such.
                     * 
                     * @todo Notice that this approach requires us to discard
                     * the statement type metadata. The merge sort approach
                     * could retain that metadata, returning the maximum over
                     * the statement types for duplicates (always promoting to
                     * explicit if any of the duplicates are explicit, and
                     * otherwise to axiom if any of the duplicates are axioms).
                     */
                    tmp = new SPO(tmp.s(), tmp.p(), tmp.o(),
                            IRawTripleStore.NULL);
                    
                    if(set.contains(tmp)) {
                        
                        continue;
                        
                    }
                    
                    set.add(tmp);

                    next = tmp; 

                }
                
                if(next == null) {
                    
                    exhausted = true;
                    
                }
                
                return next != null;
                
            }

            public ISPO next() {

                if (!hasNext())
                    throw new NoSuchElementException();

                final ISPO tmp = next;

                next = null;

                return tmp;
                
            }

            public void remove() {
                
                throw new UnsupportedOperationException();
                
            }

            /**
             * Return task which will submit tasks draining the iterators for
             * each access path onto the caller's buffer.
             * 
             * @param buffer
             *            The elements drained from the iterators will be added
             *            to this buffer.
             * 
             * @return The task whose future is set on the buffer.
             * 
             * @todo if the outer task is interrupted, will that interrupt
             *       propagate to the inner tasks? If not, then use a volatile
             *       boolean halted pattern here.
             */
            private Callable<Void> newRunIteratorsTask(
                    final BlockingBuffer<ISPO> buffer) {

                return new Callable<Void>() {

                    /**
                     * Outer callable submits tasks for execution.
                     */
                    public Void call() throws Exception {

                        final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

                        for (BigdataURI g : defaultGraphs) {

                            final long termId = g.getTermId();

                            if (termId == IRawTripleStore.NULL) {

                                // unknown URI means no data for that graph.
                                continue;

                            }

                            tasks.add(new DrainIteratorTask(termId));

                        }

                        try {

                            if (log.isDebugEnabled())
                                log.debug("Running " + tasks.size() + " tasks");
                            
                            executor
                                    .runTasks(tasks, timeout, unit, maxParallel);

                        } catch (Throwable ex) {

                            throw new RuntimeException(ex);

                        } finally {

                            // nothing more can be written onto the buffer.
                            buffer.close();
                            
                        }

                        return null;

                    }

                };

            }

            /**
             * Inner callable runs an iterator for a specific access path,
             * draining the iterator onto the blocking buffer.
             */
            private final class DrainIteratorTask implements Callable<Void> {

                final long termId;

                public DrainIteratorTask(final long termId) {

                    this.termId = termId;

                }

                public Void call() throws Exception {

                    if (log.isDebugEnabled())
                        log.debug("Running iterator: c=" + termId);

                    final IAccessPath<ISPO> ap = bindContext(termId);

                    final IChunkedOrderedIterator<ISPO> itr = ap.iterator(
                            offset, limit, capacity);

                    try {

                        long n = 0;

                        while (itr.hasNext()) {

                            // @todo chunk at a time processing.
                            buffer.add(itr.next());
                            
                            n++;

                        }

                        if (log.isDebugEnabled())
                            log.debug("Ran iterator: c=" + termId
                                    + ", nvisited=" + n);
                        
                    } finally {

                        itr.close();

                    }

                    return null;

                }

            } // class DrainIteratorTask

        } // class InnerIterator

        /**
         * FIXME cache the range counts for reuse.
         */
        public long rangeCount(final boolean exact) {
            
            if (exact) {

                /*
                 * Exact range count.
                 * 
                 * Note: The only way to get the exact range count is to run the
                 * iterator so we can decide which triples are duplicates by the
                 * graphs in the default graphs set.
                 * 
                 * How efficient this is therefore depends directly on how the
                 * iterator is implemented.
                 */
                
                final IChunkedOrderedIterator<ISPO> itr = iterator();

                long n = 0;

                try {

                    while(itr.hasNext()) {
                        
                        itr.next();
                        
                        n++;
                        
                    }
                    
                } finally {

                    itr.close();

                }

                return n;

            } else {

                /*
                 * Estimated range count.
                 * 
                 * Take the sum over the estimated range count of each access
                 * path using limited (chunked) parallel evaluation since there
                 * could be a lot of graphs in the default graph. While the sum
                 * can overestimate (it presumes that there are no duplicates),
                 * but using max can underestimate since it presumes that all
                 * tuples from distinct graphs are duplicates.
                 */
             
                final List<Callable<Long>> tasks = new LinkedList<Callable<Long>>();
                
                for(BigdataURI g : defaultGraphs) {

                    final long termId = g.getTermId();

                    if (termId == IRawTripleStore.NULL) {
                        
                        // unknown URI means no data for that graph.
                        continue;
                        
                    }

                    tasks.add(new RangeCountTask(termId));
                 
                }

                try {
                    
                    final List<Long> counts = executor.runTasks(tasks, timeout,
                            unit, maxParallel);

                    long n = 0;

                    for (Long c : counts) {

                        n += c.longValue();

                    }

                    return n;
                    
                } catch (Throwable ex) {
                    
                    throw new RuntimeException(ex);
                    
                }

            }

        }

        /**
         * Fast range count for the access path for a graph.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        private class RangeCountTask implements Callable<Long> {

            private final long c;

            /**
             * 
             * @param c
             *            The graph identifier.
             */
            public RangeCountTask(final long c) {

                this.c = c;

            }

            public Long call() throws Exception {

                final IAccessPath<ISPO> ap = bindContext(c);

                return Long.valueOf(ap.rangeCount(false/* exact */));

            }

        }

        /**
         * Return a new {@link SPOAccessPath} where the context position has
         * been bound to the specified constant. This is used to constrain an
         * access path to each graph in the set of default graphs when
         * evaluating a SPARQL query against the "default graph".
         * <p>
         * Note: The added constraint may mean that a different index provides
         * more efficient traversal.
         * 
         * @param c
         *            The context term identifier.
         * 
         * @return The constrained {@link IAccessPath}.
         */
        public SPOAccessPath bindContext(final long c) {

            if (c == IRawTripleStore.NULL) {

                // or return EmptyAccessPath.
                throw new IllegalArgumentException();

            }

            /*
             * Constrain the access path by setting the context position on its
             * predicate.
             * 
             * Note: This option will always do better when you are running
             * against local data (LocalTripleStore).
             */

            final SPOPredicate p = ((SPOPredicate) getPredicate())
                    .setC(new Constant<Long>(Long.valueOf(c)));

            /*
             * Let the relation figure out which access path is best given that
             * added constraint.
             */

            return (SPOAccessPath) accessPath.getRelation().getAccessPath(p);

        }

    } // class DefaultGraphAccessPath

}
