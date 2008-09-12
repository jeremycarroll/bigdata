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
/*
 * Created on Oct 29, 2007
 */

package com.bigdata.relation.rule.eval;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.service.ClientIndexView;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * Evaluation of an {@link IRule} using nested subquery (one or more JOINs plus
 * any {@link IElementFilter}s specified for the predicates in the tail or
 * {@link IConstraint}s on the {@link IRule} itself). The subqueries are formed
 * into tasks and submitted to an {@link ExecutorService}. The effective
 * parallelism is limited by the #of elements visited in a chunk for the first
 * join dimension, as only those subqueries will be parallelized. Subqueries for
 * 2nd+ join dimensions are run in the caller's thread to ensure liveness.
 * 
 * @todo Scale-out joins should be distributed. A scale-out join run with this
 *       task will use {@link ClientIndexView}s. All work (other than the
 *       iterator scan) will be performed on the client running the join.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NestedSubqueryWithJoinThreadsTask implements IStepTask {

    protected static final Logger log = Logger.getLogger(NestedSubqueryWithJoinThreadsTask.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /*
     * from the ctor.
     */
    protected final IRule rule;
    protected final IJoinNexus joinNexus;
    protected final IBuffer<ISolution[]> buffer;
    protected final RuleState ruleState;
    protected final RuleStats ruleStats;
    protected final int tailCount;

    /**
     * The maximum #of subqueries for the first join dimension that will be
     * issued in parallel. Use ZERO(0) to avoid the use of the
     * {@link #joinService} entirely and ONE (1) to submit a single task at a
     * time to the {@link #joinService}.
     */
    protected final int maxParallelSubqueries;

    /**
     * The {@link ExecutorService} to which parallel subqueries are submitted.
     */
    protected final ThreadPoolExecutor joinService;
    
    /**
     * 
     * @param rule
     *            The rule to be executed.
     * @param joinNexus
     *            The {@link IJoinNexus}.
     * @param buffer
     *            A thread-safe buffer onto which chunks of {@link ISolution}
     *            will be flushed during rule execution.
     */
    public NestedSubqueryWithJoinThreadsTask(final IRule rule,
            final IJoinNexus joinNexus, final IBuffer<ISolution[]> buffer) {

        if (rule == null)
            throw new IllegalArgumentException();

        if( joinNexus == null)
             throw new IllegalArgumentException();
        
        if (buffer == null)
            throw new IllegalArgumentException();

        this.rule = rule;
        
        this.joinNexus = joinNexus;
        
        this.buffer = buffer;

        this.ruleState = new RuleState(rule, joinNexus);

        // note: evaluation order is fixed by now.
        this.ruleStats = joinNexus.getRuleStatisticsFactory().newInstance(rule,
                ruleState.plan, ruleState.keyOrder);
        
        this.tailCount = rule.getTailCount();
        
        this.maxParallelSubqueries = joinNexus.getMaxParallelSubqueries();
        
        this.joinService = (ThreadPoolExecutor) (maxParallelSubqueries == 0 ? null
                : joinNexus.getIndexManager().getExecutorService());
        
    }
    
    /**
     * Recursively evaluate the subqueries.
     */
    final public RuleStats call() {

        if(DEBUG) {
            
            log.debug("begin:\nruleState=" + ruleState + "\nplan="
                    + ruleState.plan);
            
        }

        if (ruleState.plan.isEmpty()) {

            if (INFO)
                log.info("Rule proven to have no solutions.");
            
            return ruleStats;
            
        }
        
        final long begin = System.currentTimeMillis();

        final IBindingSet bindingSet = joinNexus.newBindingSet(rule);

        apply(0/* orderIndex */, bindingSet);

        ruleStats.elapsed += System.currentTimeMillis() - begin;

        if(INFO) {
            
            log.info("done:"
                    + "\nmaxParallelSubqueries="+maxParallelSubqueries
                    + "\nruleState=" + ruleState 
                    + ruleStats
//                    +"\nbufferClass="+buffer.getClass().getName()
                    );
            
        }
        
        return ruleStats;
        
    }
    
    /**
     * Return the index of the tail predicate to be evaluated at the given index
     * in the evaluation order.
     * 
     * @param orderIndex
     *            The evaluation order index.
     * @return The tail index to be evaluated at that index in the evaluation
     *         order.
     */
    final protected int getTailIndex(int orderIndex) {
        
        final int tailIndex = ruleState.order[orderIndex];
        
        assert orderIndex >= 0 && orderIndex < tailCount : "orderIndex="
                + orderIndex + ", rule=" + rule;
        
        return tailIndex;
        
    }
    
    /**
     * Evaluate a join dimension. A private <em>non-thread-safe</em>
     * {@link IBuffer} will allocated and used to buffer a chunk of results. The
     * buffer will be flushed when it overflows and regardless when
     * {@link #apply(int, IBindingSet)} is done. When flushed, it will emit a
     * single chunk onto the thread-safe {@link #buffer}.
     * 
     * @param orderIndex
     *            The current index in the evaluation order[] that is being
     *            scanned.
     * @param bindingSet
     *            The bindings from the prior join(s) (if any).
     */
    protected void apply(final int orderIndex, IBindingSet bindingSet) {
        
        /*
         * Per-subquery buffer : NOT thread-safe (for better performance).
         * 
         * Note: parallel subquery MUST use distinct buffer instances for each
         * task executing in parallel.
         * 
         * FIXME allocate chunkSize based on expected maxCardinality, or perhaps
         * the subqueryCount #of the left-hand join dimension divided into the
         * range count of the right hand join dimension (assuming that there is
         * a shared variable) with a floor 1000 elements and otherwise the
         * default from IJoinNexus? The goal is to reduce the churn in the
         * nursery.
         */
        final IBuffer<ISolution> tmp = joinNexus.newUnsynchronizedBuffer(
                buffer, joinNexus.getChunkCapacity());

        // run the (sub-)query.
        apply(orderIndex, bindingSet, tmp);

        // flush buffer onto the chunked buffer.
        tmp.flush();

    }
    
    /**
     * Evaluate a join dimension.
     * 
     * @param orderIndex
     *            The current index in the evaluation order[] that is being
     *            scanned.
     * @param bindingSet
     *            The bindings from the prior join(s) (if any).
     * @param buffer
     *            The buffer onto which {@link ISolution}s will be written.
     *            <p>
     *            Note: For performance reasons, this buffer is NOT thread-safe.
     *            Therefore parallel subquery MUST use distinct buffer instances
     *            each of which flushes a chunk at a time onto the thread-safe
     *            buffer specified to the ctor.
     */
    final protected void apply(final int orderIndex,
            final IBindingSet bindingSet, final IBuffer<ISolution> buffer) {

        // Obtain the iterator for the current join dimension.
        final IChunkedOrderedIterator itr = getAccessPath(orderIndex,
                bindingSet).iterator();
        
        try {

            final int tailIndex = getTailIndex(orderIndex);

            /*
             * Handles non-optionals and optionals with solutions in the
             * data.
             */
            
            final long solutionsBefore = ruleStats.solutionCount.get();
            
            while (itr.hasNext()) {

                if (orderIndex + 1 < tailCount) {

                    // Nested subquery.

                    final Object[] chunk;
                    if (reorderChunkToTargetOrder) {
                        
                        /*
                         * Re-order the chunk into the target order for the
                         * _next_ access path.
                         * 
                         * FIXME This imples that we also know the set of
                         * indices on which we need to read for a rule before we
                         * execute the rule. That knowledge should be captured
                         * and fed into the LDS and EDS/JDS rule execution logic
                         * in order to optimize JOINs.
                         */
                        
                        // target chunk order.
                        final IKeyOrder targetKeyOrder = ruleState.keyOrder[getTailIndex(orderIndex + 1)];

                        // Next chunk of results from the current access path.
                        chunk = itr.nextChunk(targetKeyOrder);
                        
                    } else {
                        
                        // Next chunk of results from the current access path.
                        chunk = itr.nextChunk();
                        
                    }

                    ruleStats.chunkCount[tailIndex]++;

                    // Issue the nested subquery.
                    runSubQueries(orderIndex, chunk, bindingSet, buffer);

                } else {

                    // bottomed out.
                    
                    /*
                     * Next chunk of results from that access path. The order of
                     * the elements in this chunk does not matter since this is
                     * the last join dimension.
                     */
                    final Object[] chunk = itr.nextChunk();

                    ruleStats.chunkCount[tailIndex]++;

                    // evaluate the chunk and emit any solutions.
                    emitSolutions(orderIndex, chunk, bindingSet, buffer);
                    
                }

            } // while

            final long nsolutions = ruleStats.solutionCount.get() - solutionsBefore;
            
            if (nsolutions == 0L) {
                
                applyOptional(orderIndex, bindingSet, buffer);
                
            }
            
        } finally {

            itr.close();

        }

    }

    /**
     * Method to be invoked IFF there were no solutions in the data that
     * satisified the constraints on the rule. If the tail is optional, then
     * subquery evaluation will simply skip the tail and proceed with the
     * successor of the tail in the evaluation order. If the tail is the last
     * tail in the evaluation order, then a solution will be emitted for the
     * binding set.
     * 
     * @param orderIndex
     *            The index into the evaluation order.
     * @param bindingSet
     *            The bindings from the prior join(s) (if any).
     * @param buffer
     *            A buffer onto which {@link ISolution}s will be written - this
     *            object is NOT thread-safe.
     */
    protected void applyOptional(final int orderIndex,
            final IBindingSet bindingSet, IBuffer<ISolution> buffer) {

        final int tailIndex = getTailIndex(orderIndex);
        
        if( rule.getTail(tailIndex).isOptional()) {
            
            if (orderIndex + 1 < tailCount) {

                // ignore optional with no solutions in the data.
                apply(orderIndex + 1, bindingSet, buffer);

            } else {

                // emit solution since last tail is optional.
                final ISolution solution = joinNexus.newSolution(rule,
                        bindingSet);

                ruleStats.solutionCount.incrementAndGet();

                buffer.add(solution);

            }

        }
        
    }
    
    /**
     * Return the {@link IAccessPath} for the tail predicate to be evaluated at
     * the given index in the evaluation order.
     * 
     * @param orderIndex
     *            The index into the evaluation order.
     * @param bindingSet
     *            The bindings from the prior join(s) (if any).
     * 
     * @return The {@link IAccessPath}.
     */
    protected IAccessPath getAccessPath(final int orderIndex,
            final IBindingSet bindingSet) {

        final int tailIndex = getTailIndex(orderIndex);

        final IPredicate predicate = rule.getTail(tailIndex)
                .asBound(bindingSet);

        final IAccessPath accessPath = joinNexus.getTailAccessPath(predicate);

        if (DEBUG) {

            log.debug("orderIndex=" + orderIndex + ", tailIndex=" + tailIndex
                    + ", tail=" + ruleState.rule.getTail(tailIndex)
                    + ", bindingSet=" + bindingSet + ", accessPath="
                    + accessPath);

        }

        return accessPath;

    }

    /**
     * Evaluate the right-hand side (aka the subquery) of the join for each
     * element in the chunk. This method will not return until all subqueries
     * for the chunk have been evaluated.
     * 
     * @param orderIndex
     *            The current index in the evaluation order.
     * @param chunk
     *            A chunk of elements from the left-hand side of the join.
     * @param bindingSet
     *            The bindings from the prior joins (if any).
     * @param buffer
     *            A buffer onto which {@link ISolution}s will be written - this
     *            object is NOT thread-safe.
     */
    protected void runSubQueries(final int orderIndex, final Object[] chunk,
            final IBindingSet bindingSet, IBuffer<ISolution> buffer) {
        
        /*
         * Note: At this stage all we want to do is build the subquery tasks.
         * For the _most_ part , the joinService should choose whether to run
         * the tasks in the caller's thread, start a new thread, or run leave
         * them on the work queue for a bit.
         * 
         * The CRITICAL exception is that if ALL workers wait on tasks in the
         * work queue then the JOIN will NOT progress.
         * 
         * This problem arises because of the control structure is recursion. An
         * element [e] from a chunk on the 1st join dimension can cause a large
         * number of tasks to be executed and the task for [e] is not complete
         * until those tasks are complete. This means that [e] is tying up a
         * worker thread while all nested subqueries for [e] are evaluated.
         * 
         * We work around this by only parallelizing subqueries for each element
         * in each chunk of the 1st join dimension. We can not simply let the
         * size of the thread pool grow without bound as it will (very) rapidly
         * exhaust the machine resources for some queries. Forcing the
         * evaluation of subqueries after the 1st join dimension in the caller's
         * thread ensures liveness while providing an effective parallelism up
         * to the minimum of {chunk size, the #of elements visited on the first
         * chunk, and the maximum size of the thread pool}.
         * 
         * Note: Many joins rapidly become more selective with a good evaluation
         * plan and there is an expectation that the #of results for the
         * subquery will be small. However, this is NOT always true. Some
         * queries will have a large fan out from the first join dimension.
         * 
         * Note: The other reason to force the subquery to run in the caller's
         * thread is when there are already too many threads executing
         * concurrently in this thread pool. (Note that this also reflects other
         * tasks that may be executing in parallel with this rule evaluation if
         * they are running on the same service).
         * 
         * Note: If there is only one element in the chunk then there is no
         * point in using the thread pool for the subquery. A lot of queries
         * rapidly become fully bound and therefore fall into this category.
         * Those subqueries are just run in the caller's thread.
         */
        
        if (maxParallelSubqueries==0 || orderIndex > 0 || chunk.length <= 1
//                || !useJoinService
//                || (orderIndex > 2 || joinService.getQueue().size() > 100)
                ) {
            
            /*
             * Force the subquery to run in the caller's thread (does not
             * allocate a task, just runs the subqueries directly).
             */
            
            runSubQueriesInCallersThread(orderIndex, chunk, bindingSet, buffer);
            
        } else {

            /*
             * Allocate a task for each subquery and queue it on the
             * joinService. The joinService will make a decision whether to run
             * the task in the caller's thread, to allocate a new thread, or to
             * let it wait on the work queue until a thread becomes available.
             */
            
            runSubQueriesOnThreadPool(orderIndex, chunk, bindingSet);
            
        }

    }

    /**
     * Determines whether we will sort the elements in a chunk into the natural
     * order for the index for the target join dimension.
     */
    private static final boolean reorderChunkToTargetOrder = true;
        
    /**
     * Runs the subquery in the caller's thread (this was the original
     * behavior).
     * 
     * @param orderIndex
     *            The current index in the evaluation order.
     * @param chunk
     *            A chunk of elements from the left-hand side of the join.
     * @param bindingSet
     *            The bindings from the prior joins (if any).
     * @param buffer
     *            A buffer onto which {@link ISolution}s will be written - this
     *            object is NOT thread-safe.
     */
    protected void runSubQueriesInCallersThread(final int orderIndex,
            final Object[] chunk, final IBindingSet bindingSet,
            final IBuffer<ISolution> buffer) {

        final int tailIndex = getTailIndex(orderIndex);
        
        for (Object e : chunk) {

            if (DEBUG) {
                log.debug("Considering: " + e.toString() + ", tailIndex="
                        + orderIndex + ", rule=" + rule.getName());
            }

            ruleStats.elementCount[tailIndex]++;

            /*
             * Then bind this statement, which propagates bindings to the next
             * predicate (if the bindings are rejected then the solution would
             * violate the constaints on the JOIN).
             */

            ruleState.clearDownstreamBindings(orderIndex + 1, bindingSet);

            if (ruleState.bind(tailIndex, e, bindingSet)) {

                // run the subquery.

                ruleStats.subqueryCount[tailIndex]++;

                apply(orderIndex + 1, bindingSet, buffer);

            }

        }

    }

    /**
     * Variant that creates a {@link SubqueryTask} for each element of the chunk
     * and submits those tasks to the {@link #joinService}. This method will
     * not return until all subqueries for the chunk have been evaluated.
     * <p>
     * The {@link #joinService} will decide for each task whether to allocate a
     * new thread, to run it on an existing thread, to leave it on the work
     * queue for a while, or to execute it in the caller's thread (the latter is
     * selected via a rejected exection handler option).
     * <p>
     * Note: This requires that we clone the {@link IBindingSet} so that each
     * parallel task will have its own state.
     * <p>
     * Note: The tasks should be executed (more or less) in order so as to
     * maximum the effect of ordered reads on the next join dimension.
     * 
     * @param orderIndex
     *            The current index in the evaluation order.
     * @param chunk
     *            A chunk of elements from the left-hand side of the join.
     * @param bindingSet
     *            The bindings from the prior joins (if any).
     * 
     * @todo Develop a pattern for feeding the {@link ExecutorService} such that
     *       it maintains at least N threads from a producer and no more than M
     *       threads overall. We need that pattern here for better sustained
     *       throughput and in the map/reduce system as well (it exists there
     *       but needs to be refactored and aligned with the simpler thread pool
     *       exposed by the {@link IIndexManager}).
     */
    protected void runSubQueriesOnThreadPool(final int orderIndex,
            final Object[] chunk, final IBindingSet bindingSet) {

        final int tailIndex = getTailIndex(orderIndex);

        /*
         * At most one task per element in this chunk, but never more than
         * [maxParallelSubqueries] tasks at once.
         * 
         * Note: We are not allowed to modify this while the tasks are being
         * executed so we create a new instance of the array each time we submit
         * some tasks for execution!
         */
        List<Callable<Void>> tasks = null;
        
        // #of elements remaining in the chunk.
        int nremaining = chunk.length;
        
        // for each element in the chunk.
        for (Object e : chunk) {

            if (DEBUG) {
                log.debug("Considering: " + e.toString() + ", tailIndex="
                        + orderIndex + ", rule=" + rule.getName());
            }

            ruleStats.elementCount[tailIndex]++;
            
            /*
             * Then bind this statement, which propagates bindings to the next
             * predicate (if the bindings are rejected then the solution would
             * violate the constaints on the JOIN).
             */

            // clone the binding set.
            final IBindingSet bset = bindingSet.clone();
            
            // Note: not necessary since we are cloning the bindingset.
//            ruleState.clearDownstreamBindings(orderIndex + 1, bindingSet);

            if (ruleState.bind(tailIndex, e, bset)) {

                // we will run this subquery.
                ruleStats.subqueryCount[tailIndex]++;

                if (tasks == null) {

                    // maximum #of subqueries to issue.
                    final int capacity = Math.min(maxParallelSubqueries,
                            nremaining);

                    // allocate for the new task(s).
                    tasks = new ArrayList<Callable<Void>>(capacity);
                    
                }
                
                // create a task for the subquery.
                tasks.add(new SubqueryTask<Void>(orderIndex + 1, bset));

                if (tasks.size() == maxParallelSubqueries) {

                    submitTasks(tasks);

                    tasks = null;
                    
                }

            }

            /*
             * Note: one more element from the chunk has been consumed. This is
             * true regardless of whether the binding was allowed and a subquery
             * resulted.
             */
            nremaining--;
            
        }

        if (tasks != null) {

            // submit any remaining tasks.

            submitTasks(tasks);
            
        }

    }
    
    /**
     * Submit subquery tasks, wait until they are done, and verify that all
     * tasks were executed without error.
     */
    protected void submitTasks(final List<Callable<Void>> tasks ) {
        
        if (tasks.isEmpty()) {
            
            // No tasks.
            return;
            
        }
        
        final List<Future<Void>> futures;
        try {

            // submit tasks and await completion of those tasks.
            futures = joinService.invokeAll(tasks);
            
            for(Future<Void> f : futures) {
                
                // verify that no task failed.
                f.get();
                
            }
            
        } catch (InterruptedException ex) {

            throw new RuntimeException("Terminated by interrupt", ex);

        } catch (ExecutionException ex) {

            throw new RuntimeException("Join failed: " + ex, ex);

        }

    }
    
    /**
     * This class is used when we want to evaluate the subqueries in parallel.
     * The inner task uses
     * {@link NestedSubqueryWithJoinThreadsTask#apply(int, IBindingSet, IBuffer)}
     * to evaluate a subquery.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class SubqueryTask<E> implements Callable<E> {
        
        protected final int orderIndex;
        protected final IBindingSet bindingSet;
        
        public SubqueryTask(final int orderIndex, final IBindingSet bindingSet) {
            
            this.orderIndex = orderIndex;
            
            this.bindingSet = bindingSet;
            
        }
        
        /**
         * Run the subquery - it will use a private {@link IBuffer} for better
         * throughput and thread safety.
         */
        public E call() throws Exception {

            apply(orderIndex, bindingSet);

            return null;
            
        }
        
    }

    /**
     * Consider each element in the chunk in turn. If the element satisifies the
     * JOIN criteria, then emit an {@link ISolution} for the {@link IRule}.
     * 
     * @param orderIndex
     *            The index in the evaluation order.
     * @param chunk
     *            A chunk of elements from the right-most join dimension.
     * @param bindingSet
     *            The bindings from the prior joins.
     * @param buffer
     *            A buffer onto which the solutions will be written - this
     *            object is NOT thread-safe.
     */
    protected void emitSolutions(final int orderIndex, final Object[] chunk,
            final IBindingSet bindingSet, final IBuffer<ISolution> buffer) {

        final int tailIndex = getTailIndex(orderIndex);

        for (Object e : chunk) {

            if (DEBUG) {
                log.debug("Considering: " + e.toString() + ", orderIndex="
                        + orderIndex + ", rule=" + rule.getName());
            }

            ruleStats.elementCount[tailIndex]++;

            // bind variables from the current element.
            if (ruleState.bind(tailIndex, e, bindingSet)) {

                /*
                 * emit entailment
                 */

                if (DEBUG) {
                    log.debug("solution: " + bindingSet);
                }

                final ISolution solution = joinNexus.newSolution(rule,
                        bindingSet);

                ruleStats.solutionCount.incrementAndGet();

                buffer.add(solution);

            }

        }

    }

}
