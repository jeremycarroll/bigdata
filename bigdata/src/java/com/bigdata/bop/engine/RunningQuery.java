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
 * Created on Aug 31, 2010
 */
package com.bigdata.bop.engine;

import java.rmi.RemoteException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bset.CopyBindingSetOp;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.util.concurrent.Haltable;

/**
 * Metadata about running queries.
 */
public class RunningQuery implements Future<Map<Integer,BOpStats>>, IRunningQuery {

    private final static transient Logger log = Logger
            .getLogger(RunningQuery.class);

    /**
     * Logger for the {@link ChunkTask}.
     */
    private final static Logger chunkTaskLog = Logger
            .getLogger(ChunkTask.class);

    /**
     * The run state of the query and the result of the computation iff it
     * completes execution normally (without being interrupted, cancelled, etc).
     */
    final private Haltable<Map<Integer,BOpStats>> future = new Haltable<Map<Integer,BOpStats>>();

    /**
     * The runtime statistics for each {@link BOp} in the query and
     * <code>null</code> unless this is the query controller.
     */
    final private ConcurrentHashMap<Integer/* bopId */, BOpStats> statsMap;

    /**
     * The class executing the query on this node.
     */
    final private QueryEngine queryEngine;

    /** The unique identifier for this query. */
    final private long queryId;

    /**
     * The query deadline. The value is the system clock time in milliseconds
     * when the query is due and {@link Long#MAX_VALUE} if there is no deadline.
     * In order to have a guarantee of a consistent clock, the deadline is
     * interpreted by the query controller.
     */
    final private AtomicLong deadline = new AtomicLong(Long.MAX_VALUE);

    /**
     * <code>true</code> iff the outer {@link QueryEngine} is the controller for
     * this query.
     */
    final private boolean controller;

    /**
     * The client executing this query (aka the query controller).
     * <p>
     * Note: The proxy is primarily for light weight RMI messages used to
     * coordinate the distributed query evaluation. Ideally, all large objects
     * will be transfered among the nodes of the cluster using NIO buffers.
     */
    final private IQueryClient clientProxy;

    /** The query. */
    final private BindingSetPipelineOp query;

    /**
     * The buffer used for the overall output of the query pipeline.
     * 
     * FIXME SCALEOUT: This should only exist on the query controller. Other
     * nodes will send {@link IChunkMessage}s to the query controller. s/o will
     * use an operator with {@link BOpEvaluationContext#CONTROLLER} in order to
     * ensure that the results are transferred to the query controller. When a
     * {@link SliceOp} is used, this is redundant. The operator in other cases
     * can be a {@link CopyBindingSetOp} whose {@link BOpEvaluationContext} has
     * been overridden.
     */
    final private IBlockingBuffer<IBindingSet[]> queryBuffer;

    /**
     * An index from the {@link BOp.Annotations#BOP_ID} to the {@link BOp}.
     */
    protected final Map<Integer, BOp> bopIndex;

    /**
     * A collection of {@link Future}s for currently executing operators for
     * this query.
     */
    private final ConcurrentHashMap<BSBundle, Future<?>> operatorFutures = new ConcurrentHashMap<BSBundle, Future<?>>();

    /**
     * A lock guarding {@link RunState#runningTaskCount},
     * {@link RunState#availableChunkCount},
     * {@link RunState#availableChunkCountMap}. This is <code>null</code> unless
     * this is the query controller.
     * 
     * @see RunState
     */
    private final ReentrantLock runStateLock;

    /**
     * The run state of this query and <code>null</code> unless this is the
     * query controller.
     */
    final private RunState runState;
    
    /**
     * The chunks available for immediate processing (they must have been
     * materialized).
     * <p>
     * Note: This is package private so it will be visible to the
     * {@link QueryEngine}.
     */
    final/* private */BlockingQueue<IChunkMessage<IBindingSet>> chunksIn = new LinkedBlockingDeque<IChunkMessage<IBindingSet>>();

    /**
     * Set the query deadline. The query will be cancelled when the deadline is
     * passed. If the deadline is passed, the query is immediately cancelled.
     * 
     * @param deadline
     *            The deadline.
     * @throws IllegalArgumentException
     *             if the deadline is non-positive.
     * @throws IllegalStateException
     *             if the deadline was already set.
     * @throws UnsupportedOperationException
     *             unless node is the query controller.
     */
    public void setDeadline(final long deadline) {

        if(!controller)
            throw new UnsupportedOperationException();
        
        if (deadline <= 0)
            throw new IllegalArgumentException();

        // set the deadline.
        if (!this.deadline
                .compareAndSet(Long.MAX_VALUE/* expect */, deadline/* update */)) {
            // the deadline is already set.
            throw new IllegalStateException();
        }

        if (deadline < System.currentTimeMillis()) {
            // deadline has already expired.
            cancel(true/* mayInterruptIfRunning */);
        }

    }

    /**
     * The class executing the query on this node.
     */
    public QueryEngine getQueryEngine() {
        
        return queryEngine;
        
    }

    /**
     * The client executing this query (aka the query controller).
     * <p>
     * Note: The proxy is primarily for light weight RMI messages used to
     * coordinate the distributed query evaluation. Ideally, all large objects
     * will be transfered among the nodes of the cluster using NIO buffers.
     */
    public IQueryClient getQueryController() {

        return clientProxy;
        
    }
    
    /**
     * The unique identifier for this query.
     */
    public long getQueryId() {
        
        return queryId;
        
    }

    /**
     * Return the operator tree for this query.
     */
    public BindingSetPipelineOp getQuery() {
        return query;
    }
    
    /**
     * Return <code>true</code> iff this is the query controller.
     */
    public boolean isController() {
    
        return controller;
        
    }

    /**
     * Return the current statistics for the query and <code>null</code> unless
     * this is the query controller. For {@link BindingSetPipelineOp} operator
     * which is evaluated there will be a single entry in this map.
     */
    public Map<Integer/*bopId*/,BOpStats> getStats() {
        
        return statsMap;
        
    }

    /**
     * 
     * @param queryId
     * @param begin
     * @param clientProxy
     * @param query
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>readTimestamp</i> is {@link ITx#UNISOLATED}
     *             (queries may not read on the unisolated indices).
     * @throws IllegalArgumentException
     *             if the <i>writeTimestamp</i> is neither
     *             {@link ITx#UNISOLATED} nor a read-write transaction
     *             identifier.
     */
    public RunningQuery(final QueryEngine queryEngine, final long queryId,
//            final long begin, 
            final boolean controller,
            final IQueryClient clientProxy, final BindingSetPipelineOp query
            ) {

        if (queryEngine == null)
            throw new IllegalArgumentException();

        if (clientProxy == null)
            throw new IllegalArgumentException();
        
        if (query == null)
            throw new IllegalArgumentException();
        
        this.queryEngine = queryEngine;

        this.queryId = queryId;
        
        this.controller = controller;
        
        this.clientProxy = clientProxy;
        
        this.query = query;
        
        this.bopIndex = BOpUtility.getIndex(query);
        
        this.statsMap = controller ? new ConcurrentHashMap<Integer, BOpStats>()
                : null;

        runStateLock = controller ? new ReentrantLock() : null;
        
        runState = controller ? new RunState(this) : null;
        
        this.queryBuffer = newQueryBuffer();
        
    }

    /**
     * Return the buffer on which the solutions will be written (if any). This
     * is based on the top-level operator in the query plan.
     * 
     * @return The buffer for the solutions -or- <code>null</code> if the
     *         top-level operator in the query plan is a mutation operator.
     */
    protected IBlockingBuffer<IBindingSet[]> newQueryBuffer() {

        if (query.isMutation())
            return null;

        return ((BindingSetPipelineOp) query).newBuffer();
        
    }

    /**
     * Take a chunk generated by some pass over an operator and make it
     * available to the target operator. How this is done depends on whether the
     * query is running against a standalone database or the scale-out database.
     * <p>
     * Note: The return value is used as part of the termination criteria for
     * the query.
     * <p>
     * The default implementation supports a standalone database. The generated
     * chunk is left on the Java heap and handed off synchronously using
     * {@link QueryEngine#add(IChunkMessage)}. That method will queue the chunk
     * for asynchronous processing.
     * 
     * @param sinkId
     *            The identifier of the target operator.
     * @param sink
     *            The intermediate results to be passed to that target operator.
     * 
     * @return The #of chunks made available for consumption by the sink. This
     *         will always be ONE (1) for scale-up. For scale-out, there will be
     *         one chunk per index partition over which the intermediate results
     *         were mapped.
     */
    protected <E> int handleOutputChunk(final int sinkId,
            final IBlockingBuffer<IBindingSet[]> sink) {

        /*
         * Note: The partitionId will always be -1 in scale-up.
         */
        final LocalChunkMessage<IBindingSet> chunk = new LocalChunkMessage<IBindingSet>(
                clientProxy, queryId, sinkId, -1/* partitionId */, sink
                        .iterator());

        queryEngine.acceptChunk(chunk);

        return 1;

    }   
    
    /**
     * Make a chunk of binding sets available for consumption by the query.
     * <p>
     * Note: this is invoked by {@link QueryEngine#acceptChunk(IChunkMessage)}
     * 
     * @param msg
     *            The chunk.
     */
    protected void acceptChunk(final IChunkMessage<IBindingSet> msg) {

        if (msg == null)
            throw new IllegalArgumentException();

        if (!msg.isMaterialized())
            throw new IllegalStateException();

        // verify still running.
        future.halted();

        // add chunk to be consumed.
        chunksIn.add(msg);

        if (log.isDebugEnabled())
            log.debug("queryId=" + queryId + ", chunksIn.size()="
                    + chunksIn.size() + ", msg=" + msg);

    }

    /**
     * The run state for the query.
     */
    static private class RunState {

        /**
         * The query.
         */
        private final RunningQuery query;

        /**
         * The query identifier.
         */
        private final long queryId;

        /**
         * The #of tasks for this query which have started but not yet halted
         * and ZERO (0) if this is not the query coordinator.
         * <p>
         * This is guarded by the {@link #runningStateLock}.
         */
        private long runningTaskCount = 0;

        /**
         * The #of chunks for this query of which a running task has made
         * available but which have not yet been accepted for processing by
         * another task and ZERO (0) if this is not the query coordinator.
         * <p>
         * This is guarded by the {@link #runningStateLock}.
         */
        private long availableChunkCount = 0;

        /**
         * A map reporting the #of chunks available for each operator in the
         * pipeline (we only report chunks for pipeline operators). The total
         * #of chunks available across all operators in the pipeline is reported
         * by {@link #availableChunkCount}.
         * <p>
         * The movement of the intermediate binding set chunks forms an acyclic
         * directed graph. This map is used to track the #of chunks available
         * for each bop in the pipeline. When a bop has no more incoming chunks,
         * we send an asynchronous message to all nodes on which that bop had
         * executed informing the {@link QueryEngine} on that node that it
         * should immediately release all resources associated with that bop.
         * <p>
         * This is guarded by the {@link #runningStateLock}.
         */
        private final Map<Integer/* bopId */, AtomicLong/* availableChunkCount */> availableChunkCountMap = new LinkedHashMap<Integer, AtomicLong>();

        /**
         * A collection reporting on the #of instances of a given {@link BOp}
         * which are concurrently executing.
         * <p>
         * This is guarded by the {@link #runningStateLock}.
         */
        private final Map<Integer/* bopId */, AtomicLong/* runningCount */> runningCountMap = new LinkedHashMap<Integer, AtomicLong>();

        /**
         * A collection of the operators which have executed at least once.
         * <p>
         * This is guarded by the {@link #runningStateLock}.
         */
        private final Set<Integer/* bopId */> startedSet = new LinkedHashSet<Integer>();

        public RunState(final RunningQuery query) {

            this.query = query;

            this.queryId = query.queryId;

        }

        public void startQuery(final IChunkMessage<?> msg) {

            query.lifeCycleSetUpQuery();
            
            final Integer bopId = Integer.valueOf(msg.getBOpId());
            
            availableChunkCount++;
            {
                AtomicLong n = availableChunkCountMap.get(bopId);
                if (n == null)
                    availableChunkCountMap.put(bopId, n = new AtomicLong());
                n.incrementAndGet();
            }

            if (log.isInfoEnabled())
                log.info("queryId=" + queryId + ",runningTaskCount="
                        + runningTaskCount + ",availableChunks="
                        + availableChunkCount);

            System.err.println("startQ : bopId=" + bopId + ",running="
                    + runningTaskCount + ",available=" + availableChunkCount);

        }

        public void startOp(final StartOpMessage msg) {

            final Integer bopId = Integer.valueOf(msg.bopId);

            runningTaskCount++;
            {
                AtomicLong n = runningCountMap.get(bopId);
                if (n == null)
                    runningCountMap.put(bopId, n = new AtomicLong());
                n.incrementAndGet();
                if (startedSet.add(bopId)) {
                    // first evaluation pass for this operator.
                    query.lifeCycleSetUpOperator(bopId);
                }
            }

            availableChunkCount -= msg.nchunks;

            {
                AtomicLong n = availableChunkCountMap.get(bopId);
                if (n == null)
                    throw new AssertionError();
                n.addAndGet(-msg.nchunks);
            }

            System.err.println("startOp: bopId=" + bopId + ",running="
                    + runningTaskCount + ",available=" + availableChunkCount
                    + ",fanIn=" + msg.nchunks);

            // check deadline.
            if (query.deadline.get() < System.currentTimeMillis()) {

                if (log.isTraceEnabled())
                    log.trace("expired: queryId=" + queryId + ", deadline="
                            + query.deadline);
                
                query.future.halt(new TimeoutException());
                
                query.cancel(true/* mayInterruptIfRunning */);
                
            }

        }

        /**
         * Update termination criteria counters.
         */
        public void haltOp(final HaltOpMessage msg) {

            // chunks generated by this task.
            final int fanOut = msg.sinkChunksOut + msg.altSinkChunksOut;
            availableChunkCount += fanOut;
            if (msg.sinkId != null) {
                AtomicLong n = availableChunkCountMap.get(msg.sinkId);
                if (n == null)
                    availableChunkCountMap
                            .put(msg.sinkId, n = new AtomicLong());
                n.addAndGet(msg.sinkChunksOut);
            }
            if (msg.altSinkId != null) {
                AtomicLong n = availableChunkCountMap.get(msg.altSinkId);
                if (n == null)
                    availableChunkCountMap.put(msg.altSinkId,
                            n = new AtomicLong());
                n.addAndGet(msg.altSinkChunksOut);
            }
            // one less task is running.
            runningTaskCount--;
            {
                final AtomicLong n = runningCountMap.get(msg.bopId);
                if (n == null)
                    throw new AssertionError();
                n.decrementAndGet();
            }
            // Figure out if this operator is done.
            if (isOperatorDone(msg.bopId)) {
                /*
                 * No more chunks can appear for this operator so invoke its end
                 * of life cycle hook.
                 */
                query.lifeCycleTearDownOperator(msg.bopId);
            }
            System.err.println("haltOp : bopId=" + msg.bopId + ",running="
                    + runningTaskCount + ",available=" + availableChunkCount
                    + ",fanOut=" + fanOut);
            assert runningTaskCount >= 0 : "runningTaskCount="
                    + runningTaskCount;
            assert availableChunkCount >= 0 : "availableChunkCount="
                    + availableChunkCount;
            if (log.isTraceEnabled())
                log.trace("bopId=" + msg.bopId + ",partitionId="
                        + msg.partitionId + ",serviceId="
                        + query.queryEngine.getServiceUUID() + ", nchunks="
                        + fanOut + " : runningTaskCount=" + runningTaskCount
                        + ", availableChunkCount=" + availableChunkCount);
            // test termination criteria
            if (msg.cause != null) {
                // operator failed on this chunk.
                log.error("Error: Canceling query: queryId=" + queryId
                        + ",bopId=" + msg.bopId + ",partitionId="
                        + msg.partitionId, msg.cause);
                query.future.halt(msg.cause);
                query.cancel(true/* mayInterruptIfRunning */);
            } else if (runningTaskCount == 0 && availableChunkCount == 0) {
                // success (all done).
                if (log.isTraceEnabled())
                    log.trace("success: queryId=" + queryId);
                query.future.halt(query.getStats());
                query.cancel(true/* mayInterruptIfRunning */);
            } else if (query.deadline.get() < System.currentTimeMillis()) {
                if (log.isTraceEnabled())
                    log.trace("expired: queryId=" + queryId + ", deadline="
                            + query.deadline);
                query.future.halt(new TimeoutException());
                query.cancel(true/* mayInterruptIfRunning */);
            }
        }

        /**
         * Return <code>true</code> the specified operator can no longer be
         * triggered by the query. The specific criteria are that no operators
         * which are descendants of the specified operator are running or have
         * chunks available against which they could run. Under those conditions
         * it is not possible for a chunk to show up which would cause the
         * operator to be executed.
         * 
         * @param bopId
         *            Some operator identifier.
         * 
         * @return <code>true</code> if the operator can not be triggered given
         *         the current query activity.
         * 
         * @throws IllegalMonitorStateException
         *             unless the {@link #runStateLock} is held by the caller.
         */
        protected boolean isOperatorDone(final int bopId) {

            return PipelineUtility.isDone(bopId, query.getQuery(),
                    query.bopIndex, runningCountMap, availableChunkCountMap);

        }

    } // class RunState

    /**
     * Invoked once by the query controller with the initial
     * {@link IChunkMessage} which gets the query moving.
     */
    void startQuery(final IChunkMessage<IBindingSet> msg) {

        if (!controller)
            throw new UnsupportedOperationException();
        
        if (msg == null)
            throw new IllegalArgumentException();
        
        if (msg.getQueryId() != queryId) // @todo equals() if queryId is UUID.
            throw new IllegalArgumentException();
        
        runStateLock.lock();
        
        try {
        
            runState.startQuery(msg);
            
            queryEngine.acceptChunk(msg);
            
        } finally {
            
            runStateLock.unlock();
            
        }

    }

    /**
     * Message provides notice that the operator has started execution and will
     * consume some specific number of binding set chunks.
     * 
     * @param msg The {@link StartOpMessage}.
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    public void startOp(final StartOpMessage msg) {

        if (!controller)
            throw new UnsupportedOperationException();
        
        runStateLock.lock();
        
        try {
        
            runState.startOp(msg);

        } finally {
            
            runStateLock.unlock();
            
        }
        
    }

    /**
     * Message provides notice that the operator has ended execution. The
     * termination conditions for the query are checked. (For scale-out, the
     * node node controlling the query needs to be involved for each operator
     * start/stop in order to make the termination decision atomic).
     * 
     * @param msg The {@link HaltOpMessage}
     * 
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    public void haltOp(final HaltOpMessage msg) {
        
        if (!controller)
            throw new UnsupportedOperationException();

        // update per-operator statistics.
        final BOpStats tmp = statsMap.putIfAbsent(msg.bopId, msg.taskStats);

        if (tmp != null)
            tmp.add(msg.taskStats);

        runStateLock.lock();
        
        try {
        
            runState.haltOp(msg);
            
        } finally {
            
            runStateLock.unlock();
            
        }
        
    }

    /**
     * Hook invoked the first time the given operator is evaluated for the
     * query. This may be used to set up life cycle resources for the operator,
     * such as a distributed hash table on a set of nodes identified by
     * annotations of the operator.
     * 
     * @param bopId
     *            The operator identifier.
     */
    protected void lifeCycleSetUpOperator(final int bopId) {
     
        System.err.println("lifeCycleSetUpOperator: queryId=" + queryId
                + ", bopId=" + bopId);

    }

    /**
     * Hook invoked the after the given operator has been evaluated for the
     * query for what is known to be the last time. This may be used to tear
     * down life cycle resources for the operator, such as a distributed hash
     * table on a set of nodes identified by annotations of the operator.
     * 
     * @param bopId
     *            The operator identifier.
     * 
     * @todo Prevent retrigger?  See {@link #cancel(boolean)}.
     */
    protected void lifeCycleTearDownOperator(final int bopId) {

        System.err.println("lifeCycleTearDownOperator: queryId=" + queryId
                + ", bopId=" + bopId);

    }

    /**
     * Hook invoked the before any operator is evaluated for the query. This may
     * be used to set up life cycle resources for the query.
     */
    protected void lifeCycleSetUpQuery() {

        System.err.println("lifeCycleSetUpQuery: queryId=" + queryId);

    }

    /**
     * Hook invoked when the query terminates. This may be used to tear down
     * life cycle resources for the query.
     */
    protected void lifeCycleTearDownQuery() {

        System.err.println("lifeCycleTearDownQuery: queryId=" + queryId);

    }
    
    /**
     * Return a {@link FutureTask} which will consume the binding set chunk. The
     * caller must run the {@link FutureTask}.
     * 
     * @param chunk
     *            A chunk to be consumed.
     */
    @SuppressWarnings("unchecked")
    protected FutureTask<Void> newChunkTask(
            final IChunkMessage<IBindingSet> chunk) {

        // create runnable to evaluate a chunk for an operator and partition.
        final Runnable r = new ChunkTask(chunk);

        // wrap runnable.
        final FutureTask<Void> f2 = new FutureTask(r, null/* result */);

        // add to list of active futures for this query.
        operatorFutures.put(new BSBundle(chunk.getBOpId(), chunk
                .getPartitionId()), f2);

        // return : caller will execute.
        return f2;

    }

    /**
     * Runnable evaluates an operator for some chunk of inputs. In scale-out,
     * the operator may be evaluated against some partition of a scale-out
     * index.
     */
    private class ChunkTask implements Runnable {

        /** Alias for the {@link ChunkTask}'s logger. */
        private final Logger log = chunkTaskLog;

        /** The index of the bop which is being evaluated. */
        private final int bopId;

        /**
         * The index partition against which the operator is being evaluated and
         * <code>-1</code> if the operator is not being evaluated against a
         * shard.
         */ 
        private final int partitionId;

        /** The operator which is being evaluated. */
        private final BOp bop;

        /**
         * The index of the operator which is the default sink for outputs
         * generated by this evaluation. This is the
         * {@link BOp.Annotations#BOP_ID} of the parent of this operator. This
         * will be <code>null</code> if the operator does not have a parent and
         * is not a query since no outputs will be generated in that case.
         */
        private final Integer sinkId;

        /**
         * The index of the operator which is the alternative sink for outputs
         * generated by this evaluation. This is <code>null</code> unless the
         * operator explicitly specifies an alternative sink using
         * {@link BindingSetPipelineOp.Annotations#ALT_SINK_REF}.
         */
        private final Integer altSinkId;

        /**
         * The sink on which outputs destined for the {@link #sinkId} operator
         * will be written and <code>null</code> if {@link #sinkId} is
         * <code>null</code>.
         */
        private final IBlockingBuffer<IBindingSet[]> sink;

        /**
         * The sink on which outputs destined for the {@link #altSinkId}
         * operator will be written and <code>null</code> if {@link #altSinkId}
         * is <code>null</code>.
         */
        private final IBlockingBuffer<IBindingSet[]> altSink;

        /**
         * The evaluation context for this operator.
         */
        private final BOpContext<IBindingSet> context;

        /**
         * {@link FutureTask} which evaluates the operator (evaluation is
         * delegated to this {@link FutureTask}).
         */
        private final FutureTask<Void> ft;

        /**
         * Create a task to consume a chunk. This looks up the {@link BOp} which
         * is the target for the message in the {@link RunningQuery#bopIndex},
         * creates the sink(s) for the {@link BOp}, creates the
         * {@link BOpContext} for that {@link BOp}, and wraps the value returned
         * by {@link PipelineOp#eval(BOpContext)} in order to handle the outputs
         * written on those sinks.
         * 
         * @param chunk
         *            A message containing the materialized chunk and metadata
         *            about the operator which will consume that chunk.
         */
        public ChunkTask(final IChunkMessage<IBindingSet> chunk) {
            bopId = chunk.getBOpId();
            partitionId = chunk.getPartitionId();
            bop = bopIndex.get(bopId);
            if (bop == null) {
                throw new NoSuchBOpException(bopId);
            }
            if (!(bop instanceof BindingSetPipelineOp)) {
                /*
                 * @todo evaluation of element[] pipelines needs to use pretty
                 * much the same code, but it needs to be typed for E[] rather
                 * than IBindingSet[].
                 * 
                 * @todo evaluation of Monet style BATs would also operate under
                 * different assumptions, closer to those of an element[].
                 */
                throw new UnsupportedOperationException(bop.getClass()
                        .getName());
            }
            
            // self
            final BindingSetPipelineOp op = ((BindingSetPipelineOp) bop);
            
            // parent (null if this is the root of the operator tree).
            final BOp p = BOpUtility.getParent(query, op);
            
            // sink (null unless parent is defined)
            sinkId = p == null ? null : (Integer) p
                    .getProperty(BindingSetPipelineOp.Annotations.BOP_ID);
            
            // altSink (null when not specified).
            altSinkId = (Integer) op
                    .getProperty(BindingSetPipelineOp.Annotations.ALT_SINK_REF);
            
            if (altSinkId != null && !bopIndex.containsKey(altSinkId))
                throw new NoSuchBOpException(altSinkId);
            
            if (altSinkId != null && sinkId == null) {
                throw new RuntimeException(
                        "The primary sink must be defined if the altSink is defined: "
                                + bop);
            }

            if (sinkId != null && altSinkId != null
                    && sinkId.intValue() == altSinkId.intValue()) {
                throw new RuntimeException(
                        "The primary and alternative sink may not be the same operator: "
                                + bop);
            }

            sink = (p == null ? queryBuffer : op.newBuffer());

            altSink = altSinkId == null ? null : op.newBuffer();
            
            // context
            context = new BOpContext<IBindingSet>(RunningQuery.this,
                    partitionId, op.newStats(), chunk.getChunkAccessor()
                            .iterator(), sink, altSink);

            // FutureTask for operator execution (not running yet).
            ft = op.eval(context);
            
        }

        /**
         * Evaluate the {@link IChunkMessage}.
         */
        public void run() {
            final UUID serviceId = queryEngine.getServiceUUID();
            int fanIn = 1;
            int sinkChunksOut = 0;
            int altSinkChunksOut = 0;
            try {
                clientProxy.startOp(new StartOpMessage(queryId,
                        bopId, partitionId, serviceId, fanIn));
                if (log.isDebugEnabled())
                    log.debug("Running chunk: queryId=" + queryId + ", bopId="
                            + bopId + ", bop=" + bop);
                ft.run(); // run
                ft.get(); // verify success
                if (sink != null && sink != queryBuffer && !sink.isEmpty()) {
                    /*
                     * Handle sink output, sending appropriate chunk
                     * message(s).
                     * 
                     * Note: This maps output over shards/nodes in s/o.
                     */
                    sinkChunksOut += handleOutputChunk(sinkId, sink);
                }
                if (altSink != null && altSink != queryBuffer
                        && !altSink.isEmpty()) {
                    /*
                     * Handle alt sink output , sending appropriate chunk
                     * message(s).
                     * 
                     * Note: This maps output over shards/nodes in s/o.
                     */
                    altSinkChunksOut += handleOutputChunk(altSinkId,
                            altSink);
                }
                clientProxy.haltOp(new HaltOpMessage(queryId, bopId,
                        partitionId, serviceId, null/* cause */,
                        sinkId, sinkChunksOut, altSinkId,
                        altSinkChunksOut, context.getStats()));
            } catch (Throwable t) {
                try {
                    clientProxy.haltOp(new HaltOpMessage(queryId,
                            bopId, partitionId, serviceId,
                            t/* cause */, sinkId, sinkChunksOut, altSinkId,
                            altSinkChunksOut, context.getStats()));
                } catch (RemoteException e) {
                    cancel(true/* mayInterruptIfRunning */);
                    log.error("queryId=" + queryId + ", bopId=" + bopId, e);
                }
            }

        } // run()
        
    } // class ChunkTask
    
    /**
     * Return an iterator which will drain the solutions from the query. The
     * query will be cancelled if the iterator is
     * {@link ICloseableIterator#close() closed}.
     */
    public IAsynchronousIterator<IBindingSet[]> iterator() {

        return queryBuffer.iterator();
        
    }

    /*
     * Future
     * 
     * Note: This is implemented using delegation to the Haltable so we can hook
     * various methods in order to clean up the state of a completed query.
     */

    public void halt() {

        cancel(true/* mayInterruptIfRunning */);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Cancelled queries :
     * <ul>
     * <li>must reject new chunks</li>
     * <li>must cancel any running operators</li>
     * <li>must not begin to evaluate operators</li>
     * <li>must release all of their resources</li>
     * <li>must not cause the solutions to be discarded before the client can
     * consume them.</li>
     * </ul>
     */
    final public boolean cancel(final boolean mayInterruptIfRunning) {
        // halt the query.
        boolean cancelled = future.cancel(mayInterruptIfRunning);
        // cancel any running operators for this query.
        for (Future<?> f : operatorFutures.values()) {
            if (f.cancel(mayInterruptIfRunning))
                cancelled = true;
        }
        if (queryBuffer != null) {
            // close the output sink.
            queryBuffer.close();
        }
        // life cycle hook for the end of the query.
        lifeCycleTearDownQuery();
        // remove from the collection of running queries.
        queryEngine.runningQueries.remove(queryId, this);
        // true iff we cancelled something.
        return cancelled;
    }

    final public Map<Integer, BOpStats> get() throws InterruptedException,
            ExecutionException {

        return future.get();
        
    }

    final public Map<Integer, BOpStats> get(long arg0, TimeUnit arg1)
            throws InterruptedException, ExecutionException, TimeoutException {
        
        return future.get(arg0, arg1);
        
    }

    final public boolean isCancelled() {
        
        return future.isCancelled();
        
    }

    final public boolean isDone() {

        return future.isDone();
        
    }

    public IBigdataFederation<?> getFederation() {
        
        return queryEngine.getFederation();
        
    }

    public IIndexManager getIndexManager() {
        
        return queryEngine.getIndexManager();
        
    }

}
