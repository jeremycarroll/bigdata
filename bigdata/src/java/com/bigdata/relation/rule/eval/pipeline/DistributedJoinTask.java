package com.bigdata.relation.rule.eval.pipeline;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.concurrent.NamedLock;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IRuleState;
import com.bigdata.relation.rule.eval.ISolution;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.AbstractDistributedFederation;
//BTM - PRE_CLIENT_SERVICE import com.bigdata.service.AbstractScaleOutFederation;
//BTM import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
//BTM import com.bigdata.service.IDataService;
import com.bigdata.service.Session;
import com.bigdata.striterator.IKeyOrder;

//BTM
import com.bigdata.service.ISession;
import com.bigdata.service.IService;
import com.bigdata.service.Service;
import com.bigdata.service.ShardManagement;
import com.bigdata.service.ShardService;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ScaleOutIndexManager;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.util.Util;
import java.rmi.server.ExportException;

/**
 * Implementation used by scale-out deployments. There will be one instance
 * of this task per index partition on which the rule will read. Those
 * instances will be in-process on the {@link ShardService} hosting that
 * index partition. Instances are created on the {@link ShardService} using
 * the {@link JoinTaskFactoryTask} helper class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DistributedJoinTask extends JoinTask {

    /**
     * When <code>true</code>, enables a trace on {@link System#err} of
     * the code polling the source {@link IAsynchronousIterator}s from
     * which this {@link DistributedJoinTask} draws its {@link IBindingSet}
     * chunks.
     */
    static private final boolean trace = false;

    /**
     * The federation is used to obtain locator scans for the access paths.
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE     final protected AbstractScaleOutFederation fed;
    private IIndexManager indexMgr;
    private ScaleOutIndexManager scaleOutIndexMgr;
    private ILocalResourceManagement localResourceMgr;
    private IBigdataDiscoveryManagement discoveryMgr;
    private final Session dataServiceSession;
    private final String dataServiceHostname;
    private final String dataServiceName;
//BTM - PRE_CLIENT_SERVICE - END

    /**
     * The {@link IJoinNexus} for the {@link IBigdataFederation}. This is
     * mainly used to setup the {@link #solutionBuffer} since it needs to
     * write on the scale-out index while the {@link AccessPathTask} will
     * read on the local index partition view.
     */
    final protected IJoinNexus fedJoinNexus;

    /**
     * A (proxy for) the {@link Future} for this {@link DistributedJoinTask}.
     */
    protected Future<Void> futureProxy;
    
    /**
     * @see IRuleState#getKeyOrder()
     */
    final private IKeyOrder[] keyOrders;

    /**
     * The name of the scale-out index associated with the next
     * {@link IPredicate} in the evaluation order and <code>null</code>
     * iff this is the last {@link IPredicate} in the evaluation order [logging only.]
     */
    final private String nextScaleOutIndexName;

    /**
     * Sources for {@link IBindingSet} chunks that will be processed by this
     * {@link JoinTask}. There will be one such source for each upstream
     * {@link JoinTask} that targets this {@link JoinTask}.
     * <p>
     * Note: This is a thread-safe collection since new sources may be added
     * asynchronously during processing.
     */
    final private Vector<IAsynchronousIterator<IBindingSet[]>> sources = new Vector<IAsynchronousIterator<IBindingSet[]>>();

    /**
     * <code>false</code> until all binding sets have been consumed and the
     * join task has made an atomic decision that it will not accept any new
     * sources. Note that the join task may still be consuming binding sets once
     * this flag is set - it is not necessarily done with its work, just no
     * willing to accept new {@link #sources}.
     * 
     * @todo rename as sourcesClosed
     */
    private boolean sourcesExhausted = false;
    
    /**
     * The {@link ShardService} on which this task is executing. This is used to
     * remove the entry for the task from {@link ShardService#getSession()}.
     */
//BTM    private final DataService dataService;

    /**
     * The {@link JoinTaskSink}s for the downstream
     * {@link DistributedJoinTask}s onto which the generated
     * {@link IBindingSet}s will be written. This is <code>null</code>
     * for the last join since we will write solutions onto the
     * {@link #getSolutionBuffer()} instead.
     * 
     * @todo configure capacity based on expectations of index partition
     *       fan-out for this join dimension
     */
    final private Map<PartitionLocator, JoinTaskSink> sinkCache;

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE     public DistributedJoinTask(
//BTM - PRE_CLIENT_SERVICE //            final String scaleOutIndexName,
//BTM - PRE_CLIENT_SERVICE             final IRule rule,//
//BTM - PRE_CLIENT_SERVICE             final IJoinNexus joinNexus,//
//BTM - PRE_CLIENT_SERVICE             final int[] order,//
//BTM - PRE_CLIENT_SERVICE             final int orderIndex,//
//BTM - PRE_CLIENT_SERVICE             final int partitionId,//
//BTM - PRE_CLIENT_SERVICE             final AbstractScaleOutFederation fed,//
//BTM - PRE_CLIENT_SERVICE             final IIndexManager indexManager,
//BTM - PRE_CLIENT_SERVICE             final ILocalResourceManagement localResourceManager,
//BTM - PRE_CLIENT_SERVICE             final IBigdataDiscoveryManagement discoveryManager,
//BTM - PRE_CLIENT_SERVICE             final IJoinMaster master,//
//BTM - PRE_CLIENT_SERVICE             final UUID masterUUID,//
//BTM - PRE_CLIENT_SERVICE             final IAsynchronousIterator<IBindingSet[]> src,//
//BTM - PRE_CLIENT_SERVICE             final IKeyOrder[] keyOrders,//
//BTM - PRE_CLIENT_SERVICE //BTM            final DataService dataService,//
//BTM - PRE_CLIENT_SERVICE             final Session dataServiceSession,
//BTM - PRE_CLIENT_SERVICE             final String dataServiceHostname,
//BTM - PRE_CLIENT_SERVICE             final String dataServiceName,
//BTM - PRE_CLIENT_SERVICE             final IVariable[][] requiredVars//
//BTM - PRE_CLIENT_SERVICE             ) {

    public DistributedJoinTask
               (final IRule rule,
                final IJoinNexus joinNexus,
                final int[] order,
                final int orderIndex,
                final int partitionId,
                final IIndexManager indexManager,
                final ILocalResourceManagement localResourceManager,
                final IBigdataDiscoveryManagement discoveryManager,
                final IConcurrencyManager concurrencyManager,
                final IJoinMaster master,
                final UUID masterUUID,
                final IAsynchronousIterator<IBindingSet[]> src,
                final IKeyOrder[] keyOrders,
                final IVariable[][] requiredVars)
    {
//BTM - PRE_CLIENT_SERVICE - END

        super(
                /*com.bigdata.util.Util.getIndexPartitionName(scaleOutIndexName,
                        partitionId),*/ rule, joinNexus, order, orderIndex,
                partitionId, master, masterUUID, requiredVars);

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE         if (fed == null)
//BTM - PRE_CLIENT_SERVICE             throw new IllegalArgumentException();

//BTM - PRE_CLIENT_SERVICE - END

        if (src == null)
            throw new IllegalArgumentException();

//BTM        if (dataService == null)
//BTM            throw new IllegalArgumentException();
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE if (dataServiceSession == null) {
//BTM - PRE_CLIENT_SERVICE     throw new IllegalArgumentException("null dataServiceSession");
//BTM - PRE_CLIENT_SERVICE }
        if (indexManager == null) {
            throw new IllegalArgumentException("null indexManager");
        }
        if (localResourceManager == null) {
            throw new IllegalArgumentException("null localResourceManager");
        }
        if (discoveryManager == null) {
            throw new IllegalArgumentException("null discoveryManager");
        }
//BTM - PRE_CLIENT_SERVICE - END

        // Note: This MUST be the index manager for the local data service.
        if(joinNexus instanceof IBigdataFederation) {
            throw new IllegalArgumentException
                          ("joinNexus is instance of IBigdataFederation");
        }

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE         this.fed = fed;
        this.indexMgr = indexManager;
        this.scaleOutIndexMgr = 
            (this.indexMgr instanceof ScaleOutIndexManager ?
                 (ScaleOutIndexManager)(this.indexMgr) : null);
        this.localResourceMgr = localResourceManager;
        this.discoveryMgr = discoveryManager;
//BTM - PRE_CLIENT_SERVICE - END

        this.keyOrders = keyOrders;

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE //BTM        this.dataService = dataService;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE this.dataServiceSession = dataServiceSession;
//BTM - PRE_CLIENT_SERVICE this.dataServiceHostname = (dataServiceHostname == null ? "UNKNOWN" : dataServiceHostname);
//BTM - PRE_CLIENT_SERVICE this.dataServiceName = (dataServiceName == null ? "UNKNOWN" : dataServiceName);

        this.dataServiceSession = (this.localResourceMgr).getSession();
        String hostname = (this.localResourceMgr).getHostname();
        this.dataServiceHostname = (hostname == null ? "UNKNOWN" : hostname);
        String serviceName = (this.localResourceMgr).getServiceName();
        this.dataServiceName = (serviceName == null ? "UNKNOWN" : serviceName);
//BTM - PRE_CLIENT_SERVICE - END

        // This is the index manager for the federation (scale-out indices).
//BTM - PRE_CLIENT_SERVICE  this.fedJoinNexus = joinNexus.getJoinNexusFactory().newInstance(fed);
        this.fedJoinNexus =
            joinNexus.getJoinNexusFactory().newInstance(this.indexMgr,
                                                        concurrencyManager,
                                                        this.discoveryMgr);

        if (lastJoin) {

            sinkCache = null;

            nextScaleOutIndexName = null;

            final ActionEnum action = fedJoinNexus.getAction();

            if (action.isMutation()) {

                /*
                 * Note: The solution buffer for mutation operations
                 * is obtained locally from a joinNexus that is
                 * backed by the federation NOT the local index
                 * manager. (This is because the solution buffer
                 * needs to write on the scale-out indices.)
                 */

                final IJoinNexus tmp = fedJoinNexus;

                /*
                 * The view of the mutable relation for the _head_ of the
                 * rule.
                 */

                final IMutableRelation relation = (IMutableRelation) tmp
                        .getHeadRelationView(rule.getHead());

                switch (action) {

                case Insert: {

                    solutionBuffer = tmp.newInsertBuffer(relation);

                    break;

                }

                case Delete: {

                    solutionBuffer = tmp.newDeleteBuffer(relation);

                    break;

                }

                default:
                    throw new AssertionError();

                }

            } else {

                /*
                 * The solution buffer for queries is obtained from the
                 * master.
                 */

                try {

                    solutionBuffer = masterProxy.getSolutionBuffer();

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

        } else {

            final IPredicate nextPredicate = rule
                    .getTail(order[orderIndex + 1]);

            final String namespace = nextPredicate.getOnlyRelationName();

            nextScaleOutIndexName = namespace +"."
                    + keyOrders[order[orderIndex + 1]];

            solutionBuffer = null;

            sinkCache = new LinkedHashMap<PartitionLocator, JoinTaskSink>();

            //                System.err.println("orderIndex=" + orderIndex + ", resources="
            //                        + Arrays.toString(getResource()) + ", nextPredicate="
            //                        + nextPredicate + ", nextScaleOutIndexName="
            //                        + nextScaleOutIndexName);

        }

        addSource(src);

    }

    /**
     * Adds a source from which this {@link DistributedJoinTask} will read
     * {@link IBindingSet} chunks.
     * 
     * @param source
     *            The source.
     * 
     * @return <code>true</code> iff the source was accepted.
     * 
     * @throws IllegalArgumentException
     *             if the <i>source</i> is <code>null</code>.
     */
    public boolean addSource(final IAsynchronousIterator<IBindingSet[]> source) {

        if (source == null)
            throw new IllegalArgumentException();

        lock.lock();

        try {

            if (sourcesExhausted) {

                // new source declarations are rejected.

                if (INFO)
                    log.info("source rejected: orderIndex=" + orderIndex
                            + ", partitionId=" + partitionId);

                return false;

            }

            sources.add(source);

            stats.fanIn++;

        } finally {

            lock.unlock();

        }
    
        if (DEBUG)
            log.debug("orderIndex=" + orderIndex + ", partitionId="
                    + partitionId + ", fanIn=" + stats.fanIn + ", fanOut="
                    + stats.fanOut);

        return true;
        
    }

    final protected IBuffer<ISolution[]> getSolutionBuffer() {

        return solutionBuffer;

    }

    private final IBuffer<ISolution[]> solutionBuffer;

    /**
     * Sets a flag preventing new sources from being declared and closes all
     * known {@link #sources} and removes this task from the {@link Session}.
     */
    protected void closeSources() {

        if (INFO)
            log.info(toString());
        
        lock.lock();

        try {

            sourcesExhausted = true;

            final IAsynchronousIterator[] a = sources
                    .toArray(new IAsynchronousIterator[] {});

            for (IAsynchronousIterator source : a) {

                source.close();

            }

            removeFromSession();
            
        } finally {

            lock.unlock();

        }

    }

    /**
     * Remove the task from the session, but only if the task in the session is
     * this task (it will have been overwritten if this task decides not to
     * accept more sources and another source shows up).
     */
    private void removeFromSession() {

        lock.lock();

        try {

            // @todo allocate this in the ctor.
            final String namespace = JoinTaskFactoryTask.getJoinTaskNamespace(
                    masterUUID, orderIndex, partitionId);

            /*
             * Note: If something else has the entry in the session then that is
             * Ok, but we need to make sure that we don't remove it by accident!
             */

//BTM            dataService.getSession().remove(namespace, this);
dataServiceSession.remove(namespace, this);
            
        } finally {

            lock.unlock();

        }

    }
    
    /**
     * This lock is used to make {@link #nextChunk()} and
     * {@link #addSource(IAsynchronousIterator)} into mutually exclusive
     * operations. {@link #nextChunk()} is the reader.
     * {@link #addSource(IAsynchronousIterator)} is the writer. These operations
     * need to be exclusive and atomic so that the termination condition of
     * {@link #nextChunk()} is consistent -- it should terminate when there are
     * no sources remaining. The first source is added when the
     * {@link DistributedJoinTask} is created. Additional sources are added (and
     * can result in a fan-in greater than one) when a
     * {@link JoinTaskFactoryTask} identifies that there is an existing
     * {@link DistributedJoinTask} and is able to atomically assign a new source
     * to that {@link DistributedJoinTask}. If the atomic assignment of the new
     * source fails (because all sources are exhausted before the assignment
     * occurs) then a new {@link DistributedJoinTask} will be created for the
     * same {@link DistributedJoinMasterTask}, orderIndex, and index partition
     * identifier and the source will be assigned to that
     * {@link DistributedJoinTask} instead.
     * 
     * @todo javadoc update
     */
//    private ReadWriteLock lock = new ReentrantReadWriteLock(false/* fair */);
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Returns a chunk of {@link IBindingSet}s by combining chunks from the
     * various source {@link JoinTask}s.
     * 
     * @return A chunk assembled from one or more chunks from one or more of
     *         the source {@link JoinTask}s.
     */
    protected IBindingSet[] nextChunk() throws InterruptedException {

        if (sourcesExhausted) {

            // nothing remaining in any accepted source.

            return null;

        }

        if (DEBUG)
            log.debug("Reading chunk of bindings from source(s): orderIndex="
                    + orderIndex + ", partitionId=" + partitionId);

        // #of elements in the combined chunk(s)
        int bindingSetCount = 0;

        // source chunks read so far.
        final List<IBindingSet[]> chunks = new LinkedList<IBindingSet[]>();

        /*
         * Assemble a chunk of suitable size
         * 
         * @todo don't wait too long. if we have some data then it is probably
         * better to process that data rather than waiting beyond a timeout for
         * a full chunk. also, make sure that we are neither yielding nor
         * spinning too long in this loop. However, the loop must wait if there
         * is nothing available and the sources are not yet exhausted.
         * 
         * @todo config. we need a different capacity here than the one used for
         * batch index operations. on the order of 100 should work well.
         * 
         * Note: The termination conditions under which we will return [null]
         * indicating that no more binding sets can be read are: (a) [halt] is
         * true; (b) [sourcesExhausted] is true; or (c) all sources are
         * exhausted and we are able to acquire the lock.
         * 
         * Once we do acquire the lock we set [sourcesExhausted] to true and any
         * subsequent request to add another source to this joinTask will fail.
         * This has the consequence that a new JoinTask will be created if a new
         * source has been identified once this task halts.
         */

        final int chunkCapacity = 100;// joinNexus.getChunkCapacity();

        while (!sourcesExhausted) {

            while (!halt && !sources.isEmpty()
                    && bindingSetCount < chunkCapacity) {

                if (trace)
                    System.err.print("\norderIndex=" + orderIndex);

                if (trace)
                    System.err.print(": reading");

                // if (DEBUG)
                // log.debug("Testing " + nsources + " sources: orderIndex="
                // + orderIndex + ", partitionId=" + partitionId);

                // clone to avoid concurrent modification of sources during
                // traversal.
                final IAsynchronousIterator<IBindingSet[]>[] sources = (IAsynchronousIterator<IBindingSet[]>[]) this.sources
                        .toArray(new IAsynchronousIterator[] {});

                // #of sources that are exhausted.
                int nexhausted = 0;

                for (int i = 0; i < sources.length
                        && bindingSetCount < chunkCapacity; i++) {

                    if (trace)
                        System.err.print(" <<(" + i + ":" + sources.length
                                + ")");

                    final IAsynchronousIterator<IBindingSet[]> src = sources[i];

                    // if there is something to read on that source.
                    if (src.hasNext(1L, TimeUnit.MILLISECONDS)) {

                        /*
                         * Read the chunk, waiting up to the timeout for
                         * additional chunks from this source which can be
                         * combined together by the iterator into a single
                         * chunk.
                         * 
                         * @todo config chunkCombiner timeout here and
                         * experiment with the value with varying fanIns.
                         */
                        final IBindingSet[] chunk = src.next(10L,
                                TimeUnit.MILLISECONDS);

                        /*
                         * Note: Since hasNext() returned [true] for this source
                         * we SHOULD get a chunk back since it is known to be
                         * there waiting for us. The timeout should only give
                         * the iterator an opportunity to combine multiple
                         * chunks together if they are already in the iterator's
                         * queue (or if they arrive in a timely manner).
                         */
                        assert chunk != null;

                        chunks.add(chunk);

                        bindingSetCount += chunk.length;

                        if (trace)
                            System.err.print("[" + chunk.length + "]");

                        if (DEBUG)
                            log.debug("Read chunk from source: sources[" + i
                                    + "], chunkSize=" + chunk.length
                                    + ", orderIndex=" + orderIndex
                                    + ", partitionId=" + partitionId);

                    } else if (src.isExhausted()) {

                        nexhausted++;

                        if (trace)
                            System.err.print("X{" + nexhausted + "}");

                        if (DEBUG)
                            log.debug("Source is exhausted: nexhausted="
                                    + nexhausted);

                        // no longer consider an exhausted source.
                        if (!this.sources.remove(src)) {

                            // could happen if src.equals() is not defined.
                            throw new AssertionError("Could not find source: "
                                    + src);

                        }

                    }

                }

                if (nexhausted == sources.length) {

                    /*
                     * All sources on which we were reading in this loop have
                     * been exhausted.
                     * 
                     * Note: we may have buffered some data, which is checked
                     * below.
                     * 
                     * Note: new sources may have been added concurrently, so we
                     * get the lock and then test the [sources] field, not just
                     * the local array.
                     */

                    lock.lock();

                    try {

                        if (this.sources.isEmpty()) {

                            if (INFO)
                                log.info("Sources are exhausted: orderIndex="
                                        + orderIndex + ", partitionId="
                                        + partitionId);

                            sourcesExhausted = true;

                            /*
                             * Remove ourselves from the Session since we will
                             * no longer accept any new sources.
                             */
                            
                            removeFromSession();
                            
                        }

                    } finally {

                        lock.unlock();

                    }

                    break;

                }

            }

            if (halt)
                throw new RuntimeException(firstCause.get());

            /*
             * Combine the chunks.
             */

            if (!chunks.isEmpty()) {

                return combineChunks(chunks, bindingSetCount);

            }

        } // while(!sourcesExhausted)

        /*
         * Termination condition: we did not get any data from any source, we
         * are not permitting any new sources, and there are no sources
         * remaining.
         */

        if (DEBUG)
            log.debug("Sources are exhausted: orderIndex=" + orderIndex
                    + ", partitionId=" + partitionId);

        if (trace)
            System.err.print(" exhausted");

        return null;

    }
    
    /**
     * Combine the chunk(s) into a single chunk.
     * 
     * @param chunks
     *            A list of chunks read from the {@link #sources}.
     * @param bindingSetCount
     *            The #of bindingSets in those chunks.
     * @return
     */
    protected IBindingSet[] combineChunks(final List<IBindingSet[]> chunks,
            final int bindingSetCount) {
        
        final int chunkCount = chunks.size();

        assert chunkCount > 0; // at least one chunk.
        
        assert bindingSetCount > 0; // at least on bindingSet.
        
        final IBindingSet[] chunk;

        if (chunkCount == 1) {

            // Only one chunk is available.

            chunk = chunks.get(0);

        } else {

            // Combine 2 or more chunks.

            chunk = new IBindingSet[bindingSetCount];

            final Iterator<IBindingSet[]> itr = chunks.iterator();

            int offset = 0;

            while (itr.hasNext()) {

                final IBindingSet[] a = itr.next();

                System.arraycopy(a, 0, chunk, offset, a.length);

                offset += a.length;

            }

        }

        if (halt)
            throw new RuntimeException(firstCause.get());

        if (DEBUG) {

            log.debug("Read chunk(s): nchunks=" + chunkCount
                    + ", #bindingSets=" + chunk.length + ", orderIndex="
                    + orderIndex + ", partitionId=" + partitionId);
        }

        stats.bindingSetChunksIn += chunkCount;
        stats.bindingSetsIn += bindingSetCount;

        if (trace)
            System.err.print(" chunk[" + chunk.length + "]");

        return chunk;

    }

    protected AbstractUnsynchronizedArrayBuffer<IBindingSet> newUnsyncOutputBuffer() {

        final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncOutputBuffer;

        /*
         * On overflow, the generated binding sets are mapped across the
         * JoinTaskSink(s) for the target index partition(s).
         */

        final int chunkCapacity = fedJoinNexus.getChunkCapacity();

        if (lastJoin) {

            /*
             * Accepted binding sets are flushed to the solution buffer.
             */

            unsyncOutputBuffer = new UnsynchronizedSolutionBuffer<IBindingSet>(
                    this, fedJoinNexus, chunkCapacity);

        } else {

            /*
             * Accepted binding sets are flushed to the next join dimension.
             * 
             * Note: The index is key-range partitioned. Each bindingSet
             * will be mapped across the index partition(s) on which the
             * generated access path for that bindingSet will have to read.
             * There will be a JoinTask associated with each such index
             * partition. That JoinTask will execute locally on the
             * shard service which hosts that index partition.
             */

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            unsyncOutputBuffer = new UnsyncDistributedOutputBuffer<IBindingSet>(
//BTM - PRE_CLIENT_SERVICE                    fed, this, chunkCapacity);
            unsyncOutputBuffer =
                new UnsyncDistributedOutputBuffer<IBindingSet>
                        (scaleOutIndexMgr, this, chunkCapacity);
//BTM - PRE_CLIENT_SERVICE - END

        }

        return unsyncOutputBuffer;

    }

    /**
     * Notifies each sink that this {@link DistributedJoinTask} will no
     * longer generate new {@link IBindingSet} chunks and then waits for the
     * sink task(s) to complete.
     * <p>
     * Note: Closing the {@link BlockingBuffer} from which a sink
     * {@link JoinTask} is reading will cause the source iterator for that
     * sink task to eventually return <code>false</code> indicating that
     * it is exhausted (assuming that the sink keeps reading on the
     * iterator).
     * 
     * @throws InterruptedException
     *             if interrupted while awaiting the future for a sink.
     */
    @Override
    protected void flushAndCloseBuffersAndAwaitSinks()
            throws InterruptedException, ExecutionException {

        if (DEBUG)
            log.debug("orderIndex="
                    + orderIndex
                    + ", partitionId="
                    + partitionId
                    + (lastJoin ? ", lastJoin" : ", sinkCount="
                            + sinkCache.size()));

        /*
         * For the last join dimension the JoinTask instead writes onto the
         * [solutionBuffer]. For query, that is the shared solution buffer
         * and will be a proxied object. For mutation, that is a per
         * JoinTask buffer that writes onto the target relation. In the
         * latter case we MUST report the mutationCount returned by flushing
         * the solutionBuffer via JoinStats to the master.
         * 
         * Note: JoinTask#flushUnsyncBuffers() will already have been
         * invoked so all generated binding sets will already be in the sync
         * buffer ready for output.
         */
        if (lastJoin) {

            assert sinkCache == null;

            if (DEBUG)
                log.debug("\nWill flush buffer containing "
                        + getSolutionBuffer().size() + " solutions.");

            final long counter = getSolutionBuffer().flush();

            if (DEBUG)
                log.debug("\nFlushed buffer: mutationCount=" + counter);

            if (joinNexus.getAction().isMutation()) {

                /*
                 * Apply mutationCount to the JoinStats so that it will be
                 * reported back to the JoinMasterTask.
                 */

                stats.mutationCount.addAndGet(counter);

            }

        } else {

            /*
             * Close sinks.
             * 
             * For all but the lastJoin, the buffers are writing onto the
             * per-sink buffers. We flush and close those buffers now. The sink
             * JoinTasks drain those buffers. Once the buffers are closed, the
             * sink JoinTasks will eventually exhaust the buffers.
             * 
             * Note: This flushes the buffers using a thread pool which should
             * give better throughput when the fanOut is GT ONE (1).
             */

            {

                if (halt)
                    throw new RuntimeException(firstCause.get());

                final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

                final Iterator<JoinTaskSink> itr = sinkCache.values()
                        .iterator();

                while (itr.hasNext()) {

                    final JoinTaskSink sink = itr.next();

                    tasks.add(new FlushAndCloseSinkBufferTask(sink));

                }

//BTM - PRE_CLIENT_SERVICE  final List<Future<Void>> futures = fed.getExecutorService().invokeAll(tasks);
                final List<Future<Void>> futures =
                          localResourceMgr.getThreadPool().invokeAll(tasks);
                for (Future f : futures) {

                    // make sure that all tasks were successful.
                    f.get();

                }
                
            }

            // Await sinks.
            {

                final Iterator<JoinTaskSink> itr = sinkCache.values()
                        .iterator();

                while (itr.hasNext()) {

                    if (halt)
                        throw new RuntimeException(firstCause.get());

                    final JoinTaskSink sink = itr.next();

                    final Future f = sink.getFuture();

                    if (DEBUG)
                        log.debug("Waiting for Future: sink=" + sink);

                    // will throw any exception from the sink's Future.
                    f.get();

                }

            }

        } // else (lastJoin)

        if (DEBUG)
            log.debug("Done: orderIndex="
                    + orderIndex
                    + ", partitionId="
                    + partitionId
                    + (lastJoin ? "lastJoin" : ", sinkCount="
                            + sinkCache.size()));

    }

    /**
     * Flushes any buffered data for a {@link JoinTaskSink} and closes the
     * {@link BlockingBuffer} for that sink so that the sink {@link JoinTask}'s
     * iterator can eventually drain the buffer and report that it is exhausted.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class FlushAndCloseSinkBufferTask implements Callable<Void> {

        final private JoinTaskSink sink;

        public FlushAndCloseSinkBufferTask(final JoinTaskSink sink) {

            if (sink == null)
                throw new IllegalArgumentException();

            this.sink = sink;

        }

        public Void call() throws Exception {

            if (halt)
                throw new RuntimeException(firstCause.get());

            if (DEBUG)
                log.debug("Closing sink: sink=" + sink
                        + ", unsyncBufferSize=" + sink.unsyncBuffer.size()
                        + ", blockingBufferSize=" + sink.blockingBuffer.size());

            // flush to the blockingBuffer.
            sink.unsyncBuffer.flush();

            // close the blockingBuffer.
            sink.blockingBuffer.close();

            return null;

        }

    }
    
    /**
     * Cancel all {@link DistributedJoinTask}s that are sinks for this
     * {@link DistributedJoinTask}.
     */
    @Override
    protected void cancelSinks() {

        // no sinks.
        if (lastJoin)
            return;

        if (DEBUG)
            log.debug("orderIndex=" + orderIndex + ", partitionId="
                    + partitionId + ", sinkCount=" + sinkCache.size());

        final Iterator<JoinTaskSink> itr = sinkCache.values().iterator();

        while (itr.hasNext()) {

            final JoinTaskSink sink = itr.next();

            sink.unsyncBuffer.reset();

            sink.blockingBuffer.reset();

            sink.blockingBuffer.close();

            sink.getFuture().cancel(true/* mayInterruptIfRunning */);

        }

        if (DEBUG)
            log.debug("Done: orderIndex=" + orderIndex + ", partitionId="
                    + partitionId + ", sinkCount=" + sinkCache.size());

    }

    /**
     * Return the sink on which we will write {@link IBindingSet} for the
     * index partition associated with the specified locator. The sink will
     * be backed by a {@link DistributedJoinTask} running on the
     * {@link ShardService} that is host to that index partition. The
     * scale-out index will be the scale-out index for the next
     * {@link IPredicate} in the evaluation order.
     * 
     * @param locator
     *            The locator for the index partition.
     * 
     * @return The sink.
     * 
     * @throws ExecutionException
     *             If the {@link JoinTaskFactoryTask} fails.
     * @throws InterruptedException
     *             If the {@link JoinTaskFactoryTask} is interrupted.
     * 
     * @todo Review this as a possible concurrency bottleneck. The operation
     *       can have significant latency since RMI is required on a cache
     *       miss to lookup or create the {@link JoinTask} on the target
     *       dataService. Therefore we should probably allow concurrent
     *       callers and establish a {@link NamedLock} that serializes
     *       callers seeking the {@link JoinTaskSink} for the same index
     *       partition identifier. Note that the limit on the #of possible
     *       callers is the permitted parallelism for processing the source
     *       {@link IBindingSet}s, e.g., the #of {@link ChunkTask}s that
     *       can execute in parallel for a given {@link JoinTask}.
     */
    synchronized protected JoinTaskSink getSink(final PartitionLocator locator)
            throws InterruptedException, ExecutionException {

        JoinTaskSink sink = sinkCache.get(locator);

        if (sink == null) {

//            /*
//             * Cache miss.
//             * 
//             * First, obtain an exclusive resource lock on the sink task
//             * namespace and see if there is an instance of the required join
//             * task running somewhere. If there is, then request the proxy for
//             * its Future from the dataService on which it is executing.
//             * 
//             * Otherwise, we are holding an exclusive lock on the sink task
//             * namespace. Select a dataService instance on which the desired
//             * index partition is replicated and then create a join task on that
//             * instance, register the join task under the lock, and return its
//             * Future.
//             * 
//             * Finally, release the exclusive lock.
//             * 
//             * Note: The JoinTask must acquire the same lock in order to
//             * conclude that it is done with its work and may exit. The lock
//             * therefore provides for an atomic decision vis-a-vis whether we
//             * need to create a new join task or use an existing one as well as
//             * whether an existing join task may exit.
//             * 
//             * @todo since replication is not implemented we don't need to store
//             * anything under the namespace while we hold a lock. however, this
//             * shows a pattern where we would like to do that in the future. I
//             * believe that ZooKeeper would support this. If we do store
//             * something, then be sure that we also clean it up when we are done
//             * with the master instance.
//             */
//
//            final String namespace;
//            try {
//                namespace = masterProxy.getUUID() + "/" + orderIndex + "/"
//                        + partitionId;
//            } catch (IOException ex) {
//                throw new RuntimeException(ex);
//            }
//    
//            final IResourceLock lock;
//            try {
//                lock = fed.getResourceLockService().acquireExclusiveLock(namespace);
//            } catch (IOException ex) {
//                throw new RuntimeException(ex);
//            }
//
//            try {
                
                /*
                 * Allocate/discover JoinTask on the target data service and
                 * obtain a sink reference for its future and buffers.
                 * 
                 * Note: The JoinMasterTask uses very similar logic to setup the
                 * first join dimension. Of course, it gets to assume that there
                 * is no such JoinTask in existence at the time.
                 */

                final int nextOrderIndex = orderIndex + 1;

                if (DEBUG)
                    log.debug("Creating join task: nextOrderIndex="
                            + nextOrderIndex + ", indexName="
                            + nextScaleOutIndexName + ", partitionId="
                            + locator.getPartitionId());

                final UUID sinkUUID = locator.getDataServiceUUID();
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM                final IDataService dataService;
//BTM - PRE_CLIENT_SERVICE final ShardService dataService;
//BTM - PRE_CLIENT_SERVICE                 if (sinkUUID.equals(fed.getServiceUUID())) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE 				/*
//BTM - PRE_CLIENT_SERVICE 				 * As an optimization, special case when the downstream
//BTM - PRE_CLIENT_SERVICE 				 * data service is _this_ data service.
//BTM - PRE_CLIENT_SERVICE 				 */
//BTM - PRE_CLIENT_SERVICE //BTM                	dataService = (IDataService)fed.getService();
//BTM - PRE_CLIENT_SERVICE dataService = (ShardService)fed.getService();
//BTM - PRE_CLIENT_SERVICE                     
//BTM - PRE_CLIENT_SERVICE                 } else {
//BTM - PRE_CLIENT_SERVICE                 
//BTM - PRE_CLIENT_SERVICE                 	dataService = fed.getDataService(sinkUUID);
//BTM - PRE_CLIENT_SERVICE                 	
//BTM - PRE_CLIENT_SERVICE                 }
                 final ShardService dataService =
                                        discoveryMgr.getDataService(sinkUUID);

//BTM - PRE_CLIENT_SERVICE                sink = new JoinTaskSink(fed, locator, this);
                sink = new JoinTaskSink(locator, this);
//BTM - PRE_CLIENT_SERVICE - END

                /*
                 * Export async iterator proxy.
                 * 
                 * Note: This proxy is used by the sink to draw chunks from the
                 * source JoinTask(s).
                 */
                final IAsynchronousIterator<IBindingSet[]> sourceItrProxy;
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE                if (fed.isDistributed()) {
//BTM - PRE_CLIENT_SERVICE                    sourceItrProxy = ((AbstractDistributedFederation) fed)
//BTM - PRE_CLIENT_SERVICE                            .getProxy(sink.blockingBuffer.iterator(), joinNexus
//BTM - PRE_CLIENT_SERVICE                                    .getBindingSetSerializer(), joinNexus
//BTM - PRE_CLIENT_SERVICE                                    .getChunkOfChunksCapacity());
//BTM - PRE_CLIENT_SERVICE                } else {
//BTM - PRE_CLIENT_SERVICE                    sourceItrProxy = sink.blockingBuffer.iterator();
//BTM - PRE_CLIENT_SERVICE                }
//BTM - PRE_CLIENT_SERVICE
                if (indexMgr instanceof ScaleOutIndexManager) {
                    //scaleout ==> distributed
                    try {
                        sourceItrProxy = 
                            Util.wrapIterator
                                 (sink.blockingBuffer.iterator(),
                                  joinNexus.getBindingSetSerializer(),
                                  joinNexus.getChunkOfChunksCapacity());
                    } catch(ExportException e) {// maintain original behavior?
                        throw new RuntimeException
                           ("DistributedJoinTask.getSink: "
                            +"failed on export of iterator wrapper", e);
                    }
                } else {
                    sourceItrProxy = sink.blockingBuffer.iterator();
                }
//BTM - PRE_CLIENT_SERVICE - END

                // the future for the factory task (not the JoinTask).
                final Future factoryFuture;
                try {

                    final JoinTaskFactoryTask factoryTask = new JoinTaskFactoryTask(
                        nextScaleOutIndexName, rule, joinNexus
                                .getJoinNexusFactory(), order, nextOrderIndex,
                        locator.getPartitionId(), masterProxy, masterUUID,
                        sourceItrProxy, keyOrders, requiredVars);

                    // submit the factory task, obtain its future.
//BTM                    factoryFuture = dataService.submit(factoryTask);
factoryFuture = ((ShardManagement)dataService).submit(factoryTask);

                } catch (IOException ex) {

                    // RMI problem.
                    throw new RuntimeException(ex);

                }

                /*
                 * Obtain the future for the JoinTask from the factory task's
                 * Future.
                 */

                sink.setFuture((Future) factoryFuture.get());

                stats.fanOut++;

                sinkCache.put(locator, sink);
                
//            } finally {
//
//                try {
//                    lock.unlock();
//                } catch (IOException ex) {
//                    throw new RuntimeException(ex);
//                }
//                
//            }
            
        }

        return sink;

    }

    /**
     * Logs an error in {@link JoinTask#call()} on the local log file and adds
     * some metadata about the operation which was being executed. This does not
     * imply that the error originates with this join task. You have to inspect
     * the error messages, the order in which the joins were being evaluated,
     * and even correlate the {@link JoinTask#masterUUID} in order to figure out
     * what really happened.
     */
    @Override
    protected void logCallError(final Throwable t) {

//BTM        log.error("hostname=" + dataService.getHostname() + ", serviceName="
//BTM                + dataService.getServiceName() + ", joinTask=" + toString()
//BTM                + ", rule=" + rule, t);

//BTM - FOR_CLIENT_SERVICE
log.error("hostname=" + dataServiceHostname + ", serviceName=" + dataServiceName + ", joinTask=" + toString() + ", rule=" + rule, t);

    }
    
}
