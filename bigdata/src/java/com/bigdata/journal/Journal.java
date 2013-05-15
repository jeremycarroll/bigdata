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
package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.GlobalFileSystemHelper;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ReadCommittedView;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.config.IntegerValidator;
import com.bigdata.config.LongValidator;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.HATXSGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.msg.HAGatherReleaseTimeRequest;
import com.bigdata.ha.msg.HANotifyReleaseTimeRequest;
import com.bigdata.ha.msg.HANotifyReleaseTimeResponse;
import com.bigdata.ha.msg.IHAGatherReleaseTimeRequest;
import com.bigdata.ha.msg.IHANotifyReleaseTimeRequest;
import com.bigdata.ha.msg.IHANotifyReleaseTimeResponse;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumException;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StaleLocatorReason;
import com.bigdata.rwstore.IHistoryManager;
import com.bigdata.rwstore.IRawTx;
import com.bigdata.rwstore.RWStore;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.ClocksNotSynchronizedException;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.LatchedExecutor;
import com.bigdata.util.concurrent.ShutdownHelper;
import com.bigdata.util.concurrent.ThreadPoolExecutorBaseStatisticsTask;

/**
 * Concrete implementation suitable for a local and unpartitioned database.
 * <p>
 * Note: This implementation does NOT not support partitioned indices. Because
 * all data must reside on a single journal resource there is no point to a
 * view. Views are designed to have data on a mixture of the live journal, one
 * or more historical journals, and one or more {@link IndexSegment}s.
 * 
 * @see ResourceManager, which supports views.
 */
public class Journal extends AbstractJournal implements IConcurrencyManager,
        /*ILocalTransactionManager,*/ IResourceManager {

    /**
     * Logger.
     */
    private static final Logger log = Logger.getLogger(Journal.class);

    /**
     * @see http://sourceforge.net/apps/trac/bigdata/ticket/443 (Logger for
     *      RWStore transaction service and recycler)
     */
    private static final Logger txLog = Logger.getLogger("com.bigdata.txLog");

    /**
     * Object used to manage local transactions. 
     */
    private final AbstractLocalTransactionManager localTransactionManager; 

    /**
     * Object used to manage tasks executing against named indices.
     */
    private final ConcurrencyManager concurrencyManager;

    /**
     * Options understood by the {@link Journal}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public interface Options extends com.bigdata.journal.Options,
            com.bigdata.journal.ConcurrencyManager.Options,
            com.bigdata.journal.TemporaryStoreFactory.Options,
            com.bigdata.journal.QueueStatsPlugIn.Options,
            com.bigdata.journal.PlatformStatsPlugIn.Options,
            com.bigdata.journal.HttpPlugin.Options
            // Note: Do not import. Forces bigdata-ganglia dependency.
            // com.bigdata.journal.GangliaPlugIn.Options
            {

        /**
         * The capacity of the {@link HardReferenceQueue} backing the
         * {@link IResourceLocator} maintained by the {@link Journal}. The
         * capacity of this cache indirectly controls how many
         * {@link ILocatableResource}s the {@link Journal} will hold open.
         * <p>
         * The effect of this parameter is indirect owning to the semantics of
         * weak references and the control of the JVM over when they are
         * cleared. Once an {@link ILocatableResource} becomes weakly reachable,
         * the JVM will eventually GC the object. Since objects which are
         * strongly reachable are never cleared, this provides our guarantee
         * that resources are never closed if they are in use.
         * 
         * @see #DEFAULT_LOCATOR_CACHE_CAPACITY
         */
        String LOCATOR_CACHE_CAPACITY = Journal.class.getName()
                + ".locatorCacheCapacity";

        String DEFAULT_LOCATOR_CACHE_CAPACITY = "20";
        
        /**
         * The timeout in milliseconds for stale entries in the
         * {@link IResourceLocator} cache -or- ZERO (0) to disable the timeout
         * (default {@value #DEFAULT_LOCATOR_CACHE_TIMEOUT}). When this timeout
         * expires, the reference for the entry in the backing
         * {@link HardReferenceQueue} will be cleared. Note that the entry will
         * remain in the {@link IResourceLocator} cache regardless as long as it
         * is strongly reachable.
         */
        String LOCATOR_CACHE_TIMEOUT = Journal.class.getName()
                + ".locatorCacheTimeout";

        String DEFAULT_LOCATOR_CACHE_TIMEOUT = "" + (60 * 1000);

        /**
         * The #of threads that will be used to read on the local disk.
         * 
         * @see Journal#getReadExecutor()
         */
        String READ_POOL_SIZE = Journal.class.getName() + ".readPoolSize";

        String DEFAULT_READ_POOL_SIZE = "0";
        
    }
    
    /**
     * Create or re-open a journal.
     * 
     * @param properties
     *            See {@link com.bigdata.journal.Options}.
     */
    public Journal(final Properties properties) {
        
        this(properties, null/* quorum */);
    
    }

    public Journal(final Properties properties,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum) {

        super(properties, quorum);

        tempStoreFactory = new TemporaryStoreFactory(properties);
        
        executorService = (ThreadPoolExecutor) Executors
                .newCachedThreadPool(new DaemonThreadFactory(getClass()
                        .getName()
                        + ".executorService"));

//        if (Boolean.valueOf(properties.getProperty(
//                Options.COLLECT_QUEUE_STATISTICS,
//                Options.DEFAULT_COLLECT_QUEUE_STATISTICS))) {
            
            scheduledExecutorService = Executors
                    .newSingleThreadScheduledExecutor(new DaemonThreadFactory(
                            getClass().getName() + ".sampleService"));
            
//        } else {
//         
//            scheduledExecutorService = null;
//            
//        }
        
        {
            
            final int readPoolSize = Integer.valueOf(properties.getProperty(
                    Options.READ_POOL_SIZE, Options.DEFAULT_READ_POOL_SIZE));
            
            if (readPoolSize > 0) {

                readService = new LatchedExecutor(executorService,
                        readPoolSize);

            } else {

                readService = null;
                
            }

        }

        resourceLocator = newResourceLocator();
        
        resourceLockManager = new ResourceLockService();

        localTransactionManager = newLocalTransactionManager();

        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, this);
        
        getExecutorService().execute(new StartDeferredTasksTask());
        
    }

    /**
     * Ensure that the WORM mode of the journal always uses
     * {@link Long#MAX_VALUE} for
     * {@link AbstractTransactionService.Options#MIN_RELEASE_AGE}.
     * 
     * @param properties
     *            The properties.
     *            
     * @return The argument, with the minReleaseAge overridden if necessary.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/391
     */
    private Properties checkProperties(final Properties properties) {
        if (getBufferStrategy() instanceof WORMStrategy) {
            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, ""
                            + Long.MAX_VALUE);
        }
        return properties;
    }

    /**
     * Factory for the {@link IResourceLocator} for the {@link Journal}.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected IResourceLocator<?> newResourceLocator() {

        final int cacheCapacity = getProperty(Options.LOCATOR_CACHE_CAPACITY,
                Options.DEFAULT_LOCATOR_CACHE_CAPACITY,
                IntegerValidator.GT_ZERO);

        final long cacheTimeout = getProperty(Options.LOCATOR_CACHE_TIMEOUT,
                Options.DEFAULT_LOCATOR_CACHE_TIMEOUT, LongValidator.GTE_ZERO);

        return new DefaultResourceLocator(this, null/* delegate */,
                cacheCapacity, cacheTimeout);

    }
    
    /**
     * Inner class used to coordinate the distributed protocol for achieving an
     * atomic consensus on the new <i>releaseTime</i> for the services joined
     * with a met quorum.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class BarrierState implements Runnable {
        
        /**
         * The token that must remain valid.
         * 
         * TODO HA TXS: We should also verify that the responses we collect are
         * for the same request. This could be done using a request UUID or
         * one-up request counter. That would guard against having a service
         * reconnect and respond late once the leader had gotten to another
         * commit point.
         */
        final private long token;
        
        /**
         * Local HA service implementation (non-Remote).
         */
        final private QuorumService<HAGlue> quorumService;
        
        /** The services joined with the met quorum, in their join order. */
        final private UUID[] joinedServiceIds;
        
        /**
         * {@link CyclicBarrier} used to coordinate the protocol for achiving an
         * atomic consensus on the new <i>releaseTime</i> for the services
         * joined with a met quorum.
         * <p>
         * Note: The {@link #barrier} provides visibilty for the fields that are
         * modified by {@link #run()} so we do not need additional locks or
         * atomics for synchronizing these state updates.
         */
        final private CyclicBarrier barrier;

//        /**
//         * The {@link Future} for the RMI to each follower that is joined with
//         * the met quorum.
//         */
//        final private Map<UUID, Future<Void>> futures = new HashMap<UUID, Future<Void>>();

        /**
         * A timestamp taken on the leader when we start the protocol to
         * discover the new releaseTime consensus.
         */
        final private long timestampOnLeader;
        
        /**
         * This is the earliest visible commit point on the leader.
         */
        final private IHANotifyReleaseTimeRequest leadersValue;
        
        /**
         * The message from each of those followers providing their local
         * earliest visible commit point. 
         */
        final private Map<UUID, IHANotifyReleaseTimeRequest> responses = new ConcurrentHashMap<UUID, IHANotifyReleaseTimeRequest>();

        /**
         * The value from {@link #responses} associated with the earliest commit
         * point. This is basis for the "censensus" across the services.
         */
        private IHANotifyReleaseTimeRequest minimumResponse = null;

        /**
         * The consensus value. This is a restatement of the data in from the
         * {@link #minimumResponse}.
         */
        protected IHANotifyReleaseTimeResponse consensus = null;

//        private Quorum<HAGlue,QuorumService<HAGlue>> getQuorum() {
//            
//            return Journal.this.getQuorum();
//            
//        }
        
        private HATXSGlue getService(final UUID serviceId) {

            return quorumService.getService(serviceId);
            
        }

//        /**
//         * Cancel the requests on the remote services (RMI). This is a best effort
//         * implementation. Any RMI related errors are trapped and ignored in order
//         * to be robust to failures in RMI when we try to cancel the futures.
//         */
//        private <F extends Future<T>, T> void cancelRemoteFutures(
//                final F[] remoteFutures) {
//
//            if (log.isInfoEnabled())
//                log.info("");
//
//            for (F rf : remoteFutures) {
//
//                try {
//
//                    rf.cancel(true/* mayInterruptIfRunning */);
//
//                } catch (Throwable t) {
//
//                    // ignored (to be robust).
//
//                }
//
//            }
//
//        }

        public BarrierState() {

            token = getQuorum().token();

            getQuorum().assertLeader(token);

            // Local HA service implementation (non-Remote).
            quorumService = getQuorum().getClient();

            // The services joined with the met quorum, in their join order.
            joinedServiceIds = getQuorum().getJoined();

            leadersValue = ((InnerJournalTransactionService) getTransactionService())
                    .newHANotifyReleaseTimeRequest(quorumService.getServiceId());

            // Note: Local method call.
            timestampOnLeader = leadersValue.getTimestamp();

//            /*
//             * Only the followers will countDown() at the barrier. The leader
//             * will await() until the barrier breaks.
//             */
            final int nparties = joinedServiceIds.length;// - 1;

            barrier = new CyclicBarrier(nparties, this);

        }

        /**
         * Find the minimum value across the responses when the {@link #barrier}
         * breaks.
         */
        @Override
        public void run() {

            if (log.isInfoEnabled())
                log.info("leader: " + leadersValue);
            
            // This is the timestamp from the BarrierState ctor.
            final long timeLeader = leadersValue.getTimestamp();
            
            // Start with the leader's value (from ctor).
            minimumResponse = leadersValue;

            for (IHANotifyReleaseTimeRequest response : responses.values()) {

                if (log.isTraceEnabled())
                    log.trace("follower: " + response);

                if (minimumResponse.getPinnedCommitCounter() > response
                        .getPinnedCommitCounter()) {

                    minimumResponse = response;

                }

                /*
                 * Verify that the timestamp from the ctor is BEFORE the
                 * timestamp assigned by the follower for its response.
                 */
                assertBefore(timeLeader, response.getTimestamp());

            }

            // Restate the consensus as an appropriate message object.
            consensus = new HANotifyReleaseTimeResponse(
                    minimumResponse.getPinnedCommitTime(),
                    minimumResponse.getPinnedCommitCounter());

            if (log.isInfoEnabled())
                log.info("consensus: " + consensus);

        }

        /**
         * Send an {@link IHAGatherReleaseTimeRequest} message to each follower.
         * Block until the responses are received.
         * <p>
         * Note: Like the 2-phase commit, the overall protocol should succeed if
         * we can get <code>((k+1)/2)</code> services that do not fail this
         * step. Thus for HA3, we should allow one error on a follower, the
         * leader is sending the messages and is presumed to succeed, and one
         * follower COULD fail without failing the protocol. If the protocol
         * does fail we have to fail the commit, so getting this right is
         * NECESSARY. At a mimimum, we must not fail if all joined services on
         * entry to this method respond without failing (that is, succeed if no
         * services fail during this protocol) - this is implemented.
         * 
         * @throws InterruptedException
         * @throws BrokenBarrierException
         * @throws TimeoutException 
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/673" >
         *      Native thread leak in HAJournalServer process </a>
         */
        private void messageFollowers(final long token, final long timeout,
                final TimeUnit units) throws IOException, InterruptedException,
                BrokenBarrierException, TimeoutException {

            getQuorum().assertLeader(token);

//            /*
//             * Future for gather task for each follower.
//             * 
//             * Note: These are asynchronous remote Futures. They must not escape
//             * the local scope and must be cancelled regardless of the outcome.
//             * 
//             * Note: DGC for these remote causes a native thread leak on the
//             * followers. To avoid that, I am attempting to rely on proxies for
//             * remote futures that do not use DGC. In this case, I believe that
//             * it will work since the Future is in scope on the follower (it is
//             * the future for the GatherTask running on the follower) and thus
//             * we should not need DGC to keep the follower from finalizing the
//             * remote futures on which this method is relying.
//             * 
//             * @see https://sourceforge.net/apps/trac/bigdata/ticket/673
//             */
//            @SuppressWarnings("unchecked")
//            final Future<Void>[] remoteFutures = new Future[joinedServiceIds.length];
//            final boolean[] remoteDone = new boolean[joinedServiceIds.length];
            
            try {

                final IHAGatherReleaseTimeRequest msg = new HAGatherReleaseTimeRequest(
                        token, timestampOnLeader);

                // Do not send message to self (leader is at index 0).
                for (int i = 1; i < joinedServiceIds.length; i++) {

                    final UUID serviceId = joinedServiceIds[i];

                    /*
                     * Runnable which will execute this message on the remote
                     * service.
                     */
                    
                    // Resolve joined service.
                    final HATXSGlue service = getService(serviceId);
                    
                    // Message joined service (can throw NPE if service is gone).
                    service.gatherMinimumVisibleCommitTime(msg);

//                    // add to list of futures we will check.
//                    remoteFutures[i] = rf;

                }

//                /*
//                 * Check the futures for the other services in the quorum.
//                 */
//                final List<Throwable> causes = new LinkedList<Throwable>();
//                for (int i = 1; i < remoteFutures.length; i++) {
//                    final Future<Void> rf = remoteFutures[i];
//                    boolean success = false;
//                    try {
//                        rf.get();
//                        success = true;
//                        remoteDone[i] = true;
//                    } catch (InterruptedException ex) {
//                        log.error(ex, ex);
//                        causes.add(ex);
//                    } catch (ExecutionException ex) {
//                        log.error(ex, ex);
//                        causes.add(ex);
//                        remoteDone[i] = true;
//                    } catch (RuntimeException ex) {
//                        /*
//                         * Note: ClientFuture.get() can throw a RuntimeException
//                         * if there is a problem with the RMI call. In this case
//                         * we do not know whether the Future is done.
//                         */
//                        log.error(ex, ex);
//                        causes.add(ex);
//                    } finally {
//                        if (!success) {
//                            // Cancel the request on the remote service (RMI).
//                            try {
//                                rf.cancel(true/* mayInterruptIfRunning */);
//                            } catch (Throwable t) {
//                                // ignored.
//                            }
//                            remoteDone[i] = true;
//                        }
//                    }
//                }

//                spinWaitBarrier(getQuorum(), barrier, token, timeout, units);
                
                /*
                 * This sets up a task that will monitor the quorum state and
                 * then interrupt this Thread if it is blocked at the barrier
                 * [actually, it uses barrier.reset(), which appears to be a
                 * litle safer].
                 * 
                 * If this service is no longer the quorum leader or if any of
                 * the services leave that were joined with the met quorum when
                 * we started the release time consensus protocol, then we have
                 * to reset() the barrier. We achieve this by interrupting the
                 * Thread (actually it now uses barrier.reset()).
                 * 
                 * Note: CyclicBarrier.await(timeout,unit) causes the barrier to
                 * break if the timeout is exceeded. Therefore is CAN NOT be
                 * used in preference to this pattern.
                 */
                {
//                    final Thread blockedAtBarrier = Thread.currentThread();

                    final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

                    final long initialDelay = 100; // milliseconds.
                    final long delay = initialDelay;

                    final ScheduledFuture<?> scheduledFuture = scheduledExecutorService
                            .scheduleWithFixedDelay(new Runnable() {
                                public void run() {
                                    try {
                                        
                                        // Verify service is still leader.
                                        quorum.assertLeader(token);

                                        // Verify service self-recognizes as leader.
                                        if (getHAStatus() != HAStatusEnum.Leader) {

                                            throw new QuorumException();

                                        }

                                        // Verify messaged services still
                                        // joined.
                                        assertServicesStillJoined(quorum);
                                        
                                    } catch (QuorumException ex) {

                                        if (!barrier.isBroken()) {

                                            barrier.reset();
                                            
                                        }
                                        
                                    } catch (RuntimeException ex) {

                                        if (InnerCause.isInnerCause(ex,
                                                InterruptedException.class)) {

                                            // Normal termination.
                                            return;
                                            
                                        }

                                        /*
                                         * Something went wrong in the
                                         * monitoring code. 
                                         */
                                        
                                        log.error(ex, ex);

                                        if (!barrier.isBroken()) {

                                            /*
                                             * Force the barrier to break since
                                             * we will no longer be monitoring
                                             * the quorum state.
                                             */
                                            barrier.reset();

                                        }

                                    }

                                }
                            }, initialDelay, delay, TimeUnit.MILLISECONDS);

                    try {

                        /*
                         * Throws InterruptedException, BrokenBarrierException.
                         */
                        barrier.await();

                    } finally {

                        scheduledFuture.cancel(true/* mayInterruptIfRunning */);

                    }

                }

//                /*
//                 * If there were any errors, then throw an exception listing them.
//                 */
//                if (!causes.isEmpty()) {
//                    // Note: Cancelled below.
////                    // Cancel remote futures.
////                    cancelRemoteFutures(remoteFutures);
//                    // Throw exception back to the leader.
//                    if (causes.size() == 1)
//                        throw new RuntimeException(causes.get(0));
//                    throw new RuntimeException("remote errors: nfailures="
//                            + causes.size(), new ExecutionExceptions(causes));
//                }

            } finally {
//                /*
//                 * Regardless of outcome or errors above, ensure that all remote
//                 * futures are cancelled.
//                 */
//                for (int i = 0; i < remoteFutures.length; i++) {
//                    final Future<Void> rf = remoteFutures[i];
//                    if (!remoteDone[i]) {
//                        // Cancel the request on the remote service (RMI).
//                        try {
//                            rf.cancel(true/* mayInterruptIfRunning */);
//                        } catch (Throwable t) {
//                            // ignored.
//                        }
//                    }
//                }
                if (!barrier.isBroken()) {
                    /*
                     * If there were any followers that did not message the
                     * leader and cause the barrier to be decremented, then we
                     * need to decrement the barrier for those followers now in
                     * order for it to break.
                     * 
                     * There is no method to decrement by a specific number
                     * (unlike a semaphore), but you can reset() the barrier,
                     * which will cause a BrokenBarrierException for all Threads
                     * waiting on the barrier.
                     * 
                     * FIXME HA TXS: A reset() here does not allow us to proceed
                     * with the consensus protocol unless all services
                     * "vote yes". Thus, a single node failure during the
                     * release time consensus protocol will cause the commit to
                     * fail. [Actually, we could use getNumberWaiting(). If it
                     * is a bare majority, then we could force the barrier to
                     * meet break (either with reset or with running an await()
                     * in other threads) and take the barrier break action
                     * ourselves. E.g., in the thread that calls
                     * barrier.reset()].
                     */
                    barrier.reset();
                }

            }// finally

        }

//        /**
//         * Wait on the {@link CyclicBarrier}, but do this in a loop so we can
//         * watch for a quorum break or service leave.
//         * 
//         * @param quorum
//         * @param barrier
//         * @param timeout
//         * @param units
//         * 
//         * @throws BrokenBarrierException
//         * @throws InterruptedException
//         * @throws TimeoutException
//         */
//        private void spinWaitBarrier(
//                final Quorum<HAGlue, QuorumService<HAGlue>> quorum,
//                final CyclicBarrier barrier, final long token,
//                final long timeout, final TimeUnit unit)
//                throws BrokenBarrierException, InterruptedException,
//                TimeoutException {
//
//            if (log.isInfoEnabled())
//                log.info("Waiting at barrier: #parties=" + barrier.getParties()
//                        + ", #waiting=" + barrier.getNumberWaiting()
//                        + ", isBroken=" + barrier.isBroken() + ", token="
//                        + token + ", timeout=" + timeout + ", unit=" + unit);
//
//            // How lock to block in each iteration.
//            final long blockNanos = TimeUnit.MILLISECONDS.toNanos(10000);
//
//            final long begin = System.nanoTime();
//            final long nanos = unit.toNanos(timeout);
//            long remaining = nanos;
//            long nspin = 0L;
//
//            try {
//
//                while (remaining > 0) {
//
//                    nspin++;
//
//                    remaining = nanos - (System.nanoTime() - begin);
//
//                    try {
//
//                        // Verify that this service remains the leader.
//                        quorum.assertLeader(token);
//
//                        // Verify messaged services are still joined.
//                        assertServicesStillJoined(quorum);
//                        
//                        /*
//                         * If we observe a serviceLeave for any service that we
//                         * are awaiting, then we need to stop waiting on that
//                         * service. This could be achieved by running a Thread
//                         * that did a barrier.await() on the behalf of that
//                         * service, but only if that service has not yet
//                         * responded with its input for the consensus protocol
//                         * [if it has responded then it is probably already at
//                         * barrier.await() in a Thread on the leader for that
//                         * follower.]
//                         */
//                        final long awaitNanos = Math.min(blockNanos, remaining);
//
//                        /*
//                         * Await barrier, up to the timeout.
//                         * 
//                         * Note: Contrary to the javadoc, barrier.await(timeout)
//                         * will break the barrier if the timeout is exceeded!!!
//                         */
//                        barrier.await(awaitNanos, TimeUnit.NANOSECONDS);
//
//                        // Done.
//                        return;
//
//                    } catch (TimeoutException e) {
//                        // Spin.
//                        continue;
//                    } catch (InterruptedException e) {
//                        throw e;
//                    } catch (BrokenBarrierException e) {
//                        throw e;
//                    }
//
//                }
//
//            } finally {
//
//                /*
//                 * Note: On exit, the caller must reset() the barrier if it is
//                 * not yet broken.
//                 */
//
//                if (log.isInfoEnabled())
//                    log.info("barrier: #parties=" + barrier.getParties()
//                            + ", #waiting=" + barrier.getNumberWaiting()
//                            + ", isBroken=" + barrier.isBroken() + ", #spin="
//                            + nspin);
//
//            }
//
//        }
        
        /**
         * Verify that the services that were messaged for the release time
         * consensus protocol are still joined with the met quorum.
         * 
         * @throws QuorumException
         *             if one of the joined services leaves.
         */
        private void assertServicesStillJoined(
                final Quorum<HAGlue, QuorumService<HAGlue>> quorum)
                throws QuorumException {

            final UUID[] tmp = quorum.getJoined();

            for (UUID serviceId : joinedServiceIds) {

                boolean found = false;
                for (UUID t : tmp) {
                    if (serviceId.equals(t)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {

                    throw new QuorumException(
                            "Service leave during consensus protocol: "
                                    + serviceId);

                }

            }

        }

    }

//    /**
//     * The maximum error allowed (milliseconds) in the clocks.
//     */
//    private static final long epsilon = 3;
    
    /**
     * Assert that t1 LT t2.
     * 
     * @param t1
     * @param t2
     * 
     * @throws ClocksNotSynchronizedException
     */
    private void assertBefore(final long t1, final long t2) {

        if (t1 < t2)
            return;

        throw new ClocksNotSynchronizedException();

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extends the {@link JournalTransactionService} to provide protection for
     * the session protection mode of the {@link RWStore} and to support the
     * {@link HATXSGlue} interface.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     * @see <a href=
     *      "https://docs.google.com/document/d/14FO2yJFv_7uc5N0tvYboU-H6XbLEFpvu-G8RhAzvxrk/edit?pli=1#"
     *      > HA TXS Design Document </a>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/623" > HA
     *      TXS / TXS Bottleneck </a>
     */
    private class InnerJournalTransactionService extends
            JournalTransactionService {

        protected InnerJournalTransactionService() {

            super(checkProperties(properties), Journal.this);

            final long lastCommitTime = Journal.this.getLastCommitTime();

            if (lastCommitTime != 0L) {

                /*
                 * Notify the transaction service on startup so it can set the
                 * effective release time based on the last commit time for the
                 * store.
                 * 
                 * Note: For HA, the releaseTime is updated by the consensus
                 * protocol once a quorum is met. Before the quorum meets (and
                 * before a service joins with a met quorum) each service will
                 * track its own releaseTime. Therefore, during startup, the
                 * quorum will be null or HAStatusEnum will be NotReady so the
                 * TXS will automatically track the release time until the
                 * service joins with a met quorum.
                 */
                
                if (log.isInfoEnabled())
                    log.info("Startup: lastCommitTime=" + lastCommitTime);
                
                updateReleaseTimeForBareCommit(lastCommitTime);

            }
        
        }

        /**
         * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/445" >
         *      RWStore does not track tx release correctly </a>
         */
        final private ConcurrentHashMap<Long, IRawTx> m_rawTxs = new ConcurrentHashMap<Long, IRawTx>();

        /**
         * This lock is used to ensure that the following actions are MUTEX:
         * <ul>
         * <li>The barrier where we obtain a consensus among the services joined
         * with the met quorum concerning the new release time.</li>
         * <li>A remote service that wishes to join an already met quorum.</li>
         * <li>A new transaction start that would read on a commit point which
         * is LT than the readsOnCommitTime of the earliestActiveTx for this
         * service but GT earliest visible commit point for this service (as
         * determined by the releaseTime on the transaction service).</li>
         * </ul>
         * Any of these actions must contend for the {@link #barrierLock}.
         */
        final private ReentrantLock barrierLock = new ReentrantLock();
        
//        final private Condition barrierBroke = barrierLock.newCondition();
        
        /**
         * This is used to coordinate the protocol for achiving an atomic
         * consensus on the new <i>releaseTime</i> for the services joined with
         * a met quorum.
         */
        final private AtomicReference<BarrierState> barrierRef = new AtomicReference<BarrierState>();
        
        @Override
        public void runWithBarrierLock(final Runnable r) {
            
            barrierLock.lock();
            try {
                r.run();
            } finally {
                barrierLock.unlock();
            }
            
        }
        
        /**
         * {@inheritDoc}
         * <p>
         * We need obtain a distributed consensus for the services joined with
         * the met quorum concerning the earliest commit point that is pinned by
         * the combination of the active transactions and the minReleaseAge on
         * the TXS.
         * <p>
         * New transaction starts during this critical section will block (on
         * the leader or the folllower) unless they are guaranteed to be
         * allowable, e.g., based on the current minReleaseAge, the new tx would
         * read from the most recent commit point, the new tx would ready from a
         * commit point that is already pinned by an active transaction on that
         * node, etc.
         * 
         * @throws IOException
         * @throws BrokenBarrierException
         */
        // Note: Executed on the leader.
        @Override
        public void updateReleaseTimeConsensus(final long timeout,
                final TimeUnit units) throws IOException, InterruptedException,
                TimeoutException, BrokenBarrierException {

            final long token = getQuorum().token();
            
            final BarrierState barrierState;
            
            barrierLock.lock();

            try {

                getQuorum().assertLeader(token);

                if (!barrierRef.compareAndSet(null/* expectedValue */,
                        barrierState = new BarrierState()/* newValue */)) {

                    throw new IllegalStateException();

                }

                try {

                    /*
                     * Message the followers and block until the barrier breaks.
                     */
                    barrierState.messageFollowers(token, timeout, units);

                } finally {

                    // Clear the barrierRef.
                    if (!barrierRef.compareAndSet(barrierState/* expected */,
                            null)) {

                        throw new AssertionError();

                    }

                }

                /*
                 * Update the release time on the leader
                 */
                
                final IHANotifyReleaseTimeResponse consensus = barrierState.consensus;

                if (consensus == null) {

                    throw new RuntimeException("No consensus");

                }
                
                final long consensusValue = consensus.getCommitTime();

                final long newReleaseTime = Math.max(0L, consensusValue - 1);
                
                if (log.isInfoEnabled())
                    log.info("Advancing releaseTime on leader: "
                            + newReleaseTime);

                setReleaseTime(newReleaseTime);

            } finally {

                barrierLock.unlock();

            }

        }
        
        /**
         * {@inheritDoc}
         * <p>
         * Overridden to notice whether this service is using the consensus
         * protocol to update the releaseTime or updating it automatically as
         * transactions complete.
         * 
         * @see <a href=
         *      "https://sourceforge.net/apps/trac/bigdata/ticket/530#comment:116">
         *      Journal HA </a>
         */
        @Override
        protected boolean isReleaseTimeConsensusProtocol() {

            final HAStatusEnum haStatus = getHAStatus();
            
            if (haStatus == null || haStatus == HAStatusEnum.NotReady) {
            
                /*
                 * Since we are not HA or this service is not HAReady, we will
                 * not use the consensus protocol to update the releaseTime.
                 * 
                 * Therefore the releaseTime is updated here since we will not
                 * (actually, did not) run the consensus protocol to update it.
                 */

                return false;
                
            }
            
            /*
             * Note: When we are using a 2-phase commit, the leader can not
             * update the release time from commit() using this methods. It
             * must rely on the consensus protocol to update the release
             * time instead.
             */

            return true;

        }
        
//        /**
//         * {@inheritDoc}
//         * <p>
//         * Note: When we are using a 2-phase commit, the leader can not update
//         * the release time from commit() using this methods. It must rely on
//         * the consensus protocol to update the release time instead.
//         * 
//         * @see <a href=
//         *      "https://sourceforge.net/apps/trac/bigdata/ticket/530#comment:116">
//         *      Journal HA </a>
//         */
//        @Override
//        protected void updateReleaseTimeForBareCommit(final long commitTime) {
//
//            final HAStatusEnum haStatus = getHAStatus();
//            
//            if (haStatus == null || haStatus == HAStatusEnum.NotReady) {
//
//                /*
//                 * Since we are not HA or this service is not HAReady, we will
//                 * not use the consensus protocol to update the releaseTime.
//                 * 
//                 * Therefore the releaseTime is updated here since we will not
//                 * (actually, did not) run the consensus protocol to update it.
//                 */
//                super.updateReleaseTimeForBareCommit(commitTime);
//                
//            } else {
//                
//                /*
//                 * Note: When we are using a 2-phase commit, the leader can not
//                 * update the release time from commit() using this methods. It
//                 * must rely on the consensus protocol to update the release
//                 * time instead.
//                 */
//            
//            }
//            
//        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to take the necessary lock since we are invoking this
         * method from contexts in which the lock would not otherwise be held.
         */
        @Override
        protected void setReleaseTime(final long newValue) {

            if (newValue < 0)
                throw new IllegalArgumentException();

            lock.lock();
            
            try {
            
                super.setReleaseTime(newValue);
                
            } finally {
                
                lock.unlock();
                
            }

        }

        /**
         * Factory for {@link IHANotifyReleaseTimeRequest} messages. This is
         * used by both the leader and the followers.
         * 
         * @param serviceId
         *            The {@link UUID} for this service.
         * @return The new message.
         */
        protected IHANotifyReleaseTimeRequest newHANotifyReleaseTimeRequest(
                final UUID serviceId) {

            // On AbstractTransactionService.
            final long effectiveReleaseTimeForHA = getEffectiveReleaseTimeForHA();

            // On AbstractJournal
            final ICommitRecord commitRecord = getEarliestVisibleCommitRecordForHA(effectiveReleaseTimeForHA);

            final long commitCounter = commitRecord == null ? 0
                    : commitRecord.getCommitCounter();

            final long commitTime = commitRecord == null ? 0
                    : commitRecord.getTimestamp();

            final long now = getLocalTransactionManager().nextTimestamp();

            final IHANotifyReleaseTimeRequest req = new HANotifyReleaseTimeRequest(
                    serviceId, commitTime, commitCounter, now);

            if (log.isTraceEnabled())
                log.trace("releaseTime=" + getReleaseTime()//
                        + ",effectiveReleaseTimeForHA="
                        + effectiveReleaseTimeForHA //
                        + ",rootBlock=" + getRootBlockView() //
                        + ",req=" + req//
                        );

            return req;

        }

        /**
         * Return the {@link GatherTask} that will be executed by the follower.
         */
        @Override
        public Callable<Void> newGatherMinimumVisibleCommitTimeTask(
                final IHAGatherReleaseTimeRequest req) {

            return new GatherTask(req);

        }

        /**
         * {@inheritDoc}
         * <p>
         * Note: This method is implemented by {@link AbstractJournal.BasicHA}
         * which calls through to
         * {@link #newGatherMinimumVisibleCommitTimeTask(IHAGatherReleaseTimeRequest)}
         * 
         * @throws UnsupportedOperationException
         */
        @Override
        public void gatherMinimumVisibleCommitTime(
                final IHAGatherReleaseTimeRequest req) throws IOException {

            throw new UnsupportedOperationException();
            
        }
        
        /**
         * "Gather" task runs on the followers.
         * <p>
         * Note: The gather task scopes the consensus protocol on the follower.
         * It contends for the {@link #barrierLock} (on the follower) in order
         * to be MUTEX with new read-only tx starts on the follower which (a)
         * occur during the consensus protocol; and (b) would read on a commit
         * point that is not pinned by any of an active transaction on the
         * follower, the minReleaseAge, or being the most recent commit point.
         * These are the criteria that allow {@link #newTx(long)} to NOT contend
         * for the {@link #barrierLock}.
         * 
         * @see #newTx(long)
         */
        private class GatherTask implements Callable<Void> {

            private final IHAGatherReleaseTimeRequest req;

            public GatherTask(final IHAGatherReleaseTimeRequest req) {

                if (req == null)
                    throw new IllegalArgumentException();

                this.req = req;

            }
            
            public Void call() throws Exception {

                if (log.isInfoEnabled())
                    log.info("Running gather on follower: " + getServiceUUID());

                HAGlue leader = null;
                
                boolean didNotifyLeader = false;
                
                barrierLock.lock(); // take lock on follower!

                try {

                    // This timestamp is used to help detect clock skew.
                    final long now = nextTimestamp();

                    // Verify event on leader occurs before event on follower.
                    assertBefore(req.getTimestampOnLeader(), now);

                    final long token = req.token();

                    getQuorum().assertQuorum(token);

                    final QuorumService<HAGlue> quorumService = getQuorum()
                            .getClient();

                    if (!quorumService.isFollower(token))
                        throw new QuorumException();

                    leader = quorumService.getLeader(token);

                    final IHANotifyReleaseTimeRequest req2 = newHANotifyReleaseTimeRequest(quorumService
                            .getServiceId());
                    
                    /*
                     * RMI to leader.
                     * 
                     * Note: Will block until barrier breaks on the leader.
                     */
                    
                    didNotifyLeader = true;

                    final IHANotifyReleaseTimeResponse consensusReleaseTime = leader
                            .notifyEarliestCommitTime(req2);

                    /*
                     * Now spot check the earliest active tx on this follower.
                     * We want to make sure that this tx is not reading against
                     * a commit point whose state would be released by the new
                     * consensus releaseTime.
                     * 
                     * If everything is Ok, we update the releaseTime on the
                     * follower.
                     */

                    lock.lock();
                    
                    try {
                    
                        // the earliest active tx on this follower.
                        final TxState txState = getEarliestActiveTx();

                        // Consensus for new earliest visible commit time.
                        final long t2 = consensusReleaseTime.getCommitTime();

                        if (txState != null
                                && txState.getReadsOnCommitTime() < t2) {

                            /*
                             * At least one transaction exists on the follower
                             * that is reading on a commit point LT the commit
                             * point which would be released. This is either a
                             * failure in the logic to compute the consensus
                             * releaseTime or a failure to exclude new
                             * transaction starts on the follower while
                             * computing the new consensus releaseTime.
                             */

                            throw new AssertionError(
                                    "The releaseTime consensus would release a commit point with active readers"
                                            + ": consensus=" + consensusReleaseTime
                                            + ", earliestActiveTx=" + txState);

                        }

                        final long newReleaseTime = Math.max(0L,
                                consensusReleaseTime.getCommitTime() - 1);

                        if (log.isInfoEnabled())
                            log.info("Advancing releaseTime on follower: "
                                    + newReleaseTime);

                        // Update the releaseTime on the follower
                        setReleaseTime(newReleaseTime);

                    } finally {
                        
                        lock.unlock();
                        
                    }

                    // Done.
                    return null;
                    
                } catch (Throwable t) {

                    log.error(t, t);
                    
                    if (!didNotifyLeader && leader != null) {

                        /*
                         * Send a [null] to the leader so it does not block
                         * forever waiting for our response.
                         */
                        
                        try {
                            leader.notifyEarliestCommitTime(null/* resp */);
                        } catch (Throwable t2) {
                            log.error(t2, t2);
                        }

                    }

                    throw new Exception(t);
                    
                } finally {

                    barrierLock.unlock();

                }
                
            }

        } // GatherTask
        
        /**
         * {@inheritDoc}
         * <p>
         * Note: Message sent by follower (RMI). Method executes on leader.
         * <p>
         * We pass the message through to the {@link BarrierState} object.
         * <p>
         * Note: We MUST NOT contend for the {@link #barrierLock} here. That
         * lock is held by the Thread that invoked
         * {@link #updateReleaseTimeConsensus()}.
         * 
         * TODO HA TXS: We should ensure that the [req] is for the same gather()
         * request as this barrier instance. That will let us detect a service
         * that responds late (after a transient disconnect) when the leader has
         * moved on to another commit. See BarrierState#token for more on this.
         * [Note that [req] can be [null if the follower was unable to produce a
         * valid response.]
         */
        @Override
        public IHANotifyReleaseTimeResponse notifyEarliestCommitTime(
                final IHANotifyReleaseTimeRequest req) throws IOException,
                InterruptedException, BrokenBarrierException {

            final BarrierState barrierState = barrierRef.get();

            if (barrierState == null)
                throw new IllegalStateException();

            try {

                getQuorum().assertLeader(barrierState.token);

                // ServiceId of the follower (NPE if req is null).
                final UUID followerId = req.getServiceUUID();

                // Make a note of the message from this follower.
                barrierState.responses.put(followerId, req);
                
            } finally {
                
                /*
                 * Block until barrier breaks.
                 * 
                 * Note: We want to await() on the barrier even if there is an
                 * error in the try{} block. This is necessary to decrement the
                 * barrier count down to zero.
                 */

                // follower blocks on Thread on the leader here.
                barrierState.barrier.await();
                
            }

            // Return the consensus.
            final IHANotifyReleaseTimeResponse resp = barrierState.consensus;

            if (resp == null) {

                throw new RuntimeException("No consensus");

            }

            return resp;
  
        }

        /**
         * Helper method returns the {@link HAStatusEnum} -or- <code>null</code>
         * if this is not HA or if the {@link Quorum} is not running. This is a
         * <em>low latency local</em> method call. The code path is against the
         * local (non-remote) HAGlue object. It is NOT an RMI.
         * 
         * @return The {@link HAStatusEnum} or <code>null</code>.
         */
        private final HAStatusEnum getHAStatus() {
            
            // Quorum iff HA.
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

            if(quorum == null) {
             
                // Not HA.
                return null;
                
            }
            
            // Note: This is the local service interface.
            final HAGlue localService;
            try {

                localService = quorum.getClient().getService();
                
            } catch (IllegalStateException ex) {
                /*
                 * Quorum client is not running (not started or terminated).
                 */
                return null;

            }
            
            // Note: Invocation against local HAGlue object (NOT RMI).
            try {

                return localService.getHAStatus();
                
            } catch (IOException ex) {
                
                // Note: Exception is never thrown (not RMI).
                throw new RuntimeException(ex);
                
            }

        }
        
        @Override
        public long newTx(final long timestamp) {

            if (TimestampUtility.isReadWriteTx(timestamp)) {

                // The caller has provided a TxId, not a timestamp.
                throw new IllegalArgumentException();
                
            }
            
            // The HAStatusEnum -or- null if not HA.
            final HAStatusEnum haStatus = getHAStatus();

            if (haStatus == null) {
             
                // Not HA.
                return _newTx(timestamp);
                
            }

            if (haStatus == HAStatusEnum.NotReady) {

                // Not ready.
                throw new QuorumException();

            }

            if (timestamp == ITx.UNISOLATED && haStatus != HAStatusEnum.Leader) {

                // Read/Write Tx starts are only allowed on the Leader.
                throw new QuorumException("Not quorum leader");

            }

            if (timestamp == ITx.UNISOLATED || timestamp == ITx.READ_COMMITTED) {

                /*
                 * A read-write tx reads on the current commit point.
                 * 
                 * A read-committed tx reads on the current commit point.
                 * 
                 * The current commit point is always visible, so these requests
                 * are non-blocking.
                 * 
                 * Note: We have verified that this service is the quorum leader
                 * above if the request is for a read-write tx.
                 */
        
                return _newTx(timestamp);
                
            }

            /*
             * The request is a read-only tx against some specific historical
             * commit point. It will be allowed (without blocking at the
             * barrier) if the commit point is known to be pinned based on
             * either the minReleaseAge or the earliestActiveTx. We use the
             * AbstractTransactionService's lock to make these inspections
             * atomic.
             */
            
            lock.lock(); // Note: AbstractTransactionService.lock

            try {

                final long now = nextTimestamp();

                final long minReleaseAge = getMinReleaseAge();

                final long ageOfTxView = now - timestamp;

                if (ageOfTxView < minReleaseAge) {

                    // Start tx. Commit point pinned by minReleaseAge.
                    return _newTx(timestamp);

                }

                /*
                 * Handle commit point pinned by earliestActiveTx's
                 * readsOnCommitTime.
                 */
                {

                    final TxState state = getEarliestActiveTx();

                    if (state != null && state.getReadsOnCommitTime() <= timestamp) {

                        // Start Tx. Commit point pinned by earliestActiveTx.
                        return _newTx(timestamp);

                    }

                }

                final IRootBlockView rootBlock = getRootBlockView();
                
                if (rootBlock.getCommitCounter() == 0L) {

                    // Start Tx. No commits so nothing could be released.
                    return _newTx(timestamp);

                }

                if (rootBlock.getLastCommitTime() <= timestamp) {

                    // Tx reads on most recent commit point.
                    return _newTx(timestamp);

                }

            } finally {
            
                lock.unlock();
                
            }

            /*
             * Must block at barrier.
             */
            
            barrierLock.lock();
            
            try {

                return this._newTx(timestamp);
                                
            } finally {
                
                barrierLock.unlock();
                
            }

        }
        
        /**
         * Core impl.
         * <p>
         * This code pre-increments the active transaction count within the
         * RWStore before requesting a new transaction from the transaction
         * service. This ensures that the RWStore does not falsely believe
         * that there are no open transactions during the call to
         * AbstractTransactionService#newTx().
         * <p>
         * Note: This code was moved into the inner class extending the
         * {@link JournalTransactionService} in order to ensure that we
         * follow this pre-incremental pattern for an {@link HAJournal} as
         * well.
         * 
         * @see <a
         *      href="https://sourceforge.net/apps/trac/bigdata/ticket/440#comment:13">
         *      BTree can not be case to Name2Addr </a>
         * @see <a
         *      href="https://sourceforge.net/apps/trac/bigdata/ticket/530">
         *      Journal HA </a>
         */
        private final long _newTx(final long timestamp) {

            IRawTx tx = null;
            try {
            
                final IBufferStrategy bufferStrategy = getBufferStrategy();

                if (bufferStrategy instanceof IHistoryManager) {

                    // pre-increment the active tx count.
                    tx = ((IHistoryManager) bufferStrategy).newTx();
                    
                }

                return super.newTx(timestamp);

            } finally {

                if (tx != null) {

                    /*
                     * If we had pre-incremented the transaction counter in
                     * the RWStore, then we decrement it before leaving this
                     * method.
                     */

                    tx.close();

                }

            }

        }
        
        @Override
        public long commit(final long tx) {

            final TxState state = getTxState(tx);

            final Quorum<HAGlue, QuorumService<HAGlue>> quorum = getQuorum();

            if (quorum != null && state != null && !state.isReadOnly()) {

                /*
                 * Commit on write transaction. We must be the quorum leader.
                 */

                final long token = getQuorumToken();

                getQuorum().assertLeader(token);

            }

            return super.commit(tx);

        }
        @Override
        protected void activateTx(final TxState state) {
            if (txLog.isInfoEnabled())
                txLog.info("OPEN : txId=" + state.tx
                        + ", readsOnCommitTime=" + state.getReadsOnCommitTime());
            final IBufferStrategy bufferStrategy = Journal.this.getBufferStrategy();
            if (bufferStrategy instanceof IHistoryManager) {
                final IRawTx tx = ((IHistoryManager)bufferStrategy).newTx();
                if (m_rawTxs.put(state.tx, tx) != null) {
                    throw new IllegalStateException(
                            "Unexpected existing RawTx");
                }
            }
            super.activateTx(state);
        }
        @Override
        protected void deactivateTx(final TxState state) {
            if (txLog.isInfoEnabled())
                txLog.info("CLOSE: txId=" + state.tx
                        + ", readsOnCommitTime=" + state.getReadsOnCommitTime());
            /*
             * Note: We need to deactivate the tx before RawTx.close() is
             * invoked otherwise the activeTxCount will never be zero inside
             * of RawTx.close() and the session protection mode of the
             * RWStore will never be able to release storage.
             */
            super.deactivateTx(state);
            
            final IRawTx tx = m_rawTxs.remove(state.tx);
            if (tx != null) {
                tx.close();
            }
        }

    } // class InnerJournalTransactionService
    
    protected JournalTransactionService newTransactionService() {
        
        final JournalTransactionService abstractTransactionService = new InnerJournalTransactionService();
   
        return abstractTransactionService;
        
    }
    
    protected AbstractLocalTransactionManager newLocalTransactionManager() {

        final JournalTransactionService abstractTransactionService = newTransactionService();

        abstractTransactionService.start();
        
        return new AbstractLocalTransactionManager() {

            public AbstractTransactionService getTransactionService() {
                
                return abstractTransactionService;
                
            }

            /**
             * Extended to shutdown the embedded transaction service.
             */
            @Override
            public void shutdown() {

                ((JournalTransactionService) getTransactionService())
                        .shutdown();

                super.shutdown();

            }

            /**
             * Extended to shutdown the embedded transaction service.
             */
            @Override
            public void shutdownNow() {

                ((JournalTransactionService) getTransactionService())
                        .shutdownNow();

                super.shutdownNow();

            }
        
        };

    }
    
    public AbstractLocalTransactionManager getLocalTransactionManager() {

        return localTransactionManager;

    }

    /**
     * Interface defines and documents the counters and counter namespaces
     * reported by the {@link Journal} and the various services which it uses.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static interface IJournalCounters extends
            ConcurrencyManager.IConcurrencyManagerCounters,
//            ...TransactionManager.XXXCounters,
            ResourceManager.IResourceManagerCounters
            {
       
        /**
         * The namespace for the counters pertaining to the {@link ConcurrencyManager}.
         */
        String concurrencyManager = "Concurrency Manager";

        /**
         * The namespace for the counters pertaining to the named indices.
         */
        String indexManager = "Index Manager";

        /**
         * The namespace for the counters pertaining to the {@link ILocalTransactionService}.
         */
        String transactionManager = "Transaction Manager";
        
        /**
         * The namespace for counters pertaining to the
         * {@link Journal#getExecutorService()}.
         */
        String executorService = "Executor Service";
        
        /**
         * Performance counters for the query engine associated with this
         * journal (if any).
         */
        String queryEngine = "Query Engine";
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to attach additional performance counters.
     */
    @Override
    public CounterSet getCounters() {

        final CounterSet root = new CounterSet();

        // Host wide performance counters (collected from the OS).
        {

            final AbstractStatisticsCollector t = getPlatformStatisticsCollector();

            if (t != null) {

                root.attach(t.getCounters());

            }
            
        }
        
        // JVM wide performance counters.
        {
            
            final CounterSet tmp = root.makePath("JVM");
            
            tmp.attach(AbstractStatisticsCollector.getMemoryCounterSet());
            
        }

        // Journal performance counters.
        {

            final CounterSet tmp = root.makePath("Journal");

			tmp.attach(super.getCounters());

			// Live index counters iff available.
            {

                final CounterSet indexCounters = getIndexCounters();

                if (indexCounters != null) {
                    
                    tmp.makePath(IJournalCounters.indexManager).attach(
                            indexCounters);
                    
                }

            }

			tmp.makePath(IJournalCounters.concurrencyManager)
                    .attach(concurrencyManager.getCounters());

            tmp.makePath(IJournalCounters.transactionManager)
                    .attach(localTransactionManager.getCounters());

            {

                final IPlugIn<Journal, ThreadPoolExecutorBaseStatisticsTask> plugin = pluginQueueStats
                        .get();

                if (plugin != null) {

                    final ThreadPoolExecutorBaseStatisticsTask t = plugin
                            .getService();

                    if (t != null) {

                        tmp.makePath(IJournalCounters.executorService).attach(
                                t.getCounters());

                    }

                }
                
            }

        }
        
        // Lookup an existing query engine, but do not cause one to be created.
        final QueryEngine queryEngine = QueryEngineFactory
                .getExistingQueryController(this);

        if (queryEngine != null) {

            final CounterSet tmp = root.makePath(IJournalCounters.queryEngine);

            tmp.attach(queryEngine.getCounters());
            
        }

        return root;
        
    }
    
    /*
     * IResourceManager
     */

    @Override
    public File getTmpDir() {
        
        return tmpDir;
        
    }
    
    /**
     * The directory in which the journal's file is located -or-
     * <code>null</code> if the journal is not backed by a file.
     */
    @Override
    public File getDataDir() {
        
        final File file = getFile();
        
        if (file == null) {

            return null;

        }
        
        return file.getParentFile();
        
    }

    /**
     * Note: This will only succeed if the <i>uuid</i> identifies <i>this</i>
     * journal.
     */
    public IRawStore openStore(final UUID uuid) {
    
        if(uuid == getRootBlockView().getUUID()) {
            
            return this;
            
        }

        throw new UnsupportedOperationException();
        
    }
        
    /**
     * Always returns an array containing a single {@link BTree} which is the
     * {@link BTree} loaded from the commit record whose commit timestamp is
     * less than or equal to <i>timestamp</i> -or- <code>null</code> if there
     * are no {@link ICommitRecord}s that satisfy the probe or if the named
     * index was not registered as of that timestamp.
     * 
     * @param name
     * @param timestamp
     * 
     * @throws UnsupportedOperationException
     *             If the <i>timestamp</i> is {@link ITx#READ_COMMITTED}. You
     *             MUST use {@link #getIndex(String, long)} in order to obtain a
     *             view that has {@link ITx#READ_COMMITTED} semantics.
     */
    public AbstractBTree[] getIndexSources(final String name,
            final long timestamp) {

        final BTree btree;
        
        if (timestamp == ITx.UNISOLATED) {
        
            /*
             * Unisolated operation on the live index.
             */
            
            // MAY be null.
            btree = getIndex(name);

        } else if (timestamp == ITx.READ_COMMITTED) {

            /*
             * BTree does not know how to update its view with intervening
             * commits. Further, for a variety of reasons including the
             * synchronization problems that would be imposed, there are no
             * plans for BTree to be able to provide read-committed semantics.
             * Instead a ReadCommittedView is returned by
             * getIndex(name,timestamp) when ITx#READ_COMMITTED is requested and
             * this method is not invoked.
             */
            throw new UnsupportedOperationException("Read-committed view");
            
//            /*
//             * Read committed operation against the most recent commit point.
//             * 
//             * Note: This commit record is always defined, but that does not
//             * mean that any indices have been registered.
//             */
//
//            final ICommitRecord commitRecord = getCommitRecord();
//
//            final long ts = commitRecord.getTimestamp();
//
//            if (ts == 0L) {
//
//                log.warn("Nothing committed: name="+name+" - read-committed operation.");
//
//                return null;
//
//            }
//
//            // MAY be null.
//            btree = getIndex(name, commitRecord);
//
//            if (btree != null) {
//
////                /*
////                 * Mark the B+Tree as read-only.
////                 */
////                
////                btree.setReadOnly(true);
//
//                assert ((BTree) btree).getLastCommitTime() != 0;
////                btree.setLastCommitTime(commitRecord.getTimestamp());
//                
//            }
            
        } else {

            /*
             * A specified historical index commit point.
             * 
             * @see <a
             * href="http://sourceforge.net/apps/trac/bigdata/ticket/546" > Add
             * cache for access to historical index views on the Journal by name
             * and commitTime. </a>
             */
            
            final long ts = Math.abs(timestamp);

//            final ICommitRecord commitRecord = getCommitRecord(ts);
//
//            if (commitRecord == null) {
//
//                log.warn("No commit record: name=" + name + ", timestamp=" + ts);
//
//                return null;
//                
//            }
//
//            // MAY be null
//            btree = getIndex(name, commitRecord);

            // MAY be null
            btree = (BTree) super.getIndex(name, ts);

            if (btree != null) {

//                /*
//                 * Mark the B+Tree as read-only.
//                 */
//                
//                btree.setReadOnly(true);
                
                assert btree.getLastCommitTime() != 0;
//                btree.setLastCommitTime(commitRecord.getTimestamp());
                
            }

        }
        
        /* 
         * No such index as of that timestamp.
         */

        if (btree == null) {

            if (log.isInfoEnabled())
                log.info("No such index: name=" + name + ", timestamp="
                        + timestamp);

            return null;

        }

        return new AbstractBTree[] {

                btree

        };

    }

    /**
     * Always returns <i>this</i>.
     */
    final public AbstractJournal getLiveJournal() {

        return this;

    }
    
    /**
     * Always returns <i>this</i>.
     */
    final public AbstractJournal getJournal(final long timestamp) {
        
        return this;
        
    }

    /**
     * Compacts the named indices found on this journal as of the most recent
     * commit point, writing their view onto a new Journal. This method MAY be
     * used concurrently with the {@link Journal} but writes after the selected
     * commit point WILL NOT be reflected in the output file. Typical uses are
     * to reduce the space required by the backing store, to improve locality in
     * the backing store, and to make a backup of the most recent commit point.
     * 
     * @param outFile
     *            The file on which the new journal will be created.
     * 
     * @return The {@link Future} on which you must {@link Future#get() wait}
     *         for the {@link CompactTask} to complete. The already open journal
     *         is accessible using {@link Future#get()}. If you are backing up
     *         data, then be sure to shutdown the returned {@link Journal} so
     *         that it can release its resources.
     */
    public Future<Journal> compact(final File outFile) {

        return executorService.submit(new CompactTask(this, outFile,
                getLastCommitTime()));
        
    }

    @Override
	public void dropIndex(final String name) {

		final BTreeCounters btreeCounters = getIndexCounters(name);

		super.dropIndex(name);

		if (btreeCounters != null) {

			// Conditionally remove the counters for the old index.
			indexCounters.remove(name, btreeCounters);

		}
    	
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Note: {@link ITx#READ_COMMITTED} views are given read-committed semantics
     * using a {@link ReadCommittedView}.  This means that they can be cached
     * since the view will update automatically as commits are made against
     * the {@link Journal}.
     *  
     * @see IndexManager#getIndex(String, long)
     */
    @Override
    public ILocalBTreeView getIndex(final String name, final long timestamp) {
        
        if (name == null) {

            throw new IllegalArgumentException();

        }

        final boolean isReadWriteTx = TimestampUtility.isReadWriteTx(timestamp);

//        final Tx tx = (Tx) (isReadWriteTx ? getConcurrencyManager()
//                .getTransactionManager().getTx(timestamp) : null);
        final Tx tx = (Tx) getConcurrencyManager().getTransactionManager()
                .getTx(timestamp);

        if (isReadWriteTx) {

            if (tx == null) {

                log.warn("Unknown transaction: name=" + name + ", tx="
                        + timestamp);

                return null;

            }

            tx.lock.lock();

            try {

                if (!tx.isActive()) {

                    // typically this means that the transaction has already
                    // prepared.
                    log.warn("Transaction not active: name=" + name + ", tx="
                            + timestamp + ", prepared=" + tx.isPrepared()
                            + ", complete=" + tx.isComplete() + ", aborted="
                            + tx.isAborted());

                    return null;

                }

            } finally {

                tx.lock.unlock();

            }
                                
        }
        
        if( isReadWriteTx && tx == null ) {
        
            /*
             * Note: This will happen both if you attempt to use a transaction
             * identified that has not been registered or if you attempt to use
             * a transaction manager after the transaction has been either
             * committed or aborted.
             */
            
            log.warn("No such transaction: name=" + name + ", tx=" + timestamp);

            return null;
            
        }
        
        final boolean readOnly = TimestampUtility.isReadOnly(timestamp);
//        final boolean readOnly = (timestamp < ITx.UNISOLATED)
//                || (isReadWriteTx && tx.isReadOnly());

        final ILocalBTreeView tmp;

        if (isReadWriteTx) {

            /*
             * Isolated operation.
             * 
             * Note: The backing index is always a historical state of the named
             * index.
             * 
             * Note: Tx.getIndex() will pass through the actual commit time of
             * the ground state against which the transaction is reading (if it
             * is available, which it is on the local Journal).
             * 
             * @see <a
             * href="https://sourceforge.net/apps/trac/bigdata/ticket/266">
             * Refactor native long tx id to thin object</a>
             */

            final ILocalBTreeView isolatedIndex = tx.getIndex(name);

            if (isolatedIndex == null) {

                log.warn("No such index: name=" + name + ", tx=" + timestamp);

                return null;

            }

            tmp = isolatedIndex;

        } else {
            
            /*
             * Non-transactional view.
             */

            if (readOnly) {

                if (timestamp == ITx.READ_COMMITTED) {

                    // read-committed
                    
                    tmp = new ReadCommittedView(this, name);

                } else {

                    if (tx != null) {
  
                        /*
                         * read-only transaction
                         * 
                         * @see <a href=
                         * "http://sourceforge.net/apps/trac/bigdata/ticket/546"
                         * > Add cache for access to historical index views on
                         * the Journal by name and commitTime. </a>
                         */

                        final AbstractBTree[] sources = getIndexSources(name,
                                tx.getReadsOnCommitTime());

                        if (sources == null) {

                            log.warn("No such index: name=" + name
                                    + ", timestamp=" + timestamp);

                            return null;

                        }

                        assert sources[0].isReadOnly();

                        tmp = (BTree) sources[0];
                        
                    } else {

                        // historical read not protected by a transaction

                        final AbstractBTree[] sources = getIndexSources(name,
                                timestamp);

                        if (sources == null) {

                            log.warn("No such index: name=" + name
                                    + ", timestamp=" + timestamp);

                            return null;

                        }

                        assert sources[0].isReadOnly();

                        tmp = (BTree) sources[0];

                    }

                }
                
            } else {
                
                /*
                 * Writable unisolated index.
                 * 
                 * Note: This is the "live" mutable index. This index is NOT
                 * thread-safe. A lock manager is used to ensure that at most
                 * one task has access to this index at a time.
                 */

                assert timestamp == ITx.UNISOLATED;
                
                final AbstractBTree[] sources = getIndexSources(name, ITx.UNISOLATED);
                
                if (sources == null) {

                    if (log.isInfoEnabled())
                        log.info("No such index: name="+name+", timestamp="+timestamp);
                    
                    return null;
                    
                }

                assert ! sources[0].isReadOnly();

                tmp = (BTree) sources[0];

            }

        }
        
        /*
         * Make sure that it is using the canonical counters for that index.
         * 
         * Note: AbstractTask also does this for UNISOLATED indices which it
         * loads by itself as part of providing ACID semantics for add/drop
         * of indices.
         */

        tmp.getMutableBTree().setBTreeCounters(getIndexCounters(name));

        return tmp;

    }

    /**
     * Always returns the {@link BTree} as the sole element of the array since
     * partitioned indices are not supported.
     */
    public AbstractBTree[] getIndexSources(final String name,
            final long timestamp, final BTree btree) {
        
        return new AbstractBTree[] { btree };
        
    }

    /**
     * Create a new transaction on the {@link Journal}.
     * <p>
     * Note: This is a convenience method. The implementation of this method is
     * delegated to the object returned by {@link #getTransactionService()}.
     * 
     * @param timestamp
     *            A positive timestamp for a historical read-only transaction as
     *            of the first commit point LTE the given timestamp,
     *            {@link ITx#READ_COMMITTED} for a historical read-only
     *            transaction as of the most current commit point on the
     *            {@link Journal} as of the moment that the transaction is
     *            created, or {@link ITx#UNISOLATED} for a read-write
     *            transaction.
     * 
     * @return The transaction identifier.
     * 
     * @see ITransactionService#newTx(long)
     */
    final public long newTx(final long timestamp) {

        /*
         * Note: The RWStore native tx pre-increment logic is now handled by
         * _newTx() in the inner class that extends JournalTransactionService.
         */
        try {

            return getTransactionService().newTx(timestamp);

        } catch (IOException ioe) {

            /*
             * Note: IOException is declared for RMI but will not be thrown
             * since the transaction service is in fact local.
             */

            throw new RuntimeException(ioe);

        }

    }

    /**
     * Abort a transaction.
     * <p>
     * Note: This is a convenience method. The implementation of this method is
     * delegated to the object returned by {@link #getTransactionService()}.
     * 
     * @param tx
     *            The transaction identifier.
     *            
     * @see ITransactionService#abort(long)
     */
    final public void abort(final long tx) {

        try {

            /*
             * Note: TransactionService will make call back to the
             * localTransactionManager to handle the client side of the
             * protocol.
             */
            
            getTransactionService().abort(tx);

        } catch (IOException e) {
            
            /*
             * Note: IOException is declared for RMI but will not be thrown
             * since the transaction service is in fact local.
             */
            
            throw new RuntimeException(e);
            
        }

    }

    /**
     * Commit a transaction.
     * <p>
     * Note: This is a convenience method. The implementation of this method is
     * delegated to the object returned by {@link #getTransactionService()}.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @return The commit time assigned to that transaction.
     * 
     * @see ITransactionService#commit(long)
     */
    final public long commit(final long tx) throws ValidationError {

        try {

            /*
             * Note: TransactionService will make call back to the
             * localTransactionManager to handle the client side of the
             * protocol.
             */

            return getTransactionService().commit(tx);

        } catch (IOException e) {

            /*
             * Note: IOException is declared for RMI but will not be thrown
             * since the transaction service is in fact local.
             */

            throw new RuntimeException(e);

        }

    }

//    /**
//     * @deprecated This method in particular should be hidden from the
//     *             {@link Journal} as it exposes the {@link ITx} which really
//     *             deals with the client-side state of a transaction and which
//     *             should not be visible to applications - they should just use
//     *             the [long] transaction identifier.
//     */
//    public ITx getTx(long startTime) {
//    
//        return localTransactionManager.getTx(startTime);
//        
//    }

    /**
     * Returns the next timestamp from the {@link ILocalTransactionManager}.
     * <p>
     * Note: This is a convenience method. The implementation of this method is
     * delegated to the object returned by {@link #getTransactionService()}.
     * 
     * @deprecated This is here for historical reasons and is only used by the
     *             test suite. Use {@link #getLocalTransactionManager()} and
     *             {@link ITransactionService#nextTimestamp()}.
     * 
     * @see ITransactionService#nextTimestamp()
     */
    final public long nextTimestamp() {
    
        return localTransactionManager.nextTimestamp();
    
    }

    /*
     * IConcurrencyManager
     */
    
    public ConcurrencyManager getConcurrencyManager() {
        
        return concurrencyManager;
        
    }
    
    /**
     * Note: The transaction service is shutdown first, then the
     * {@link #executorService}, then the {@link IConcurrencyManager}, the
     * {@link ITransactionService} and finally the {@link IResourceLockService}.
     */
    synchronized public void shutdown() {
        
        if (!isOpen())
            return;

        /*
         * Shutdown the transaction service. This will not permit new
         * transactions to start and will wait until running transactions either
         * commit or abort.
         */
        localTransactionManager.shutdown();

        {

            final IPlugIn<?, ?> plugIn = pluginGanglia.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(false/* immediateShutdown */);

            }

        }
        
        {
        
            final IPlugIn<?, ?> plugIn = pluginQueueStats.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(false/* immediateShutdown */);

            }
            
        }

        {
         
            final IPlugIn<?, ?> plugIn = pluginPlatformStats.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(false/* immediateShutdown */);

            }
            
        }
        
        if (scheduledExecutorService != null) {

            scheduledExecutorService.shutdown();
            
        }
        
        // optional httpd service for the local counters.
        {
            
            final IPlugIn<?, ?> plugIn = pluginHttpd.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(false/* immediateShutdown */);

            }
            
        }
        
        /*
         * Shutdown the executor service. This will wait for any tasks being run
         * on that service by the application to complete.
         */
        try {

            new ShutdownHelper(executorService, 1000/* logTimeout */,
                    TimeUnit.MILLISECONDS) {
               
                protected void logTimeout() {

                    log.warn("Waiting on task(s)"
                            + ": elapsed="
                            + TimeUnit.NANOSECONDS.toMillis(elapsed())
                            + "ms, #active="
                            + ((ThreadPoolExecutor) executorService)
                                    .getActiveCount());
                    
                }

            };

        } catch (InterruptedException ex) {

            log.warn("Immediate shutdown: "+ex);
            
            // convert to immediate shutdown.
            shutdownNow();
            
            return;

        }

        /*
         * Shutdown the concurrency manager - this will allow existing
         * non-transactional operations to complete but prevent additional
         * operations from starting.
         */
        concurrencyManager.shutdown();
        
        super.shutdown();
        
    }

    /**
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionService} and finally the {@link IResourceManager}.
     */
    synchronized public void shutdownNow() {

        if (!isOpen())
            return;

        /*
         * Note: The ganglia plug in is executed on the main thread pool. We
         * need to terminate it in order for the thread pool to shutdown.
         */
        {

            final IPlugIn<?, ?> plugIn = pluginGanglia.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(true/* immediateShutdown */);

            }
            
        }

        {

            final IPlugIn<?, ?> plugIn = pluginQueueStats.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(true/* immediateShutdown */);

            }
            
        }

        {
         
            final IPlugIn<?, ?> plugIn = pluginPlatformStats.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(true/* immediateShutdown */);

            }
            
        }

        if (scheduledExecutorService != null)
            scheduledExecutorService.shutdownNow();
        
        // optional httpd service for the local counters.
        {
            
            final IPlugIn<?, ?> plugIn = pluginHttpd.get();

            if (plugIn != null) {

                // stop if running.
                plugIn.stopService(false/* immediateShutdown */);

            }
            
        }

        // Note: can be null if error in ctor.
        if (executorService != null)
            executorService.shutdownNow();

        // Note: can be null if error in ctor.
        if (concurrencyManager != null)
            concurrencyManager.shutdownNow();

        // Note: can be null if error in ctor.
        if (localTransactionManager != null)
            localTransactionManager.shutdownNow();

        super.shutdownNow();
        
    }
    
//    public void deleteResources() {
//        
//        super.deleteResources();
//        
//        // Note: can be null if error in ctor.
//        if (tempStoreFactory != null)
//            tempStoreFactory.closeAll();
//
//    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the {@link TemporaryStoreFactory}.
     */
    @Override
    protected void _close() {

        super._close();

        // Note: can be null if error in ctor.
        if (tempStoreFactory != null)
            tempStoreFactory.closeAll();

    }
    
    public <T> Future<T> submit(AbstractTask<T> task) {

        return concurrencyManager.submit(task);
        
    }

    @SuppressWarnings("rawtypes")
    public List<Future> invokeAll(
            Collection<? extends AbstractTask> tasks, long timeout,
            TimeUnit unit) throws InterruptedException {
        
        return concurrencyManager.invokeAll(tasks, timeout, unit);
        
    }

    public <T> List<Future<T>> invokeAll(
            Collection<? extends AbstractTask<T>> tasks)
            throws InterruptedException {
        
        return concurrencyManager.invokeAll(tasks);
        
    }

    public IResourceManager getResourceManager() {
        
        return concurrencyManager.getResourceManager();
        
    }

    public ILocalTransactionManager getTransactionManager() {

//        return concurrencyManager.getTransactionManager();
        return localTransactionManager;
        
    }
    
    public ITransactionService getTransactionService() {
    	
//       return getTransactionManager().getTransactionService();
        return localTransactionManager.getTransactionService();

    }

    public WriteExecutorService getWriteService() {

        return concurrencyManager.getWriteService();
        
    }

    /*
     * IResourceManager
     */
    
    /**
     * Note: This implementation always returns <code>false</code>. As a
     * consequence the journal capacity will simply be extended by
     * {@link #write(ByteBuffer)} until the available disk space is exhausted.
     * 
     * @return This implementation returns <code>false</code> since overflow
     *         is NOT supported.
     */
    public boolean shouldOverflow() {

        return false;

    }
    
    /**
     * Note: This implementation always returns <code>false</code>.
     */
    public boolean isOverflowEnabled() {
        
        return false;
        
    }
    
    public Future<Object> overflow() {
        
        throw new UnsupportedOperationException();
        
    }

//    /**
//     * This request is always ignored for a {@link Journal} since it does not
//     * have any resources to manage.
//     */
//    public void setReleaseTime(final long releaseTime) {
//
//        if (releaseTime < 0L) {
//
//            // Not a timestamp.
//            throw new IllegalArgumentException();
//            
//        }
//
//        // ignored.
//        
//    }

    /**
     * @throws UnsupportedOperationException
     *             since {@link #overflow()} is not supported.
     */
    public File getIndexSegmentFile(IndexMetadata indexMetadata) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public IBigdataFederation<?> getFederation() {

        throw new UnsupportedOperationException();
        
    }
    
    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public DataService getDataService() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public UUID getDataServiceUUID() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Always returns <code>null</code> since index partition moves are not
     * supported.
     */
    public StaleLocatorReason getIndexPartitionGone(String name) {
        
        return null;
        
    }

    /*
     * global row store.
     */
    public SparseRowStore getGlobalRowStore() {

        return getGlobalRowStoreHelper().getGlobalRowStore();

    }

//    /**
//     * Return a view of the global row store as of the specified timestamp. This
//     * is mainly used to provide access to historical views. 
//     * 
//     * @param timestamp
//     *            The specified timestamp.
//     * 
//     * @return The global row store view -or- <code>null</code> if no view
//     *         exists as of that timestamp.
//     */
    public SparseRowStore getGlobalRowStore(final long timestamp) {

        return getGlobalRowStoreHelper().get(timestamp);

    }

    /**
     * Return the {@link GlobalRowStoreHelper}.
     * <p>
     * Note: An atomic reference provides us with a "lock" object which doubles
     * as a reference. We are not relying on its CAS properties.
     */
    private final GlobalRowStoreHelper getGlobalRowStoreHelper() {
        
        GlobalRowStoreHelper t = globalRowStoreHelper.get();

        if (t == null) {

            synchronized (globalRowStoreHelper) {

                /*
                 * Note: Synchronized to avoid race conditions when updating
                 * (this allows us to always return our reference if we create a
                 * new helper instance).
                 */

                t = globalRowStoreHelper.get();

                if (t == null) {

                    globalRowStoreHelper
                            .set(t = new GlobalRowStoreHelper(this));

                }

            }

        }

        return globalRowStoreHelper.get();
    }

    final private AtomicReference<GlobalRowStoreHelper> globalRowStoreHelper = new AtomicReference<GlobalRowStoreHelper>();

    /*
     * global file system.
     * 
     * Note: An atomic reference provides us with a "lock" object which doubles
     * as a reference. We are not relying on its CAS properties.
     */
    public BigdataFileSystem getGlobalFileSystem() {

        GlobalFileSystemHelper t = globalFileSystemHelper.get();
        
        if (t == null) {

            synchronized (globalFileSystemHelper) {

                /*
                 * Note: Synchronized to avoid race conditions when updating
                 * (this allows us to always return our reference if we create a
                 * new helper instance).
                 */

                t = globalFileSystemHelper.get();

                if (t == null) {

                    globalFileSystemHelper
                            .set(t = new GlobalFileSystemHelper(this));

                }
                
            }

        }

        return globalFileSystemHelper.get().getGlobalFileSystem();

    }
    final private AtomicReference<GlobalFileSystemHelper> globalFileSystemHelper = new AtomicReference<GlobalFileSystemHelper>();

    protected void discardCommitters() {

        super.discardCommitters();

        synchronized (globalRowStoreHelper) {

            /*
             * Note: Synchronized even though atomic. We are using this as an
             * mutable lock object without regard to its CAS behavior.
             */

            globalRowStoreHelper.set(null);

        }
        
        synchronized (globalFileSystemHelper) {

            /*
             * Note: Synchronized even though atomic. We are using this as an
             * mutable lock object without regard to its CAS behavior.
             */

            globalFileSystemHelper.set(null);
            
        }

    }
    
    public TemporaryStore getTempStore() {
        
        return tempStoreFactory.getTempStore();
        
    }
    private final TemporaryStoreFactory tempStoreFactory;

    public IResourceLocator<?> getResourceLocator() {

        assertOpen();
        
        return resourceLocator;
        
    }
    private final IResourceLocator<?> resourceLocator;
    
    public IResourceLockService getResourceLockService() {
        
        assertOpen();
        
        return resourceLockManager;
        
    }
    private final ResourceLockService resourceLockManager;

    public ExecutorService getExecutorService() {
        
        assertOpen();
        
        return executorService;
        
    }
    private final ThreadPoolExecutor executorService;

    /**
     * Used to sample and report on the queue associated with the
     * {@link #executorService}. May be used to schedule other tasks as well.
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /*
     * plugins.
     */
    
    private final AtomicReference<IPlugIn<Journal, ThreadPoolExecutorBaseStatisticsTask>> pluginQueueStats = new AtomicReference<IPlugIn<Journal,ThreadPoolExecutorBaseStatisticsTask>>();
    private final AtomicReference<IPlugIn<Journal, AbstractStatisticsCollector>> pluginPlatformStats = new AtomicReference<IPlugIn<Journal, AbstractStatisticsCollector>>();
    private final AtomicReference<IPlugIn<Journal, ?>> pluginHttpd = new AtomicReference<IPlugIn<Journal, ?>>();
    
    /**
     * An optional plug in for Ganglia.
     * <p>
     * Note: The plug in concept was introduced to decouple the ganglia
     * component. Do not introduce imports into the {@link Journal} class that
     * would make the ganglia code a required dependency!
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/609">
     *      bigdata-ganglia is required dependency for Journal </a>
     */
    private final AtomicReference<IPlugIn<Journal, ?>> pluginGanglia = new AtomicReference<IPlugIn<Journal, ?>>();

    /**
     * Host wide performance counters (collected from the OS) (optional).
     * 
     * @see PlatformStatsPlugIn
     */
    protected AbstractStatisticsCollector getPlatformStatisticsCollector() {

        final IPlugIn<Journal, AbstractStatisticsCollector> plugin = pluginPlatformStats
                .get();

        if (plugin == null)
            return null;

        final AbstractStatisticsCollector t = plugin.getService();

        return t;

    }
    
    /**
     * An executor service used to read on the local disk.
     * 
     * @todo This is currently used by prefetch. We should generalize this
     *       mechanism, probably moving it to the {@link IResourceManager}, and
     *       use it to do all IO, ideally using the JSR 166 fork/join
     *       mechanisms.
     *       <p>
     *       This should be reconciled with the {@link ConcurrencyManager},
     *       which has distinct {@link ExecutorService}s for readers and writers
     *       which control the per-task concurrency while this controls the disk
     *       read concurrency.
     *       <p>
     *       We could use the same pool for readers and writers on the disk.
     */
    public LatchedExecutor getReadExecutor() {
        
//        assertOpen();
        
        return readService;
        
    }
    private final LatchedExecutor readService;

    /**
     * This task runs once starts an (optional)
     * {@link AbstractStatisticsCollector} and an (optional) httpd service.
     * <p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class StartDeferredTasksTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final private Logger log = Logger.getLogger(StartDeferredTasksTask.class);

        private StartDeferredTasksTask() {
        }

        public void run() {

            try {
                
                startDeferredTasks();
                
            } catch (Throwable t) {

                log.error(t, t);

                return;
                
            }

        }

        /**
         * Starts performance counter collection.
         */
        protected void startDeferredTasks() throws IOException {

            // start collection on various work queues.
            {

                final IPlugIn<Journal, ThreadPoolExecutorBaseStatisticsTask> tmp = new QueueStatsPlugIn();
                
                tmp.startService(Journal.this);
                
                // Save reference iff started.
                pluginQueueStats.set(tmp);
                
            }
            
            // start collecting performance counters (if enabled).
            {

                final IPlugIn<Journal, AbstractStatisticsCollector> tmp = new PlatformStatsPlugIn();
                
                tmp.startService(Journal.this);
                
                pluginPlatformStats.set(tmp);
                
            }

            // start the local httpd service reporting on this service.
            {

                final IPlugIn<Journal, CounterSetHTTPD> tmp = new HttpPlugin();
                
                tmp.startService(Journal.this);
                
                pluginHttpd.set(tmp);
                
            }

            /**
             * Start embedded ganglia peer. It will develop a snapshot of the
             * metrics in memory for all nodes reporting in the ganglia network
             * and will self-report metrics from the performance counter
             * hierarchy to the ganglia network.
             * 
             * Note: Do NOT invoke this plug in unless it will start and run to
             * avoid a CLASSPATH dependency on bigdata-ganglia when it is not
             * used. The plugin requires platform statistics collection to run,
             * so if you do not want to have a CLASSPATH dependency on ganglia,
             * you need to disable the PlatformStatsPlugIn.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/609">
             *      bigdata-ganglia is required dependency for Journal </a>
             */
            if (getPlatformStatisticsCollector() != null) {

                final IPlugIn<Journal, ?> tmp = new GangliaPlugIn();

                tmp.startService(Journal.this);

                if (tmp.isRunning()) {

                    // Save reference iff started.
                    pluginGanglia.set(tmp);

                }

            }

        }

    } // class StartDeferredTasks

    public ScheduledFuture<?> addScheduledTask(final Runnable task,
            final long initialDelay, final long delay, final TimeUnit unit) {

        if (task == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("Scheduling task: task=" + task.getClass()
                    + ", initialDelay=" + initialDelay + ", delay=" + delay
                    + ", unit=" + unit);

		return scheduledExecutorService.scheduleWithFixedDelay(task,
				initialDelay, delay, unit);

    }

	/**
	 * {@inheritDoc}
	 * 
	 * @see Options#COLLECT_PLATFORM_STATISTICS
	 */
    final public boolean getCollectPlatformStatistics() {
		return Boolean.valueOf(properties.getProperty(
				Options.COLLECT_PLATFORM_STATISTICS,
				Options.DEFAULT_COLLECT_PLATFORM_STATISTICS));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see Options#COLLECT_QUEUE_STATISTICS
	 */
	final public boolean getCollectQueueStatistics() {
		return Boolean.valueOf(properties.getProperty(
				Options.COLLECT_QUEUE_STATISTICS,
				Options.DEFAULT_COLLECT_QUEUE_STATISTICS));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see Options#HTTPD_PORT
	 */
	final public int getHttpdPort() {
		return Integer.valueOf(properties.getProperty(Options.HTTPD_PORT,
				Options.DEFAULT_HTTPD_PORT));
	}

    /*
     * Per index counters.
     */

    /**
     * Canonical per-index {@link BTreeCounters}. These counters are set on each
     * {@link AbstractBTree} that is materialized by
     * {@link #getIndexOnStore(String, long, IRawStore)}. The same
     * {@link BTreeCounters} object is used for the unisolated, read-committed,
     * read-historical and isolated views of the index and for each source in
     * the view regardless of whether the source is a mutable {@link BTree} on
     * the live journal or a read-only {@link BTree} on a historical journal.
     * 
     * @see #getIndexCounters(String)
     * @see #dropIndex(String)
     */
    final private ConcurrentHashMap<String/* name */, BTreeCounters> indexCounters = new ConcurrentHashMap<String, BTreeCounters>();

    public BTreeCounters getIndexCounters(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        // first test for existence.
        BTreeCounters t = indexCounters.get(name);

        if (t == null) {

            // not found.  create a new instance.
            t = new BTreeCounters();

            // put iff absent.
            final BTreeCounters oldval = indexCounters.putIfAbsent(name, t);

            if (oldval != null) {

                // someone else got there first so use their instance.
                t = oldval;

            } else {
                
                if (log.isInfoEnabled())
                    log.info("New counters: indexPartitionName=" + name);
                
            }
            
        }

        assert t != null;
        
        return t;
        
    }

    /**
	 * A Journal level semaphore used to restrict applications to a single
	 * unisolated connection. The "unisolated" connection is an application
	 * level construct which supports highly scalable ACID operations but only a
	 * single such "connection" can exist at a time for a Journal. This
	 * constraint arises from the need for the application to coordinate
	 * operations on the low level indices and commit/abort processing while it
	 * holds the permit.
	 * <p>
	 * Note: If by some chance the permit has become "lost" it can be rebalanced
	 * by {@link Semaphore#release()}. However, uses of this {@link Semaphore}
	 * should ensure that it is release along all code paths, including a
	 * finalizer if necessary.
	 */   
	private final Semaphore unisolatedSemaphore = new Semaphore(1/* permits */,
			false/* fair */);

	/**
	 * Acquire a permit for the UNISOLATED connection.
	 * 
	 * @throws InterruptedException
	 */
	public void acquireUnisolatedConnection() throws InterruptedException {

		unisolatedSemaphore.acquire();

		if (log.isDebugEnabled())
			log.debug("acquired semaphore: availablePermits="
					+ unisolatedSemaphore.availablePermits());

		if (unisolatedSemaphore.availablePermits() != 0) {
			/*
			 * Note: This test can not be made atomic with the Semaphore API. It
			 * is possible unbalanced calls to release() could drive the #of
			 * permits in the Semaphore above ONE (1) since the Semaphore
			 * constructor does not place an upper bound on the #of permits, but
			 * rather sets the initial #of permits available. An attempt to
			 * acquire a permit which has a post-condition with additional
			 * permits available will therefore "eat" a permit.
			 */
			throw new IllegalStateException();
		}

	}

	/**
	 * Release the permit for the UNISOLATED connection.
	 * 
	 * @throws IllegalStateException
	 *             unless the #of permits available is zero.
	 */
	public void releaseUnisolatedConnection() {

		if (log.isDebugEnabled())
			log.debug("releasing semaphore: availablePermits="
					+ unisolatedSemaphore.availablePermits());

		if (unisolatedSemaphore.availablePermits() != 0) {
			/*
			 * Note: This test can not be made atomic with the Semaphore API. It
			 * is possible that a concurrent call could drive the #of permits in
			 * the Semaphore above ONE (1) since the Semaphore constructor does
			 * not place an upper bound on the #of permits, but rather sets the
			 * initial #of permits available.
			 */
			throw new IllegalStateException();
		}

		unisolatedSemaphore.release();

	}

}
