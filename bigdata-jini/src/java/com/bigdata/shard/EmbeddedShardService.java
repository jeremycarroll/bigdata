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

package com.bigdata.shard;

import static com.bigdata.shard.Constants.*;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IDataServiceCounters;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.IServiceCounters;
import com.bigdata.counters.LoadBalancerReportingTask;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.counters.ReadBlockCounters;
import com.bigdata.counters.httpd.HttpReportingServer;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.jini.BigdataDiscoveryManager;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.DistributedCommitTask;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.GetIndexMetadataTask;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.ITx;
import com.bigdata.journal.LocalTransactionManager;
import com.bigdata.journal.RangeIteratorTask;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.journal.RunState;
import com.bigdata.journal.ScaleOutIndexManager;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.journal.TransactionService;
import com.bigdata.journal.Tx;
import com.bigdata.journal.JournalTransactionService.SinglePhaseCommit;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.resources.LocalResourceManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.service.DataTaskWrapper;
import com.bigdata.service.IDataServiceCallable;
import com.bigdata.service.IFederationCallable;
import com.bigdata.service.ISession;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.ITxCommitProtocol;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.MetadataIndexCachePolicy;
import com.bigdata.service.OverflowAdmin;
import com.bigdata.service.Service;
import com.bigdata.service.Session;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardManagement;
import com.bigdata.service.ShardService;
import com.bigdata.util.config.LogUtil;
import com.bigdata.util.httpd.AbstractHTTPD;
import com.bigdata.zookeeper.ZooKeeperAccessor;

import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import org.apache.zookeeper.data.ACL;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.IOException;
import java.io.Writer;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// Make package protected in the future when possible
public
class EmbeddedShardService implements ShardService,
                                      ShardManagement,
                                      ITxCommitProtocol,
                                      OverflowAdmin,
                                      Service,
                                      ISession,
                                      IServiceShutdown
{
    public static Logger logger =
        LogUtil.getLog4jLogger((EmbeddedShardService.class).getName());

    /**
     * Options understood by the shard service.
     */
    public static interface Options extends com.bigdata.journal.Options,
            com.bigdata.journal.ConcurrencyManager.Options,
            com.bigdata.resources.ResourceManager.Options,
            com.bigdata.counters.AbstractStatisticsCollector.Options,
            com.bigdata.service.IBigdataClient.Options
            // @todo local tx manager options?
    {
        String THREAD_POOL_SIZE = "threadPoolSize";
        String DEFAULT_THREAD_POOL_SIZE = 
           new Integer(Constants.DEFAULT_THREAD_POOL_SIZE).toString();
    }

    private ZooKeeperAccessor zkAccessor;
    private List<ACL> zkAcl;
    private String zkRoot;
    private Properties properties;

    private IBigdataDiscoveryManagement discoveryMgr;
    private ILocalResourceManagement localResources;
    private ScaleOutIndexManager indexMgr;
    private ResourceManager resourceMgr;
    private LocalTransactionManager localTransactionMgr;
    private ConcurrencyManager concurrencyMgr;

    private ReadBlockCounters readBlockApiCounters = new ReadBlockCounters();

//BTM - BEGIN - fields from AbstractFederation -------------------------------
    private ScheduledFuture<?> eventTaskFuture;
    private ScheduledFuture<?> lbsReportingTaskFuture;
    private long lbsReportingPeriod;
    private AbstractHTTPD httpServer;
    private String httpServerUrl;//URL used to access the httpServer
    private int httpdPort;
//BTM - END   - fields from AbstractFederation -------------------------------

    private ScheduledFuture deferredInitTaskFuture;
    private boolean deferredInitDone = false;

    protected EmbeddedShardService
                  (final UUID serviceUUID,
                   final String hostname,
                   final ServiceDiscoveryManager sdm,
                   final TransactionService embeddedTxnService,
                   final LoadBalancer embeddedLoadBalancer,
                   final ZooKeeperAccessor zookeeperAccessor,
                   final List<ACL> zookeeperAcl,
                   final String zookeeperRoot,
                   final int threadPoolSize,
                   final int indexCacheSize,
                   final long indexCacheTimeout,
                   final MetadataIndexCachePolicy metadataIndexCachePolicy,
                   final int resourceLocatorCacheSize,
                   final long resourceLocatorCacheTimeout,
                   final int defaultRangeQueryCapacity,
                   final boolean batchApiOnly,
                   final long taskTimeout,
                   final int maxParallelTasksPerRequest,
                   final int maxStaleLocatorRetries,
                   final boolean collectQueueStatistics,
                   final boolean collectPlatformStatistics,
                   final long lbsReportingPeriod,
                   final int httpdPort,
                   Properties properties)
    {
        this.zkAccessor = zookeeperAccessor;
        this.zkAcl = zookeeperAcl;
        this.zkRoot = zookeeperRoot;
        this.httpdPort = httpdPort;
        this.properties = (Properties) properties.clone();

        (this.properties).setProperty
            ( AbstractStatisticsCollector.Options.PROCESS_NAME,
              "service" + ICounterSet.pathSeparator + SERVICE_TYPE
              + ICounterSet.pathSeparator + serviceUUID.toString() );

        this.discoveryMgr = 
            new BigdataDiscoveryManager(sdm,
                                        embeddedTxnService,
                                        embeddedLoadBalancer,
                                        null, //embeddedShardLocator
                                        null, //embeddedDataServiceMap
                                        logger);
        this.localResources = 
            new LocalResourceManager(SERVICE_TYPE,
                                     SERVICE_NAME,
                                     serviceUUID,
                                     hostname,
                                     this.discoveryMgr,
                                     logger,
                                     threadPoolSize,
                                     collectQueueStatistics,
                                     collectPlatformStatistics,
                                     this.properties);

        this.indexMgr = new ScaleOutIndexManager(this.discoveryMgr,
                                                 this.localResources,
                                                 this.zkAccessor,
                                                 this.zkAcl,
                                                 this.zkRoot,
                                                 indexCacheSize,
                                                 indexCacheTimeout,
                                                 metadataIndexCachePolicy,
                                                 resourceLocatorCacheSize,
                                                 resourceLocatorCacheTimeout,
                                                 defaultRangeQueryCapacity,
                                                 batchApiOnly,
                                                 taskTimeout,
                                                 maxParallelTasksPerRequest,
                                                 maxStaleLocatorRetries,
                                                 logger,
                                                 this.properties);

        this.lbsReportingPeriod = lbsReportingPeriod;

        // Note that this service employs a StoreManager (in the
        // ResourceManager) whose creation depends on the existence
        // of a transaction service. Additionally, this service
        // also employs counters dependent on the existence of 
        // a load balancer to which the counters send events.
        // Unfortunately, when the ServicesManagerService is used
        // to start this service, these dependencies can cause
        // problems for the ServicesManagerService. This is because
        // the order the services are started by the ServicesManagerService
        // can be random, and if this service is the first service
        // the ServicesManagerService attempts to start (or whose
        // starting is attempted before the transaction service 
        // and/or the load balancer), then unless this service
        // returns an indication to the ServicesManagerService that
        // it has successfully started within the time period
        // expected, the ServicesManagerService will declare that
        // this service is faulty and kill the process in which
        // this service was started. To address this issue, this
        // service executes an instance of DeferredInitTask to
        // create the ResourceManager and set up the counters and
        // events asynchronously; which allows the transaction
        // service and load balancer to be started and discovered
        // after this service has been started by the 
        // ServicesManagerService.

        deferredInitDone = deferredInit();
        if (!deferredInitDone) {
            DeferredInitTask deferredInitTask = new DeferredInitTask();
            this.deferredInitTaskFuture = 
                ((this.localResources).getScheduledExecutor())
                                      .scheduleWithFixedDelay
                                          (deferredInitTask,
                                           20L*1000L,//initial delay
                                           30L*1000L,//period
                                           TimeUnit.MILLISECONDS);
        }
    }

    // Required by Service interface

    public UUID getServiceUUID() {
        return localResources.getServiceUUID();
    }

    public Class getServiceIface() {
        return localResources.getServiceIface();
    }

    public String getServiceName() {
        return localResources.getServiceName();
    }

    public String getHostname() {
        return localResources.getHostname();
    }

    // Required by ISession interface

    public Session getSession() {
        return localResources.getSession();
    }

    // Required by IServiceShutdown interface

    /**
     * Note: "open" is determined by the {@link ConcurrencyManager#isOpen()}
     *       but the service is not usable until
     *       {@link StoreManager#isStarting()} returns <code>false</code>
     *       (there is asynchronous processing involved in reading the
     *       existing store files or creating the first store file and
     *       the service cannot be used until that processing has been
     *       completed). The {@link ConcurrencyManager} will block for
     *       a while waiting for the {@link StoreManager} startup to
     *       complete and will reject tasks if startup processing does
     *       not complete within a timeout.
     */
    public boolean isOpen() {
        return ( (concurrencyMgr != null) && (concurrencyMgr.isOpen()) );
    }

    synchronized public void shutdown() {
logger.warn("SSSS SHARD SERVICE EmbeddedShardService.shutdown");
        if (!isOpen()) return;

        //false ==> allow in-progress tasks to complete
        if (deferredInitTaskFuture != null) {
            deferredInitTaskFuture.cancel(false);
        }

        if (concurrencyMgr != null) {
            concurrencyMgr.shutdown();
        }
        if (localTransactionMgr != null) {
            localTransactionMgr.shutdown();
        }
        if (resourceMgr != null) {
            resourceMgr.shutdown();
        }

        if (lbsReportingTaskFuture != null) {
            lbsReportingTaskFuture.cancel(false);
        }
        if (eventTaskFuture != null) {
            eventTaskFuture.cancel(false);
        }

        if (indexMgr != null) indexMgr.destroy();
        if (localResources != null) {
            localResources.terminate(EXECUTOR_TERMINATION_TIMEOUT);
        }

        if(httpServer != null) {//same logic as AbstractFederation.shutdown
            httpServer.shutdown();
            httpServer = null;
            httpServerUrl = null;
        }
    }

    synchronized public void shutdownNow() {
        shutdown();
    }

    // Required by ShardManagement interface

    public IndexMetadata getIndexMetadata(String name, long timestamp)
               throws IOException,
                      InterruptedException,
                      ExecutionException
    {
        setupLoggingContext();
        try {
            // Choose READ_COMMITTED iff UNISOLATED was requested.
            final long startTime = 
                       (timestamp == ITx.UNISOLATED ? 
                            ITx.READ_COMMITTED : timestamp);
            final AbstractTask task = 
                  new GetIndexMetadataTask
                          (concurrencyMgr, startTime, name);
            return (IndexMetadata) concurrencyMgr.submit(task).get();
        } finally {
            clearLoggingContext();
        }
    }

    public ResultSet rangeIterator(long tx,
                                   String name,
                                   byte[] fromKey,
                                   byte[] toKey,
                                   int capacity,
                                   int flags,
                                   IFilterConstructor filter)
           throws InterruptedException, ExecutionException
    {
        setupLoggingContext();
        try {
            if (name == null) {
                throw new IllegalArgumentException("null name");
            }
            /*
             * Figure out if the iterator is read-only for the time that it
             * executes on the data service. For this case, we ignore the CURSOR
             * flag since modifications during iterator execution on the data
             * service can only be introduced via a filter or the REMOVEALL
             * flag. The caller will be used a chunked iterator. Therefore if
             * they choose to delete tuples while visiting the elements in the
             * ResultSet then the deletes will be issued as separate requests.
             */
            final boolean readOnly = ((flags & IRangeQuery.READONLY) != 0)
                    || (filter == null &&
//                       ((flags & IRangeQuery.CURSOR) == 0) &&
                       ((flags & IRangeQuery.REMOVEALL) == 0)
                       );

            long timestamp = tx;

            if (timestamp == ITx.UNISOLATED && readOnly) {

                // If the iterator is readOnly then READ_COMMITTED has the same
                // semantics as UNISOLATED and provides better concurrency since
                // it reduces contention for the writeService.
                timestamp = ITx.READ_COMMITTED;
            }

//            final long startTime = (tx == ITx.UNISOLATED
//                        && ((flags & IRangeQuery.REMOVEALL)==0)? ITx.READ_COMMITTED
//                        : tx);

            final RangeIteratorTask task = new RangeIteratorTask(
                    concurrencyMgr, timestamp, name, fromKey, toKey,
                    capacity, flags, filter);

            // submit the task and wait for it to complete.
            return (ResultSet) concurrencyMgr.submit(task).get();
        } finally {
            clearLoggingContext();
        }
    }

    public <T> Future<T> submit(IDataServiceCallable<T> task) {
        setupLoggingContext();
        try {
            if (task == null) {
                throw new IllegalArgumentException("null task");
            }
            // submit the task and return its Future.
            return (localResources.getThreadPool()).submit
                       ( new DataTaskWrapper
                                 (indexMgr, resourceMgr, concurrencyMgr,
                                  localResources, discoveryMgr, task) );
        } finally {
            clearLoggingContext();
        }
    }

    /**
     * Note: This chooses {@link ITx#READ_COMMITTED} if the the index has
     * {@link ITx#UNISOLATED} isolation and the {@link IIndexProcedure} is
     * a read-only operation. This provides better concurrency on the
     * shard service by moving read-only operations off of the
     * <code>com.bigdata.journal.WriteExecutorService</code>.
     * <p>
     * Note: When the shard service is accessed via RMI the {@link Future}
     * MUST be a proxy. This gets handled by the concrete server implementation.
     */
    public Future submit(final long tx,
                         final String name,
                         final IIndexProcedure proc)
    {
        setupLoggingContext();
        try {
            if (name == null) {
                throw new IllegalArgumentException("null name");
            }

            if (proc == null) {
                throw new IllegalArgumentException("null proc");
            }

            // Choose READ_COMMITTED iff proc is read-only and
            // UNISOLATED was requested.
            final long timestamp = 
                (tx == ITx.UNISOLATED && proc.isReadOnly() ?
                     ITx.READ_COMMITTED : tx);

            // wrap the caller's task.
            final AbstractTask task = 
                new IndexProcedureTask
                    (concurrencyMgr, timestamp, name, proc);
            
            // submit the procedure and await its completion.
            Future retFuture = concurrencyMgr.submit(task);
            return retFuture;
        } catch(Throwable t) {
            logger.warn("EmbeddedShardService.submit(tx, name, proc) "
                        +">>> "+t, t);
        } finally {
            clearLoggingContext();
        }
        return null;
    }

    public boolean purgeOldResources(final long timeout,
                                     final boolean truncateJournal)
                       throws InterruptedException
    {
        // delegate all the work.
        return resourceMgr.purgeOldResources(timeout, truncateJournal);
    }
    
    // Required by ITxCommitProtocol interface

    public void setReleaseTime(final long releaseTime) {
        setupLoggingContext();
        try {
            resourceMgr.setReleaseTime(releaseTime);
        } finally {
            clearLoggingContext();
        }
    }

    /**
     * Note: This is basically identical to the standalone journal case.
     * 
     * @see JournalTransactionService#commitImpl(long)}.
     */
    public long singlePhaseCommit(final long tx)
                throws ExecutionException, InterruptedException, IOException
    {
        setupLoggingContext();
        try {

            if(TimestampUtility.isReadOnly(tx)) {
                /*
                 * A read-only transaction.
                 * 
                 * Note: We do not maintain state on the client for read-only
                 * transactions. The state for a read-only transaction is
                 * captured by its transaction identifier and by state on the
                 * transaction service, which maintains a read lock.
                 * 
                 * Note: Thrown exception since this method will not be invoked
                 * by the txService for a read-only tx.
                 */
                throw new IllegalArgumentException("read-only transaction");
            }
            
            final Tx localState = (Tx) localTransactionMgr.getTx(tx);

            if (localState == null) {//not an active transaction
                throw new IllegalStateException("transaction not active");
            }
            
            /*
             * Note: This code is shared (copy-by-value) by the
             * JournalTransactionService commitImpl(...)
             */
            final ManagedJournal journal = resourceMgr.getLiveJournal();            
            {
                /*
                 * A transaction with an empty write set can commit immediately
                 * since validation and commit are basically NOPs (this is
                 * the same as the read-only case.)
                 * 
                 * Note: We lock out other operations on this tx so that this
                 * decision will be atomic.
                 */
                localState.lock.lock();
                try {

                    if (localState.isEmptyWriteSet()) {// Sort of a NOP commit. 
                        
                        localState.setRunState(RunState.Committed);

                        ((LocalTransactionManager) journal
                            .getLocalTransactionManager())
                                .deactivateTx(localState);
                        return 0L;
                    }
                } finally {
                    localState.lock.unlock();
                }
            }//end block

            final AbstractTask<Void> task = 
                new SinglePhaseCommit(concurrencyMgr,
                                      journal.getLocalTransactionManager(),
                                      localState);
            try {
                
                /*
                 * FIXME This is not working yet. If we submit directly to the
                 * concurrency manager, then there is a ClassCastException on
                 * the DirtyListener. If we submit directly to the WriteService
                 * then the task does not hold its locks. None of these options
                 * work. The write service really needs a refactor (to be state
                 * based rather like the new lock service) before I finish the
                 * distributed commit protocol.
                 */
                // submit and wait for the result.
                concurrencyMgr.submit(task).get();
//                .getWriteService().submit(task).get();
//                .getWriteService().getLockManager().submit(task.getResource(), task).get();

                /*
                 * FIXME The state changes for the local tx should be atomic across
                 * this operation. In order to do that we have to make those changes
                 * inside of SinglePhaseTask while it is holding the lock, but after
                 * it has committed. Perhaps the best way to do this is with a pre-
                 * and post- call() API since we can not hold the lock across the
                 * task otherwise (it will deadlock).
                 */

                localState.lock.lock();
                try {
                    localState.setRunState(RunState.Committed);

                    ((LocalTransactionManager) journal
                        .getLocalTransactionManager()).deactivateTx(localState);
                } finally {
                    localState.lock.unlock();
                }
            } catch (Throwable t) {
//                logger.error(t.getMessage(), t);

                localState.lock.lock();
                try {
                    localState.setRunState(RunState.Aborted);

                    ((LocalTransactionManager) journal
                        .getLocalTransactionManager()).deactivateTx(localState);
                    throw new RuntimeException(t);
                } finally {
                    localState.lock.unlock();
                }
            }

            /*
             * Note: This is returning the commitTime set on the task when it was
             * committed as part of a group commit.
             */
//            logger.warn("\n" + state + "\n" + localState);
            return task.getCommitTime();
        } finally {
            clearLoggingContext();
        }
    }

    public void prepare(final long tx, final long revisionTime)
            throws ExecutionException, InterruptedException, IOException
    {
        setupLoggingContext();
        try {
            if(TimestampUtility.isReadOnly(tx)) {
                /*
                 * A read-only transaction.
                 * 
                 * Note: We do not maintain state on the client for read-only
                 * transactions. The state for a read-only transaction is captured
                 * by its transaction identifier and by state on the transaction
                 * service, which maintains a read lock.
                 * 
                 * Note: Thrown exception since this method will not be invoked
                 * by the txService for a read-only tx.
                 */
                throw new IllegalArgumentException("read-only transaction");
            }
            
            final Tx state = (Tx) localTransactionMgr.getTx(tx);
            if (state == null) {//not an active transaction.
                throw new IllegalStateException("transaction not active");
            }
            
            // Submit the task and await its future
            concurrencyMgr.submit
                ( new DistributedCommitTask(concurrencyMgr,
                                            resourceMgr,
                                            localResources.getServiceUUID(),
                                            state,
                                            revisionTime) ).get();
        } finally {
            clearLoggingContext();
        }
    }

    public void abort(final long tx) throws IOException {
        setupLoggingContext();
        try {

            final Tx localState = (Tx) localTransactionMgr.getTx(tx);
            if (localState == null) {
                throw new IllegalArgumentException("null localState");
            }
            localState.lock.lock();

            try {
                localState.setRunState(RunState.Aborted);
            } finally {
                localState.lock.unlock();
            }
        } finally {
            clearLoggingContext();
        }
    }

    // Required by ShardService interface

    public void registerIndex(String name, IndexMetadata metadata)
                    throws IOException,
                           InterruptedException,
                           ExecutionException
    {
        setupLoggingContext();
        try {
            if (metadata == null) {
                throw new IllegalArgumentException("null metadata");
            }
            final AbstractTask task = 
                  new RegisterIndexTask(concurrencyMgr,
                                        name, metadata);
            concurrencyMgr.submit(task).get();
        } finally {
            clearLoggingContext();
        }
    }
    
    public void dropIndex(String name) 
                    throws IOException,
                           InterruptedException,
                           ExecutionException
    {
        setupLoggingContext();
        try {
            final AbstractTask task = 
                  new DropIndexTask(concurrencyMgr, name);
            concurrencyMgr.submit(task).get();
        } finally {
            clearLoggingContext();
        }
    }

    /**
     * @todo this operation should be able to abort an
     *       {@link IBlock#inputStream() read} that takes too long or if there
     *       is a need to delete the resource.
     * 
     * @todo this should be run on the read service.
     * 
     * @todo coordinate close out of stores.
     * 
     * @todo efficient (stream-based) read from the journal (IBlockStore API).
     *       This is a fully buffered read and will cause heap churn.
     */
    public IBlock readBlock(IResourceMetadata resource, final long addr) {

        if (resource == null) {
            throw new IllegalArgumentException("null resource");
        }

        if (addr == 0L) {
            throw new IllegalArgumentException("addr == 0");
        }

        setupLoggingContext();

        final long begin = System.nanoTime();
        try {
            final IRawStore store = 
                      resourceMgr.openStore(resource.getUUID());
    
            if (store == null) {
                logger.warn("resource not available ["+resource+"]");

                readBlockApiCounters.readBlockErrorCount++;

                throw new IllegalStateException
                              ("resource not available ["+resource+"]");
            }
            final int byteCount = store.getByteCount(addr);

            return new IBlock() {

                public long getAddress() {
                    return addr;
                }
    
                // @todo reuse buffers
                public InputStream inputStream() {

                    // this is when it actually reads the data.
                    final ByteBuffer buf = store.read(addr);

                    // #of bytes buffered.
                    readBlockApiCounters.readBlockBytes += byteCount;

                    // caller will read from this object.
                    return new ByteBufferInputStream(buf);
                }
    
                public int length() {
                    return byteCount;
                }
            };//end new IBlock
            
        } finally {
            readBlockApiCounters.readBlockCount++;
            readBlockApiCounters.readBlockNanos = System.nanoTime() - begin;
            clearLoggingContext();
        }
    }

    // Required by OverflowAdmin interface (for testing & benchmarking)

    public void forceOverflow(final boolean immediate,
                              final boolean compactingMerge)
                throws IOException, InterruptedException, ExecutionException
    {
        setupLoggingContext();
        try {
            if ( !(resourceMgr instanceof ResourceManager) ) {
                throw new UnsupportedOperationException
                              ("not instance of ResourceManager "
                               +"["+resourceMgr.getClass()+"]");
            }

            final Callable task =
                           new ForceOverflowTask(resourceMgr, compactingMerge);

            logger.warn("Will force overflow: immediate=" + immediate
                        + ", compactingMerge=" + compactingMerge);
            
            if (immediate) {

                // Run the task on the write service. The task writes a small
                // record on the journal in order to make sure that it is dirty
                // and then sets the flag to force overflow with the next
                // commit. Since the task runs on the write service and since
                // the journal is dirty, a group commit will occur and
                // synchronous overflow processing will occur before this method
                // returns.
                // 
                // Note: the resource itself is arbitrary - there is no index
                // by that name.
                concurrencyMgr.submit
                    ( new AbstractTask( concurrencyMgr,
                                        ITx.UNISOLATED,
                                        new String[] { "__forceOverflow" } )
                      {
                          @Override
                          protected Object doTask() throws Exception {

                              // write a one byte record on the journal.
                              getJournal().write(ByteBuffer.wrap(new byte[]{1}));
                        
                              // run task that will set the overflow flag.
                              return task.call();
                          }
                      }).get();
                
            } else {
                // Provoke overflow with the next group commit. All this does is
                // set the flag that will cause overflow to occur with the next
                // group commit. Since the task does not run on the write
                // service it will return immediately.
                try {
                    task.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            clearLoggingContext();
        }
    }

    public long getAsynchronousOverflowCounter() throws IOException {
        setupLoggingContext();
        try {
            if ( !(resourceMgr instanceof ResourceManager) ) {
                throw new UnsupportedOperationException
                              ("not instance of ResourceManager "
                               +"["+resourceMgr.getClass()+"]");
            }
            return resourceMgr.getAsynchronousOverflowCount();
        } finally {
            clearLoggingContext();
        }
    }
    
    public boolean isOverflowActive() throws IOException {
        setupLoggingContext();
        try {
            if ( !(resourceMgr instanceof ResourceManager) ) {
                throw new UnsupportedOperationException
                              ("not instance of ResourceManager "
                               +"["+resourceMgr.getClass()+"]");
            }

            // overflow processing is enabled but not allowed, which
            // means that overflow processing is occurring right now.

            return (  resourceMgr.isOverflowEnabled() &&
                     !resourceMgr.isOverflowAllowed() );
        } finally {
            clearLoggingContext();
        }
    }

    // NOTE: the following methods are needed for embedded testing.
    //       Currently, EmbeddedFederation creates the shard locator
    //       service (embeddedMds) after it creates this class, but the
    //       IBigdataDiscoveryManagement utility created by this class
    //       has to return a non-null value when its getMetadataService
    //       method is called (ex. TestMove). Therefore, the setter
    //       methods below are provided so that EmbeddedFederation can
    //       call them after the embeddedMds is created and the
    //       embeddedDsMap is populated, so that this class can then
    //       pass a non-null reference to the setter methods of the
    //       corresponding setter methods. The getter method is
    //       provided because the DataService class provided such a
    //       method and EmbeddedFederation assumes that method
    //       exists on the embedded ref.

    public void setEmbeddedMds(ShardLocator embeddedMds) {
        ((BigdataDiscoveryManager)discoveryMgr).setEmbeddedMds
                                                    (embeddedMds);
    }

    public void setEmbeddedDsMap(Map<UUID, ShardService> embeddedDsMap) {
        ((BigdataDiscoveryManager)discoveryMgr).setEmbeddedDsMap
                                                    (embeddedDsMap);
    }

    public ResourceManager getResourceManager() {
        return resourceMgr;
    }

    // public method(s) needed from DataService class

    //from DataService - destroy persisted state
    synchronized public void destroy() {
logger.warn("\nSSSSS SHARD SERVICE EmbeddedShardService.destroy PRE-delete >>> resourceMgr.isOpen() = "+resourceMgr.isOpen()+"\n");
        resourceMgr.shutdownNow();//sets isOpen to false
        try {
            resourceMgr.deleteResources();
        } catch(Throwable t) { 
            logger.warn("exception on resourceMgr.deleteResources\n", t);
        }
logger.warn("\nSSSSS SHARD SERVICE EmbeddedShardService.destroy POST-delete >>> resourceMgr.isOpen() = "+resourceMgr.isOpen()+"\n");

        final File file = getHTTPDURLFile();
        if(file.exists()) {
            file.delete();
        }
        shutdownNow();
    }

    // Private methods

    // Returns the file to which the URL of the embedded httpd
    // service is written.
    private File getHTTPDURLFile() {
        return new File(resourceMgr.getDataDir(), "httpd.url");
    }

    private boolean deferredInit() {

        // StoreManager depends on the transaction service
        if (discoveryMgr.getTransactionService() == null) return false;

        if (this.resourceMgr == null) {
System.out.println("\nEmbeddedShardService >>> NEW StoreManager - BEGIN");
            this.resourceMgr = 
                new ShardResourceManager(this,
                                         this.discoveryMgr,
                                         this.localResources,
                                         this.indexMgr,
                                         this.properties);
System.out.println("\nEmbeddedShardService >>> NEW StoreManager - END");

            this.localTransactionMgr =
                     new LocalTransactionManager(discoveryMgr);
            this.concurrencyMgr = 
                new ConcurrencyManager(this.properties,
                                       this.localTransactionMgr,
                                       this.resourceMgr);
            //WARN: circular refs
            (this.resourceMgr).setConcurrencyManager(this.concurrencyMgr);
            (this.indexMgr).setConcurrencyManager(this.concurrencyMgr);
        }

        // Events and counters depend on the load balancer
        if (discoveryMgr.getLoadBalancerService() == null) return false;

//BTM - from AbstractFederation - start deferred tasks

        //start task to report counters to the load balancer

        LoadBalancerReportingTask lbsReportingTask = 
            new LoadBalancerReportingTask(this.resourceMgr,
                                          this.concurrencyMgr,
                                          this.localResources,
                                          this.discoveryMgr,
                                          logger);
        this.lbsReportingTaskFuture = 
            ((this.localResources).getScheduledExecutor())
                                      .scheduleWithFixedDelay
                                          (lbsReportingTask,
                                           lbsReportingPeriod,//initial delay
                                           lbsReportingPeriod,
                                           TimeUnit.MILLISECONDS);

        //start an http daemon from which interested parties can query
        //counter and/or statistics information with http get commands

        String httpServerPath = 
                   (this.localResources).getServiceCounterPathPrefix();
        try {
            this.httpServerUrl = 
                "http://"
                +AbstractStatisticsCollector.fullyQualifiedHostName
                +":"+this.httpdPort+"/?path="
                +URLEncoder.encode(httpServerPath, "UTF-8");
        } catch(java.io.UnsupportedEncodingException e) {
            logger.warn("failed to initialize httpServerUrl", e);
        }


        try {
            httpServer = 
                new HttpReportingServer(this.httpdPort,
                                        this.resourceMgr,
                                        this.concurrencyMgr,
                                        this.localResources,
                                        logger);
        } catch (IOException e) {
            logger.error("failed to start http server "
                         +"[port="+this.httpdPort
                         +", path="+httpServerPath+"]", e);
            return false;
        }
        if(httpServer != null) {
            if( logger.isDebugEnabled() ) {
                logger.debug("started http daemon "
                             +"[access URL="+this.httpServerUrl+"]");
            }
            // add counter reporting the access url to load balancer
            ((this.localResources).getServiceCounterSet())
                .addCounter
                     (IServiceCounters.LOCAL_HTTPD,
                     new OneShotInstrument<String>(this.httpServerUrl));
        }

//BTM - BEGIN ScaleOutIndexManager Note
//BTM -       The call to embeddedIndexStore.didStart was previously
//BTM -       commented out during the data service conversion. But
//BTM -       the method didStart() on the original EmbeddedIndexStore and
//BTM -       AbstractFederation calls the private method setupCounters;
//BTM -       which seems to be important for at least the shard (data)
//BTM -       service. The tests still passed without calling that method,
//BTM -       but we should consider calling it at this point (the problem
//BTM -       is that it waits on the resource manager to finish
//BTM -       initializing, which waits on the transaction service to be
//BTM -       discovered). Setting up these counters seem to be important
//BTM -       only for the shard (data) service rather than the other
//BTM -       services. So we should consider adding setupCounters to this
//BTM -       class, and calling it here instead of calling
//BTM -       embeddedIndexStore.didStart() or AbstractFederation.didStart().
//BTM -       
//BTM        embeddedIndexStore.didStart();

        setupCounters();

//BTM - END ScaleOutIndexManager Note

        return true;
    }

    private void setupLoggingContext() {

        try {
            MDC.put("serviceUUID", localResources.getServiceUUID());
            MDC.put("serviceName", localResources.getServiceName());
            MDC.put("hostname", localResources.getHostname());
        } catch(Throwable t) { /* swallow */ }
    }

    /**
     * Clear the logging context.
     */
    private void clearLoggingContext() {
        MDC.remove("serviceName");
        MDC.remove("thisServiceUUID");
        MDC.remove("hostname");
    }

    // Nested classes

    class ShardResourceManager extends ResourceManager {

        private ShardService dataService;
        private IBigdataDiscoveryManagement discoveryManager;
        private ILocalResourceManagement localResources;
        private ScaleOutIndexManager indexManager;

        ShardResourceManager(ShardService dataService,
                             IBigdataDiscoveryManagement discoveryManager,
                             ILocalResourceManagement localResources,
                             ScaleOutIndexManager indexManager,
                             Properties properties)
        {
            super(properties);
            this.dataService = dataService;
            this.discoveryManager = discoveryManager;
            this.localResources = localResources;
            this.indexManager = indexManager;
        }

        @Override
        public IBigdataDiscoveryManagement getDiscoveryManager() {
            return discoveryManager;
        }

        @Override
        public ILocalResourceManagement getLocalResourceManager() {
            return localResources;
        }

        @Override
        public ScaleOutIndexManager getIndexManager() {
            return indexManager;
        }
            
        @Override
        public ShardService getDataService() {
            return dataService;
        }
            
        @Override
        public UUID getDataServiceUUID() {
            return ((Service)dataService).getServiceUUID();
        }
    }

    /**
     * Task sets the flag that will cause overflow processing to be triggered on
     * the next group commit.
     */
    private class ForceOverflowTask implements Callable<Void> {

        private ResourceManager resourceManager;
        private final boolean compactingMerge;
        
        public ForceOverflowTask(final ResourceManager resourceManager,
                                 final boolean compactingMerge)
        {
            this.resourceManager = resourceManager;
            this.compactingMerge = compactingMerge;
        }
        
        public Void call() throws Exception {
            if (resourceManager.isOverflowAllowed()) {
                if (compactingMerge) {
                    resourceManager.compactingMerge.set(true);
                }
                // trigger overflow on the next group commit.
                resourceManager.forceOverflow.set(true);
            }
            return null;
        }
    }

    class DeferredInitTask implements Runnable {

        public DeferredInitTask() { }

        public void run() {
            try {
                if (!deferredInitDone) {
System.out.println("\n*** EmbededShardService#DeferredInitTask: DO DEFERRED INIT \n");
                    deferredInitDone = deferredInit();
                } else {
System.out.println("\n*** EmbededShardService#DeferredInitTask: DEFERRED INIT DONE >>> CANCELLING TASK\n");
                    deferredInitDone = true;
                    if (deferredInitTaskFuture != null) {
                        deferredInitTaskFuture.cancel(false);
                        deferredInitTaskFuture = null;
                    }
                }
            } catch (Throwable t) {
System.out.println("\n*** EmbededShardService#DeferredInitTask: EXCEPTION >>> "+t+"\n");
                logger.error("deferred initialization failure", t);
                deferredInitDone = true;
                if (deferredInitTaskFuture != null) {
                    deferredInitTaskFuture.cancel(false);
                    deferredInitTaskFuture = null;
                }
            }
        }
    }

//BTM - see the note at the end of the deferredInit method

    /**
     * Sets up shard service specific counters.
     * <p>
     * NOTE: the method setupCounters calls the method getCounters on
     *       the resource manager which calls the method getLiveJournal
     *       on the StoreManager which calls the method assertRunning
     *       (also on the StoreManager). The assertRunning method throws
     *       an IllegalStateException if the store manager is open but
     *       still in the 'isStarting' state. This can happen because
     *       the constructor for the store manager submits the execution
     *       of the StoreManager's startup processing to a separate thread;
     *       which means that the object that instantiates the
     *       ResourceManager (and ultimately the StoreManager, since
     *       the ResourceManager extends OverflowManager which extends
     *       IndexManager which extends StoreManager) continues -- and
     *       possibly completes -- its own intialization processing before
     *       the StoreManager completes its startup processing and sets
     *       the 'isStarting' state to false.
     * <p>
     *       One of the things that can slow down the StoreManager's
     *       initialization process in its StartupTask is the fact that
     *       the StartupTask WAITS FOREVER for a transaction service to be
     *       discovered by its federation object/discovery manager. As part
     *       of the smart proxy conversion work, the wait forever loop was
     *       changed to a loop that only waits for N seconds for the
     *       transaction service to be discovered; for some value of N.
     *       But even that wait loop can cause the object that instantiated
     *       the ResourceManager to complete it's initialization processing
     *       before the StoreManager completes its startup processing and
     *       sets the 'isStarting' state to false; resulting in an
     *       IllegalStateException when this method is called by the object.
     * <p>
     *       The reason that a wait loop for the transaction service is still
     *       used in the StoreManager, is to maintain the same logic as the
     *       old purely remote implementation of the data (shard) service.
     *       At some point after the smart proxy conversion work is complete,
     *       the need to wait for the transaction service in the StoreManager
     *       should be revisited and a determination should be made on
     *       whether or not there is a better way to initialize the
     *       Resource/StoreManager created by the EmbeddedShardService.
     *       For now though, this method will wait for Resource/StoreManager
     *       to complete its startup processing before setting up the
     *       necessary counters.
     */
    private synchronized void setupCounters() {

        logger.debug("waiting for resource manager to complete "
                     +"start up processing");
        boolean resourceMgrStarted = false;
        for(int i=0;i<60;i++) {
            //must be open but no longer in a starting state
            if( resourceMgr.isOpen() && !resourceMgr.isStarting() ) {
                resourceMgrStarted = true;
                break;
            }
            try { Thread.sleep(1000L); } catch(InterruptedException ie) { }

        }
        if(resourceMgrStarted) {
            logger.debug("resource manager startup processing complete");
        } else {
            logger.warn("resource manager startup processing NOT complete "
                        +"-- NO TRANSACTION SERVICE DISCOVERED");
            logger.warn("\n>>>> discovered transaction service ---- NO\n");
        }

        if (localResources.getServiceUUID() == null) {
            throw new IllegalStateException("null serviceUUID");
        }
            
        if( !isOpen() ) {// service has already been closed
            logger.warn("service is not open");
            return;
        }

        // Service specific counters.

        final CounterSet serviceRoot = localResources.getServiceCounterSet();

        serviceRoot.makePath
            (IDataServiceCounters.resourceManager).attach
                                      ( resourceMgr.getCounters() );

        serviceRoot.makePath
            (IDataServiceCounters.concurrencyManager).attach
                                      ( concurrencyMgr.getCounters() );

        serviceRoot.makePath
            (IDataServiceCounters.transactionManager).attach
                                      ( localTransactionMgr.getCounters() );

        // block API.
        {
            CounterSet tmp = serviceRoot.makePath("Block API");

            tmp.addCounter("Blocks Read", new Instrument<Long>() {
                public void sample() {
                    setValue(readBlockApiCounters.readBlockCount);
                }
            });

            tmp.addCounter (
                    "Blocks Read Per Second",
                     new Instrument<Double>() {

                         public void sample() {
                             // @todo encapsulate this logic.
                             long secs = 
                                 TimeUnit.SECONDS.convert
                                     (readBlockApiCounters.readBlockNanos,
                                      TimeUnit.NANOSECONDS);

                               final double v;
                               if (secs == 0L) {
                                   v = 0d;
                               } else {
                                   v = 
                                    readBlockApiCounters.readBlockCount / secs;
                               }
                               setValue(v);

                         }//end sample

                     }// end new Instrument

            );//end addCounter

        }//end block API

        logHttpServerUrl(new File(resourceMgr.getDataDir(), "httpd.url"));
    }

    /**
     * Writes the access url to the associated shard service's daemon onto
     * the given <code>File</code>.
     *
     * (Taken from <code>DefaultServiceFederationDelegate.logHttpdURL</code>).
     */
    private void logHttpServerUrl(final File file) {
        if( (file == null) || (httpServerUrl == null) ) return;
        file.delete(); // in case an old version of the given file exists
        try {
            Writer w = new BufferedWriter( new FileWriter(file) );
            try {
                w.write(httpServerUrl);
            } finally {
                w.close();
            }
        } catch (IOException ex) {
            logger.warn("failure on write of http daemon access url "
                        +"[file="+file+", url="+httpServerUrl+"]" );
        }
    }
}



