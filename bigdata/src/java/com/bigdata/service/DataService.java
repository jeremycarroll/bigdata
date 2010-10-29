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
 * Created on Mar 14, 2007
 */

package com.bigdata.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.Banner;
//BTM import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
//BTM import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IDataServiceCounters;
import com.bigdata.counters.Instrument;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.journal.IResourceManager;
//BTM import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.journal.RunState;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.journal.Tx;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.journal.ConcurrencyManager.IConcurrencyManagerCounters;
import com.bigdata.journal.JournalTransactionService.SinglePhaseCommit;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.OverflowManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager;
import com.bigdata.resources.IndexManager.IIndexManagerCounters;
import com.bigdata.resources.ResourceManager.IResourceManagerCounters;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.service.jini.DataServer;

//BTM
import com.bigdata.counters.ReadBlockCounters;
import com.bigdata.journal.DistributedCommitTask;
import com.bigdata.journal.GetIndexMetadataTask;
import com.bigdata.journal.RangeIteratorTask;
import com.bigdata.journal.TransactionService;

//BTM - PRE_FRED_3481
import com.bigdata.journal.IIndexManager;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.resources.ILocalResourceManagement;

/**
 * An implementation of a network-capable {@link ShardService}. The service is
 * started using the {@link DataServer} class. Operations are submitted using an
 * {@link IConcurrencyManager#submit(AbstractTask)} and will run with the
 * appropriate concurrency controls as imposed by that method.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see DataServer, which is used to start this service.
 * 
 * @todo Startup should be broken into two aspects: local startup and service
 *       connect and disconnect events. For example, we on the tx service
 *       connect the store manager should notify the tx service of the last
 *       commit time on the live journal. On disconnect, the data service needs
 *       to go offline. The metadata service is required only for overflow
 *       processing, but if it remains down then we will eventually need to
 *       bring the data service offline when the buffered writes would cause the
 *       live journal to no longer be fully buffered as the overflow processing
 *       time will be increased if we need to read through to the disk during
 *       overflow.
 * 
 * @todo Write benchmark test to measure interhost transfer rates. Should be
 *       100Mbits/sec (~12M/sec) on a 100BaseT switched network. With full
 *       duplex in the network and the protocol, that rate should be
 *       bidirectional. Can that rate be sustained with a fully connected
 *       bi-directional transfer?
 * 
 * FIXME Probably ALL of the methods {@link ShardService} should be subsumed
 * under {@link #submit(Callable)} or
 * {@link #submit(long, String, IIndexProcedure)} so they do not block on the
 * {@link DataService} and thereby absorb a thread.
 * 
 * @todo Review JERI options to support secure RMI protocols. For example, using
 *       SSL or an SSH tunnel. For most purposes I expect bigdata to operate on
 *       a private network, but replicate across gateways is also a common use
 *       case. Do we have to handle it specially?
 */
abstract public class DataService extends AbstractService
    implements IDataService, IServiceShutdown, ISession //IWritePipeline
{

    protected static final Logger log = Logger.getLogger(DataService.class);

    /**
     * Options understood by the {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static interface Options extends com.bigdata.journal.Options,
            com.bigdata.journal.ConcurrencyManager.Options,
            com.bigdata.resources.ResourceManager.Options,
            com.bigdata.counters.AbstractStatisticsCollector.Options,
            com.bigdata.service.IBigdataClient.Options
            // @todo local tx manager options?
            {

    }
    
    /**
     * @todo improve reporting here and for block write as well (goes through
     *       unisolated tasks at the present).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
//BTM    protected static class ReadBlockCounters {
//BTM        
//BTM        /** #of block read requests. */
//BTM        long readBlockCount, readBlockErrorCount, readBlockBytes, readBlockNanos;
//BTM        
//BTM        public ReadBlockCounters() {
//BTM        
//BTM        }
//BTM        
//BTM    }
    
    /**
     * Counters for the block read API.
     */
    final private ReadBlockCounters readBlockApiCounters = new ReadBlockCounters();

    /**
     * Object manages the resources hosted by this {@link DataService}.
     */
    private ResourceManager resourceManager;

    /**
     * Object provides concurrency control for the named resources (indices).
     */
    private ConcurrencyManager concurrencyManager;

    /**
     * Object supports local transactions and does handshaking with the
     * {@link DistributedTransactionService}.
     */
    private DataServiceTransactionManager localTransactionManager;
    
    /**
     * The object used to manage the local resources.
     */
    public ResourceManager getResourceManager() {
        
        return resourceManager;
        
    }

    /**
     * The object used to control access to the local resources.
     */
    public ConcurrencyManager getConcurrencyManager() {
        
        return concurrencyManager;
        
    }

    /**
     * The object used to coordinate transactions executing against local
     * resources.
     */
    public ILocalTransactionManager getLocalTransactionManager() {
        
        return localTransactionManager; 
        
    }
    
    /**
     * Returns the {@link IResourceManager}.
     * 
     * @param properties
     *            Properties to configure that object.
     * 
     * @return The {@link IResourceManager}.
     */
    protected IResourceManager newResourceManager(final Properties properties) {

        return new ResourceManager(properties) {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE            public IBigdataFederation getFederation() {
//BTM - PRE_CLIENT_SERVICE                
//BTM - PRE_CLIENT_SERVICE                return DataService.this.getFederation();
//BTM - PRE_CLIENT_SERVICE                                
//BTM - PRE_CLIENT_SERVICE            }

        @Override
        public IBigdataDiscoveryManagement getDiscoveryManager() {
            return (IBigdataDiscoveryManagement)(DataService.this.getFederation());
        }

        @Override
        public ILocalResourceManagement getLocalResourceManager() {
            return (ILocalResourceManagement)(DataService.this.getFederation());
        }

        @Override
        public IIndexManager getIndexManager() {
            return (IIndexManager)(DataService.this.getFederation());
        }
//BTM - PRE_CLIENT_SERVICE - END
            
            public DataService getDataService() {
                
                return DataService.this;
                
            }
            
            public UUID getDataServiceUUID() {

                return DataService.this.getServiceUUID();
                
            }
            
//            /**
//             * @todo this must report the entire service failover chain.
//             */
//            public UUID[] getDataServiceUUIDs() {
//
//                return new UUID[] {
//                        
//                    getDataServiceUUID()
//                        
//                };
//                
//            }
            
        };

    }

    /**
     * A clone of properties specified to the ctor.
     */
    private final Properties properties;

    /**
     * An object wrapping the properties specified to the ctor.
     */
    public Properties getProperties() {

        return new Properties(properties);
        
    }

    /**
     * The dynamic property set associated with the service instance.
     */
    private final Session session = new Session();
    
    public Session getSession() {

        return session;
        
    }
    
    /**
     * Core constructor - you MUST {@link #start()} the {@link DataService}
     * before it can be used.
     * 
     * @param properties
     *            The configuration properties.
     * 
     * @see Options
     * 
     * @see #start()
     */
    protected DataService(final Properties properties) {
        
        // show the copyright banner during statup.
        Banner.banner();

        this.properties = (Properties) properties.clone();
        
    }
    
    /**
     * Note: "open" is judged by the {@link ConcurrencyManager#isOpen()} but the
     * {@link DataService} is not usable until {@link StoreManager#isStarting()}
     * returns <code>false</code> (there is asynchronous processing involved
     * in reading the existing store files or creating the first store file and
     * you can not use the {@link DataService} until that processing has been
     * completed). The {@link ConcurrencyManager} will block for a while waiting
     * for the {@link StoreManager} startup to complete and will reject tasks if
     * startup processing does not complete within a timeout.
     */
    public boolean isOpen() {
        
        final ConcurrencyManager tmp = this.concurrencyManager;

        return tmp != null && tmp.isOpen();
        
    }
    
    /**
     * Concrete implementation manages the local state of transactions executing
     * on a {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public class DataServiceTransactionManager extends
            AbstractLocalTransactionManager {

//BTM        public ITransactionService getTransactionService() {
public TransactionService getTransactionService() {

            return DataService.this.getFederation().getTransactionService();

        }

        /**
         * Exposed to {@link ITxCommitProtocol#singlePhaseCommit(long)}
         */
        public void deactivateTx(final Tx localState) {

            super.deactivateTx(localState);

        }

    }

    /**
     * Starts the {@link DataService}.
     * 
     * @todo it would be nice if {@link #start()} could restart after
     *       {@link #shutdown()} but that is hardly necessary.
     */
    @Override
    synchronized public DataService start() {

        if (isOpen()) {

            throw new IllegalStateException();

        }

System.err.println("\n---------- BEGIN DataService --------------");
properties.list(System.err);
System.err.println("---------- END DataService --------------\n");
        resourceManager = (ResourceManager) newResourceManager(properties);
       
        localTransactionManager = new DataServiceTransactionManager();
        
        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, resourceManager);

        if (resourceManager instanceof ResourceManager) {

            /*
             * Startup the resource manager.
             */

            ((ResourceManager) resourceManager)
                    .setConcurrencyManager(concurrencyManager);

        }
        
        return this;
        
    }
    
    /**
     * Delegate handles custom counters for the {@link ResourceManager}, local
     * {@link AbstractTransactionService} and the {@link ConcurrencyManager}, dynamic
     * re-attachment of counters, etc. This delegate must be set on the
     * {@link AbstractClient} for those additional features to work.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static public class DataServiceFederationDelegate extends
            DefaultServiceFederationDelegate<DataService> {

        public DataServiceFederationDelegate(DataService service) {

            super(service);
            
        }
        
        /**
         * Dynamically detach and attach the counters for the named indices
         * underneath of the {@link IndexManager}.
         * <p>
         * Note: This method limits the frequency of update to no more than once
         * every 5 seconds.
         * <p>
         * Note: {@link OverflowManager#overflow()} is responsible for
         * reattaching the counters for the live {@link ManagedJournal} during
         * synchronous overflow.
         */
        @Override
        synchronized public void reattachDynamicCounters() {

            final long now = System.currentTimeMillis();

            final long elapsed = now - lastReattachMillis;

            if (service.isOpen() && service.resourceManager.isRunning()
                    && elapsed > 5000/* ms */) {

            	// inherit base class behavior
            	super.reattachDynamicCounters();
            	
                // The service's counter set hierarchy.
                final CounterSet serviceRoot = service.getFederation()
                        .getServiceCounterSet();

                // The lock manager
                {
                    
                    // the lock manager is a direct child of this node.
                    final CounterSet tmp = (CounterSet) serviceRoot
                            .makePath(IDataServiceCounters.concurrencyManager
                                    + ICounterSet.pathSeparator
                                    + IConcurrencyManagerCounters.writeService);

                    synchronized (tmp) {

                        /*
                         * Note: We detach and then attach since that wipes out
                         * any counter set nodes for queues which no longer
                         * exist. Otherwise they will build up forever.
                         */

                        // detach the old counters.
                        tmp.detach(IConcurrencyManagerCounters.LockManager);

                        // attach the the new counters.
                        ((CounterSet) tmp
                                .makePath(IConcurrencyManagerCounters.LockManager))
                                .attach(service.concurrencyManager
                                        .getWriteService().getLockManager()
                                        .getCounters());

                    }

                }
                
                // The live indices.
                {
                
                    /*
                     * The counters for the index manager within the service's
                     * counter hierarchy.
                     * 
                     * Note: The indices are a direct child of this node.
                     */
                    final CounterSet tmp = (CounterSet) serviceRoot
                            .getPath(IDataServiceCounters.resourceManager
                                    + ICounterSet.pathSeparator
                                    + IResourceManagerCounters.IndexManager);

                    synchronized (tmp) {

                        /*
                         * Note: We detach and then attach since that wipes out
                         * any counter set nodes for index partitions which no
                         * longer exist. Otherwise they will build up forever.
                         */
                        final boolean exists = tmp
                                .getPath(IIndexManagerCounters.Indices) != null;

                        // detach the index partition counters.
                        tmp.detach(IIndexManagerCounters.Indices);

                        // attach the current index partition counters.
                        ((CounterSet) tmp
                                .makePath(IIndexManagerCounters.Indices))
                                .attach(service.resourceManager
                                        .getIndexCounters());

                        if (log.isInfoEnabled())
                            log
                                    .info("Attached index partition counters: preexisting="
                                            + exists
                                            + ", path="
                                            + tmp.getPath());

                    }

                }

                lastReattachMillis = now;

            }

        }

        private long lastReattachMillis = 0L;

        public boolean isServiceReady() {
            
            if(!service.resourceManager.isOpen()) {
                
                /*
                 * This will happen if the store manager is unable to discover
                 * the timestamp service. It will halt its startup process and
                 * report that it is closed. At that point the data service can
                 * not start and will shutdown.
                 */

                log.fatal("Store manager not open - will shutdown.");

                // shutdown the data service.
                service.shutdownNow();

                // collection was not started.
                return false;

            }

            if (!service.resourceManager.isRunning()) {

                log.warn("Resource manager is not running yet.");

                return false;

            }

            return true;

        }

        /**
         * Extended to setup {@link DataService} specific counters and to write
         * the client URL onto a file in the service's data directory.
         */
        public void didStart() {

            super.didStart();

            setupCounters();

            logHttpdURL(service.getHTTPDURLFile());

        }

        /**
         * Sets up {@link DataService} specific counters.
         * 
         * @see IDataServiceCounters
         */
        protected void setupCounters() {

            if (getServiceUUID() == null) {

                throw new IllegalStateException(
                        "The ServiceUUID is not available yet");

            }
            
            if(!service.isOpen()) {
                
                /*
                 * The service has already been closed.
                 */
                
                log.warn("Service is not open.");
                
                return;
                
            }

            /*
             * Service specific counters.
             */

            final CounterSet serviceRoot = service.getFederation()
                    .getServiceCounterSet();

            serviceRoot.makePath(IDataServiceCounters.resourceManager).attach(
                    service.resourceManager.getCounters());

            serviceRoot.makePath(IDataServiceCounters.concurrencyManager)
                    .attach(service.concurrencyManager.getCounters());

            serviceRoot.makePath(IDataServiceCounters.transactionManager)
                    .attach(service.localTransactionManager.getCounters());

            // block API.
            {

                CounterSet tmp = serviceRoot.makePath("Block API");

                tmp.addCounter("Blocks Read", new Instrument<Long>() {
                    public void sample() {
                        setValue(service.readBlockApiCounters.readBlockCount);
                    }
                });

                tmp.addCounter("Blocks Read Per Second",
                        new Instrument<Double>() {
                            public void sample() {

                                // @todo encapsulate this logic.

                                long secs = TimeUnit.SECONDS
                                        .convert(
                                                service.readBlockApiCounters.readBlockNanos,
                                                TimeUnit.NANOSECONDS);

                                final double v;

                                if (secs == 0L)
                                    v = 0d;
                                else
                                    v = service.readBlockApiCounters.readBlockCount
                                            / secs;

                                setValue(v);

                            }
                        });

            }

        }

    }
    
    /**
     * Polite shutdown does not accept new requests and will shutdown once the
     * existing requests have been processed.
     */
    synchronized public void shutdown() {

        if (!isOpen())
            return;

        if (concurrencyManager != null) {
            concurrencyManager.shutdown();
//            concurrencyManager = null;
        }

        if (localTransactionManager != null) {
            localTransactionManager.shutdown();
//            localTransactionManager = null;
        }

        if (resourceManager != null) {
            resourceManager.shutdown();
//            resourceManager = null;
        }

        super.shutdown();
        
    }

    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon as
     * possible.
     */
    synchronized public void shutdownNow() {

        if (!isOpen())
            return;

        if (concurrencyManager != null) {
            concurrencyManager.shutdownNow();
//            concurrencyManager = null;
        }

        if (localTransactionManager != null) {
            localTransactionManager.shutdownNow();
//            localTransactionManager = null;
        }

        if (resourceManager != null) {
            resourceManager.shutdownNow();
//            resourceManager = null;
        }

        super.shutdownNow();

    }
    
    synchronized public void destroy() {
        
        super.destroy();
        
        resourceManager.deleteResources();
        
        final File file = getHTTPDURLFile();
        
        if(file.exists()) {
            
            file.delete();
            
        }
        
//        super.destroy();
        
    }

    /**
     * The file on which the URL of the embedded httpd service is written.
     */
    protected File getHTTPDURLFile() {

        return new File(getResourceManager().getDataDir(), "httpd.url");

    }

    /**
     * Interface defines and documents the counters and counter namespaces
     * reported by the {@link DataService} and the various services which it
     * uses.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
//BTM - BEGIN
//BTM    public static interface IDataServiceCounters extends
//BTM            ConcurrencyManager.IConcurrencyManagerCounters,
//BTM//            ...TransactionManager.XXXCounters,
//BTM            ResourceManager.IResourceManagerCounters
//BTM            {
//BTM       
        /**
         * The namespace for the counters pertaining to the {@link ConcurrencyManager}.
         */
//BTM        String concurrencyManager = "Concurrency Manager";

        /**
         * The namespace for the counters pertaining to the {@link ILocalTransactionService}.
         */
//BTM        String transactionManager = "Transaction Manager";
//BTM        
        /**
         * The namespace for the counters pertaining to the {@link ResourceManager}.
         */
//BTM        String resourceManager = "Resource Manager";
//BTM        
//BTM    }
//BTM - END
        
    /*
     * ITxCommitProtocol.
     */
    
    public void setReleaseTime(final long releaseTime) {
        
        setupLoggingContext();
        
        try {
            
            getResourceManager().setReleaseTime(releaseTime);
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    /**
     * Note: This is basically identical to the standalone journal case.
     * 
     * @see JournalTransactionService#commitImpl(long)}.
     */
    public long singlePhaseCommit(final long tx) throws ExecutionException,
            InterruptedException, IOException {
        
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

                throw new IllegalArgumentException();
                
            }
            
            final Tx localState = (Tx) getLocalTransactionManager().getTx(tx);

            if (localState == null) {

                /*
                 * This is not an active transaction.
                 */

                throw new IllegalStateException();

            }
            
            /*
             * Note: This code is shared (copy-by-value) by the
             * JournalTransactionService commitImpl(...)
             */
            final ManagedJournal journal = getResourceManager().getLiveJournal();
            
            {

                /*
                 * A transaction with an empty write set can commit immediately
                 * since validation and commit are basically NOPs (this is the same
                 * as the read-only case.)
                 * 
                 * Note: We lock out other operations on this tx so that this
                 * decision will be atomic.
                 */

                localState.lock.lock();

                try {

                    if (localState.isEmptyWriteSet()) {

                        /*
                         * Sort of a NOP commit. 
                         */
                        
                        localState.setRunState(RunState.Committed);

                        ((DataServiceTransactionManager) journal
                                .getLocalTransactionManager())
                                .deactivateTx(localState);
                        
//                        state.setRunState(RunState.Committed);
                        
                        return 0L;

                    }

                } finally {

                    localState.lock.unlock();

                }

            }

            final IConcurrencyManager concurrencyManager = /*journal.*/getConcurrencyManager();

            final AbstractTask<Void> task = new SinglePhaseCommit(
                    concurrencyManager, journal.getLocalTransactionManager(),
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
                concurrencyManager
                .submit(task).get();
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

                    ((DataServiceTransactionManager) journal
                            .getLocalTransactionManager())
                            .deactivateTx(localState);
                
//                    state.setRunState(RunState.Committed);

                } finally {
                    
                    localState.lock.unlock();
                    
                }

            } catch (Throwable t) {

//                log.error(t.getMessage(), t);

                localState.lock.lock();

                try {

                    localState.setRunState(RunState.Aborted);

                    ((DataServiceTransactionManager) journal
                            .getLocalTransactionManager())
                            .deactivateTx(localState);

//                    state.setRunState(RunState.Aborted);

                    throw new RuntimeException(t);
                    
                } finally {
                    
                    localState.lock.unlock();

                }

            }

            /*
             * Note: This is returning the commitTime set on the task when it was
             * committed as part of a group commit.
             */
            
//            log.warn("\n" + state + "\n" + localState);

            return task.getCommitTime();

        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public void prepare(final long tx, final long revisionTime)
            throws ExecutionException, InterruptedException, IOException {
        
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
                
                throw new IllegalArgumentException();
                
            }
            
            final Tx state = (Tx) getLocalTransactionManager().getTx(tx);

            if (state == null) {

                /*
                 * This is not an active transaction.
                 */

                throw new IllegalStateException();

            }
            
            /*
             * Submit the task and await its future
             */

            concurrencyManager.submit(
                    new DistributedCommitTask(concurrencyManager,
                            resourceManager, getServiceUUID(), state,
                            revisionTime)).get();

            // Done.
            
        } finally {

            clearLoggingContext();

        }

    }

    /**
     * Task handling the distributed commit protocol for the
     * {@link ShardService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
//BTM - BEGIN
//BTM    private static class DistributedCommitTask extends AbstractTask<Void> {
//BTM
//BTM        // ctor arg.
//BTM        private final ResourceManager resourceManager;
//BTM        private UUID dataServiceUUID;
//BTM        private final Tx state;
//BTM        private final long revisionTime;
//BTM        
//BTM        // derived.
//BTM        private final long tx;
//BTM        
        /**
         * @param concurrencyManager
         * @param resourceManager
         * @param dataServiceUUID
         * @param localState
         * @param revisionTime
         */
//BTM        public DistributedCommitTask(
//BTM                final ConcurrencyManager concurrencyManager,//
//BTM                final ResourceManager resourceManager,//
//BTM                final UUID dataServiceUUID,//
//BTM                final Tx localState,//
//BTM                final long revisionTime//
//BTM        ) {
//BTM
//BTM            super(concurrencyManager, ITx.UNISOLATED, localState
//BTM                    .getDirtyResource());
//BTM
//BTM            if (resourceManager == null)
//BTM                throw new IllegalArgumentException();
//BTM
//BTM            if (localState == null)
//BTM                throw new IllegalArgumentException();
//BTM            
//BTM            if (revisionTime == 0L)
//BTM                throw new IllegalArgumentException();
//BTM            
//BTM            if (revisionTime <= localState.getStartTimestamp())
//BTM                throw new IllegalArgumentException();
//BTM
//BTM            this.resourceManager = resourceManager;
//BTM
//BTM            this.dataServiceUUID = dataServiceUUID;
//BTM
//BTM            this.state = localState;
//BTM
//BTM            this.revisionTime = revisionTime;
//BTM
//BTM            this.tx = localState.getStartTimestamp();
//BTM
//BTM        }
//BTM
        /**
         * FIXME Finish, write tests and debug.
         */
//BTM        @Override
//BTM        protected Void doTask() throws Exception {
//BTM
//BTM//BTM            final ITransactionService txService = resourceManager
//BTMfinal TransactionService txService = resourceManager
//BTM                    .getLiveJournal().getLocalTransactionManager()
//BTM                    .getTransactionService();
//BTM
//BTM            prepare();
//BTM
//BTM            final long commitTime = txService.prepared(tx, dataServiceUUID);
//BTM
//BTM            // obtain the exclusive write lock on journal.
//BTM            lockJournal();
//BTM            try {
//BTM
//BTM                // Commit using the specified commit time.
//BTM                commit(commitTime);
//BTM
//BTM                boolean success = false;
//BTM                try {
//BTM
                    /*
                     * Wait until the entire distributed transaction is
                     * committed.
                     */
//BTM                    success = txService.committed(tx, dataServiceUUID);
//BTM
//BTM                } finally {
//BTM
//BTM                    if (!success) {
//BTM
//BTM                        // Rollback the journal.
//BTM                        rollback();
//BTM
//BTM                    }
//BTM
//BTM                }
//BTM                
//BTM            } finally {
//BTM
//BTM                // release the exclusive write lock on journal.
//BTM                unlockJournal();
//BTM
//BTM            }
//BTM
//BTM            return null;
//BTM
//BTM        }
//BTM
        /**
         * Prepare the transaction (validate and merge down onto the unisolated
         * indices and then checkpoints those indices).
         * <p>
         * Note: This presumes that we are already holding exclusive write locks
         * on the named indices such that the pre-conditions for validation and
         * its post-conditions can not change until we either commit or discard
         * the transaction.
         * <p>
         * Note: The indices need to be isolated as by {@link AbstractTask} or
         * they will be enrolled onto {@link Name2Addr}'s commitList when they
         * become dirty and then checkpointed and included with the NEXT commit.
         * <p>
         * For this reason, the {@link DistributedCommitTask} is an UNISOLATED
         * task so that we can reuse the existing mechanisms as much as
         * possible.
         * 
         * FIXME This will work if we can grab the write service lock from
         * within the task (which will mean changing that code to allow the lock
         * with the caller only still running or simply waiting until we are
         * signaled by the txService that all participants are either go
         * (continue execution and will commit at the next group commit, but
         * then we need a protocol to impose the correct commit time, e.g., by
         * passing it on the task and ensuring that there is no other tx ready
         * in the commit group) or abort (just throw an exception).
         */
//BTM        protected void prepare() {
//BTM            
//BTM            state.prepare(revisionTime);
//BTM            
//BTM        }
//BTM
        /**
         * Obtain the exclusive lock on the write service. This will prevent any
         * other tasks using the concurrency API from writing on the journal.
         */
//BTM        protected void lockJournal() {
//BTM
//BTM            throw new UnsupportedOperationException();
//BTM            
//BTM        }
//BTM        
//BTM        protected void unlockJournal() {
//BTM            
//BTM            throw new UnsupportedOperationException();
//BTM            
//BTM        }
//BTM        
        /**
         * Commit the transaction using the specified <i>commitTime</i>.
         * <p>
         * Note: There are no persistent side-effects unless this method returns
         * successfully.
         * 
         * @param commitTime
         *            The commit time that must be used.
         */
//BTM        protected void commit(final long commitTime) {
//BTM
            /*
             * @todo enroll the named indices onto Name2Addr's commitList (this
             * basically requires breaking the isolation imposed by the
             * AbstractTask).
             */
//BTM
//BTM            if (true)
//BTM                throw new UnsupportedOperationException();
//BTM
//BTM            final ManagedJournal journal = resourceManager.getLiveJournal();
//BTM            
//BTM            // atomic commit.
//BTM            journal.commitNow(commitTime);
//BTM
//BTM        }
//BTM        
        /**
         * Discard the last commit, restoring the journal to the previous commit
         * point.
         */
//BTM        protected void rollback() {
//BTM            
//BTM            final ManagedJournal journal = resourceManager.getLiveJournal();
//BTM            
//BTM            journal.rollback();
//BTM            
//BTM        }
//BTM        
//BTM    }
//BTM - END

    public void abort(final long tx) throws IOException {

        setupLoggingContext();

        try {

            final Tx localState = (Tx) getLocalTransactionManager().getTx(tx);

            if (localState == null)
                throw new IllegalArgumentException();

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

    /**
     * Forms the name of the index corresponding to a partition of a named
     * scale-out index as <i>name</i>#<i>partitionId</i>.
     * <p>
     * Another advantage of this naming scheme is that index partitions are just
     * named indices and all of the mechanisms for operating on named indices
     * and for concurrency control for named indices apply automatically. Among
     * other things, this means that different tasks can write concurrently on
     * different partitions of the same named index on a given
     * {@link DataService}.
     * 
     * @return The name of the index partition.
     */
//BTM - MOVED to bigdata-jini/src/java/com/bigdata/util/Util.java
//BTM    public static final String getIndexPartitionName(final String name,
//BTM            final int partitionId) {
//BTM
//BTM        if (name == null) {
//BTM
//BTM            throw new IllegalArgumentException();
//BTM            
//BTM        }
//BTM
//BTM        if (partitionId == -1) {
//BTM
//BTM            // Not a partitioned index.
//BTM            return name;
//BTM            
//BTM        }
//BTM        
//BTM        return name + "#" + partitionId;
//BTM
//BTM    }

    /**
     * Returns either {@link IDataService} or {@link IMetadataService} as
     * appropriate.
     */
    public Class getServiceIface() {

        final Class serviceIface;
        
        if(DataService.this instanceof IMetadataService) {
        
            serviceIface = IMetadataService.class;
            
        } else {
            
            serviceIface = IDataService.class;
            
        }
        
        return serviceIface;

    }
    
    public void registerIndex(String name, IndexMetadata metadata)
            throws IOException, InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            if (metadata == null)
                throw new IllegalArgumentException();

            final AbstractTask task = new RegisterIndexTask(concurrencyManager,
                    name, metadata);
            
            concurrencyManager.submit(task).get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }
    
    public void dropIndex(String name) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {
        
            final AbstractTask task = new DropIndexTask(concurrencyManager,
                    name);
            
            concurrencyManager.submit(task).get();

        } finally {
            
            clearLoggingContext();
            
        }

    }
   
    public IndexMetadata getIndexMetadata(String name, long timestamp)
            throws IOException, InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            // Choose READ_COMMITTED iff UNISOLATED was requested.
            final long startTime = (timestamp == ITx.UNISOLATED
                    ? ITx.READ_COMMITTED
                    : timestamp);

            final AbstractTask task = new GetIndexMetadataTask(
                    concurrencyManager, startTime, name);

            return (IndexMetadata) concurrencyManager.submit(task).get();

        } finally {

            clearLoggingContext();

        }
        
    }

    /**
     * Retrieves the {@link IndexMetadata} for the named index as of the
     * specified timestamp.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
//BTM    public static class GetIndexMetadataTask extends AbstractTask {
//BTM
//BTM        public GetIndexMetadataTask(ConcurrencyManager concurrencyManager,
//BTM                long startTime, String name) {
//BTM
//BTM            super(concurrencyManager, startTime, name);
//BTM            
//BTM        }
//BTM
//BTM        @Override
//BTM        protected IndexMetadata doTask() throws Exception {
//BTM            
//BTM            return getIndex(getOnlyResource()).getIndexMetadata();
//BTM            
//BTM        }
//BTM        
//BTM    }
    
    /**
     * Note: This chooses {@link ITx#READ_COMMITTED} if the the index has
     * {@link ITx#UNISOLATED} isolation and the {@link IIndexProcedure} is an
     * read-only operation. This provides better concurrency on the
     * {@link DataService} by moving read-only operations off of the
     * {@link WriteExecutorService}.
     * <p>
     * Note: When the {@link DataService} is accessed via RMI the {@link Future}
     * MUST be a proxy. This gets handled by the concrete server implementation.
     */
    public Future submit(final long tx, final String name,
            final IIndexProcedure proc) {

        setupLoggingContext();

        try {
    
            if (name == null)
                throw new IllegalArgumentException();

            if (proc == null)
                throw new IllegalArgumentException();
            
            // Choose READ_COMMITTED iff proc is read-only and UNISOLATED was requested.
            final long timestamp = (tx == ITx.UNISOLATED
                        && proc.isReadOnly() ? ITx.READ_COMMITTED
                        : tx);

            // wrap the caller's task.
            final AbstractTask task = new IndexProcedureTask(
                    concurrencyManager, timestamp, name, proc);
            
            // submit the procedure and await its completion.
            return concurrencyManager.submit(task);
        
        } finally {
            
            clearLoggingContext();
            
        }

    }

    /**
     * Note: When the {@link DataService} is accessed via RMI the {@link Future}
     * MUST be a proxy. This gets handled by the concrete server implementation.
     * 
     * @see AbstractDistributedFederation#getProxy(Future)
     * 
     * @todo we should probably put the federation object in a sandbox in order
     *       to prevent various operations by tasks running in the
     *       {@link DataService} using the {@link IDataServiceCallable}
     *       interface to gain access to the {@link DataService}'s federation.
     *       for example, if they use {@link AbstractFederation#shutdownNow()}
     *       then the {@link DataService} itself would be shutdown.
     */
    public <T> Future<T> submit(final IDataServiceCallable<T> task) {

        setupLoggingContext();

        try {

            if (task == null)
                throw new IllegalArgumentException();

            // submit the task and return its Future.
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE  return getFederation().getExecutorService().submit(new DataTaskWrapper(getFederation(), this, task));
            return getFederation().getExecutorService().submit
                       (new DataTaskWrapper( (IIndexManager)getFederation(),
                                             getResourceManager(),
                                             getConcurrencyManager(),
                                             (ILocalResourceManagement)getFederation(),
                                             (IBigdataDiscoveryManagement)getFederation(),
                                             task));
//BTM - PRE_CLIENT_SERVICE - END
        } finally {

            clearLoggingContext();

        }

    }
    
    /**
     * Encapsulate the {@link Future} within a proxy that may be marshalled by
     * RMI and sent to a remote client. The client will interact with the
     * unmarshalled {@link Future}, which in turn will use RMI to control the
     * original {@link Future} within the {@link DataService}.
     * <p>
     * The default implementation simply returns the <i>future</i> and MUST be
     * overriden when remote clients will use RMI to execute methods on the
     * {@link DataService}.
     * 
     * @param future
     *            The future.
     * 
     * @return The encapsulated future.
     */
    protected Future wrapFuture(Future future) {
        
        return future;
        
    }
    
    public ResultSet rangeIterator(long tx, String name, byte[] fromKey,
            byte[] toKey, int capacity, int flags, IFilterConstructor filter)
            throws InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            if (name == null)
                throw new IllegalArgumentException();
            
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

                /*
                 * If the iterator is readOnly then READ_COMMITTED has the same
                 * semantics as UNISOLATED and provides better concurrency since
                 * it reduces contention for the writeService.
                 */

                timestamp = ITx.READ_COMMITTED;

            }

//            final long startTime = (tx == ITx.UNISOLATED
//                        && ((flags & IRangeQuery.REMOVEALL)==0)? ITx.READ_COMMITTED
//                        : tx);

            final RangeIteratorTask task = new RangeIteratorTask(
                    concurrencyManager, timestamp, name, fromKey, toKey,
                    capacity, flags, filter);

            // submit the task and wait for it to complete.
            return (ResultSet) concurrencyManager.submit(task).get();
        
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

        if (resource == null)
            throw new IllegalArgumentException();

        if (addr == 0L)
            throw new IllegalArgumentException();

        setupLoggingContext();

        final long begin = System.nanoTime();
        
        try {
            
            final IRawStore store = resourceManager.openStore(resource.getUUID());
    
            if (store == null) {
    
                log.warn("Resource not available: " + resource);
    
                readBlockApiCounters.readBlockErrorCount++;

                throw new IllegalStateException("Resource not available");
    
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
    
            };
            
        } finally {
            
            readBlockApiCounters.readBlockCount++;

            readBlockApiCounters.readBlockNanos = System.nanoTime() - begin;

            clearLoggingContext();
            
        }
                 
    }
    
    /**
     * Task for running a rangeIterator operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
//BTM//BTM    static protected class RangeIteratorTask extends AbstractTask {
//BTMstatic public class RangeIteratorTask extends AbstractTask {
//BTM
//BTM        private final byte[] fromKey;
//BTM        private final byte[] toKey;
//BTM        private final int capacity;
//BTM        private final int flags;
//BTM        private final IFilterConstructor filter;
//BTM        
//BTM        public RangeIteratorTask(ConcurrencyManager concurrencyManager,
//BTM                long startTime, String name, byte[] fromKey, byte[] toKey,
//BTM                int capacity, int flags, IFilterConstructor filter) {
//BTM
//BTM            super(concurrencyManager, startTime, name);
//BTM
//BTM            this.fromKey = fromKey;
//BTM            this.toKey = toKey;
//BTM            this.capacity = capacity;
//BTM            this.flags = flags;
//BTM            this.filter = filter; // MAY be null.
//BTM
//BTM        }
//BTM
//BTM        public ResultSet doTask() throws Exception {
//BTM
//BTM            final IIndex ndx = getIndex(getOnlyResource());
//BTM            
            /*
             * Figure out the upper bound on the #of tuples that could be
             * materialized.
             * 
             * Note: the upper bound on the #of key-value pairs in the range is
             * truncated to an [int].
             */
//BTM            
//BTM            final int rangeCount = (int) ndx.rangeCount(fromKey, toKey);
//BTM
//BTM            final int limit = (rangeCount > capacity ? capacity : rangeCount);
//BTM
            /*
             * Iterator that will visit the key range.
             * 
             * Note: We always visit the keys regardless of whether we pass them
             * on to the caller. This is necessary in order for us to set the
             * [lastKey] field on the result set and that is necessary to
             * support continuation queries.
             */
//BTM            
//BTM            final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey, limit,
//BTM                    flags | IRangeQuery.KEYS, filter);
//BTM            
            /*
             * Populate the result set from the iterator.
             */
//BTM
//BTM            return new ResultSet(ndx, capacity, flags, itr);
//BTM
//BTM        }
//BTM        
//BTM    }

    /*
     * Overflow processing API 
     */

    public void forceOverflow(final boolean immediate,
            final boolean compactingMerge) throws IOException,
            InterruptedException, ExecutionException {
    
        setupLoggingContext();

        try {

            if (!(resourceManager instanceof ResourceManager)) {

                throw new UnsupportedOperationException();

            }
            
            final Callable task = new ForceOverflowTask(compactingMerge);
            
            log.warn("Will force overflow: immediate=" + immediate
                    + ", compactingMerge=" + compactingMerge);
            
            if (immediate) {

                /*
                 * Run the task on the write service. The task writes a small
                 * record on the journal in order to make sure that it is dirty
                 * and then sets the flag to force overflow with the next
                 * commit. Since the task runs on the write service and since
                 * the journal is dirty, a group commit will occur and
                 * synchronous overflow processing will occur before this method
                 * returns.
                 * 
                 * Note: the resource itself is arbitrary - there is no index
                 * by that name.
                 */

                getConcurrencyManager().submit(
                        new AbstractTask(getConcurrencyManager(),
                                ITx.UNISOLATED,
                                new String[] { "__forceOverflow" }) {

                    @Override
                    protected Object doTask() throws Exception {

                        // write a one byte record on the journal.
                        getJournal().write(ByteBuffer.wrap(new byte[]{1}));
                        
                        // run task that will set the overflow flag.
                        return task.call();
                        
                    }
                    
                }).get();
                
            } else {

                /*
                 * Provoke overflow with the next group commit. All this does is
                 * set the flag that will cause overflow to occur with the next
                 * group commit. Since the task does not run on the write
                 * service it will return immediately.
                 */
                
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

    public boolean purgeOldResources(final long timeout,
            final boolean truncateJournal) throws InterruptedException {

        // delegate all the work.
        return getResourceManager().purgeOldResources(timeout, truncateJournal);
        
    }
    
    /**
     * Task sets the flag that will cause overflow processing to be triggered on
     * the next group commit.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private class ForceOverflowTask implements Callable<Void> {

        private final boolean compactingMerge;
        
        public ForceOverflowTask(final boolean compactingMerge) {
            
            this.compactingMerge = compactingMerge;
            
        }
        
        public Void call() throws Exception {

//            final WriteExecutorService writeService = concurrencyManager
//                    .getWriteService();

            final ResourceManager resourceManager = (ResourceManager) DataService.this.resourceManager;

            if (resourceManager.isOverflowAllowed()) {

                if (compactingMerge) {

                    resourceManager.compactingMerge.set(true);

                }

                // trigger overflow on the next group commit.
//                writeService.forceOverflow.set(true);
                resourceManager.forceOverflow.set(true);

            }

            return null;

        }

    }

    public long getAsynchronousOverflowCounter() throws IOException {

        setupLoggingContext();

        try {

            if (!(resourceManager instanceof ResourceManager)) {

                throw new UnsupportedOperationException();

            }

            return resourceManager.getAsynchronousOverflowCount();

        } finally {

            clearLoggingContext();

        }
        
    }
    
    public boolean isOverflowActive() throws IOException {
        
        setupLoggingContext();

        try {

            if (!(resourceManager instanceof ResourceManager)) {

                throw new UnsupportedOperationException();

            }

            /*
             * overflow processing is enabled but not allowed, which means that
             * overflow processing is occurring right now.
             */
            return resourceManager.isOverflowEnabled()
                    && !resourceManager.isOverflowAllowed();

        } finally {

            clearLoggingContext();

        }
        
    }
   
}
