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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IReadOnlyOperation;
import com.bigdata.btree.ITupleFilter;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.IIndexProcedure.ISimpleIndexProcedure;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;
import com.bigdata.util.MillisecondTimestampFactory;

/**
 * An implementation of a network-capable {@link IDataService}. The service is
 * started using the {@link DataServer} class. Operations are submitted using an
 * {@link IConcurrentManager#submit(AbstractTask)} and will run with the
 * appropriate concurrency controls as imposed by that method.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see DataServer, which is used to start this service.
 * 
 * @todo Participate in 1-phase (local) and 2-/3- phrase (distributed) commits
 *       with an {@link ITransactionManagerService} service. The data service
 *       needs to notify the {@link ITransactionManagerService} each time an
 *       isolated writer touches a named index so that the transaction manager
 *       can build up the set of resources that must be locked during the
 *       validate/commit phrase.
 * 
 * @todo Write benchmark test to measure interhost transfer rates. Should be
 *       100Mbits/sec (~12M/sec) on a 100BaseT switched network. With full
 *       duplex in the network and the protocol, that rate should be
 *       bidirectional. Can that rate be sustained with a fully connected
 *       bi-directional transfer?
 * 
 * @todo RPC requests are currently made via RPC using JERI. While you can elect
 *       to use the TCP/NIO server via configuration options (see
 *       http://java.sun.com/products/jini/2.0.1/doc/api/net/jini/jeri/tcp/package-summary.html),
 *       there will still be a thread allocated per concurrent RPC and no
 *       throttling will be imposed by JERI.
 *       <p>
 *       The present design of the {@link IDataService} API requires that a
 *       server thread be dedicated to each request against that interface - in
 *       this way it exactly matches the RPC semantics supported by JERI. The
 *       underlying reason is that the RPC calls are all translated into
 *       {@link Future}s when the are submitted via
 *       {@link ConcurrencyManager#submit(AbstractTask)}. The
 *       {@link DataService} itself then invokes {@link Future#get()} in order
 *       to await the completion of the request and return the response (object
 *       or thrown exception).
 *       <p>
 *       A re-design based on an asynchronous response from the server could
 *       remove this requirement, thereby allowing a handful of server threads
 *       to handle a large volume of concurrent client requests. The design
 *       would use asynchronous callback to the client via JERI RPC calls to
 *       return results, indications that the operation was complete, or
 *       exception information. A single worker thread on the server could
 *       monitor the various futures and RPC clients when responses become
 *       available or on request timeout.
 *       <p>
 *       See {@link NIODataService}, which contains some old code that can be
 *       refactored for an NIO interface to the data service.
 *       <p>
 *       Another option to throttle requests is to use a blocking queue to
 *       throttle the #of tasks that are submitted to the data service. Latency
 *       should be imposed on threads submitting tasks as the queue grows in
 *       order to throttle clients. If the queue becomes full
 *       {@link RejectedExecutionException} will be thrown, and the client will
 *       have to handle that. In contrast, if the queue never blocks and never
 *       imposes latency on clients then it is possible to flood the data
 *       service with requests, even through they will be processed by no more
 *       than {@link ConcurrentManager.Options#WRITE_SERVICE_MAXIMUM_POOL_SIZE}
 *       threads.
 * 
 * @todo Review JERI options to support secure RMI protocols. For example, using
 *       SSL or an SSH tunnel. For most purposes I expect bigdata to operate on
 *       a private network, but replicate across gateways is also a common use
 *       case. Do we have to handle it specially?
 */
abstract public class DataService implements IDataService, IWritePipeline,
        IServiceShutdown {

    public static final Logger log = Logger.getLogger(DataService.class);

    /**
     * Options understood by the {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options {
        
    }
    
    /**
     * FIXME Discover the {@link ITransactionManager} service and use it as the
     * source of timestamps! Make sure that the {@link IBigdataClient} and
     * {@link IBigdataFederation} implementations likewise discover the
     * {@link ITransactionManager} service and use it rather than directly
     * issuing {@link ITransactionManager} requests to a {@link DataService}!
     */
    private static final MillisecondTimestampFactory timestampFactory = new MillisecondTimestampFactory();
    
    final protected IResourceManager resourceManager;
    final protected ConcurrencyManager concurrencyManager;
    final protected AbstractLocalTransactionManager localTransactionManager;

    /**
     * The object used to manage the local resources.
     */
    public IResourceManager getResourceManager() {
        
        return resourceManager;
        
    }

    /**
     * The object used to control access to the local resources.
     */
    public IConcurrencyManager getConcurrencyManager() {
        
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
     * The {@link MetadataService}.
     */
    abstract protected IMetadataService getMetadataService();
    
    /**
     * Returns the {@link IResourceManager}.
     * 
     * @param properties Properties to configure that object.
     * 
     * @return The {@link IResourceManager}.
     */
    protected IResourceManager newResourceManager(Properties properties) {

        return new ResourceManager(properties) {
            
            public IMetadataService getMetadataService() {
                
                return DataService.this.getMetadataService();
                                
            }
            
            public UUID getDataServiceUUID() {

                try {
                    
                    return DataService.this.getServiceUUID();
                
                } catch (IOException e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
            }
            
            /**
             * @todo this must report the entire service failover chain.
             */
            public UUID[] getDataServiceUUIDs() {

                return new UUID[] {
                        
                        getDataServiceUUID()
                        
                };
                
            }
            
        };

    }
    
    /**
     * 
     * @param properties
     */
    public DataService(Properties properties) {
        
        resourceManager = newResourceManager(properties);
        
        localTransactionManager = new AbstractLocalTransactionManager(resourceManager) {

            public long nextTimestamp() {

                return timestampFactory.nextMillis();
                
            }
            
        };
        
        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, resourceManager);

        localTransactionManager.setConcurrencyManager(concurrencyManager);

        if(resourceManager instanceof ResourceManager) {

            /*
             * Startup the resource manager.
             */
            
            ((ResourceManager)resourceManager).setConcurrencyManager(concurrencyManager);

            ((ResourceManager)resourceManager).start();
            
        }
        
    }

    /**
     * Polite shutdown does not accept new requests and will shutdown once the
     * existing requests have been processed.
     * <p>
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionManager} and finally the {@link IResourceManager}.
     */
    public void shutdown() {
        
        concurrencyManager.shutdown();
        
        localTransactionManager.shutdown();

        resourceManager.shutdown();
        
    }
    
    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon as
     * possible.
     * <p>
     * Note: The {@link IConcurrencyManager} is shutdown first, then the
     * {@link ITransactionManager} and finally the {@link IResourceManager}.
     */
    public void shutdownNow() {
  
        concurrencyManager.shutdownNow();

        localTransactionManager.shutdownNow();

        resourceManager.shutdownNow();

    }

    /**
     * The unique identifier for this data service.
     * 
     * @return The unique data service identifier.
     */
    public abstract UUID getServiceUUID() throws IOException;
    
    /*
     * ITxCommitProtocol.
     */
    
    public long commit(long tx) throws IOException {
        
        setupLoggingContext();
        
        try {
        
            // will place task on writeService and block iff necessary.
            return localTransactionManager.commit(tx);
        
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public void abort(long tx) throws IOException {

        setupLoggingContext();

        try {

            // will place task on writeService iff read-write tx.
            localTransactionManager.abort(tx);
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    /*
     * IDataService.
     */
    
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
    public static final String getIndexPartitionName(String name,
            int partitionId) {

        if (name == null) {

            throw new IllegalArgumentException();
            
        }

        if (partitionId == -1) {

            // Not a partitioned index.
            return name;
            
        }
        
        return name + "#" + partitionId;

    }
    
    public String getStatistics() throws IOException {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("dataService: uuid=" + getServiceUUID());

        sb.append("\nResourceManager:");

        sb.append(resourceManager.getStatistics());
        
        sb.append("\nConcurrencyManager:");

        sb.append(concurrencyManager.getStatistics());

        sb.append("\nLocalTransactionManager:");

        sb.append(localTransactionManager.getStatistics());
        
        return sb.toString();
        
    }

    /**
     * Sets up the {@link MDC} logging context. You should do this on every
     * client facing point of entry and then call {@link #clearLoggingContext()}
     * in a <code>finally</code> clause. You can extend this method to add
     * additional context.
     * <p>
     * This implementation add the "serviceUUID" parameter to the {@link MDC}.
     * The serviceUUID is, in general, assigned asynchronously by the service
     * registrar. Once the serviceUUID becomes available it will be added to the
     * {@link MDC}. This datum can be injected into log messages using
     * %X{serviceUUID} in your log4j pattern layout.
     */
    protected void setupLoggingContext() {

        try {
            
            // Note: This _is_ a local method call.
            
            UUID serviceUUID = getServiceUUID();
            
            // Will be null until assigned by the service registrar.
            
            if (serviceUUID == null) {

                return;
                
            }
            
            // Add to the logging context for the current thread.
            
            MDC.put("serviceUUID", serviceUUID.toString());

        } catch(Throwable t) {
            /*
             * Ignore.
             */
        }
        
    }

    /**
     * Clear the logging context.
     */
    protected void clearLoggingContext() {
        
        MDC.remove("serviceUUID");
        
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
   
    public IndexMetadata getIndexMetadata(String name,long timestamp) throws IOException {

        setupLoggingContext();
        
        try {

            /*
             * Note: This does not use concurrency controls but we are 
             * performing a purely read-only operation.
             */
            final IIndex ndx = resourceManager.getIndex(name,timestamp);
            
            if(ndx == null) {
                
                throw new NoSuchIndexException(name);
                
            }
            
            return ndx.getIndexMetadata()/*.clone()?*/;
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public String getStatistics(String name, long timestamp) throws IOException {

        setupLoggingContext();
        
        try {

            /*
             * Note: This does not use concurrency controls but we are 
             * performing a purely read-only operation.
             */
            String statistics = resourceManager.getStatistics(name, timestamp);
            
            if(statistics == null) {
                
                throw new NoSuchIndexException(name);
                
            }
            
            return statistics;
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }
    
    /**
     * Note: This chooses {@link ITx#READ_COMMITTED} if the the index has
     * {@link ITx#UNISOLATED} isolation and the {@link IIndexProcedure} is an
     * {@link IReadOnlyOperation} operation. This provides better concurrency on
     * the {@link DataService} by moving read-only operations off of the
     * {@link WriteExecutorService}.
     */
    public Object submit(long tx, String name, IIndexProcedure proc)
            throws InterruptedException, ExecutionException {

        setupLoggingContext();

        try {
    
            // Choose READ_COMMITTED iff proc is read-only and UNISOLATED was requested.
            final long startTime = (tx == ITx.UNISOLATED
                        && proc instanceof IReadOnlyOperation ? ITx.READ_COMMITTED
                        : tx);

            // submit the procedure.
            final AbstractTask task = new IndexProcedureTask(
                    concurrencyManager, startTime, name, proc);
            
            if(proc instanceof IDataServiceIndexProcedure) {
                
                ((IDataServiceIndexProcedure)proc).setMetadataService( getMetadataService() );
                
            }
            
            // await its completion.
            return concurrencyManager.submit(task).get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }

    public ResultSet rangeIterator(long tx, String name, byte[] fromKey,
            byte[] toKey, int capacity, int flags, ITupleFilter filter)
            throws InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            if (name == null)
                throw new IllegalArgumentException();
            
            final RangeIteratorTask task = new RangeIteratorTask(
                    concurrencyManager, tx, name, fromKey, toKey, capacity,
                    flags, filter);
    
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
     */
    public IBlock readBlock(IResourceMetadata resource, final long addr) {

        if (resource == null)
            throw new IllegalArgumentException();

        if (addr == 0L)
            throw new IllegalArgumentException();

        setupLoggingContext();

        try {
            
            final IRawStore store = resourceManager.openStore(resource.getUUID());
    
            if (store == null) {
    
                log.warn("Resource not available: " + resource);
    
                throw new IllegalStateException("Resource not available");
    
            }
    
            // @todo efficient (stream-based) read from the journal (IBlockStore
            // API).  This is a fully buffered read and will cause heap churn.
    
            return new IBlock() {
    
                public long getAddress() {
                    return addr;
                }
    
                // @todo reuse buffers
                public InputStream inputStream() {
    
                    ByteBuffer buf = store.read(addr);
    
                    return new ByteBufferInputStream(buf);
    
                }
    
                public int length() {
    
                    return store.getByteCount(addr);
    
                }
    
            };
            
        } finally {
            
            clearLoggingContext();
            
        }
                 
    }
    
    /**
     * Task for running a rangeIterator operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class RangeIteratorTask extends AbstractTask {

        private final byte[] fromKey;
        private final byte[] toKey;
        private final int capacity;
        private final int flags;
        private final ITupleFilter filter;
        
        public RangeIteratorTask(ConcurrencyManager concurrencyManager,
                long startTime, String name, byte[] fromKey, byte[] toKey,
                int capacity, int flags, ITupleFilter filter) {

            super(concurrencyManager, startTime, name);

            this.fromKey = fromKey;
            this.toKey = toKey;
            this.capacity = capacity;
            this.flags = flags;
            this.filter = filter; // MAY be null.

        }

        public Object doTask() throws Exception {

            return new ResultSet(getIndex(getOnlyResource()), fromKey, toKey,
                    capacity, flags, filter);

        }
        
    }

    /**
     * Method sets flag to force overflow processing for the
     * {@link IResourceManager} and waits until overflow processing is complete.
     * 
     * @throws InterruptedException
     */
    public void forceOverflow() throws IOException {
    
        setupLoggingContext();

        try {

            if (!(resourceManager instanceof ResourceManager)) {

                throw new UnsupportedOperationException();

            }

            final WriteExecutorService writeService = concurrencyManager
                    .getWriteService();

            final ResourceManager resourceManager = (ResourceManager) this.resourceManager;

            if (resourceManager.isOverflowAllowed()) {

                log.info("Setting flag to force overflow processing");

                writeService.forceOverflow.set(true);

            }

            while (writeService.forceOverflow.get()) {

                try {

                    log.info("Waiting for overflow procssing to complete");

                    Thread.sleep(250/* ms */);

                } catch (InterruptedException e) {

                    throw new RuntimeException(e);

                }

            }

            log.info("Overflow processing complete.");

        } finally {

            clearLoggingContext();

        }
        
    }
    
    /**
     * An interface that provides access to the federation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IDataServiceIndexProcedure extends ISimpleIndexProcedure {
        
        /**
         * Invoked before the task is executed to given the procedure a
         * reference to the {@link IMetadataService} for the federation. Among
         * other things this can be used to resolve a data service {@link UUID}
         * to an {@link IDataService}.
         */
        public void setMetadataService(IMetadataService metadataService);
        
    }
    
}
