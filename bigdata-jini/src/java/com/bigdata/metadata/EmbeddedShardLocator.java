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

package com.bigdata.metadata;

import static com.bigdata.metadata.Constants.*;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.ICounterSet;
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.BigdataDiscoveryManager;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.GetIndexMetadataTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IndexExistsException;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.LocalTransactionManager;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.RangeIteratorTask;
import com.bigdata.journal.ScaleOutIndexManager;
import com.bigdata.journal.TransactionService;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.resources.LocalResourceManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.DataTaskWrapper;
import com.bigdata.service.IDataServiceCallable;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.ISession;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.MetadataIndexCachePolicy;
import com.bigdata.service.Service;
import com.bigdata.service.Session;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardManagement;
import com.bigdata.service.ShardService;
import com.bigdata.util.Util;
import com.bigdata.util.config.LogUtil;
import com.bigdata.zookeeper.ZooKeeperAccessor;

import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.bigdata.service.IFederationCallable;//BTM - not needed?

// Make package protected in the future when possible
public 
class EmbeddedShardLocator implements ShardLocator,
                                      ShardManagement,
                                      Service,
                                      ISession,
                                      IServiceShutdown
{
    public static Logger logger =
        LogUtil.getLog4jLogger((EmbeddedShardLocator.class).getName());

    /**
     * Error message when a request is made to register a scale-out index but
     * delete markers are not enabled for that index.
     */
    protected static final String ERR_DELETE_MARKERS =
                                      "Delete markers not enabled";
    
    /**
     * Return the name of the metadata index.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The name of the corresponding {@link MetadataIndex} that is used
     *         to manage the partitions in the named scale-out index.
     * 
     * @see Util#getIndexPartitionName(String, int)
     */
    public static String getMetadataIndexName(String name) {
        return METADATA_INDEX_NAMESPACE + name;
    }
    
    /**
     * The namespace for the metadata indices.
     */
    public static final String METADATA_INDEX_NAMESPACE = "metadata-";

    public static interface Options extends 
                  com.bigdata.journal.Options,
                  com.bigdata.journal.ConcurrencyManager.Options,
                  com.bigdata.resources.ResourceManager.Options,
                  com.bigdata.counters.AbstractStatisticsCollector.Options,
                  com.bigdata.service.IBigdataClient.Options
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

//BTM - BEGIN - fields from AbstractFederation --------------------------------
    private ScheduledFuture<?> eventTaskFuture;
//BTM - END   - fields from AbstractFederation --------------------------------

    /**
     * @param properties
     */
    protected EmbeddedShardLocator
                  (final UUID serviceUUID,
                   final String hostname,
                   final ServiceDiscoveryManager sdm,
                   final TransactionService embeddedTxnService,
                   final LoadBalancer embeddedLoadBalancer,
                   final Map<UUID, ShardService> embeddedDataServiceMap,
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
                   Properties properties)
    {
        this.zkAccessor = zookeeperAccessor;
        this.zkAcl = zookeeperAcl;
        this.zkRoot = zookeeperRoot;
        this.properties = (Properties) properties.clone();

        (this.properties).setProperty
            ( AbstractStatisticsCollector.Options.PROCESS_NAME,
              "service" + ICounterSet.pathSeparator + SERVICE_TYPE
              + ICounterSet.pathSeparator + serviceUUID.toString() );

        this.discoveryMgr = 
            new BigdataDiscoveryManager(sdm,
                                        embeddedTxnService,
                                        embeddedLoadBalancer,
                                        this, //embeddedShardLocator
                                        embeddedDataServiceMap,
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

System.out.println("\nEmbeddedShardLocator >>> NEW StoreManager - BEGIN");
        this.resourceMgr = 
            new MdsResourceManager(this.discoveryMgr,
                                   this.localResources,
                                   this.indexMgr,
                                   this.properties);
System.out.println("\nEmbeddedShardLocator >>> NEW StoreManager - END");

        this.localTransactionMgr = new LocalTransactionManager(discoveryMgr);
        this.concurrencyMgr = 
            new ConcurrencyManager(this.properties,
                                   this.localTransactionMgr,
                                   this.resourceMgr);
        //WARN: circular refs
        (this.resourceMgr).setConcurrencyManager(this.concurrencyMgr);
        (this.indexMgr).setConcurrencyManager(this.concurrencyMgr);

//BTM - from AbstractFederation constructor and addScheduledTask

        //start event queue/sender task (sends events every 2 secs)

        long sendEventsDelay = 100L;//one-time initial delay
        long sendEventsPeriod = 2000L;
        this.eventTaskFuture = 
            (localResources.getScheduledExecutor()).scheduleWithFixedDelay
                                              (localResources.getEventQueueSender(),
                                               sendEventsDelay,
                                               sendEventsPeriod,
                                               TimeUnit.MILLISECONDS);
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
logger.warn("ZZZZZ SHARD LOCATOR EmbeddedShardLocator.shutdown");
        if (!isOpen()) return;

        if (concurrencyMgr != null) {
            concurrencyMgr.shutdown();
        }
        if (localTransactionMgr != null) {
            localTransactionMgr.shutdown();
        }
        if (resourceMgr != null) {
            resourceMgr.shutdown();
        }

        //false ==> allow in-progress tasks to complete
        eventTaskFuture.cancel(false);

        if (indexMgr != null) indexMgr.destroy();
        if (localResources != null) {
            localResources.terminate(EXECUTOR_TERMINATION_TIMEOUT);
        }
    }

    synchronized public void shutdownNow() {
        shutdown();
    }

    // Required by the ShardManagement interface

    public IndexMetadata getIndexMetadata(String name, long timestamp)
            throws IOException, InterruptedException, ExecutionException {
logger.warn("\n*** EmbeddedShardLocator.getIndexMetata: name="+name+", timestamp="+timestamp+"\n");
        setupLoggingContext();
        try {
            // Choose READ_COMMITTED iff UNISOLATED was requested.
            final long startTime = (timestamp == ITx.UNISOLATED
                    ? ITx.READ_COMMITTED
                    : timestamp);
            final AbstractTask task = 
                      new GetIndexMetadataTask(concurrencyMgr, startTime, name);
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
                  throws IOException, InterruptedException, ExecutionException
    {
logger.warn("\n*** EmbeddedShardLocator.rangeIterator: tx="+tx+", name="+name+", capacity="+capacity+", flags="+flags+"\n");
        setupLoggingContext();
        try {

            if (name == null) {
                throw new IllegalArgumentException("null name");
            }

            final boolean readOnly = 
              (    (flags & IRangeQuery.READONLY) != 0)
                || (filter == null && ((flags & IRangeQuery.REMOVEALL) == 0) );

            long timestamp = tx;
            if (timestamp == ITx.UNISOLATED && readOnly) {

                 // If the iterator is readOnly then READ_COMMITTED has
                 // the same semantics as UNISOLATED and provides better
                 // concurrency since it reduces contention for the
                 // writeService.
                timestamp = ITx.READ_COMMITTED;
            }
            final RangeIteratorTask task = 
                      new RangeIteratorTask(concurrencyMgr, timestamp,
                                            name, fromKey, toKey, capacity,
                                            flags, filter);

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
            if (task instanceof IFederationCallable) {
//BTM           ((IFederationCallable) task).setFederation(getFederation());
                throw new UnsupportedOperationException
                              ("EmbeddedShardLocator.submit [1-arg]: "
                               +"IFederationCallable task type");
            }
            if (task instanceof IDataServiceCallable) {
//BTM                ((IDataServiceCallable) task).setDataService(this);
                throw new UnsupportedOperationException
                              ("EmbeddedShardLocator.submit [1-arg]: "
                               +"IDataServiceCallable task type");
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

//BTM - PRE_FRED_3481            if (task instanceof IFederationCallable) {
//BTM - PRE_FRED_3481//BTM           ((IFederationCallable) task).setFederation(getFederation());
//BTM - PRE_FRED_3481                throw new UnsupportedOperationException
//BTM - PRE_FRED_3481                              ("EmbeddedShardLocator.submit [3-args]: "
//BTM - PRE_FRED_3481                               +"IFederationCallable task type");
//BTM - PRE_FRED_3481            }
//BTM - PRE_FRED_3481            if (task instanceof IDataServiceCallable) {
//BTM - PRE_FRED_3481//BTM                ((IDataServiceCallable) task).setDataService(this);
//BTM - PRE_FRED_3481                throw new UnsupportedOperationException
//BTM - PRE_FRED_3481                              ("EmbeddedShardLocator.submit [3-args]: "
//BTM - PRE_FRED_3481                               +"IDataServiceCallable task type");
//BTM - PRE_FRED_3481            }
            
            // submit the procedure and await its completion.
            Future retFuture = concurrencyMgr.submit(task);
            return retFuture;
        } finally {
            
            clearLoggingContext();
            
        }
    }

    public boolean purgeOldResources(final long timeout,
                                     final boolean truncateJournal)
                   throws InterruptedException
    {
        // delegate all the work.
        return resourceMgr.purgeOldResources(timeout, truncateJournal);
    }

    // Required by ShardLocator interface

    public int nextPartitionId(String name)
                  throws IOException, InterruptedException, ExecutionException
    {
        setupLoggingContext();
        try {
            final AbstractTask task = 
                new NextPartitionIdTask
                    (concurrencyMgr, getMetadataIndexName(name));
            
            final Integer partitionId = 
                          (Integer) concurrencyMgr.submit(task).get();
        
            if (logger.isInfoEnabled())
                logger.info("Assigned partitionId=" + partitionId + ", name="
                        + name);
            
            return partitionId.intValue();
            
        } finally {
            clearLoggingContext();
        }        
    }
    
    public PartitionLocator get(String name, long timestamp, final byte[] key)
            throws InterruptedException, ExecutionException, IOException
    {
        setupLoggingContext();
        try {
            if (timestamp == ITx.UNISOLATED) {
                 // This is a read-only operation so run as read
                 // committed rather than unisolated.
                timestamp = ITx.READ_COMMITTED;
            }
            final AbstractTask task = 
                  new GetTask(concurrencyMgr,
                              timestamp,
                              getMetadataIndexName(name),
                              key);
            return (PartitionLocator) concurrencyMgr.submit(task).get();
        } finally {
            clearLoggingContext();
        }        
    }

    public PartitionLocator find(String name, long timestamp, final byte[] key)
            throws InterruptedException, ExecutionException, IOException
    {
        setupLoggingContext();
        try {
            if (timestamp == ITx.UNISOLATED) {
                // This is a read-only operation so run as read
                //committed rather than unisolated./
                timestamp = ITx.READ_COMMITTED;
            }
            final AbstractTask task = 
                  new FindTask(concurrencyMgr,
                               timestamp,
                               getMetadataIndexName(name),
                               key);
            return (PartitionLocator) concurrencyMgr.submit(task).get();
        } finally {
            clearLoggingContext();
        }
    }

    public void splitIndexPartition(String name,
                                    PartitionLocator oldLocator,
                                    PartitionLocator newLocators[])
                throws IOException, InterruptedException, ExecutionException
    {
        setupLoggingContext();
        try {
            final AbstractTask task = 
                  new SplitIndexPartitionTask(concurrencyMgr,
                                              getMetadataIndexName(name),
                                              oldLocator,
                                              newLocators);
            concurrencyMgr.submit(task).get();
        } finally {
            clearLoggingContext();
        }        
    }
    
    public void joinIndexPartition(String name,
                                   PartitionLocator[] oldLocators,
                                   PartitionLocator newLocator)
                throws IOException, InterruptedException, ExecutionException
    {
        setupLoggingContext();
        try {
            final AbstractTask task =
                  new JoinIndexPartitionTask(concurrencyMgr,
                                             getMetadataIndexName(name),
                                             oldLocators,
                                             newLocator);
            concurrencyMgr.submit(task).get();
        } finally {
            clearLoggingContext();
        }        
    }
    
    public void moveIndexPartition(String name,
                                   PartitionLocator oldLocator,
                                   PartitionLocator newLocator)
                throws IOException, InterruptedException, ExecutionException
    {
        setupLoggingContext();
        try {
            final AbstractTask task =
                  new MoveIndexPartitionTask(concurrencyMgr,
                                             getMetadataIndexName(name),
                                             oldLocator,
                                             newLocator);
            concurrencyMgr.submit(task).get();
        } finally {
            clearLoggingContext();
        }        
    }
    
    /**
     * @todo if if exits already? (and has consistent/inconsistent metadata)?
     */
    public UUID registerScaleOutIndex(final IndexMetadata metadata,
                                      final byte[][] separatorKeys,
                                      final UUID[] dataServices)
            throws IOException, InterruptedException, ExecutionException
    {
        setupLoggingContext();
        try {
            if (metadata.getName() == null) {
                throw new IllegalArgumentException
                          ("no name assigned to index in metadata template");
            }
            if(!metadata.getDeleteMarkers()) {
                metadata.setDeleteMarkers(true);
                if (logger.isInfoEnabled()) {
                    logger.info("Enabling delete markers: "
                                 +metadata.getName());
                }
            }

            final String scaleOutIndexName = metadata.getName();

            // Note: We need this in order to assert a lock on this resource!
            final String metadataIndexName = 
                             EmbeddedShardLocator.getMetadataIndexName
                                                      (scaleOutIndexName);
            final AbstractTask task = 
                new RegisterScaleOutIndexTask(discoveryMgr,
                                              concurrencyMgr,
                                              resourceMgr,
                                              metadataIndexName,
                                              metadata,
                                              separatorKeys,
                                              dataServices);
            
            final UUID managedIndexUUID = 
                           (UUID) concurrencyMgr.submit(task).get();
            return managedIndexUUID;
        } finally {
            clearLoggingContext();
        }
    }

    public void dropScaleOutIndex(final String name)
                throws IOException, InterruptedException, ExecutionException
    {
        setupLoggingContext();
        try {
            final AbstractTask task = 
                new DropScaleOutIndexTask(discoveryMgr,
                                          concurrencyMgr,
                                          getMetadataIndexName(name));
            concurrencyMgr.submit(task).get();
        } finally {
            clearLoggingContext();
        }
    }

    // public method(s) needed from MetadataService/DataService class

    synchronized public void destroy() {
logger.warn("\nZZZZZ SHARD LOCATOR EmbeddedShardLocator.destroy PRE-delete >>> resourceMgr.isOpen() = "+resourceMgr.isOpen()+"\n");
        resourceMgr.shutdownNow();//sets isOpen to false
        try {
            resourceMgr.deleteResources();
        } catch(Throwable t) { 
            logger.warn("exception on resourceMgr.deleteResources\n", t);
        }
logger.warn("\nZZZZZ SHARD LOCATOR EmbeddedShardLocator.destroy POST-delete >>> resourceMgr.isOpen() = "+resourceMgr.isOpen()+"\n");
        shutdownNow();
    }

    // NOTE: the following method is needed for embedded testing.
    //       The original MetadataService implementation class 
    //       provides this method and EmbeddedFederation assumes
    //       that method exists on the embedded ref.

    public ResourceManager getResourceManager() {
        return resourceMgr;
    }

    // Private methods

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

    // Nested classes/tasks

    /**
     * Task for {@link ShardLocator#get(String, long, byte[])}.
     */
    static private final class GetTask extends AbstractTask {
        private final byte[] key;

        public GetTask(IConcurrencyManager concurrencyManager,
                       long timestamp,
                       String resource,
                       byte[] key)
        {
            super(concurrencyManager, timestamp, resource);
            this.key = key;
        }

        @Override
        protected Object doTask() throws Exception {
            MetadataIndex ndx = (MetadataIndex) getIndex(getOnlyResource());
            return ndx.get(key);
        }
    }

    /**
     * Task for {@link ShardLocator#find(String, long, byte[])}.
     */
    static private final class FindTask extends AbstractTask {

        private final byte[] key;

        public FindTask(IConcurrencyManager concurrencyManager,
                        long timestamp,
                        String resource,
                        byte[] key)
        {
            super(concurrencyManager, timestamp, resource);
            this.key = key;
        }

        @Override
        protected Object doTask() throws Exception {
            MetadataIndex ndx = (MetadataIndex) getIndex(getOnlyResource());
            return ndx.find(key);
        }
    }

    /**
     * Task assigns the next partition identifier for a registered scale-out
     * index in a restart-safe manner.
     */
    static protected class NextPartitionIdTask extends AbstractTask {

        /**
         * @param concurrencyManager
         * @param resource
         */
        protected NextPartitionIdTask(IConcurrencyManager concurrencyManager,
                                      String resource)
        {
            super(concurrencyManager, ITx.UNISOLATED, resource);
        }

        /**
         * @return The next partition identifier as an {@link Integer}.
         */
        @Override
        protected Object doTask() throws Exception {

            final MetadataIndex ndx =
                      (MetadataIndex)getIndex(getOnlyResource());
            final int partitionId = ndx.incrementAndGetNextPartitionId();
            assert ndx.needsCheckpoint();
//            final int counter = (int) ndx.getCounter().incrementAndGet();
            return partitionId;
        }
    }
    
    /**
     * Atomic operation removes the pre-existing entry for specified index
     * partition and replaces it with N new entries giving the locators for the
     * N new index partitions created when that index partition was split.
     */
    static protected class SplitIndexPartitionTask extends AbstractTask {

        protected final PartitionLocator oldLocator;
        protected final PartitionLocator newLocators[];
        
        /**
         * @param concurrencyManager
         * @param resource
         * @param oldLocator
         * @param newLocators
         */
        protected SplitIndexPartitionTask(
                IConcurrencyManager concurrencyManager, String resource,
                PartitionLocator oldLocator,
                PartitionLocator newLocators[]) {

            super(concurrencyManager, ITx.UNISOLATED, resource);

            if (oldLocator == null) {
                throw new NullPointerException("null oldLocator");
            }
            if (newLocators == null) {
                throw new NullPointerException("null newLocators");
            }
            this.oldLocator = oldLocator;
            this.newLocators = newLocators;
        }

        @Override
        protected Object doTask() throws Exception {

            if (logger.isInfoEnabled()) {
                logger.info("name=" + getOnlyResource() + ", oldLocator="
                        + oldLocator + ", locators="
                        + Arrays.toString(newLocators));
            }
            
            final MetadataIndex mdi = 
                      (MetadataIndex)getIndex(getOnlyResource());
            
            final PartitionLocator pmd =
                  (PartitionLocator) SerializerUtil.deserialize
                      (mdi.remove(oldLocator.getLeftSeparatorKey()));
            
            if (pmd == null) {
                throw new RuntimeException("null pmd: no such locator "
                                           +"[name="+getOnlyResource()
                                           +", locator="+oldLocator+"]");
            }
            
            if(!oldLocator.equals(pmd)) {

                // Sanity check failed - old locator not equal to the locator
                //found under that key in the metadata index.

                throw new RuntimeException("oldLocator not equal to pmd: " 
                                           +"expected different locator "
                                           +"[name="+getOnlyResource()
                                           +", oldLocator="+oldLocator
                                           +", actual pmd="+pmd+"]");
            }

            final byte[] leftSeparator = oldLocator.getLeftSeparatorKey();

            // Sanity check the first locator. It's leftSeparator MUST be the
            // leftSeparator of the index partition that was split.

            if ( !BytesUtil.bytesEqual
                      (leftSeparator,newLocators[0].getLeftSeparatorKey()))
            {
                throw new RuntimeException
                              ("locators[0].leftSeparator does not agree.");
            }

            // Sanity check the last locator. It's rightSeparator MUST be the
            // rightSeparator of the index partition that was split.  For the
            // last index partition, the right separator is always null.

            {//begin block

                final int indexOf = mdi.indexOf(leftSeparator);
                byte[] rightSeparator;
                try {
                    // The key for the next index partition.
                    rightSeparator = mdi.keyAt(indexOf + 1);
                } catch (IndexOutOfBoundsException ex) {
                    // The rightSeparator for the last index partition is null.
                    rightSeparator = null;
                }

                final PartitionLocator locator =
                                           newLocators[newLocators.length - 1];
                if (rightSeparator == null) {
                    if (locator.getRightSeparatorKey() != null) {
                        throw new RuntimeException
                                      ("locators["+newLocators.length
                                       +"].rightSeparator should be null.");
                    }
                } else {
                    if ( !BytesUtil.bytesEqual(rightSeparator,
                                               locator.getRightSeparatorKey()))
                    {
                        throw new RuntimeException
                                      ("locators["+newLocators.length
                                       +"].rightSeparator does not agree.");
                    }
                }
            }//end block

            // Sanity check the partition identifers. They must be distinct from
            // one another and distinct from the old partition identifier.
            for(int i=0; i<newLocators.length; i++) {
                
                PartitionLocator tmp = newLocators[i];
                if (tmp.getPartitionId() == oldLocator.getPartitionId()) {
                    throw new RuntimeException
                                  ("same partition identifier [tmp="
                                   +tmp+", oldLocator="+oldLocator+"]");
                }

                for (int j = i + 1; j < newLocators.length; j++) {
                    if  (tmp.getPartitionId() ==
                             newLocators[j].getPartitionId() )
                    {
                        throw new RuntimeException
                                      ("same partition identifier [tmp="
                                       +tmp+", newLocators["+j+"]="
                                       +newLocators[j]+"]");
                    }
                }
            }

            for(int i=0; i<newLocators.length; i++) {
                PartitionLocator locator = newLocators[i];
                
//                PartitionLocator tmp = new PartitionLocator(
//                        locator.getPartitionId(),
//                        locator.getDataServices()
//                );

                mdi.insert(locator.getLeftSeparatorKey(),
                           SerializerUtil.serialize(locator));
            }
            return null;
        }
    }

    /**
     * Updates the {@link MetadataIndex} to reflect the join of 2 or more index
     * partitions.
     */
    static protected class JoinIndexPartitionTask extends AbstractTask {

        protected final PartitionLocator oldLocators[];
        protected final PartitionLocator newLocator;
        
        /**
         * @param concurrencyManager
         * @param resource
         * @param oldLocators
         * @param newLocator
         */
        protected JoinIndexPartitionTask(IConcurrencyManager concurrencyManager,
                                         String resource,
                                         PartitionLocator oldLocators[],
                                         PartitionLocator newLocator)
        {
            super(concurrencyManager, ITx.UNISOLATED, resource);

            if (oldLocators == null) {
                throw new NullPointerException("null oldLocators");
            }
            if (newLocator == null) {
                throw new IllegalArgumentException("null newLocator");
            }
            this.oldLocators = oldLocators;
            this.newLocator = newLocator;
        }

        @Override
        protected Object doTask() throws Exception {

            if (logger.isInfoEnabled()) {
                logger.info("name=" + getOnlyResource() + ", oldLocators="
                        + Arrays.toString(oldLocators) + ", newLocator="
                        + newLocator);
            }
            
            MetadataIndex mdi = (MetadataIndex)getIndex(getOnlyResource());

            // Sanity check the partition identifers. They must be distinct from
            // one another and distinct from the old partition identifier.
            for(int i=0; i<oldLocators.length; i++) {
                
                PartitionLocator tmp = oldLocators[i];
                if (tmp.getPartitionId() == newLocator.getPartitionId()) {
                    throw new RuntimeException
                                  ("same partition identifier [tmp="
                                   +tmp+", newLocator="+newLocator+"]");
                }

                for (int j = i + 1; j < oldLocators.length; j++) {
                    if ( tmp.getPartitionId() ==
                             oldLocators[j].getPartitionId() )
                    {
                        throw new RuntimeException
                                      ("same partition identifier [tmp="
                                       +tmp+", oldLocators["+j+"]="
                                       +oldLocators[j]+"]");
                    }
                }
            }

            // remove the old locators from the metadata index.
            for(int i=0; i<oldLocators.length; i++) {
                
                PartitionLocator locator = oldLocators[i];
                PartitionLocator pmd = 
                    (PartitionLocator) SerializerUtil.deserialize
                                  (mdi.remove(locator.getLeftSeparatorKey()));

                if (!locator.equals(pmd)) {

                    // Sanity check failed - old locator not equal to the
                    // locator found under that key in the metadata index.
                    //
                    // @todo differences in just the data service failover chain
                    // are probably not important and might be ignored.

                    throw new RuntimeException
                                  ("locator not equal to pmd [expected "
                                   +"oldLocator="+locator+", actual pmd="
                                   +pmd+"]");
                }

                // FIXME validate that the newLocator is a perfect fit
                // replacement for the oldLocators in terms of the key range
                // spanned and that there are no gaps.  Add an API constaint
                // that the oldLocators are in key order by their leftSeparator
                // key.
            }

            // add the new locator to the metadata index.
            mdi.insert(newLocator.getLeftSeparatorKey(),
                       SerializerUtil.serialize(newLocator));

            return null;
        }
    }

    /**
     * Updates the {@link MetadataIndex} to reflect the move of an index
     * partition.
     */
    static protected class MoveIndexPartitionTask extends AbstractTask {

        protected final PartitionLocator oldLocator;
        protected final PartitionLocator newLocator;
        
        /**
         * @param concurrencyManager
         * @param resource
         * @param oldLocator
         * @param newLocator
         */
        protected MoveIndexPartitionTask(IConcurrencyManager concurrencyManager,
                                         String resource,
                                         PartitionLocator oldLocator,
                                         PartitionLocator newLocator)
        {

            super(concurrencyManager, ITx.UNISOLATED, resource);

            if (oldLocator == null) {
                throw new NullPointerException("null oldLocator");
            }
            if (newLocator == null) {
                throw new NullPointerException("null newLocator");
            }
            this.oldLocator = oldLocator;
            this.newLocator = newLocator;
        }

        @Override
        protected Object doTask() throws Exception {

            if (logger.isInfoEnabled()) {
                logger.info("name=" + getOnlyResource() + ", oldLocator="
                        + oldLocator + ", newLocator=" + newLocator);
            }

            final MetadataIndex mdi =
                      (MetadataIndex) getIndex(getOnlyResource());

            // remove the old locators from the metadata index.
            final PartitionLocator pmd =
                      (PartitionLocator) SerializerUtil.deserialize
                              (mdi.remove(oldLocator.getLeftSeparatorKey()));

            if (pmd == null) {
                throw new RuntimeException
                              ("no such locator [name="+getOnlyResource()
                               +", locator="+oldLocator+"]");
            }

            if (!oldLocator.equals(pmd)) {

                // Sanity check failed - old locator not equal to the locator
                // found under that key in the metadata index.
                // 
                // @todo differences in just the data service failover chain are
                // probably not important and might be ignored.

                throw new RuntimeException
                              ("oldLocator not equal to pmd "
                               +"[expected oldLocator="+oldLocator
                               +", actual pmd="+pmd+"]");
            }

            // FIXME validate that the newLocator is a perfect fit replacement
            // for the oldLocators in terms of the key range spanned and that
            // there are no gaps. Add an API constaint that the oldLocators are
            // in key order by their leftSeparator key.

            // add the new locator to the metadata index.
            mdi.insert(newLocator.getLeftSeparatorKey(),
                       SerializerUtil.serialize(newLocator));

            return null;
        }
    }

    /**
     * Registers a metadata index for a named scale-out index and statically
     * partition the index using the given separator keys and data services.
     * 
     * @todo this does not attempt to handle errors on data services when
     *       attempting to register the index partitions. it should failover
     *       rather than just dying.
     * 
     * @todo an error during execution can result in the task aborting but any
     *       registered index partitions will already exist on the various data
     *       servers. that will make it impossible to re-register the scale-out
     *       index until those index partitions have been cleaned up, which is
     *       a more than insignificant pain (they could be cleaned up by a
     *       bottom-up index rebuild followed by dropping the rebuilt index).
     */
    static protected class RegisterScaleOutIndexTask extends AbstractTask {

        final private String scaleOutIndexName;// name of the scale-out index
        final private IndexMetadata metadata;//metadata template for the index
        final private int npartitions;//# of index partitions to create
        final private byte[][] separatorKeys;

        // service UUIDs of the data services on which to create
        // thee index partitions.
        final private UUID[] dataServiceUUIDs;

        // data services on which to create the index partitions. */
        final private ShardService[] dataServices;
        
        /**
         * Create and statically partition a scale-out index.
         * 
         * @param metadataIndexName
         *            The name of the metadata index (the resource on which the
         *            task must have a lock).
         * @param separatorKeys
         *            The array of separator keys. Each separator key is
         *            interpreted as an <em>unsigned byte[]</em>. The first
         *            entry MUST be an empty byte[]. The entries MUST be in
         *            sorted order.
         * @param dataServiceUUIDs
         *            The array of data services onto which each partition
         *            defined by a separator key will be mapped (optional). The
         *            #of entries in this array MUST agree with the #of entries
         *            in the <i>separatorKeys</i> array. When <code>null</code>
         *            the index paritions will be auto-assigned to data
         *            services.
         */
        public RegisterScaleOutIndexTask
                   (final IBigdataDiscoveryManagement discoveryManager,
                    final ConcurrencyManager concurrencyManager,
                    final IResourceManager resourceManager,
                    final String metadataIndexName,
                    final IndexMetadata metadata,
                    final byte[][] separatorKeys,
                    UUID[] dataServiceUUIDs)
        {
            super(concurrencyManager, ITx.UNISOLATED, metadataIndexName);

            if (discoveryManager == null) {
                throw new NullPointerException("null discoveryManager");
            }
            if (metadata == null) {
                throw new NullPointerException("null metadata");
            }
            if (separatorKeys == null) {
                throw new NullPointerException("null separatorKeys");
            }
            if (separatorKeys.length == 0) {
                throw new IllegalArgumentException("0 separatorKeys");
            }
            if (dataServiceUUIDs != null) {
                if (dataServiceUUIDs.length == 0) {
                    throw new IllegalArgumentException("0 dataServiceUUIDs");
                }
                if (separatorKeys.length != dataServiceUUIDs.length) {
                    throw new IllegalArgumentException
                                  ("# of separatorKeys != # of "
                                   +"dataServiceUUIDs ["+separatorKeys.length
                                   +" != "+dataServiceUUIDs.length+"]");
                }
            } else {//auto-assign index partitions to data services.
                try {
                    // discover under-utilized data service UUIDs.
                    LoadBalancer loadBalancer = 
                        discoveryManager.getLoadBalancerService();
                    if(loadBalancer != null) {
                        dataServiceUUIDs = 
                            loadBalancer.getUnderUtilizedDataServices
                                (separatorKeys.length, // minCount
                                 separatorKeys.length, // maxCount
                                 null );// exclude
                    }
                } catch(Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            
            this.scaleOutIndexName = metadata.getName();
            this.metadata = metadata;
            this.npartitions = separatorKeys.length;
            this.separatorKeys = separatorKeys;
            this.dataServiceUUIDs = dataServiceUUIDs;

            this.dataServices = new ShardService[dataServiceUUIDs.length];

            if( separatorKeys[0] == null ) {
                throw new NullPointerException
                              ("null separatorKey at index 0");
            }
            if (separatorKeys[0].length != 0) {
                throw new IllegalArgumentException
                              ("length of separatorKey at index 0 != 0 "
                               +"["+separatorKeys[0].length+"] - "
                               +"first separatorKey must be empty byte[]");
            }

            for (int i = 0; i < npartitions; i++) {
                final byte[] separatorKey = separatorKeys[i];
                if (separatorKey == null) {
                    throw new NullPointerException("null separatorKey["+i+"]");
                }
                if (i > 0) {
                    if (BytesUtil.compareBytes
                                      (separatorKey, separatorKeys[i-1])<0)
                    {   
                        throw new IllegalArgumentException
                                    ("separator keys out of order [index="+i);
                    }
                }

                final UUID uuid = dataServiceUUIDs[i];
                if (uuid == null) {
                    throw new IllegalArgumentException
                                  ("null uuid dataServiceUUIDs["+i+"]");
                }

                final ShardService dataService =
                                       discoveryManager.getDataService(uuid);
                if (dataService == null) {
                    throw new IllegalArgumentException
                                  ("unknown data service [uuid="+uuid+"]");
                }
                dataServices[i] = dataService;
            }
        }

        /**
         * Create and statically partition the scale-out index.
         * 
         * @return The UUID assigned to the managed index.
         */
        protected Object doTask() throws Exception {
            
            // the name of the metadata index itself.
            final String metadataName = getOnlyResource();
            
            // make sure there is no metadata index for that btree.
            try {
                getIndex(metadataName);
                throw new IndexExistsException(metadataName);
            } catch(NoSuchIndexException ex) {
                // ignore EXPECTED exception
            }

            // Note: there are two UUIDs here - the UUID for the metadata index
            // describing the partitions of the named scale-out index and the
            // UUID of the named scale-out index. The metadata index UUID MUST
            // be used by all B+Tree objects having data for the metadata index
            // (its mutable btrees on journals and its index segments), while
            // the managed named index UUID MUST be used by all B+Tree objects
            // having data for the named index (its mutable btrees on journals
            // and its index segments).
            
            final UUID metadataIndexUUID = UUID.randomUUID();

            // Create the metadata index.
            final MetadataIndex mdi =
                      MetadataIndex.create
                          (getJournal(), metadataIndexUUID, metadata);

            // Map the partitions onto the data services.
            final PartitionLocator[] partitions =
                      new PartitionLocator[npartitions];
            
            for (int i = 0; i < npartitions; i++) {
                final byte[] leftSeparator = separatorKeys[i];
                final byte[] rightSeparator =
                             i + 1 < npartitions ? separatorKeys[i + 1]
                                                 : null;

                final PartitionLocator pmd =
                          new PartitionLocator
                                  (mdi.incrementAndGetNextPartitionId(),
                                   dataServiceUUIDs[i],
                                   leftSeparator,
                                   rightSeparator);
                
                if (logger.isInfoEnabled()) {
                    logger.info("name=" + scaleOutIndexName + ", pmd=" + pmd);
                }

                // Map the initial partition onto that data service. This
                // requires us to compute the left and right separator keys.
                // The right separator key is just the separator key for
                // the next partition in order and null iff this is the
                // last partition.

                final IndexMetadata md = metadata.clone();
                
                // override the partition metadata.
                md.setPartitionMetadata
                    (new LocalPartitionMetadata
                         (pmd.getPartitionId(),//
                          -1,//creating new index, not moving index partition
                          leftSeparator,
                          rightSeparator,
                        /*
                         * Note: By setting this to null we are indicating to
                         * the RegisterIndexTask on the data service that it
                         * needs to set the resourceMetadata[] when the index
                         * is actually registered based on the live journal
                         * as of the when the task actually executes on the
                         * data service.
                         */
                           null,//[resources] Signal to the RegisterIndexTask.
                           null //[cause] Signal to RegisterIndexTask
//                         /*
//                          * History.
//                          */
//                         ,"createScaleOutIndex(name="+scaleOutIndexName+") "
                    ));

                // The shard service (as currently implemented) may not be
                // completely initialized if it is just being started
                // when this method is called (for example, in a test
                // environment). This is because the shard service creates
                // a StoreManager (via a Resource), which depends on
                // discovering a transaction service; and it sets up 
                // counters, which depend on discovering a load balancer.
                // Thus, to address the case where the shard service is
                // not yet ready, test for such a situation; and apply a 
                // retry-to-failure strategy
                boolean registered = false;
                try {
                    dataServices[i].registerIndex
                        ( Util.getIndexPartitionName(scaleOutIndexName,
                                                     pmd.getPartitionId()),
                          md );
                    registered = true;
                } catch(Throwable t1) {
                    if ( !Util.causeNoSuchObject(t1) ) {
                        throw new Exception(t1);
                    }
                    //wait for data service to finish initializing
                    int nWait = 5;
                    for(int n=0; n<nWait; n++) {
                        Util.delayMS(1000L);
                        try {
                            dataServices[i].registerIndex
                                ( Util.getIndexPartitionName
                                      (scaleOutIndexName,
                                       pmd.getPartitionId()),
                                  md );
                            registered = true;
                            break;
                        } catch(Throwable t2) {
                            if ( !Util.causeNoSuchObject(t2) ) {
                                throw new Exception(t2);
                            }
                        }
                    }
                }
                if (!registered) {// try one last time
                    dataServices[i].registerIndex
                        ( Util.getIndexPartitionName(scaleOutIndexName,
                                                     pmd.getPartitionId()),
                          md );
                }
                partitions[i] = pmd;
            }

            // Record each partition in the metadata index.
            for (int i = 0; i < npartitions; i++) {
//                mdi.put(separatorKeys[i], partitions[i]);
                mdi.insert(separatorKeys[i],
                           SerializerUtil.serialize(partitions[i]));
            }

            // Register the metadata index with the metadata service. This
            // registration will not be restart safe until the task commits.

            getJournal().registerIndex(metadataName, mdi);

            // Done.
            return mdi.getScaleOutIndexMetadata().getIndexUUID();
        }
    }

    /**
     * Drops a scale-out index.
     * <p>
     * Since this task is unisolated, it basically has a lock on the writable
     * version of the metadata index. It drops each index partition and finally
     * drops the metadata index itself.
     * <p>
     * Historical reads against the metadata index will continue to succeed
     * both during and after this operation has completed successfully.
     * However, {@link ITx#READ_COMMITTED} operations will succeed only until
     * this operation completes at which point the scale-out index will no
     * longer be visible.
     * <p>
     * The data comprising the scale-out index will remain available for
     * historical reads until it is released by whatever policy is in effect
     * for the {@link ResourceManager}s for the shard services on which that
     * data resides.
     * 
     * @todo This does not try to handle errors gracefully. E.g., if there is a
     *       problem with one of the data services hosting an index partition
     *       it does not fail over to the next data service for that index
     *       partition.
     */
    static public class DropScaleOutIndexTask extends AbstractTask {

        private IBigdataDiscoveryManagement discoveryManager;

        /**
         * @parma discoveryManager
         * @param journal
         * @param name
         *            The name of the metadata index for some scale-out index.
         */
        protected DropScaleOutIndexTask
                      (IBigdataDiscoveryManagement discoveryManager,
                       ConcurrencyManager concurrencyManager,
                       String name)
        {
            super(concurrencyManager, ITx.UNISOLATED, name);
            if (discoveryManager == null) {
                throw new NullPointerException("null discoveryManager");
            }
            this.discoveryManager = discoveryManager;
        }

        /**
         * Drops the index partitions and then drops the metadata index as well.
         * 
         * @return The {@link Integer} #of index partitions that were dropped.
         */
        @Override
        protected Object doTask() throws Exception {

            final MetadataIndex ndx;

            try {
                ndx = (MetadataIndex) getIndex(getOnlyResource());
            } catch (ClassCastException ex) {
                throw new UnsupportedOperationException
                              ("not a scale-out index?", ex);
            }

            // name of the scale-out index.
            final String name = ndx.getScaleOutIndexMetadata().getName();
            
            if (logger.isInfoEnabled()) {
                logger.info("Will drop index partitions for " + name);
            }

//            final ChunkedLocalRangeIterator itr = new ChunkedLocalRangeIterator(
//                    ndx, null, null, 0/* capacity */, IRangeQuery.VALS, null/* filter */);
            final ITupleIterator itr = ndx.rangeIterator(null, null,
                    0/* capacity */, IRangeQuery.VALS, null/* filter */);

            int ndropped = 0;
            while(itr.hasNext()) {
                final ITuple tuple = itr.next();

                // FIXME There is still (5/30/08) a problem with using getValueStream() here!
                final PartitionLocator pmd =
                          (PartitionLocator) SerializerUtil.deserialize
                                                 (tuple.getValue());
//                .deserialize(tuple.getValueStream());

                //Drop the index partition.
                {
                    final int partitionId = pmd.getPartitionId();
                    final UUID serviceUUID = pmd.getDataServiceUUID();
                    final ShardService dataService =
                              discoveryManager.getDataService(serviceUUID);
                    if (dataService == null) {
                        logger.warn
                            ("EmbeddedShardLocator.DropScaleOutIndexTask: "
                             +"null shard service [id="+serviceUUID+", "
                             +"partition="+partitionId+"]");
                        return 0;
                    }

                    if (logger.isInfoEnabled()) {
                        logger.info("Dropping index partition: partitionId="
                                + partitionId + ", dataService=" + dataService);
                    }
                    dataService.dropIndex
                        (Util.getIndexPartitionName(name, partitionId));
                }//end drop block

                ndropped++;
            }
            
//            // flush all delete requests.
//            itr.flush();
            
            if (logger.isInfoEnabled()) {
                logger.info
                    ("Dropped " + ndropped + " index partitions for "+ name);
            }

            // drop the metadata index as well.
            getJournal().dropIndex(getOnlyResource());

            return ndropped;
        }
    }

    class MdsResourceManager extends ResourceManager {

        private IBigdataDiscoveryManagement discoveryManager;
        private ILocalResourceManagement localResources;
        private ScaleOutIndexManager indexManager;

        MdsResourceManager(IBigdataDiscoveryManagement discoveryManager,
                           ILocalResourceManagement localResources,
                           ScaleOutIndexManager indexManager,
                           Properties properties)
        {
            super(properties);
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
            throw new UnsupportedOperationException
                          ("EmbeddedShardLocator.MdsResourceManager");
        }
            
        @Override
        public UUID getDataServiceUUID() {
            throw new UnsupportedOperationException
                          ("EmbeddedShardLocator.MdsResourceManager");
        }
    }
}
