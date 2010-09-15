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

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.io.BytesUtil;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexExistsException;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.ResourceManager;

//BTM - added because of new package
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.jini.lookup.entry.ServiceUUID;
import com.bigdata.jini.start.IServicesManagerService;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.ITransactionService;//remote impl
import com.bigdata.journal.TransactionService;//smart proxy impl
import com.bigdata.journal.Tx;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IClientService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceCallable;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.IService;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardManagement;
import com.bigdata.service.ShardService;
import com.bigdata.service.Service;
import com.bigdata.util.EntryUtil;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.config.LogUtil;

//BTM - replace with ShardService when DataService is converted
import com.bigdata.service.DataService;
import com.bigdata.service.DataService.GetIndexMetadataTask;//EmbeddedShard???
import com.bigdata.service.DataService.RangeIteratorTask;//EmbeddedShard???

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;
import net.jini.lookup.entry.Name;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class EmbeddedShardLocator implements ShardLocator,
                                             ShardManagement,
                                             Service
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
     * @see DataService#getIndexPartitionName(String, int)
     */
    public static String getMetadataIndexName(String name) {
        
        return METADATA_INDEX_NAMESPACE + name;
        
    }
    
    /**
     * The namespace for the metadata indices.
     */
    public static final String METADATA_INDEX_NAMESPACE = "metadata-";

//BTM - replace with ShardService when DataService is converted
//BTM    public static interface Options extends DataService.Options {
//BTM    }
public static interface Options extends 
                  com.bigdata.journal.Options,
                  com.bigdata.journal.ConcurrencyManager.Options,
                  com.bigdata.resources.ResourceManager.Options,
                  com.bigdata.counters.AbstractStatisticsCollector.Options,
                  com.bigdata.service.IBigdataClient.Options
{
//BTM - BEGIN - added options
    String THREAD_POOL_SIZE = "threadPoolSize";
    String DEFAULT_THREAD_POOL_SIZE = "20";
//BTM - END   - added options
}

private UUID thisServiceUUID;
private String hostname;
private ServiceDiscoveryManager sdm;//for discovering txn, lbs, shard services

private LookupCache txnServiceCache;
private LookupCache remoteTxnServiceCache;

private LookupCache lbsServiceCache;
private LookupCache remoteLbsServiceCache;

private LookupCache shardCache;
private LookupCache remoteShardCache;

private ConcurrentHashMap<UUID, IDataService> remoteShardMap = 
            new ConcurrentHashMap<UUID, IDataService>();
private ConcurrentHashMap<UUID, ShardService> shardMap = 
            new ConcurrentHashMap<UUID, ShardService>();

//for embedded federation testing
private LoadBalancer embeddedLoadBalancer;
private Map<UUID, IDataService> embeddedDataServiceMap;

private Properties properties;

private ResourceManager resourceManager;
private LocalTransactionManager localTransactionManager;
private ConcurrencyManager concurrencyManager;

//BTM - BEGIN - fields from AbstractFederation --------------------------------
private final ThreadPoolExecutor threadPool;
private final ScheduledExecutorService scheduledExecutorService =
                      Executors.newSingleThreadScheduledExecutor
                          (new DaemonThreadFactory
                                   (getClass().getName()+".sampleService"));
//BTM - END   - fields from AbstractFederation --------------------------------


    /**
     * @param properties
     */
    protected EmbeddedShardLocator
                  (final UUID serviceUUID,
                   final String hostname,
                   final ServiceDiscoveryManager sdm,
                   final TransactionService embeddedTxnService,
                   final LoadBalancer embeddedLbs,
                   final Map<UUID, IDataService> embeddedDataServiceMap,
                   final int threadPoolSize,
                   Properties properties)
    {

//BTM        super(properties);
this.properties = (Properties) properties.clone();
if (serviceUUID == null) {
    throw new NullPointerException("null serviceUUID");
}   
this.thisServiceUUID = serviceUUID;

if (hostname == null) {
    throw new NullPointerException("null hostname");
}   
this.hostname = hostname;
this.sdm = sdm;
this.embeddedDataServiceMap = embeddedDataServiceMap;
this.embeddedLoadBalancer = embeddedLbs;
if(sdm != null) {

            //for smart proxy implementations of txn, lbs, shard service
            Class[] txnServiceType = 
                            new Class[] {TransactionService.class};
            ServiceTemplate txnServiceTmpl = 
                            new ServiceTemplate(null, txnServiceType, null);

            Class[] lbsServiceType = 
                            new Class[] {TransactionService.class};
            ServiceTemplate lbsServiceTmpl = 
                            new ServiceTemplate(null, lbsServiceType, null);

            Class[] shardType = new Class[] {ShardService.class};
            ServiceTemplate shardTmpl = 
                            new ServiceTemplate(null, shardType, null);
            ServiceItemFilter shardFilter = null;

            //for remote implementation of txn, lbs, shard service
            Class[] remoteTxnServiceType = 
                            new Class[] {ITransactionService.class};
            ServiceTemplate remoteTxnServiceTmpl = 
                            new ServiceTemplate
                                    (null, remoteTxnServiceType, null);

            Class[] remoteLbsServiceType = 
                            new Class[] {ITransactionService.class};
            ServiceTemplate remoteLbsServiceTmpl = 
                            new ServiceTemplate
                                    (null, remoteLbsServiceType, null);

            Class[] remoteShardType = new Class[] {IDataService.class};
            ServiceTemplate remoteShardTmpl = 
                            new ServiceTemplate(null, remoteShardType, null);
            ServiceItemFilter remoteShardFilter = new IDataServiceOnlyFilter();

            //create caches for all of the above
            try {
                //caches for smart proxy implementations of txn, lbs, shard
                this.txnServiceCache = sdm.createLookupCache
                                     ( txnServiceTmpl, 
                                       null,
                                       new CacheListener(logger) );
                this.lbsServiceCache = sdm.createLookupCache
                                     ( lbsServiceTmpl, 
                                       null,
                                       new CacheListener(logger) );
                this.shardCache = sdm.createLookupCache
                                     ( shardTmpl, 
                                       shardFilter,
                                       new CacheListener(logger) );

                //caches for remote implementations of txn, lbs, shard
                this.remoteTxnServiceCache = sdm.createLookupCache
                                     ( remoteTxnServiceTmpl,
                                       null,
                                       new CacheListener(logger) );
                this.remoteLbsServiceCache = sdm.createLookupCache
                                     ( remoteLbsServiceTmpl,
                                       null,
                                       new CacheListener(logger) );
                this.remoteShardCache = sdm.createLookupCache
                                     ( remoteShardTmpl,
                                       remoteShardFilter,
                                       new CacheListener(logger) );
            } catch(RemoteException e) {
                logger.warn(e.getMessage(), e);
            }
}

if (threadPoolSize == 0) {
            this.threadPool = 
                (ThreadPoolExecutor) Executors.newCachedThreadPool
                    (new DaemonThreadFactory
                         (getClass().getName()+".executorService"));
} else {
            this.threadPool = 
                (ThreadPoolExecutor) Executors.newFixedThreadPool
                    (threadPoolSize, 
                     new DaemonThreadFactory
                         (getClass().getName()+".executorService"));
}

        this.resourceManager = 
            new MdsResourceManager
                (new EmbeddedIndexStore(this.thisServiceUUID,
                                        this.txnServiceCache,
                                        this.remoteTxnServiceCache,
                                        embeddedTxnService),
                 properties);
        this.localTransactionManager = 
            new LocalTransactionManager(this.txnServiceCache,
                                        this.remoteTxnServiceCache,
                                        embeddedTxnService);//for embedded fed
        this.concurrencyManager = 
            new ConcurrencyManager
                    (properties, localTransactionManager, resourceManager);
        resourceManager.setConcurrencyManager(concurrencyManager);
    }

//BTM    public EmbeddedShardLocator start() {
//BTM
//BTM        return (EmbeddedShardLocator) super.start();
//BTM
//BTM    }
    


//BTM
// Required by Service interface

    public UUID getServiceUUID() {
        return thisServiceUUID;
    }

    public Class getServiceIface() {
        return SERVICE_TYPE;
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public String getHostname() {
        return this.hostname;
    }

//BTM
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
        return concurrencyManager != null && concurrencyManager.isOpen();
    }

    synchronized public void shutdown() {
logger.warn("ZZZZZ SHARD LOCATOR EmbeddedShardLocator.shutdown");
        if (!isOpen()) return;

        if (concurrencyManager != null) {
            concurrencyManager.shutdown();
        }
        if (localTransactionManager != null) {
            localTransactionManager.shutdown();
        }
        if (resourceManager != null) {
            resourceManager.shutdown();
        }
threadPool.shutdownNow();//added by BTM
    }

    synchronized public void shutdownNow() {
logger.warn("ZZZZZ SHARD LOCATOR EmbeddedShardLocator.shutdownNow");
        if (!isOpen()) return;

        if (concurrencyManager != null) {
            concurrencyManager.shutdownNow();
        }
        if (localTransactionManager != null) {
            localTransactionManager.shutdownNow();
        }
        if (resourceManager != null) {
            resourceManager.shutdownNow();
        }
threadPool.shutdownNow();//BTM - from AbstractFederation.shutdownNow
    }

//BTM public methods added to EmbeddedShardLocator by BTM (from DataService)

    synchronized public void destroy() {
logger.warn("\nZZZZZ SHARD LOCATOR EmbeddedShardLocator.destroy PRE-delete >>> resourceManager.isOpen() = "+resourceManager.isOpen()+"\n");
        resourceManager.shutdownNow();//sets isOpen to false
        try {
            resourceManager.deleteResources();
        } catch(Throwable t) { 
            logger.warn("exception on resourceManager.deleteResources\n", t);
        }
logger.warn("\nZZZZZ SHARD LOCATOR EmbeddedShardLocator.destroy POST-delete >>> resourceManager.isOpen() = "+resourceManager.isOpen()+"\n");

        final File file = getHTTPDURLFile();
        if(file.exists()) {
            file.delete();
        }
        shutdownNow();
    }

    /**
     * Returns the object used to manage the local resources.
     */
    public ResourceManager getResourceManager() {
        return resourceManager;
    }

//BTM
// Required by ShardLocator interface


    public int nextPartitionId(String name)
                  throws IOException, InterruptedException, ExecutionException
    {
        setupLoggingContext();
        try {
            final AbstractTask task = 
                new NextPartitionIdTask
                    (concurrencyManager, getMetadataIndexName(name));
            
            final Integer partitionId = 
                          (Integer) concurrencyManager.submit(task).get();
        
            if (logger.isInfoEnabled())
                logger.info("Assigned partitionId=" + partitionId + ", name="
                        + name);
            
            return partitionId.intValue();
            
        } finally {
            clearLoggingContext();
        }        
    }
    
    public PartitionLocator get(String name, long timestamp, final byte[] key)
            throws InterruptedException, ExecutionException, IOException {
    
        setupLoggingContext();

        try {

            if (timestamp == ITx.UNISOLATED) {

                /*
                 * This is a read-only operation so run as read committed rather
                 * than unisolated.
                 */
                
                timestamp = ITx.READ_COMMITTED;

            }

            final AbstractTask task = new GetTask(concurrencyManager,
                    timestamp, getMetadataIndexName(name), key);
            
            return (PartitionLocator) concurrencyManager.submit(task).get();
            
        } finally {
            
            clearLoggingContext();
            
        }        

    }

    /**
     * Task for {@link ShardLocator#get(String, long, byte[])}.
     */
    static private final class GetTask extends AbstractTask {

        private final byte[] key;
        
        public GetTask(IConcurrencyManager concurrencyManager, long timestamp,
                String resource, byte[] key) {

            super(concurrencyManager, timestamp, resource);

            this.key = key;
            
        }

        @Override
        protected Object doTask() throws Exception {

            MetadataIndex ndx = (MetadataIndex) getIndex(getOnlyResource());

            return ndx.get(key);

        }
        
    }

    public PartitionLocator find(String name, long timestamp, final byte[] key)
            throws InterruptedException, ExecutionException, IOException {

        setupLoggingContext();

        try {
            if (timestamp == ITx.UNISOLATED) {
                /*
                 * This is a read-only operation so run as read
                 * committed rather than unisolated.
                 */
                timestamp = ITx.READ_COMMITTED;
            }
            
            final AbstractTask task = new FindTask(concurrencyManager,
                    timestamp, getMetadataIndexName(name), key);
            
            return 
                (PartitionLocator) concurrencyManager.submit(task).get();

        } finally {
            clearLoggingContext();
        }
    }

    /**
     * Task for {@link ShardLocator#find(String, long, byte[])}.
     */
    static private final class FindTask extends AbstractTask {

        private final byte[] key;
        
        public FindTask(IConcurrencyManager concurrencyManager, long timestamp,
                String resource, byte[] key) {

            super(concurrencyManager, timestamp, resource);

            this.key = key;
            
        }

        @Override
        protected Object doTask() throws Exception {

            MetadataIndex ndx = (MetadataIndex) getIndex(getOnlyResource());

            return ndx.find(key);

        }
        
    }
    
    public void splitIndexPartition(String name, PartitionLocator oldLocator,
            PartitionLocator newLocators[]) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            final AbstractTask task = new SplitIndexPartitionTask(
                    concurrencyManager, getMetadataIndexName(name),
                    oldLocator, newLocators);
            
            concurrencyManager.submit(task).get();
            
        } finally {
            
            clearLoggingContext();
            
        }        
        
    }
    
    public void joinIndexPartition(String name, PartitionLocator[] oldLocators,
            PartitionLocator newLocator) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            final AbstractTask task = new JoinIndexPartitionTask(
                    concurrencyManager, getMetadataIndexName(name),
                    oldLocators, newLocator);
            
            concurrencyManager.submit(task).get();
            
        } finally {
            
            clearLoggingContext();
            
        }        
        
    }
    
    public void moveIndexPartition(String name, PartitionLocator oldLocator,
            PartitionLocator newLocator) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            final AbstractTask task = new MoveIndexPartitionTask(
                    concurrencyManager, getMetadataIndexName(name),
                    oldLocator, newLocator);
            
            concurrencyManager.submit(task).get();
            
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
                new RegisterScaleOutIndexTask(
//BTM federation,
lbsServiceCache,
remoteLbsServiceCache,
shardMap,
remoteShardMap,
embeddedLoadBalancer,
embeddedDataServiceMap,
                                              concurrencyManager,
                                              resourceManager,
                                              metadataIndexName,
                                              metadata,
                                              separatorKeys,
                                              dataServices);
            
            final UUID managedIndexUUID = 
                           (UUID) concurrencyManager.submit(task).get();
            return managedIndexUUID;
        } finally {
            clearLoggingContext();
        }
    }
    
    public void dropScaleOutIndex(final String name) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            final AbstractTask task = 
                new DropScaleOutIndexTask(
//BTM federation,
shardMap,
remoteShardMap,
embeddedDataServiceMap,
                                           concurrencyManager,
                                           getMetadataIndexName(name));
            
            concurrencyManager.submit(task).get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }
   
    /*
     * Tasks.
     */
    
    /**
     * Task assigns the next partition identifier for a registered scale-out
     * index in a restart-safe manner.
     */
    static protected class NextPartitionIdTask extends AbstractTask {

        /**
         * @param concurrencyManager
         * @param resource
         */
        protected NextPartitionIdTask(IConcurrencyManager concurrencyManager, String resource) {

            super(concurrencyManager, ITx.UNISOLATED, resource);
            
        }

        /**
         * @return The next partition identifier as an {@link Integer}.
         */
        @Override
        protected Object doTask() throws Exception {

            final MetadataIndex ndx = (MetadataIndex)getIndex(getOnlyResource());

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

            if (oldLocator == null)
                throw new IllegalArgumentException();

            if (newLocators == null)
                throw new IllegalArgumentException();

            this.oldLocator = oldLocator;
            
            this.newLocators = newLocators;
            
        }

        @Override
        protected Object doTask() throws Exception {

            if (logger.isInfoEnabled())
                logger.info("name=" + getOnlyResource() + ", oldLocator="
                        + oldLocator + ", locators="
                        + Arrays.toString(newLocators));
            
            final MetadataIndex mdi = 
                      (MetadataIndex)getIndex(getOnlyResource());
            
            final PartitionLocator pmd = (PartitionLocator) SerializerUtil
                    .deserialize(mdi.remove(oldLocator.getLeftSeparatorKey()));
            
            if (pmd == null) {

                throw new RuntimeException("No such locator: name="
                        + getOnlyResource() + ", locator=" + oldLocator);

            }
            
            if(!oldLocator.equals(pmd)) {

                /*
                 * Sanity check failed - old locator not equal to the locator
                 * found under that key in the metadata index.
                 */

                throw new RuntimeException("Expected different locator: name="
                        + getOnlyResource() + ", oldLocator=" + oldLocator
                        + ", but actual=" + pmd);
                
            }

            final byte[] leftSeparator = oldLocator.getLeftSeparatorKey();
            
            /*
             * Sanity check the first locator. It's leftSeparator MUST be the
             * leftSeparator of the index partition that was split.
             */
            if(!BytesUtil.bytesEqual(leftSeparator,newLocators[0].getLeftSeparatorKey())) {
                
                throw new RuntimeException("locators[0].leftSeparator does not agree.");
                
            }

            /*
             * Sanity check the last locator. It's rightSeparator MUST be the
             * rightSeparator of the index partition that was split.  For the
             * last index partition, the right separator is always null.
             */
            {
                
                final int indexOf = mdi.indexOf(leftSeparator);
                byte[] rightSeparator;
                try {

                    // The key for the next index partition.

                    rightSeparator = mdi.keyAt(indexOf + 1);

                } catch (IndexOutOfBoundsException ex) {

                    // The rightSeparator for the last index partition is null.

                    rightSeparator = null;

                }

                final PartitionLocator locator = newLocators[newLocators.length - 1];
                
                if (rightSeparator == null) {

                    if (locator.getRightSeparatorKey() != null) {

                        throw new RuntimeException("locators["
                                + newLocators.length
                                + "].rightSeparator should be null.");

                    }

                } else {

                    if (!BytesUtil.bytesEqual(rightSeparator, locator
                            .getRightSeparatorKey())) {

                        throw new RuntimeException("locators["
                                + newLocators.length
                                + "].rightSeparator does not agree.");

                    }

                }
                
            }

            /*
             * Sanity check the partition identifers. They must be distinct from
             * one another and distinct from the old partition identifier.
             */

            for(int i=0; i<newLocators.length; i++) {
                
                PartitionLocator tmp = newLocators[i];

                if (tmp.getPartitionId() == oldLocator.getPartitionId()) {

                    throw new RuntimeException("Same partition identifier: "
                            + tmp + ", " + oldLocator);

                }

                for (int j = i + 1; j < newLocators.length; j++) {

                    if (tmp.getPartitionId() == newLocators[j].getPartitionId()) {

                        throw new RuntimeException(
                                "Same partition identifier: " + tmp + ", "
                                        + newLocators[j]);

                    }

                }
                    
            }

            for(int i=0; i<newLocators.length; i++) {
                
                PartitionLocator locator = newLocators[i];
                
//                PartitionLocator tmp = new PartitionLocator(
//                        locator.getPartitionId(),
//                        locator.getDataServices()
//                );

                mdi.insert(locator.getLeftSeparatorKey(), SerializerUtil
                        .serialize(locator));
                
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
        protected JoinIndexPartitionTask(
                IConcurrencyManager concurrencyManager, String resource,
                PartitionLocator oldLocators[],
                PartitionLocator newLocator) {

            super(concurrencyManager, ITx.UNISOLATED, resource);

            if (oldLocators == null)
                throw new IllegalArgumentException();

            if (newLocator == null)
                throw new IllegalArgumentException();

            this.oldLocators = oldLocators;
            
            this.newLocator = newLocator;
            
        }

        @Override
        protected Object doTask() throws Exception {

            if (logger.isInfoEnabled())
                logger.info("name=" + getOnlyResource() + ", oldLocators="
                        + Arrays.toString(oldLocators) + ", newLocator="
                        + newLocator);
            
            MetadataIndex mdi = (MetadataIndex)getIndex(getOnlyResource());

            /*
             * Sanity check the partition identifers. They must be distinct from
             * one another and distinct from the old partition identifier.
             */

            for(int i=0; i<oldLocators.length; i++) {
                
                PartitionLocator tmp = oldLocators[i];

                if (tmp.getPartitionId() == newLocator.getPartitionId()) {

                    throw new RuntimeException("Same partition identifier: "
                            + tmp + ", " + newLocator);

                }

                for (int j = i + 1; j < oldLocators.length; j++) {

                    if (tmp.getPartitionId() == oldLocators[j].getPartitionId()) {

                        throw new RuntimeException(
                                "Same partition identifier: " + tmp + ", "
                                        + oldLocators[j]);

                    }

                }
                    
            }

            // remove the old locators from the metadata index.
            for(int i=0; i<oldLocators.length; i++) {
                
                PartitionLocator locator = oldLocators[i];
                
                PartitionLocator pmd = (PartitionLocator) SerializerUtil
                        .deserialize(mdi.remove(locator.getLeftSeparatorKey()));

                if (!locator.equals(pmd)) {

                    /*
                     * Sanity check failed - old locator not equal to the
                     * locator found under that key in the metadata index.
                     * 
                     * @todo differences in just the data service failover chain
                     * are probably not important and might be ignored.
                     */

                    throw new RuntimeException("Expected oldLocator=" + locator
                            + ", but actual=" + pmd);
                    
                }

                /*
                 * FIXME validate that the newLocator is a perfect fit
                 * replacement for the oldLocators in terms of the key range
                 * spanned and that there are no gaps.  Add an API constaint
                 * that the oldLocators are in key order by their leftSeparator
                 * key.
                 */
                
            }

            // add the new locator to the metadata index.
            mdi.insert(newLocator.getLeftSeparatorKey(), SerializerUtil
                    .serialize(newLocator));
            
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
        protected MoveIndexPartitionTask(
                IConcurrencyManager concurrencyManager, String resource,
                PartitionLocator oldLocator,
                PartitionLocator newLocator) {

            super(concurrencyManager, ITx.UNISOLATED, resource);

            if (oldLocator == null)
                throw new IllegalArgumentException();

            if (newLocator == null)
                throw new IllegalArgumentException();

            this.oldLocator = oldLocator;
            
            this.newLocator = newLocator;
            
        }

        @Override
        protected Object doTask() throws Exception {

            if (logger.isInfoEnabled())
                logger.info("name=" + getOnlyResource() + ", oldLocator="
                        + oldLocator + ", newLocator=" + newLocator);

            final MetadataIndex mdi = (MetadataIndex) getIndex(getOnlyResource());

            // remove the old locators from the metadata index.
            final PartitionLocator pmd = (PartitionLocator) SerializerUtil
                    .deserialize(mdi.remove(oldLocator.getLeftSeparatorKey()));


            if (pmd == null) {

                throw new RuntimeException("No such locator: name="
                        + getOnlyResource() + ", locator=" + oldLocator);

            }
            
            if (!oldLocator.equals(pmd)) {

                /*
                 * Sanity check failed - old locator not equal to the locator
                 * found under that key in the metadata index.
                 * 
                 * @todo differences in just the data service failover chain are
                 * probably not important and might be ignored.
                 */

                throw new RuntimeException("Expected oldLocator=" + oldLocator
                        + ", but actual=" + pmd);

            }

            /*
             * FIXME validate that the newLocator is a perfect fit replacement
             * for the oldLocators in terms of the key range spanned and that
             * there are no gaps. Add an API constaint that the oldLocators are
             * in key order by their leftSeparator key.
             */

            // add the new locator to the metadata index.
            mdi.insert(newLocator.getLeftSeparatorKey(), SerializerUtil
                    .serialize(newLocator));

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

        /** The federation. */
//BTM        final private IBigdataFederation fed;
final Map<UUID, IDataService> embeddedDataServiceMap;
        /** The name of the scale-out index. */
        final private String scaleOutIndexName;
        /** The metadata template for the scale-out index. */
        final private IndexMetadata metadata;
        /** The #of index partitions to create. */
        final private int npartitions;
        /** The separator keys for those index partitions. */
        final private byte[][] separatorKeys;
        /** The service UUIDs of the data services on which to create
         *  those index partitions. */
        final private UUID[] dataServiceUUIDs;
        /** The data services on which to create those index partitions. */
//BTM - replace with ShardService when DataService is converted
        final private IDataService[] dataServices;
        
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
        public RegisterScaleOutIndexTask(
//BTM                final IBigdataFederation fed,
LookupCache lbsCache,
LookupCache remoteLbsCache,
Map<UUID, ShardService> shardMap,
Map<UUID, IDataService> remoteShardMap,
LoadBalancer embeddedLoadBalancer,
Map<UUID, IDataService> embeddedDataServiceMap,
                final ConcurrencyManager concurrencyManager,
                final IResourceManager resourceManager,
                final String metadataIndexName,
                final IndexMetadata metadata,
                final byte[][] separatorKeys,
                UUID[] dataServiceUUIDs
                )
        {
            super(concurrencyManager, ITx.UNISOLATED, metadataIndexName);

//BTM            if (fed == null)
//BTM                throw new IllegalArgumentException();
this.embeddedDataServiceMap = embeddedDataServiceMap;

            if (metadata == null)
                throw new IllegalArgumentException();

            if (separatorKeys == null)
                throw new IllegalArgumentException();

            if (separatorKeys.length == 0)
                throw new IllegalArgumentException();

            if (dataServiceUUIDs != null) {
                if (dataServiceUUIDs.length == 0) {
                    throw new IllegalArgumentException();
                }

                if (separatorKeys.length != dataServiceUUIDs.length) {
                    throw new IllegalArgumentException();
                }
            } else {
                /*
                 * Auto-assign the index partitions to data services.
                 */
                try {

                    // discover under-utilized data service UUIDs.
LoadBalancer loadBalancer = getLoadBalancer(remoteLbsCache, lbsCache, embeddedLoadBalancer);
if(loadBalancer != null) {
                    dataServiceUUIDs = 
//BTM - BEGIN       fed.getLoadBalancerService().getUnderUtilizedDataServices
loadBalancer.getUnderUtilizedDataServices
                         (separatorKeys.length, // minCount
                          separatorKeys.length, // maxCount
                          null );// exclude
}
//BTM - END
                } catch(Exception ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
            }

//BTM            this.fed = fed;
            
            this.scaleOutIndexName = metadata.getName();

            this.metadata = metadata;
            
            this.npartitions = separatorKeys.length;
            
            this.separatorKeys = separatorKeys;
            
            this.dataServiceUUIDs = dataServiceUUIDs;

//BTM - replace with ShardService when DataService is converted
            this.dataServices = new IDataService[dataServiceUUIDs.length];

            if( separatorKeys[0] == null )
                throw new IllegalArgumentException();
                
            if (separatorKeys[0].length != 0)
                throw new IllegalArgumentException(
                        "The first separatorKey must be an empty byte[].");
            
            for (int i = 0; i < npartitions; i++) {

                final byte[] separatorKey = separatorKeys[i];
                
                if (separatorKey == null) {

                    throw new IllegalArgumentException();

                }
                
                if (i > 0) {
                    
                    if(BytesUtil.compareBytes(separatorKey, separatorKeys[i-1])<0) {
                        
                        throw new IllegalArgumentException(
                                "Separator keys out of order at index=" + i);
                        
                    }
                    
                }

                final UUID uuid = dataServiceUUIDs[i];

                if (uuid == null) {

                    throw new IllegalArgumentException();

                }

//BTM - replace with ShardService when DataService is converted
//BTM                final IDataService dataService = fed.getDataService(uuid);
IDataService dataService = null;
if(embeddedDataServiceMap != null) {
    dataService = embeddedDataServiceMap.get(uuid);
} else {
    dataService = remoteShardMap.get(uuid);
//BTM - uncomment & combine w remoteShardMap when dataService converted
//BTM    if(dataService == null) {
//BTM        dataService = shardMap.get(uuid);
//BTM    }
}
                if (dataService == null) {

                    throw new IllegalArgumentException(
                            "Unknown data service: uuid=" + uuid);

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
logger.warn("\n*** calling getOnlyResource");
            final String metadataName = getOnlyResource();
            
            // make sure there is no metadata index for that btree.
            try {
                
logger.warn("\n*** calling getIndex("+metadataName+")");
                getIndex(metadataName);
                
                throw new IndexExistsException(metadataName);
                
            } catch(NoSuchIndexException ex) {

                // ignore expected exception
                
            }

            /*
             * Note: there are two UUIDs here - the UUID for the metadata index
             * describing the partitions of the named scale-out index and the
             * UUID of the named scale-out index. The metadata index UUID MUST
             * be used by all B+Tree objects having data for the metadata index
             * (its mutable btrees on journals and its index segments) while the
             * managed named index UUID MUST be used by all B+Tree objects
             * having data for the named index (its mutable btrees on journals
             * and its index segments).
             */
            
            final UUID metadataIndexUUID = UUID.randomUUID();
            
            /*
             * Create the metadata index.
             */
            
            final MetadataIndex mdi = MetadataIndex.create(getJournal(),
                    metadataIndexUUID, metadata);

            /*
             * Map the partitions onto the data services.
             */
            
            final PartitionLocator[] partitions = new PartitionLocator[npartitions];
            
            for (int i = 0; i < npartitions; i++) {
                
                final byte[] leftSeparator = separatorKeys[i];

                final byte[] rightSeparator = i + 1 < npartitions ? separatorKeys[i + 1]
                        : null;

                final PartitionLocator pmd = new PartitionLocator(//
                        mdi.incrementAndGetNextPartitionId(),//
                        dataServiceUUIDs[i],
                        leftSeparator,
                        rightSeparator
                        );
                
                if (logger.isInfoEnabled())
                    logger.info("name=" + scaleOutIndexName + ", pmd=" + pmd);

                /*
                 * Map the initial partition onto that data service. This
                 * requires us to compute the left and right separator keys. The
                 * right separator key is just the separator key for the next
                 * partition in order and null iff this is the last partition.
                 */

                final IndexMetadata md = metadata.clone();
                
                // override the partition metadata.
                md.setPartitionMetadata(new LocalPartitionMetadata(
                        pmd.getPartitionId(),//
                        -1, // we are creating a new index, not moving an index partition.
                        leftSeparator,//
                        rightSeparator,//
                        /*
                         * Note: By setting this to null we are indicating to
                         * the RegisterIndexTask on the data service that it
                         * needs to set the resourceMetadata[] when the index is
                         * actually registered based on the live journal as of
                         * the when the task actually executes on the data
                         * service.
                         */
                         null, // [resources] Signal to the RegisterIndexTask.
                         null // [cause] Signal to RegisterIndexTask
//                         /*
//                          * History.
//                          */
//                         ,"createScaleOutIndex(name="+scaleOutIndexName+") "
                    ));
                
//BTM - replace with EmbeddedShardService when DataService is converted
                dataServices[i].registerIndex(DataService
                        .getIndexPartitionName(scaleOutIndexName, pmd.getPartitionId()), md);

                partitions[i] = pmd;
                
            }

            /*
             * Record each partition in the metadata index.
             */

            for (int i = 0; i < npartitions; i++) {

//                mdi.put(separatorKeys[i], partitions[i]);
                
                mdi.insert(separatorKeys[i], SerializerUtil.serialize(partitions[i]));
            
            }

            /*
             * Register the metadata index with the metadata service. This
             * registration will not be restart safe until the task commits.
             */
            getJournal().registerIndex(metadataName, mdi);

            // Done.
            
            return mdi.getScaleOutIndexMetadata().getIndexUUID();
            
        }

        private LoadBalancer getLoadBalancer(LookupCache remoteCache,
                                             LookupCache cache,
                                             LoadBalancer embeddedLbs)
        {
            if(remoteCache != null) {
                ServiceItem lbsItem = remoteCache.lookup(null);
                if( (lbsItem == null) && (cache != null) ) {
                    lbsItem = cache.lookup(null);
                }
                if(lbsItem != null) return (LoadBalancer)lbsItem.service;
            }
            return embeddedLbs;
        }
    }

    /**
     * Drops a scale-out index.
     * <p>
     * Since this task is unisolated, it basically has a lock on the writable
     * version of the metadata index. It drops each index partition and finally
     * drops the metadata index itself.
     * <p>
     * Historical reads against the metadata index will continue to succeed both
     * during and after this operation has completed successfully. However,
     * {@link ITx#READ_COMMITTED} operations will succeed only until this
     * operation completes at which point the scale-out index will no longer be
     * visible.
     * <p>
     * The data comprising the scale-out index will remain available for
     * historical reads until it is released by whatever policy is in effect for
     * the {@link ResourceManager}s for the {@link DataService}s on which that
     * data resides.
     * 
     * @todo This does not try to handle errors gracefully. E.g., if there is a
     *       problem with one of the data services hosting an index partition it
     *       does not fail over to the next data service for that index
     *       partition.
     */
    static public class DropScaleOutIndexTask extends AbstractTask {

//BTM        private final IBigdataFederation fed;
Map<UUID, ShardService> shardMap;
Map<UUID, IDataService> remoteShardMap;
Map<UUID, IDataService> embeddedDataServiceMap;
        /**
         * @parma fed
         * @param journal
         * @param name
         *            The name of the metadata index for some scale-out index.
         */
        protected DropScaleOutIndexTask(
//BTM IBigdataFederation fed,
Map<UUID, ShardService> shardMap,
Map<UUID, IDataService> remoteShardMap,
Map<UUID, IDataService> embeddedDataServiceMap,
                ConcurrencyManager concurrencyManager, String name) {
            
            super(concurrencyManager, ITx.UNISOLATED, name);
            
//BTM            if (fed == null)
//BTM                throw new IllegalArgumentException();
            
//BTM            this.fed = fed;
this.shardMap = shardMap;
this.remoteShardMap = remoteShardMap;
this.embeddedDataServiceMap = embeddedDataServiceMap;
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

                throw new UnsupportedOperationException(
                        "Not a scale-out index?", ex);

            }

            // name of the scale-out index.
            final String name = ndx.getScaleOutIndexMetadata().getName();
            
            if (logger.isInfoEnabled())
                logger.info("Will drop index partitions for " + name);
            
//            final ChunkedLocalRangeIterator itr = new ChunkedLocalRangeIterator(
//                    ndx, null, null, 0/* capacity */, IRangeQuery.VALS, null/* filter */);
            final ITupleIterator itr = ndx.rangeIterator(null, null,
                    0/* capacity */, IRangeQuery.VALS, null/* filter */);
            
            int ndropped = 0;
            
            while(itr.hasNext()) {
                
                final ITuple tuple = itr.next();

                // FIXME There is still (5/30/08) a problem with using getValueStream() here!
                final PartitionLocator pmd = (PartitionLocator) SerializerUtil
                        .deserialize(tuple.getValue());
//                .deserialize(tuple.getValueStream());

                /*
                 * Drop the index partition.
                 */
                {
                    
                    final int partitionId = pmd.getPartitionId();
                    
                    final UUID serviceUUID = pmd.getDataServiceUUID();
                    
//BTM - replace with ShardService when DataService is converted
//BTM                    final IDataService dataService = fed
//BTM                            .getDataService(serviceUUID);
IDataService dataService = null;
if(embeddedDataServiceMap != null) {
    dataService = embeddedDataServiceMap.get(serviceUUID);
} else {
    dataService = remoteShardMap.get(serviceUUID);
//BTM - uncomment & combine w remoteShardMap when dataService converted
//BTM    if(dataService == null) {
//BTM        dataService = shardMap.get(serviceUUID);
//BTM    }
}
if (dataService == null) {
    logger.warn("EmbeddedShardLocator.DropScaleOutIndexTask: "
                +"null shard service [id="+serviceUUID+", "
                +"partition="+partitionId+"]");
    return 0;
}

                    if (logger.isInfoEnabled())
                        logger.info("Dropping index partition: partitionId="
                                + partitionId + ", dataService=" + dataService);

//BTM - replace with ShardService when DataService is converted
                    dataService.dropIndex(DataService.getIndexPartitionName(
                            name, partitionId));

                }
                
                ndropped++;
                
            }
            
//            // flush all delete requests.
//            itr.flush();
            
            if (logger.isInfoEnabled())
                logger.info("Dropped " + ndropped + " index partitions for "
                        + name);

            // drop the metadata index as well.
            getJournal().dropIndex(getOnlyResource());
            
            return ndropped;
            
        }

    }

    private void setupLoggingContext() {

        try {
            MDC.put("serviceUUID", thisServiceUUID);
            MDC.put("serviceName", SERVICE_NAME);
            MDC.put("hostname", hostname);
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

//BTM - from DataService
    /**
     * The file on which the URL of the embedded httpd service is written.
     */
    private File getHTTPDURLFile() {
        return new File(resourceManager.getDataDir(), "httpd.url");
    }

//BTM - supports service discovery debugging (added by BTM)

    private class CacheListener implements ServiceDiscoveryListener {
        private Logger logger;
        CacheListener(Logger logger) {
            this.logger = logger;
        }
	public void serviceAdded(ServiceDiscoveryEvent event) {
            ServiceItem item = event.getPostEventServiceItem();

            ServiceID serviceId = item.serviceID;
            Object service = item.service;
            Entry[] attrs = item.attributeSets;

            Class serviceType = service.getClass();

            UUID serviceUUID = null;
            String hostname = null;
            String serviceName = null;
            Class serviceIface = null;

            if( (IService.class).isAssignableFrom(serviceType) ) {

                // Avoid remote calls by getting info from attrs
                ServiceUUID serviceUUIDAttr = 
                    (ServiceUUID)(EntryUtil.getEntryByType
                        (attrs, ServiceUUID.class));
                if(serviceUUIDAttr != null) {
                    serviceUUID = serviceUUIDAttr.serviceUUID;
                } else {
                    if(service != null) {
                        try {
                            serviceUUID = 
                            ((IService)service).getServiceUUID();
                        } catch(IOException e) {
                            if(logger.isDebugEnabled()) {
                                logger.log(Level.DEBUG, 
                                           "failed to retrieve "
                                           +"serviceUUID "
                                           +"[service="+serviceType+", "
                                            +"ID="+serviceId+"]", e);
                            }
                        }
                    }
                }
                Hostname hostNameAttr =
                    (Hostname)(EntryUtil.getEntryByType
                                  (attrs, Hostname.class));
                if(hostNameAttr != null) {
                    hostname = hostNameAttr.hostname;
                } else {
                    if(service != null) {
                        try {
                            hostname = 
                            ((IService)service).getHostname();
                        } catch(IOException e) {
                            if(logger.isDebugEnabled()) {
                                logger.log(Level.DEBUG, 
                                           "failed to retrieve "
                                           +"hostname "
                                           +"[service="+serviceType+", "
                                            +"ID="+serviceId+"]", e);
                            }
                        }
                    }
                }
                Name serviceNameAttr = 
                    (Name)(EntryUtil.getEntryByType
                                         (attrs, Name.class));
                if(serviceNameAttr != null) {
                    serviceName = serviceNameAttr.name;
                } else {
                    if(service != null) {
                        try {
                            serviceName = 
                            ((IService)service).getServiceName();
                        } catch(IOException e) {
                            if(logger.isDebugEnabled()) {
                                logger.log(Level.DEBUG, 
                                           "failed to retrieve "
                                           +"serviceName "
                                           +"[service="+serviceType+", "
                                            +"ID="+serviceId+"]", e);
                            }
                        }
                    }
                }
                if( (IMetadataService.class).isAssignableFrom
                                                 (serviceType) )
                {
                    serviceIface = IMetadataService.class;
                } else if( (IDataService.class).isAssignableFrom
                                                    (serviceType) )
                {
                    serviceIface = IDataService.class;
                    remoteShardMap.put
                        (serviceUUID, (IDataService)service);
                } else if( (IClientService.class).isAssignableFrom
                                                      (serviceType) )
                {
                    serviceIface = IClientService.class;
                } else if( (ILoadBalancerService.class).isAssignableFrom
                                                      (serviceType) )
                {
                    serviceIface = ILoadBalancerService.class;
                } else if( (ITransactionService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    serviceIface = ITransactionService.class;
                } else if( (IServicesManagerService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.DEBUG, "serviceAdded "
                                   +"[service=IServicesManagerService, "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                } else {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.WARN, "UNEXPECTED serviceAdded "
                                   +"[service="+serviceType+", "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                }
            } else if( (Service.class).isAssignableFrom(serviceType) ) {
                serviceUUID = ((Service)service).getServiceUUID();
                hostname = ((Service)service).getHostname();
                serviceName = ((Service)service).getServiceName();
                serviceIface = ((Service)service).getServiceIface();

                if( (ShardService.class).isAssignableFrom(serviceType) ) {
                    shardMap.put(serviceUUID, (ShardService)service);
                }
            } else {
                if(logger.isDebugEnabled()) {
                    logger.log(Level.WARN, "UNEXPECTED serviceAdded "
                               +"[service="+serviceType+", "
                               +"ID="+serviceId+"]");
                }
                return;
            }
            if(logger.isDebugEnabled()) {
                logger.log(Level.DEBUG, "serviceAdded [service="
                           +serviceIface+", ID="+serviceId+"]");
            }
	}

	public void serviceRemoved(ServiceDiscoveryEvent event) {
            ServiceItem item = event.getPreEventServiceItem();

            ServiceID serviceId = item.serviceID;
            Object service = item.service;
            Entry[] attrs = item.attributeSets;

            Class serviceType = service.getClass();

            UUID serviceUUID = null;
            Class serviceIface = null;

            if( (IService.class).isAssignableFrom(serviceType) ) {

                // Avoid remote calls by getting info from attrs
                ServiceUUID serviceUUIDAttr = 
                    (ServiceUUID)(EntryUtil.getEntryByType
                        (attrs, ServiceUUID.class));
                if(serviceUUIDAttr != null) {
                    serviceUUID = serviceUUIDAttr.serviceUUID;
                } else {
                    if(service != null) {
                        try {
                            serviceUUID = 
                            ((IService)service).getServiceUUID();
                        } catch(IOException e) {
                            if(logger.isDebugEnabled()) {
                                logger.log(Level.DEBUG, 
                                           "failed to retrieve "
                                           +"serviceUUID "
                                           +"[service="+serviceType+", "
                                            +"ID="+serviceId+"]", e);
                            }
                        }
                    }
                }

                if( (IMetadataService.class).isAssignableFrom
                                                 (serviceType) )
                {
                    serviceIface = IMetadataService.class;
                } else if( (IDataService.class).isAssignableFrom
                                             (serviceType) )
                {
                    serviceIface = IDataService.class;
                } else if( (IClientService.class).isAssignableFrom
                                                      (serviceType) )
                {
                    serviceIface = IClientService.class;
                } else if( (ILoadBalancerService.class).isAssignableFrom
                                                      (serviceType) )
                {
                    serviceIface = ILoadBalancerService.class;
                } else if( (ITransactionService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    serviceIface = ITransactionService.class;

                } else if( (IServicesManagerService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.DEBUG, "serviceRemoved "
                                   +"[service=IServicesManagerService, "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                } else {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.WARN, "UNEXPECTED serviceRemoved "
                                   +"[service="+serviceType+", "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                } 

            } else if( (Service.class).isAssignableFrom(serviceType) ) {
                serviceUUID = ((Service)service).getServiceUUID();
                serviceIface = ((Service)service).getServiceIface();
            } else {

                if(logger.isDebugEnabled()) {
                    logger.log(Level.WARN, "UNEXPECTED serviceRemoved "
                                   +"[service="+serviceType+", "
                                   +"ID="+serviceId+"]");
                }
                return;
            }
            if(logger.isDebugEnabled()) {
                logger.log(Level.DEBUG, "serviceRemoved [service="
                           +serviceIface+", ID="+serviceId+"]");
            }

            if(serviceUUID == null) return;

            if(remoteShardMap != null) {
                remoteShardMap.remove(serviceUUID);
            }
            if(shardMap != null) {
                shardMap.remove(serviceUUID);
            }
        }

	public void serviceChanged(ServiceDiscoveryEvent event) {

            ServiceItem preItem  = event.getPreEventServiceItem();
            ServiceItem postItem = event.getPostEventServiceItem();

            ServiceID serviceId = postItem.serviceID;
            Object service = postItem.service;

            Class serviceType = service.getClass();

            Entry[] preAttrs  = preItem.attributeSets;
            Entry[] postAttrs = postItem.attributeSets; 

            UUID serviceUUID = null;
            Class serviceIface = null;

            if( (IService.class).isAssignableFrom(serviceType) ) {

                // Avoid remote calls by getting info from attrs
                ServiceUUID serviceUUIDAttr = 
                    (ServiceUUID)(EntryUtil.getEntryByType
                        (preAttrs, ServiceUUID.class));
                if(serviceUUIDAttr != null) {
                    serviceUUID = serviceUUIDAttr.serviceUUID;
                } else {
                    if(service != null) {
                        try {
                            serviceUUID = 
                            ((IService)service).getServiceUUID();
                        } catch(IOException e) {
                            if(logger.isDebugEnabled()) {
                                logger.log(Level.DEBUG, 
                                           "failed to retrieve "
                                           +"serviceUUID "
                                           +"[service="+serviceType+", "
                                            +"ID="+serviceId+"]", e);
                            }
                        }
                    }
                }

                if( (IMetadataService.class).isAssignableFrom
                                                 (serviceType) )
                {
                    serviceIface = IMetadataService.class;
                } else if( (IDataService.class).isAssignableFrom
                                             (serviceType) )
                {
                    serviceIface = IDataService.class;
                } else if( (IClientService.class).isAssignableFrom
                                                      (serviceType) )
                {
                    serviceIface = IClientService.class;
                } else if( (ITransactionService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    serviceIface = ITransactionService.class;

                } else if( (IServicesManagerService.class).isAssignableFrom
                                                        (serviceType) )
                {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.DEBUG, "serviceChanged "
                                   +"[service=IServicesManagerService, "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                } else {
                    if(logger.isDebugEnabled()) {
                        logger.log(Level.WARN, "UNEXPECTED serviceChanged "
                                   +"[service="+serviceType+", "
                                   +"ID="+serviceId+"]");
                    }
                    return;
                }
            } else if( (Service.class).isAssignableFrom(serviceType) ) {
                serviceUUID = ((Service)service).getServiceUUID();
                serviceIface = ((Service)service).getServiceIface();
            } else {
                if(logger.isDebugEnabled()) {
                    logger.log(Level.WARN, "UNEXPECTED serviceChanged "
                                   +"[service="+serviceType+", "
                                   +"ID="+serviceId+"]");
                }
                return;
            }

            if(logger.isDebugEnabled()) {
                logger.log(Level.DEBUG, "serviceChanged [service="
                           +serviceIface+", ID="+serviceId+"]");
            }

            for(int i=0; i<preAttrs.length; i++) {
                Entry pre = preAttrs[i];
                Class preType = pre.getClass();
                for(int j=0; j<postAttrs.length; j++) {
                    Entry post = postAttrs[j];
                    Class postType = post.getClass();
                    /* If same attribute type, test for and display change */
                    if(    (preType.isAssignableFrom(postType))
                        && (postType.isAssignableFrom(preType)) )
                    {
                        if(!EntryUtil.compareEntries(pre,post,logger)) {
                            if( logger.isTraceEnabled() ) {//display change
                                logger.log(Level.TRACE,
                                       ": attribute changed ["+pre+"]" );
                                logger.log(Level.TRACE,
                                       ": ===============================");
                                logger.log(Level.TRACE,
                                       ": --- PRE Change Event ---- ");
                                EntryUtil.displayEntry(pre, logger);
                                logger.log(Level.TRACE,
                                       ": ===============================");
                                logger.log(Level.TRACE,
                                       ": --- POST Change Event --- ");
                                EntryUtil.displayEntry(post, logger);
                            }
                        }
                    }
                }//end loop(post:j)
            }//end loop(pre:i)
        }
    }



//BTM - Previously defined on DataService class
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

            final AbstractTask task = new GetIndexMetadataTask(
                    concurrencyManager, startTime, name);

            return (IndexMetadata) concurrencyManager.submit(task).get();
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
                  throws InterruptedException, ExecutionException, IOException
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
                      new RangeIteratorTask(concurrencyManager, timestamp,
                                            name, fromKey, toKey, capacity,
                                            flags, filter);

            // submit the task and wait for it to complete.
            return (ResultSet) concurrencyManager.submit(task).get();
        } finally {
            clearLoggingContext();
        }
    }

    public Future submit(Callable task) {
        setupLoggingContext();
        try {
            if (task == null) {
                throw new IllegalArgumentException("null task");
            }
//PROXY-EXEC-CONFLICT            if (task instanceof IFederationCallable) {
            if (task instanceof IDataServiceCallable) {
//BTM                ((IDataServiceCallable) task).setDataService(this);
                throw new UnsupportedOperationException
                              ("EmbeddedShardLocator.submit [1-arg]: "
                               +"IDataServiceCallable task type");
            }

            // submit the task and return its Future.
            return threadPool.submit(task);
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
                new IndexProcedureTask(concurrencyManager,timestamp,name,proc);

//PROXY-EXEC-CONFLICT         if (task instanceof IFederationCallable) {
            if (task instanceof IDataServiceCallable) {
//BTM                ((IDataServiceCallable) task).setDataService(this);
                throw new UnsupportedOperationException
                              ("EmbeddedShardLocator.submit [3-args]: "
                               +"IDataServiceCallable task type");
            }
            
            // submit the procedure and await its completion.
            return concurrencyManager.submit(task);
        
        } finally {
            
            clearLoggingContext();
            
        }
    }

    public boolean purgeOldResources(final long timeout,
            final boolean truncateJournal) throws InterruptedException {

        // delegate all the work.
        return resourceManager.purgeOldResources(timeout, truncateJournal);
        
    }


    class MdsResourceManager extends ResourceManager {
//BTM        private IBigdataFederation federation;
        private IBigdataFederation indexStore;

        MdsResourceManager(IBigdataFederation indexStore,
                           Properties properties)
        {
            super(properties);
            this.indexStore = indexStore;
        }

        @Override
        public IBigdataFederation getFederation() {
            return indexStore;
//            throw new UnsupportedOperationException
//                          ("EmbeddedShardLocator.getFederation");
        }
            
        @Override
        public DataService getDataService() {
            throw new UnsupportedOperationException
                          ("EmbeddedShardLocator.MdsResourceManager");
        }
            
        @Override
        public UUID getDataServiceUUID() {
            throw new UnsupportedOperationException
                          ("EmbeddedShardLocator.MdsResourceManager");
        }
    }

    class LocalTransactionManager
                     extends AbstractLocalTransactionManager
    {
        private LookupCache txnServiceCache;
        private LookupCache remoteTxnServiceCache;
        private TransactionService embeddedTxnService;//for embedded fed tests

//BTM        private IBigdataFederation federation;
        LocalTransactionManager(LookupCache txnServiceCache,
                                LookupCache remoteTxnServiceCache,
                                TransactionService embeddedTxnService)
//BTM                              ,  IBigdataFederation federation)
        {
            this.txnServiceCache = txnServiceCache;
            this.remoteTxnServiceCache = remoteTxnServiceCache;
            this.embeddedTxnService = embeddedTxnService;
//BTM            this.federation = federation;
        }

        public TransactionService getTransactionService() {
//BTM            if(federation != null) {
//BTM                return federation.getTransactionService();
//BTM            }
            if(remoteTxnServiceCache != null) {
                ServiceItem txnItem = remoteTxnServiceCache.lookup(null);
                if( (txnItem == null) && (txnServiceCache != null) ) {
                    txnItem = txnServiceCache.lookup(null);
                }
                if(txnItem != null) return (TransactionService)txnItem.service;
            }
            return embeddedTxnService;
        }

        public void deactivateTx(final Tx localState) {
            super.deactivateTx(localState);
        }
    }

    private class IDataServiceOnlyFilter 
                  implements ServiceItemFilter
    {
	public boolean check(ServiceItem item) {
            if((item == null) || (item.service == null)) {
                return false;
            }
            Class serviceType = (item.service).getClass();
            boolean isIDataService = 
             (IDataService.class).isAssignableFrom(serviceType);
            if( !isIDataService ) return false;
            boolean isIMetadataService = 
             (IMetadataService.class).isAssignableFrom(serviceType);
            return (isIDataService && !isIMetadataService);
        }
    }
}
