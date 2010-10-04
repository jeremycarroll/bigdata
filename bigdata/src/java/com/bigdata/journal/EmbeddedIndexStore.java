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

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IDataServiceCounters;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.IProcessCounters;
import com.bigdata.counters.IStatisticsCollector;
import com.bigdata.counters.ReadBlockCounters;
import com.bigdata.event.EventQueue;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.ConcurrencyManager.IConcurrencyManagerCounters;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.LocalTransactionManager;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TemporaryStoreFactory;
import com.bigdata.journal.TransactionService;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.resources.IndexManager.IIndexManagerCounters;
import com.bigdata.resources.LocalResourceManagement;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.ResourceManager.IResourceManagerCounters;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.MetadataIndexCache;
import com.bigdata.service.MetadataIndexCachePolicy;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.Service;
import com.bigdata.service.ShardService;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.Util;
import com.bigdata.util.config.LogUtil;
import com.bigdata.util.httpd.AbstractHTTPD;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.LookupCache;

import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

//NOTE: replace IBigdataFederation with IIndexStore when
//      StoreManager.getResourceLocator is changed to no longer
//      call getFederation
//
// IBigdataFederation extends IIndexManager and IFederationDelegate
// IIndexManager extends IIndexStore
public class EmbeddedIndexStore<T> implements IBigdataFederation<T> {

    public static Logger logger =
        LogUtil.getLog4jLogger((EmbeddedIndexStore.class).getName());

    private UUID serviceUUID;
    private Class serviceType;
    private String serviceName;
    private String hostname;
    private LocalResourceManagement embeddedBackend;
    private CounterSet countersRoot;
    private IStatisticsCollector statisticsCollector;
    private Properties properties;
    private ReadBlockCounters readBlockCounters;
    private LocalTransactionManager localTxnMgr;
    private EventQueue eventQueue;
    private ExecutorService embeddedThreadPool;
    private String httpServerUrl;
    private DefaultResourceLocator resourceLocator;

    private IServiceShutdown service;
    private ResourceManager resourceMgr;
    private ConcurrencyManager concurrencyMgr;

    private long lastReattachMillis = 0L;
    private TemporaryStoreFactory tempStoreFactory;

    private MetadataIndexCache metadataIndexCache;

private LookupCache lbsServiceCache;
private LookupCache mdsServiceCache;
private LookupCache shardCache;
private LookupCache remoteShardCache;

private LoadBalancer embeddedLbs;
private ShardLocator embeddedMds;
private Map<UUID, ShardService> embeddedDsMap;

    public EmbeddedIndexStore
               (UUID serviceUUID,
                Class serviceType,
                String serviceName,
                String hostname,
LookupCache lbsServiceCache,
LookupCache mdsServiceCache,
LookupCache shardCache,
LookupCache remoteShardCache,
LoadBalancer embeddedLbs,
                LocalResourceManagement embeddedBackend,
                TemporaryStoreFactory tempStoreFactory,
                int indexCacheSize,
                long indexCacheTimeout,
                MetadataIndexCachePolicy metadataIndexCachePolicy,
                int resourceLocatorCacheSize,
                long resourceLocatorCacheTimeout,
                CounterSet countersRoot,
                IStatisticsCollector statisticsCollector,
                Properties properties,
                ReadBlockCounters readBlockCounters,
                LocalTransactionManager localTxnMgr,
                EventQueue eventQueue,
                ExecutorService embeddedThreadPool,
                String httpServerUrl)
    {
        this.serviceUUID = serviceUUID;
        this.serviceType = serviceType;
        this.serviceName = serviceName;
        this.hostname = hostname;
this.lbsServiceCache = lbsServiceCache;
this.mdsServiceCache = mdsServiceCache;
this.shardCache = shardCache;
this.remoteShardCache = remoteShardCache;
this.embeddedLbs = embeddedLbs;
        this.embeddedBackend = embeddedBackend;
        this.countersRoot = countersRoot;
        this.statisticsCollector = statisticsCollector;
        this.properties = properties;
        this.readBlockCounters = readBlockCounters;
        this.localTxnMgr = localTxnMgr;
        this.eventQueue = eventQueue;
        this.embeddedThreadPool = embeddedThreadPool;
        this.httpServerUrl = httpServerUrl;

        this.metadataIndexCache = 
            new MetadataIndexCache
                    (this, metadataIndexCachePolicy,
                     indexCacheSize, indexCacheTimeout);

        this.resourceLocator = 
            new DefaultResourceLocator(this, null, resourceLocatorCacheSize,
                                       resourceLocatorCacheTimeout);

        this.service = (IServiceShutdown)embeddedBackend;
        this.tempStoreFactory = tempStoreFactory;
    }

    // Required by ONLY IIndexStore

    public BigdataFileSystem getGlobalFileSystem() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getGlobalFileSystem");
    }

    public TemporaryStore getTempStore() {
        return tempStoreFactory.getTempStore();
    }

    public IResourceLocator getResourceLocator() {
        return resourceLocator;
    }

    public IResourceLockService getResourceLockService() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getResourceLockService");
    }

    // Required by BOTH IBigdataFederation, IIndexStore

    public IClientIndex getIndex(String name, long timestamp) {
logger.warn("\n>>>> EmbeddedIndexStore.getIndex <<<<\n");
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getIndex");
    }

    public SparseRowStore getGlobalRowStore() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getGlobalRowStore");
    }

    public ExecutorService getExecutorService() {
        return embeddedThreadPool;
    }

    public long getLastCommitTime() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getLastCommitTime");
    }

    public void destroy() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.destroy");
    }

    // Required by BOTH IBigdataFederation, IIndexManager

    public void registerIndex(IndexMetadata metadata) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.dropIndex");
    }

    public void dropIndex(String name) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.dropIndex");
    }

    // Required by ONLY IFederationDelegate

    public T getService() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getService");
    }

    public String getServiceName() {
        return serviceName;
    }

    public Class getServiceIface() {
        return serviceType;
    }

    public UUID getServiceUUID() {
        return serviceUUID;
    }

    /**
     * Dynamically detach and attach the counters for the named indices
     * underneath the <code>com.bigdata.resources.IndexManager</code>.
     * <p>
     * Note: This method limits the frequency of update to no more than once
     * every 5 seconds.
     * <p>
     * Note: <code>com.bigdata.resources.OverflowManager#overflow()</code>
     * is responsible for reattaching the counters for the live
     * {@link ManagedJournal} during synchronous overflow.
     */
    @Override
    synchronized public void reattachDynamicCounters() {

        final long now = System.currentTimeMillis();
        final long elapsed = now - lastReattachMillis;

        if( service.isOpen() &&
            getResourceMgr().isRunning() &&
            (elapsed > 5000) )
        {
//BTM            // inherit base class behavior
//BTM            super.reattachDynamicCounters();
//BTM            	
//BTM            // The service's counter set hierarchy.
//BTM            final CounterSet serviceRoot = service.getFederation().getServiceCounterSet();

//BTM - BEGIN from DefaultServiceFederationDelegate.reattachDynamicCounters
final CounterSet serviceRoot = 
    Util.getServiceCounterSet(serviceUUID, serviceType,
                              serviceName, hostname, countersRoot,
                              statisticsCollector, properties);

// Ensure path exists.
final CounterSet tmp1 = serviceRoot.makePath(IProcessCounters.Memory);

// Add counters reporting on the various DirectBufferPools.
synchronized (tmp1) {
    // detach the old counters (if any).
    tmp1.detach("DirectBufferPool");

    // attach the current counters.
    tmp1.makePath("DirectBufferPool").attach(DirectBufferPool.getCounters());
}
//BTM - END  from DefaultServiceFederationDelegate.reattachDynamicCounters

            // The lock manager
            {
                // the lock manager is a direct child of this node.
                final CounterSet tmp2 = 
                    (CounterSet) serviceRoot.makePath
                        (IDataServiceCounters.concurrencyManager
                         + ICounterSet.pathSeparator
                         + IConcurrencyManagerCounters.writeService);

                synchronized (tmp2) {

                    // Note: We detach and then attach since that wipes out
                    // any counter set nodes for queues which no longer
                    // exist. Otherwise they will build up forever.

                    // detach the old counters.
                    tmp2.detach(IConcurrencyManagerCounters.LockManager);

                    // attach the the new counters.
                    ((CounterSet) tmp2.makePath
                        (IConcurrencyManagerCounters.LockManager)).attach
                            (getConcurrencyMgr().getWriteService()
                                .getLockManager().getCounters());
                }
            }//end lock manager
                
            // The live indices.
            {
                // The counters for the index manager within the service's
                // counter hierarchy.
                //
                // Note: The indices are a direct child of this node.

                final CounterSet tmp3 = 
                    (CounterSet) serviceRoot.getPath
                        ( IDataServiceCounters.resourceManager
                          + ICounterSet.pathSeparator
                          + IResourceManagerCounters.IndexManager );

                synchronized (tmp3) {

                    // Note: We detach and then attach since that wipes out
                    // any counter set nodes for index partitions which no
                    // longer exist. Otherwise they will build up forever.

                    final boolean exists = 
                        tmp3.getPath(IIndexManagerCounters.Indices) != null;

                    // detach the index partition counters.
                    tmp3.detach(IIndexManagerCounters.Indices);

                    // attach the current index partition counters.
                    ((CounterSet) tmp3.makePath
                        (IIndexManagerCounters.Indices)).attach
                            (getResourceMgr().getIndexCounters());

                    if (logger.isDebugEnabled()) {
                        logger.debug("attached index partition counters "
                                     +"[preexisting="+exists+", path="
                                     + tmp3.getPath()+"]");
                    }
                }

            }//end live indices

            lastReattachMillis = now;
        }
    }

    public boolean isServiceReady() {

        if( !getResourceMgr().isOpen() ) {
                
            // This will happen if the store manager is unable to discover
            // the timestamp service. It will halt its startup process and
            // report that it is closed. At that point the shard service can
            // not start and will shutdown.

            logger.fatal("store manager not open [will shutdown]");

            // shutdown the data service.
            service.shutdownNow();

            // collection was not started.
            return false;
        }

        if( !getResourceMgr().isRunning() ) {
            logger.warn("resource manager not running yet");
            return false;
        }
        return true;
    }

    public void didStart() {

//BTM - NOTE: the method setupCounters called below calls the method getCounters on the resource manager which calls
//BTM -       the method getLiveJournal on the StoreManager which calls the method assertRunning (also on the StoreManager).
//BTM -       The assertRunning method throws an IllegalStateException if the store manager is open but still in 
//BTM -       the 'isStarting' state. This can happen because the constructor for the store manager submits the 
//BTM -       execution of the StoreManager's startup processing to a separate thread; which means that the object
//BTM -       that instantiates the ResourceManager (and ultimately the StoreManager since ResourceManager extends
//BTM -       OverflowManager which extends IndexManager which extends StoreManager) continues -- and possibly 
//BTM -       completes -- its own intialization processing before the StoreManager completes its startup processing
//BTM -       and sets the isStarting state to false. One of the things that can slow down the StoreManager's
//BTM -       initialization process in its StartupTask is the fact that the StartupTask WAITS FOREVER
//BTM -       for a transaction service to be discovered by its federation object. As part of this smart
//BTM -       proxy conversion work, the wait forever loop was changed to a loop that only waits for
//BTM -       N seconds for some value of N. But even this wait loop can cause the object that instantiated
//BTM -       the ResourceManager to complete it's initialization processing before the StoreManager
//BTM -       completes its startup processing and sets the isStarting state to false; resulting in an
//BTM -       IllegalStateException when this didStart method is called by the object. The reason that
//BTM -       a wait loop for the transaction service is still used in the StoreManager, and the didStart 
//BTM -       method is currently being called, is to maintain the same logic as the old purely remote
//BTM -       implementation of the data (shard) service. Once the smart proxy conversion work is complete,
//BTM -       both should be revisited and a determination should be made on whether there is a better
//BTM -       to initialize the Resource/StoreManager created by the EmbeddedShardService. For now though,
//BTM -       this method will wait for Resource/StoreManager to complete its startup processing before
//BTM -       calling setupCounters.

boolean resourceMgrStarted = false;//must be open but no longer in a starting state
for(int i=0;i<60;i++) {
    if( (getResourceMgr()).isOpen() && !(getResourceMgr()).isStarting() ) {
        resourceMgrStarted = true;
        break;
    }
    try { Thread.sleep(1000L); } catch(InterruptedException ie) { }
    logger.debug("waiting for resource manager to complete start up processing");
System.out.println("\nwaiting for resource manager to complete startup processing");
}
if(resourceMgrStarted) {
    logger.debug("\n>>>> resource manager startup processing complete ---- YES\n");
System.out.println("\n>>>> resource manager startup processing complete ---- YES\n");
} else {
    logger.debug("\n>>>> resource manager startup processing complete ---- NO\n");
System.out.println("\n>>>> resource manager startup processing complete ---- NO\n");
    logger.warn("\n>>>> discovered transaction service ---- NO\n");
}
        setupCounters();
        logHttpServerUrl(new File(getResourceMgr().getDataDir(), "httpd.url"));
    }

    public void serviceJoin(IService service, UUID serviceUUID) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.serviceJoin");
    }

    public void serviceJoin(Service service, UUID serviceUUID) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.serviceJoin");
    }

    public void serviceLeave(UUID serviceUUID) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.serviceLeave");
    }

    public AbstractHTTPD newHttpd(final int httpdPort,
                                  final CounterSet counterSet)
                             throws IOException
    {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.newHttpd");
    }

    // Required by ONLY IBigdataFederation

    public IBigdataClient<T> getClient() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getClient");
    }

    public String getHttpdURL() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getHttpdURL");
    }

    public TransactionService getTransactionService() {
        return localTxnMgr.getTransactionService();
    }

    public LoadBalancer getLoadBalancerService() {
        if(lbsServiceCache != null) {
            ServiceItem lbsItem = lbsServiceCache.lookup(null);
            if(lbsItem != null) return (LoadBalancer)lbsItem.service;
        }
        return embeddedLbs;
    }

    public ShardLocator getMetadataService() {
        if(mdsServiceCache != null) {
            ServiceItem mdsItem = mdsServiceCache.lookup(null);
            if(mdsItem != null) return (ShardLocator)mdsItem.service;
        }
        return embeddedMds;
    }

    public CounterSet getCounterSet() {
        return embeddedBackend.getCounterSet();
    }

    public CounterSet getHostCounterSet() {
        return embeddedBackend.getHostCounterSet();
    }

    public CounterSet getServiceCounterSet() {
        return embeddedBackend.getServiceCounterSet();
//        return Util.getServiceCounterSet(serviceUUID, serviceType,
//                                         serviceName, hostname, countersRoot,
//                                         statisticsCollector, properties);
    }

    public String getServiceCounterPathPrefix() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getServiceCounterPathPrefix");
    }

    public UUID[] getDataServiceUUIDs(int maxCount) {

        Set<UUID> uuidSet = new HashSet<UUID>();

        if(shardCache != null) {
            ServiceItem[] shardItems = 
                shardCache.lookup(null, maxCount);
            if(shardItems.length > 0) {
                for(int i=0; i<shardItems.length; i++) {
                    uuidSet.add
                        ( JiniUtil.serviceID2UUID
                                       (shardItems[i].serviceID) );
                }
            }
        }
        if(uuidSet.size() >= maxCount) {
            return uuidSet.toArray( new UUID[uuidSet.size()] );
        }

        //not enough smart proxy impls, try remote cache

        if(remoteShardCache != null) {
            ServiceItem[] shardItems = 
                remoteShardCache.lookup(null, maxCount);
            if(shardItems.length > 0) {
                for(int i=0; i<shardItems.length; i++) {
                    uuidSet.add
                        ( JiniUtil.serviceID2UUID
                                       (shardItems[i].serviceID) );
                }
            }
        }

        //don't mix discovery with embedded case

        if(uuidSet.size() > 0) {
            return uuidSet.toArray( new UUID[uuidSet.size()] );
        }

        //neither cache contains elements, assume embedded case

        if(embeddedDsMap != null) {
            uuidSet = embeddedDsMap.keySet();
        }
        return uuidSet.toArray( new UUID[uuidSet.size()] );
    }

    public ShardService[] getDataServices(UUID[] uuid) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getDataServices");
    }

    public ShardService getDataService(UUID serviceUUID) {

        if(shardCache != null) {
            ServiceItem[] shardItems = 
                shardCache.lookup(null, Integer.MAX_VALUE);
            if(shardItems.length > 0) {
                ServiceID id = JiniUtil.uuid2ServiceID(serviceUUID);
                for(int i=0; i<shardItems.length; i++) {
                    if( id.equals( (shardItems[i]).serviceID ) ) {
                        return (ShardService)((shardItems[i]).service);
                    }
                }
            }
        }

        //no smart proxy impl with serviceUUID, try remote cache

        if(remoteShardCache != null) {
            ServiceItem[] shardItems = 
                remoteShardCache.lookup(null, Integer.MAX_VALUE);
            if(shardItems.length > 0) {
                ServiceID id = JiniUtil.uuid2ServiceID(serviceUUID);
                for(int i=0; i<shardItems.length; i++) {
                    if( id.equals( (shardItems[i]).serviceID ) ) {
                        return (ShardService)((shardItems[i]).service);
                    }
                }
            }
        }

        //nothing in the caches with serviceUUID, try embedded map
        if(embeddedDsMap != null) return embeddedDsMap.get(serviceUUID);

        return null;
    }

    public ShardService getAnyDataService() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getAnyDataService");
    }

    public ShardService getDataServiceByName(String name) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.getDataServiceByName");
    }

    public IMetadataIndex getMetadataIndex(String name, long timestamp) {
        return metadataIndexCache.getIndex(name, timestamp);
    }

    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.registerIndex");
    }

    public UUID registerIndex(IndexMetadata metadata,
                              byte[][] separatorKeys,
                              UUID[] dataServiceUUIDs)
    {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.registerIndex");
    }

    public boolean isScaleOut() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.isScaleOut");
    }

    public boolean isDistributed() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.isDistributed");
    }

    public boolean isStable() {
        throw new UnsupportedOperationException
                          ("EmbeddedIndexStore.isStable");
    }

    public EventQueue getEventQueue() {
        return eventQueue;
    }

//BTM public methods added to EmbeddedIndexStore by BTM

// Needed for embedded testing. Currently, EmbeddedFederation creates
// the shard locator service (embeddedMds) after it creates the
// embeddedShardService which creates this class. But this class
// needs to be given a non-null reference to the embeddedMds
// once the EmbeddedFederation creates it, so that when this
// class' getMetadataService method is called (ex. TestMove),
// that reference can be returned.

    public void setEmbeddedMds(ShardLocator embeddedMds) {
        this.embeddedMds = embeddedMds;
    }

    public void setEmbeddedDsMap(Map<UUID, ShardService> embeddedDsMap) {
        this.embeddedDsMap = embeddedDsMap;
    }

    //private methods

    /**
     * Sets up shard service specific counters.
     * 
     * @see IDataServiceCounters
     */
    private synchronized void setupCounters() {

        if (serviceUUID == null) {
            throw new IllegalStateException("null serviceUUID");
        }
            
        if( !service.isOpen() ) {// service has already been closed
            logger.warn("service is not open");
            return;
        }

        // Service specific counters.

//BTM - BEGIN from DefaultServiceFederationDelegate.reattachDynamicCounters
//BTM   final CounterSet serviceRoot = service.getFederation().getServiceCounterSet();
        final CounterSet serviceRoot = 
                  Util.getServiceCounterSet
                           (serviceUUID, serviceType, serviceName, hostname,
                            countersRoot, statisticsCollector, properties);

        serviceRoot.makePath
            (IDataServiceCounters.resourceManager).attach
                                         ( getResourceMgr().getCounters() );

        serviceRoot.makePath
            (IDataServiceCounters.concurrencyManager).attach
                                         ( getConcurrencyMgr().getCounters() );

        serviceRoot.makePath
            (IDataServiceCounters.transactionManager).attach
                                         ( localTxnMgr.getCounters() );

        // block API.
        {
            CounterSet tmp = serviceRoot.makePath("Block API");

            tmp.addCounter("Blocks Read", new Instrument<Long>() {
                public void sample() {
                    setValue(readBlockCounters.readBlockCount);
                }
            });

            tmp.addCounter (
                    "Blocks Read Per Second",
                     new Instrument<Double>() {

                         public void sample() {
                             // @todo encapsulate this logic.
                             long secs = 
                                 TimeUnit.SECONDS.convert
                                     (readBlockCounters.readBlockNanos,
                                      TimeUnit.NANOSECONDS);

                               final double v;
                               if (secs == 0L) {
                                   v = 0d;
                               } else {
                                   v = 
                                    readBlockCounters.readBlockCount / secs;
                               }
                               setValue(v);

                         }//end sample

                     }// end new Instrument

            );//end addCounter

        }//end block API
    }

    private ResourceManager getResourceMgr() {
        if(this.resourceMgr == null) {
            this.resourceMgr = embeddedBackend.getResourceManager();
        }
        return this.resourceMgr;
    }

    private ConcurrencyManager getConcurrencyMgr() {
        if(this.concurrencyMgr == null) {
            this.concurrencyMgr = embeddedBackend.getConcurrencyManager();
        }
        return this.concurrencyMgr;
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
