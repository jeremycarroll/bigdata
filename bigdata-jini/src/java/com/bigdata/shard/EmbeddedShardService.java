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

import com.bigdata.counters.IDataServiceCounters;//BTM - moved from a nested class in DataService to a standalone class that can be shared

import com.bigdata.service.DefaultServiceFederationDelegate;//BTM - remove?
import com.bigdata.service.IBigdataFederation;//BTM - remove when federation dependencies are removed from codebase
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.IDataServiceCallable;//BTM - investigate
import com.bigdata.service.IFederationCallable;//BTM - investigate


import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.io.ByteBufferInputStream;
//BTM import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.IConcurrencyManager;
//BTM import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.journal.RunState;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.journal.Tx;
import com.bigdata.journal.ConcurrencyManager.IConcurrencyManagerCounters;
import com.bigdata.journal.JournalTransactionService.SinglePhaseCommit;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.IndexManager.IIndexManagerCounters;
import com.bigdata.resources.ResourceManager.IResourceManagerCounters;
import com.bigdata.resources.StoreManager.ManagedJournal;

//BTM
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.IServiceCounters;
import com.bigdata.counters.IStatisticsCollector;
import com.bigdata.counters.LoadBalancerReportingTask;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.counters.ReadBlockCounters;
import com.bigdata.counters.httpd.HttpReportingServer;
import com.bigdata.event.EventQueueSenderTask;
import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.jini.lookup.entry.ServiceUUID;
import com.bigdata.jini.start.IServicesManagerService;
import com.bigdata.journal.DistributedCommitTask;
import com.bigdata.journal.EmbeddedIndexStore;
import com.bigdata.journal.GetIndexMetadataTask;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.LocalTransactionManager;
import com.bigdata.journal.RangeIteratorTask;
import com.bigdata.journal.TemporaryStoreFactory;
import com.bigdata.journal.TransactionService;
import com.bigdata.resources.LocalResourceManagement;
import com.bigdata.service.Event; //BTM *** move to com.bigdata.event?
import com.bigdata.service.IClientService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceOnlyFilter;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.ISession;
import com.bigdata.service.IService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.ITxCommitProtocol;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.MetadataIndexCache;
import com.bigdata.service.MetadataIndexCachePolicy;
import com.bigdata.service.OverflowAdmin;
import com.bigdata.service.Service;
import com.bigdata.service.Session;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardManagement;
import com.bigdata.service.ShardService;
import com.bigdata.util.EntryUtil;
import com.bigdata.util.Util;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.TaskCounters;
import com.bigdata.util.concurrent.ThreadPoolExecutorStatisticsTask;
import com.bigdata.util.config.LogUtil;
import com.bigdata.util.httpd.AbstractHTTPD;

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
import java.io.InputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

//BTM - PRE_FRED_3481
import com.bigdata.service.DataTaskWrapper;
import com.bigdata.service.IDataServiceCallable;

// Make package protected in the future when possible
public
class EmbeddedShardService implements ShardService,
                                      ShardManagement,
                                      ITxCommitProtocol,
                                      OverflowAdmin,
                                      Service,
                                      ISession,
                                      IServiceShutdown,
                                      LocalResourceManagement
{
    public static Logger logger =
        LogUtil.getLog4jLogger((EmbeddedShardService.class).getName());

    /**
     * Options understood by the shard service.
     * 
     */
    public static interface Options extends com.bigdata.journal.Options,
            com.bigdata.journal.ConcurrencyManager.Options,
            com.bigdata.resources.ResourceManager.Options,
            com.bigdata.counters.AbstractStatisticsCollector.Options,
            com.bigdata.service.IBigdataClient.Options
            // @todo local tx manager options?
    {
//BTM - BEGIN - added options
    String THREAD_POOL_SIZE = "threadPoolSize";
    String DEFAULT_THREAD_POOL_SIZE = 
           new Integer(Constants.DEFAULT_THREAD_POOL_SIZE).toString();
//BTM - END   - added options
    }
    
    /**
     * Counters for the block read API.
     */
    final private ReadBlockCounters readBlockApiCounters = new ReadBlockCounters();

    /**
     * Object manages the resources hosted by this shard service.
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
//BTM    private DataServiceTransactionManager localTransactionManager;
private LocalTransactionManager localTransactionManager;

    /**
     * A clone of properties specified to the ctor.
     */
    private final Properties properties;

    /**
     * The dynamic property set associated with the service instance.
     */
    private final Session session = new Session();
    

private UUID thisServiceUUID;
private String hostname;
private ServiceDiscoveryManager sdm;//for discovering txn, lbs, shard services

private LookupCache txnServiceCache;
private LookupCache lbsServiceCache;
private LookupCache mdsServiceCache;
private LookupCache shardCache;
private LookupCache remoteShardCache;//need because of IMetadataService

//for embedded federation testing
private LoadBalancer embeddedLoadBalancer;

//BTM - BEGIN - fields from AbstractFederation --------------------------------
private final ThreadPoolExecutor threadPool;
private final ScheduledExecutorService scheduledExecutor =
                      Executors.newSingleThreadScheduledExecutor
                          (new DaemonThreadFactory
                                   (getClass().getName()+".sampleService"));
private ScheduledFuture<?> eventTaskFuture;
private ScheduledFuture<?> queueStatsTaskFuture;
private ScheduledFuture<?> lbsReportingTaskFuture;
private AbstractHTTPD httpServer;
private String httpServerUrl;//URL used to access the httpServer

private final TemporaryStoreFactory tempStoreFactory;

//Queue of events sent periodically to the load balancer service
private BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>();

private TaskCounters taskCounters = new TaskCounters();
private CounterSet countersRoot = new CounterSet();
private CounterSet serviceRoot;
private AbstractStatisticsCollector statisticsCollector;
private int httpdPort;

//BTM - END   - fields from AbstractFederation --------------------------------

String dbgFlnm="EmbeddedShardService.out";

//for executing IDataServiceCallable tasks
private EmbeddedIndexStore embeddedIndexStore;

    protected EmbeddedShardService
                  (final UUID serviceUUID,
                   final String hostname,
                   final ServiceDiscoveryManager sdm,
                   final TransactionService embeddedTxnService,
                   final LoadBalancer embeddedLbs,
                   final int threadPoolSize,
                   final int indexCacheSize,
                   final long indexCacheTimeout,
                   final MetadataIndexCachePolicy metadataIndexCachePolicy,
                   final int resourceLocatorCacheSize,
                   final long resourceLocatorCacheTimeout,
                   final boolean collectQueueStatistics,
                   final long lbsReportingPeriod,
                   final int httpdPort,
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
this.embeddedLoadBalancer = embeddedLbs;
this.httpdPort = httpdPort;
String httpServerPath = 
                   Util.getServiceCounterPathPrefix
                       (this.thisServiceUUID, SERVICE_TYPE, this.hostname);
try {
    this.httpServerUrl = 
                "http://"
                +AbstractStatisticsCollector.fullyQualifiedHostName
                +":"+this.httpdPort+"/?path="
                +URLEncoder.encode(httpServerPath, "UTF-8");
} catch(java.io.UnsupportedEncodingException e) {
    logger.warn("failed to initialize httpServerUrl", e);
}
if(sdm != null) {
            //for discovering txn, lbs, and shard locator services
            Class[] txnServiceType = 
                            new Class[] {TransactionService.class};
            ServiceTemplate txnServiceTmpl = 
                            new ServiceTemplate(null, txnServiceType, null);

            Class[] lbsServiceType = 
                            new Class[] {LoadBalancer.class};
            ServiceTemplate lbsServiceTmpl = 
                            new ServiceTemplate(null, lbsServiceType, null);

            Class[] mdsServiceType = 
                            new Class[] {ShardLocator.class};
            ServiceTemplate mdsServiceTmpl = 
                            new ServiceTemplate(null, mdsServiceType, null);

            Class[] shardType = new Class[] {ShardService.class};
            ServiceTemplate shardTmpl = 
                            new ServiceTemplate(null, shardType, null);
            ServiceItemFilter shardFilter = null;
            try {
                //caches for smart proxy implementations
                this.txnServiceCache = sdm.createLookupCache
                                     ( txnServiceTmpl, 
                                       null,
                                       new CacheListener(logger) );
                this.lbsServiceCache = sdm.createLookupCache
                                     ( lbsServiceTmpl, 
                                       null,
                                       new CacheListener(logger) );
                this.mdsServiceCache = sdm.createLookupCache
                                     ( mdsServiceTmpl, 
                                       null,
                                       new CacheListener(logger) );
                this.shardCache = sdm.createLookupCache
                                     ( shardTmpl, 
                                       shardFilter,
                                       new CacheListener(logger) );
            } catch(RemoteException e) {
                logger.warn(e.getMessage(), e);
            }

            //for remote implementation

            Class[] remoteShardType = new Class[] {IDataService.class};
            ServiceTemplate remoteShardTmpl = 
                            new ServiceTemplate(null, remoteShardType, null);
            ServiceItemFilter remoteShardFilter = new IDataServiceOnlyFilter();
            try {
                this.remoteShardCache = sdm.createLookupCache
                                     ( remoteShardTmpl,
                                       remoteShardFilter,
                                       new CacheListener(logger) );
            } catch(RemoteException e) {
                logger.warn(e.getMessage(), e);
            }
}//endif(sdm != null)

//BTM - trying to maintain logic from AbstractionFederation (for now)

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

        this.localTransactionManager = 
            new LocalTransactionManager(this.txnServiceCache,
                                        embeddedTxnService);//for embedded fed

        this.tempStoreFactory = new TemporaryStoreFactory(this.properties);


///////////////////////////////////////////////////////////////////////////////////////
//BTM - eventually remove this section?
//BTM - probably don't want shard service collecting platform statistics like the data service does
//BTM - but for initial testing after smart conversion, default to collecting them for now

boolean collectPlatformStatistics = Boolean.parseBoolean( this.properties.getProperty(Options.COLLECT_PLATFORM_STATISTICS, "false") );
    (this.properties).setProperty( AbstractStatisticsCollector.Options.PROCESS_NAME,
                                   "service"+ICounterSet.pathSeparator
                                            +SERVICE_TYPE+ICounterSet.pathSeparator
                                            +serviceUUID.toString() );
        this.statisticsCollector = 
            AbstractStatisticsCollector.newInstance(this.properties);
        this.serviceRoot = Util.getServiceCounterSet(this.thisServiceUUID,
                                                     SERVICE_TYPE,
                                                     SERVICE_NAME,
                                                     this.hostname,
                                                     this.countersRoot,
                                                     this.statisticsCollector,
                                                     this.properties,
                                                     true);//add basic counters
if(collectPlatformStatistics) {
        this.statisticsCollector.start();
        if( logger.isDebugEnabled() ) {
            logger.debug("collecting platform statistics ["+serviceUUID+"]");
        }
}//endif(collectPlatformStatistics)
///////////////////////////////////////////////////////////////////////////////////////

        EventQueueSenderTask eventTask = 
            new EventQueueSenderTask
                    (eventQueue, this.lbsServiceCache,
                     this.embeddedLoadBalancer, SERVICE_NAME, logger);

        this.embeddedIndexStore =
            new EmbeddedIndexStore(this.thisServiceUUID,
                                   SERVICE_TYPE,
                                   SERVICE_NAME,
                                   hostname,
this.lbsServiceCache,
this.mdsServiceCache,
this.shardCache,
this.remoteShardCache,
this.embeddedLoadBalancer,
                                   (LocalResourceManagement)this,
                                   this.tempStoreFactory,
                                   indexCacheSize,
                                   indexCacheTimeout,
                                   metadataIndexCachePolicy,
                                   resourceLocatorCacheSize,
                                   resourceLocatorCacheTimeout,
                                   this.countersRoot,
                                   this.statisticsCollector,
                                   this.properties,
                                   readBlockApiCounters,
                                   this.localTransactionManager,
                                   eventTask,
                                   this.threadPool,
                                   this.httpServerUrl);
System.out.println("\nEmbeddedShardService >>> NEW StoreManager - BEGIN");
        this.resourceManager = 
            new ShardResourceManager(this, this.embeddedIndexStore, properties);
System.out.println("\nEmbeddedShardService >>> NEW StoreManager - END");

        this.concurrencyManager = 
            new ConcurrencyManager
                    (properties, localTransactionManager, resourceManager);

        resourceManager.setConcurrencyManager(concurrencyManager);

//BTM - from AbstractFederation constructor and addScheduledTask

        //start event queue/sender task (sends events every 2 secs)

        long sendEventsDelay = 100L;//one-time initial delay
        long sendEventsPeriod = 2000L;
        this.eventTaskFuture = 
            this.scheduledExecutor.scheduleWithFixedDelay(eventTask,
                                                          sendEventsDelay,
                                                          sendEventsPeriod,
                                                          TimeUnit.MILLISECONDS);

//BTM - from AbstractFederation - start deferred tasks

        //start queue statistics collection task (runs every 1 sec)

        if(collectQueueStatistics) {
            long queueStatsDelay = 0L;//no initial delay
            long queueStatsPeriod = 1000L;
            String threadPoolName = 
                "EmbeddedShardService-Queue-Statistics-Collection";
            ThreadPoolExecutorStatisticsTask queueStatsTask =
                new ThreadPoolExecutorStatisticsTask
                    (threadPoolName, this.threadPool, taskCounters);
            (this.serviceRoot).makePath(threadPoolName).attach
                                     (queueStatsTask.getCounters());
            this.queueStatsTaskFuture = 
                this.scheduledExecutor.scheduleWithFixedDelay
                                           (queueStatsTask,
                                            queueStatsDelay,
                                            queueStatsPeriod,
                                            TimeUnit.MILLISECONDS);
        }

        //start task to report counters to the load balancer

        LoadBalancerReportingTask lbsReportingTask = 
            new LoadBalancerReportingTask
                    (embeddedIndexStore, this.thisServiceUUID,
                     this.serviceRoot, this.lbsServiceCache,
                     this.embeddedLoadBalancer, logger);
        this.lbsReportingTaskFuture = 
            this.scheduledExecutor.scheduleWithFixedDelay
                                       (lbsReportingTask,
                                        lbsReportingPeriod,//initial delay
                                        lbsReportingPeriod,
                                        TimeUnit.MILLISECONDS);

        //start an http daemon from which interested parties can query
        //counter and/or statistics information with http get commands

        try {
            httpServer = 
                new HttpReportingServer
                        (httpdPort, this.countersRoot,
                         embeddedIndexStore, logger);
        } catch (IOException e) {
            logger.error("failed to start http server "
                         +"[port="+this.httpdPort
                         +", path="+httpServerPath+"]", e);
            return;
        }
        if(httpServer != null) {
            if( logger.isDebugEnabled() ) {
                logger.debug("started http daemon "
                             +"[access URL="+this.httpServerUrl+"]");
            }
            // add counter reporting the access url to load balancer
            serviceRoot.addCounter
                (IServiceCounters.LOCAL_HTTPD,
                 new OneShotInstrument<String>(this.httpServerUrl));
        }

//BTM        embeddedIndexStore.didStart();
    }

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
// Required by ISession interface

    public Session getSession() {
        return session;
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
logger.warn("SSSS SHARD SERVICE EmbeddedShardService.shutdown");
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

//BTM - from AbstractFederation.shutdown
        if (statisticsCollector != null) {
            statisticsCollector.stop();
            statisticsCollector = null;
        }

        //false ==> allow in-progress tasks to complete
        lbsReportingTaskFuture.cancel(false);
        queueStatsTaskFuture.cancel(false);
        eventTaskFuture.cancel(false);
        Util.shutdownExecutorService
                 (scheduledExecutor, EXECUTOR_TERMINATION_TIMEOUT,
                  "EmbeddedShardService.scheduledExecutor", logger);
        threadPool.shutdownNow();

        //send one last event report (same logic as in AbstractFederation)
        new EventQueueSenderTask
                (eventQueue, lbsServiceCache, embeddedLoadBalancer,
                 SERVICE_NAME, logger).run();

        if(httpServer != null) {//same login as in AbstractFederation.shutdown
            httpServer.shutdown();
            httpServer = null;
            httpServerUrl = null;
        }

        tempStoreFactory.closeAll();
    }

    synchronized public void shutdownNow() {
logger.warn("SSSSS SHARD SERVICE EmbeddedShardService.shutdownNow");
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

//BTM - from AbstractFederation.shutdownNow

        //false ==> allow in-progress tasks to complete
        lbsReportingTaskFuture.cancel(false);
        queueStatsTaskFuture.cancel(false);
        eventTaskFuture.cancel(false);
        Util.shutdownExecutorService
                 (scheduledExecutor, EXECUTOR_TERMINATION_TIMEOUT,
                  "EmbeddedShardService.scheduledExecutor", logger);
        threadPool.shutdownNow();

        //send one last event report (same logic as in AbstractFederation)
        new EventQueueSenderTask
                (eventQueue, lbsServiceCache, embeddedLoadBalancer,
                 SERVICE_NAME, logger).run();

        if(httpServer != null) {//same login as in AbstractFederation.shutdown
            httpServer.shutdown();
            httpServer = null;
            httpServerUrl = null;
        }

        tempStoreFactory.closeAll();
    }

//BTM public methods added to EmbeddedShardService by BTM

    //from DataService

    synchronized public void destroy() {
logger.warn("\nSSSSS SHARD SERVICE EmbeddedShardService.destroy PRE-delete >>> resourceManager.isOpen() = "+resourceManager.isOpen()+"\n");
        resourceManager.shutdownNow();//sets isOpen to false
        try {
            resourceManager.deleteResources();
        } catch(Throwable t) { 
            logger.warn("exception on resourceManager.deleteResources\n", t);
        }
logger.warn("\nSSSSS SHARD SERVICE EmbeddedShardService.destroy POST-delete >>> resourceManager.isOpen() = "+resourceManager.isOpen()+"\n");

        final File file = getHTTPDURLFile();
        if(file.exists()) {
            file.delete();
        }
        shutdownNow();
    }

// Needed for embedded testing. Currently, EmbeddedFederation creates
// the shard locator service (embeddedMds) after it creates this
// class, but the EmbeddedIndexStore created by this class has
// to return a non-null value when its getMetadataService method
// is called (ex. TestMove). Therefore, this method is provided so
// that EmbeddedFederation can call it after the embeddedMds is
// created, so that this class can then pass it down to the
// EmbeddedIndexStore.

    public void setEmbeddedMds(ShardLocator embeddedMds) {
        embeddedIndexStore.setEmbeddedMds(embeddedMds);
    }

    public void setEmbeddedDsMap(Map<UUID, ShardService> embeddedDsMap) {
        embeddedIndexStore.setEmbeddedDsMap(embeddedDsMap);
    }

//BTM
// Required by LocalResourceManagement interface

    public ResourceManager getResourceManager() {
        return resourceManager;
    }

    public ConcurrencyManager getConcurrencyManager() {
        return concurrencyManager;
    }

    public IIndexManager getIndexManager() {
        return embeddedIndexStore;
    }

    public CounterSet getCounterSet() {
        return Util.getCounterSet(statisticsCollector);
    }
    
    public CounterSet getHostCounterSet() {
        return Util.getHostCounterSet(statisticsCollector);
    }

    public CounterSet getServiceCounterSet() {
        return serviceRoot;
    }

//BTM    public ILocalTransactionManager getLocalTransactionManager() {
//BTM        return localTransactionManager; 
//BTM    }

//BTM
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
                          (concurrencyManager, startTime, name);
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
                    concurrencyManager, timestamp, name, fromKey, toKey,
                    capacity, flags, filter);

            // submit the task and wait for it to complete.
            return (ResultSet) concurrencyManager.submit(task).get();
        } finally {
            clearLoggingContext();
        }
    }

//BTM - PRE_FRED_3481    public Future submit(final Callable task) {
    public <T> Future<T> submit(IDataServiceCallable<T> task) {

Util.printStr(dbgFlnm, "EmbeddedShardService.submit(task) ENTERED");

        setupLoggingContext();
        try {
            if (task == null) {
                throw new IllegalArgumentException("null task");
            }

            // Submit to the ExecutorService for the shard service's federation
            // object. This is used for tasks which are not associated with a
            // timestamp and hence not linked to any specific view of the named
            // indices.
//BTM - PRE_FRED_3481            if (task instanceof IFederationCallable) {
//BTM - PRE_FRED_3481//BTM           ((IFederationCallable) task).setFederation(getFederation());
//BTM - PRE_FRED_3481//                throw new UnsupportedOperationException
//BTM - PRE_FRED_3481//                              ("EmbeddedShardService.submit [1-arg]: "
//BTM - PRE_FRED_3481//                               +"IFederationCallable task type");
//BTM - PRE_FRED_3481Util.printStr(dbgFlnm, "EmbeddedShardService.submit(task) >>> task instanceof IFederationCallable");
//BTM - PRE_FRED_3481((IFederationCallable) task).setFederation(embeddedIndexStore);
//BTM - PRE_FRED_3481            }
//BTM - PRE_FRED_3481
//BTM - PRE_FRED_3481            if (task instanceof IDataServiceCallable) {
//BTM - PRE_FRED_3481//BTM                ((IDataServiceCallable) task).setDataService(this);
//BTM - PRE_FRED_3481//BTM                throw new UnsupportedOperationException
//BTM - PRE_FRED_3481//BTM                              ("EmbeddedShardService.submit [1-arg]: "
//BTM - PRE_FRED_3481//BTM                               +"IDataServiceCallable task type");
//BTM - PRE_FRED_3481Util.printStr(dbgFlnm, "EmbeddedShardService.submit(task) >>> task instanceof IDataServiceCallable");
//BTM - PRE_FRED_3481((IDataServiceCallable) task).setDataService(this);
//BTM - PRE_FRED_3481            }

            // submit the task and return its Future.
//BTM            return getFederation().getExecutorService().submit(task);
//BTM - PRE_FRED_3481 return threadPool.submit(task);
            return threadPool.submit
                       (new DataTaskWrapper
                                (embeddedIndexStore, this, task));
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
//Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) ENTERED");
        setupLoggingContext();
        try {
    
            if (name == null) {
//Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) >>> name = NULL >>> IllegalArgumentException");
                throw new IllegalArgumentException("null name");
            }

            if (proc == null) {
//Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) >>> proc = NULL >>> IllegalArgumentException");
                throw new IllegalArgumentException("null proc");
            }

            // Choose READ_COMMITTED iff proc is read-only and
            // UNISOLATED was requested.
            final long timestamp = 
                (tx == ITx.UNISOLATED && proc.isReadOnly() ?
                     ITx.READ_COMMITTED : tx);
//Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) >>> timestamp="+timestamp);

            // wrap the caller's task.
            final AbstractTask task = new IndexProcedureTask(
                    concurrencyManager, timestamp, name, proc);
//Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) >>> task="+task);

            
            if (task instanceof IFederationCallable) {
//BTM                ((IFederationCallable) task).setFederation(getFederation());
//                throw new UnsupportedOperationException
//                              ("EmbeddedShardService.submit [3-args]: "
//                               +"IFederationCallable task type");
//Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) >>> task instanceof IFederationCallable");
((IFederationCallable) task).setFederation(embeddedIndexStore);
            }

            if (task instanceof IDataServiceCallable) {
//BTM                ((IDataServiceCallable) task).setDataService(this);
//Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) >>> task instanceof IDataCallable");
                throw new UnsupportedOperationException
                              ("EmbeddedShardService.submit [3-args]: "
                               +"IDataServiceCallable task type");
            }
            
            // submit the procedure and await its completion.
//Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) >>> CALLING concurrencyManager.submit() - CURRENT STACK TRACE:");
//Util.printStr(dbgFlnm, Util.getCurrentStackTrace());
Future retFuture = concurrencyManager.submit(task);
//Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) >>> RETURNING "+retFuture);
return retFuture;
//BTM            return concurrencyManager.submit(task);
} catch(Throwable t) {
Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) >>> Throwable t="+t);

        } finally {
            clearLoggingContext();
        }
//Util.printStr(dbgFlnm, "EmbeddedShardService.submit(tx, name, proc) >>> RETURNING NULL");
return null;
    }

    public boolean purgeOldResources(final long timeout,
                                     final boolean truncateJournal)
                       throws InterruptedException
    {
        // delegate all the work.
//BTM        return getResourceManager().purgeOldResources(timeout, truncateJournal);
return resourceManager.purgeOldResources(timeout, truncateJournal);
    }
    
//BTM
// Required by ITxCommitProtocol interface

    /*
     * ITxCommitProtocol.
     */
    public void setReleaseTime(final long releaseTime) {
        setupLoggingContext();
        try {
//BTM            getResourceManager().setReleaseTime(releaseTime);
resourceManager.setReleaseTime(releaseTime);
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
            
//BTM            final Tx localState = (Tx) getLocalTransactionManager().getTx(tx);
final Tx localState = (Tx) localTransactionManager.getTx(tx);

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
//BTM            final ManagedJournal journal = getResourceManager().getLiveJournal();
final ManagedJournal journal = resourceManager.getLiveJournal();            
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

//BTM                        ((DataServiceTransactionManager) journal.getLocalTransactionManager()).deactivateTx(localState);
((LocalTransactionManager) journal.getLocalTransactionManager()).deactivateTx(localState);

//                        state.setRunState(RunState.Committed);
                        
                        return 0L;

                    }

                } finally {

                    localState.lock.unlock();

                }

            }

//BTM            final IConcurrencyManager concurrencyManager = /*journal.*/getConcurrencyManager();

            final AbstractTask<Void> task = 
                new SinglePhaseCommit(concurrencyManager,
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
                concurrencyManager.submit(task).get();
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

//BTM                    ((DataServiceTransactionManager) journal.getLocalTransactionManager()).deactivateTx(localState);
((LocalTransactionManager) journal.getLocalTransactionManager()).deactivateTx(localState);
                
//                    state.setRunState(RunState.Committed);

                } finally {
                    
                    localState.lock.unlock();
                    
                }

            } catch (Throwable t) {

//                logger.error(t.getMessage(), t);

                localState.lock.lock();

                try {

                    localState.setRunState(RunState.Aborted);

//BTM                    ((DataServiceTransactionManager) journal.getLocalTransactionManager()).deactivateTx(localState);
((LocalTransactionManager) journal.getLocalTransactionManager()).deactivateTx(localState);

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
            
//            logger.warn("\n" + state + "\n" + localState);

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
            
//BTM            final Tx state = (Tx) getLocalTransactionManager().getTx(tx);
final Tx state = (Tx) localTransactionManager.getTx(tx);

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

    public void abort(final long tx) throws IOException {

        setupLoggingContext();

        try {

//BTM            final Tx localState = (Tx) getLocalTransactionManager().getTx(tx);
final Tx localState = (Tx) localTransactionManager.getTx(tx);

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


//BTM
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
                  new RegisterIndexTask(concurrencyManager,
                                        name, metadata);
            concurrencyManager.submit(task).get();
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
                  new DropIndexTask(concurrencyManager, name);
            concurrencyManager.submit(task).get();
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
                      resourceManager.openStore(resource.getUUID());
    
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


//BTM
// Required by OverflowAdmin interface (for testing & benchmarking)

    public void forceOverflow(final boolean immediate,
                              final boolean compactingMerge)
                throws IOException, InterruptedException, ExecutionException
    {
        setupLoggingContext();
        try {
            if (!(resourceManager instanceof ResourceManager)) {
                throw new UnsupportedOperationException
                              ("not instance of ResourceManager "
                               +"["+resourceManager.getClass()+"]");
            }

//BTM            final Callable task = new ForceOverflowTask(compactingMerge);
final Callable task = new ForceOverflowTask(resourceManager, compactingMerge);

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

//BTM                getConcurrencyManager().submit
//BTM                    ( new AbstractTask(getConcurrencyManager(),
                concurrencyManager.submit
                    ( new AbstractTask( concurrencyManager,
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
            if (!(resourceManager instanceof ResourceManager)) {
                throw new UnsupportedOperationException
                              ("not instance of ResourceManager "
                               +"["+resourceManager.getClass()+"]");
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
                throw new UnsupportedOperationException
                              ("not instance of ResourceManager "
                               +"["+resourceManager.getClass()+"]");
            }

            // overflow processing is enabled but not allowed, which
            // means that overflow processing is occurring right now.

            return resourceManager.isOverflowEnabled()
                    && !resourceManager.isOverflowAllowed();
        } finally {
            clearLoggingContext();
        }
    }




    /**
     * The file on which the URL of the embedded httpd service is written.
     */
    protected File getHTTPDURLFile() {
//BTM        return new File(getResourceManager().getDataDir(), "httpd.url");
return new File(resourceManager.getDataDir(), "httpd.url");
    }

    /**
     * Forms the name of the index corresponding to a partition of a named
     * scale-out index as <i>name</i>#<i>partitionId</i>.
     * <p>
     * Another advantage of this naming scheme is that index partitions are just
     * named indices and all of the mechanisms for operating on named indices
     * and for concurrency control for named indices apply automatically. Among
     * other things, this means that different tasks can write concurrently on
     * different partitions of the same named index on a given shard service.
     * 
     * @return The name of the index partition.
     */
//BTM - MOVED to com.bigdata.util.Util
//BTM    public static final String getIndexPartitionName(final String name,
//BTM                                                     final int partitionId)
//BTM    {
//BTM        if (name == null) {
//BTM            throw new IllegalArgumentException("null name");
//BTM        }
//BTM
//BTM        if (partitionId == -1) {// Not a partitioned index.
//BTM            return name;
//BTM        }
//BTM        return name + "#" + partitionId;
//BTM    }


// Private methods of this class

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



// Nested classes

    class ShardResourceManager extends ResourceManager {

        private ShardService dataService;
//BTM        private IBigdataFederation federation;
        private IBigdataFederation indexStore;

        ShardResourceManager(ShardService dataService,
                             IBigdataFederation indexStore,
                             Properties properties)
        {
            super(properties);
            this.dataService = dataService;
            this.indexStore = indexStore;
        }

        @Override
        public IBigdataFederation getFederation() {
            return indexStore;
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


//BTM - supports service discovery (added by BTM)

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
                            if(logger.isTraceEnabled()) {
                                logger.log(Level.TRACE, 
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
                            if(logger.isTraceEnabled()) {
                                logger.log(Level.TRACE, 
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
        }
    }


    /**
     * Task sets the flag that will cause overflow processing to be triggered on
     * the next group commit.
     */
    private class ForceOverflowTask implements Callable<Void> {

private ResourceManager resourceMgr;
        private final boolean compactingMerge;
        
//BTM        public ForceOverflowTask(final boolean compactingMerge) {
public ForceOverflowTask(final ResourceManager resourceMgr, final boolean compactingMerge) {
    this.resourceMgr = resourceMgr;
            this.compactingMerge = compactingMerge;
            
        }
        
        public Void call() throws Exception {

//            final WriteExecutorService writeService = concurrencyManager
//                    .getWriteService();

//BTM            final ResourceManager resourceManager = (ResourceManager) DataService.this.resourceManager;

//BTM            if (resourceManager.isOverflowAllowed()) {
            if (resourceMgr.isOverflowAllowed()) {

                if (compactingMerge) {

//BTM                    resourceManager.compactingMerge.set(true);
                    resourceMgr.compactingMerge.set(true);

                }

                // trigger overflow on the next group commit.
//                writeService.forceOverflow.set(true);
//BTM                resourceManager.forceOverflow.set(true);
                resourceMgr.forceOverflow.set(true);

            }

            return null;

        }

    }

    /**
     * Concrete implementation manages the local state of transactions executing
     * on a shard service.
     */
//BTM    public class DataServiceTransactionManager extends
//BTM            AbstractLocalTransactionManager {
//BTM
//BTM//BTM        public ITransactionService getTransactionService() {
//BTMpublic TransactionService getTransactionService() {
//BTM
//BTM            return DataService.this.getFederation().getTransactionService();
//BTM
//BTM        }
//BTM
        /**
         * Exposed to {@link singlePhaseCommit(long)}
         */
//BTM        public void deactivateTx(final Tx localState) {
//BTM
//BTM            super.deactivateTx(localState);
//BTM
//BTM        }
//BTM
//BTM    }

}



