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
 * Created on Mar 28, 2008
 */

package com.bigdata.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bfs.GlobalFileSystemHelper;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IServiceCounters;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TemporaryStoreFactory;
import com.bigdata.rawstore.Bytes;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.service.IBigdataClient.Options;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.service.ndx.ScaleOutIndexCounters;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.ShutdownHelper;
import com.bigdata.util.concurrent.TaskCounters;
import com.bigdata.util.concurrent.ThreadPoolExecutorStatisticsTask;
import com.bigdata.util.httpd.AbstractHTTPD;

//BTM
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.resources.ResourceManager;
import com.bigdata.util.Util;
import com.bigdata.util.config.NicUtil;
import com.bigdata.util.config.ConfigDeployUtil;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.event.EventQueueSender;
import com.bigdata.event.EventQueueSenderTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.LocalTransactionManager;

/**
 * Abstract base class for {@link IBigdataFederation} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the client or service.
 * 
 * @todo implement {@link IServiceShutdown}. When it is declared here it messes
 *       up the Options interface hierarchy. What appears to be happening is
 *       that the IServiceShutdown.Options interface is flattened into
 *       IServiceShutdown and it shadows the Options that are being used.
 */
//BTM abstract public class AbstractFederation<T> implements IBigdataFederation<T> {
abstract public class AbstractFederation<T> implements IBigdataFederation<T>,
                                                       ILocalResourceManagement
{

    protected static final Logger log = Logger.getLogger(IBigdataFederation.class);

//BTM - FOR_CLIENT_SERVICE - BEGIN
    protected ResourceManager fedResourceMgr;
    protected IConcurrencyManager fedConcurrencyMgr;
//BTM - FOR_CLIENT_SERVICE - END

    /**
     * The client (if connected).
     */
    private AbstractClient<T> client;
    
    /**
     * <code>true</code> iff open.  Note that during shutdown this will be set
     * to <code>false</code> before the client reference is cleared in order to
     * avoid an infinite recursion when we request that the client disconnect
     * itself so its reference to the federation will be cleared along with 
     * the federation's reference to the client.
     */
    private boolean open;
    
    public AbstractClient<T> getClient() {
        
        assertOpen();
        
        return client;
        
    }
    
    final public boolean isOpen() {
        
        return open;
        
    }
    
    /**
     * Normal shutdown allows any existing client requests to federation
     * services to complete but does not schedule new requests, disconnects from
     * the federation, and then terminates any background processing that is
     * being performed on the behalf of the client (service discovery, etc).
     * <p>
     * Note: concrete implementations MUST extend this method.
     * <p>
     * Note: Clients use {@link IBigdataClient#disconnect(boolean)} to
     * disconnect from a federation. The federation implements that disconnect
     * using either {@link #shutdown()} or {@link #shutdownNow()}.
     * <p>
     * The implementation must be a NOP if the federation is already shutdown.
     */
    synchronized public void shutdown() {

        if(!isOpen()) return;

        open = false;
        
        final long begin = System.currentTimeMillis();

        if (log.isInfoEnabled())
            log.info("begin");

        try {

            // allow client requests to finish normally.
            new ShutdownHelper(threadPool, 10L/*logTimeout*/, TimeUnit.SECONDS) {
              
                @Override
                public void logTimeout() {
                    
                    log.warn("Awaiting thread pool termination: elapsed="
                            + TimeUnit.NANOSECONDS.toMillis(elapsed()) + "ms");

                }

            };

            if (statisticsCollector != null) {

                statisticsCollector.stop();

                statisticsCollector = null;

            }

            // terminate sampling and reporting tasks.
            new ShutdownHelper(scheduledExecutorService, 10L/* logTimeout */,
                    TimeUnit.SECONDS) {

                @Override
                public void logTimeout() {

                    log.warn("Awaiting sample service termination: elapsed="
                            + TimeUnit.NANOSECONDS.toMillis(elapsed()) + "ms");

                }

            };

        } catch (InterruptedException e) {

            log.warn("Interrupted awaiting thread pool termination.", e);

        }

        // drain any events in one last report.
//BTM        new SendEventsTask().run();
//BTM - PRE_CLIENT_SERVICE new SendEventsTask(events).run();
        new EventQueueSenderTask(events, this, getServiceName(), null).run();

        // optional httpd service for the local counters.
        if (httpd != null) {

            httpd.shutdown();

            httpd = null;

            httpdURL = null;

        }
        
        if (log.isInfoEnabled())
            log.info("done: elapsed=" + (System.currentTimeMillis() - begin));

        if (client != null) {

            // Force the client to release its reference to the federation.
            client.disconnect(false/*immediateShutdown*/);

            // Release our reference to the client.
            client = null;
            
        }

        tempStoreFactory.closeAll();
        
    }

    /**
     * Immediate shutdown terminates any client requests to federation services,
     * disconnects from the federation, and then terminate any background
     * processing that is being performed on the behalf of the client (service
     * discovery, etc).
     * <p>
     * Note: concrete implementations MUST extend this method to either
     * disconnect from the remote federation or close the embedded federation
     * and then clear the {@link #fed} reference so that the client is no longer
     * "connected" to the federation.
     * <p>
     * Note: Clients use {@link IBigdataClient#disconnect(boolean)} to disconnect
     * from a federation.  The federation implements that disconnect using either
     * {@link #shutdown()} or {@link #shutdownNow()}.
     * <p>
     * The implementation must be a NOP if the federation is already shutdown.
     */
    synchronized public void shutdownNow() {
        
        if(!isOpen()) return;
        
        open = false;

        final long begin = System.currentTimeMillis();
        
        if(log.isInfoEnabled())
            log.info("begin");
        
        // stop client requests.
        threadPool.shutdownNow();
        
        if (statisticsCollector != null) {

            statisticsCollector.stop();

            statisticsCollector = null;

        }

        // terminate sampling and reporting tasks immediately.
        scheduledExecutorService.shutdownNow();

        // discard any events still in the queue.
        events.clear();

        // terminate the optional httpd service for the client's live counters.
        if( httpd != null) {
            
            httpd.shutdownNow();
            
            httpd = null;
            
            httpdURL = null;
            
        }

        if (log.isInfoEnabled())
            log.info("done: elapsed=" + (System.currentTimeMillis() - begin));
        
        if (client != null) {

            // Force the client to release its reference to the federation.
            client.disconnect(true/* immediateShutdown */);

            // Release our reference to the client.
            client = null;
            
        }
        
        tempStoreFactory.closeAll();

    }

    synchronized public void destroy() {

        if (isOpen())
            shutdownNow();
        
        tempStoreFactory.closeAll();
        
    }
    
    /**
     * @throws IllegalStateException
     *                if the client has disconnected from the federation.
     */
    protected void assertOpen() {

        if (client == null) {

            throw new IllegalStateException();

        }

    }

    /**
     * Used to run application tasks.
     */
    private final ThreadPoolExecutor threadPool;
    
    /**
     * Used to sample and report on the queue associated with the
     * {@link #threadPool}.
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new DaemonThreadFactory
                    (getClass().getName()+".sampleService"));
    
    /**
     * A service which may be used to schedule performance counter sampling
     * tasks.
     */
    public ScheduledExecutorService getScheduledExecutorService() {
        
        return scheduledExecutorService;
        
    }
    
    /**
     * httpd reporting the live counters for the client while it is connected to
     * the federation.
     */
    private AbstractHTTPD httpd;
    
    /**
     * The URL that may be used to access the httpd service exposed by this
     * client.
     */
    private String httpdURL;

    final public String getHttpdURL() {
        
        return httpdURL;
        
    }
    
    /**
     * Locator for relations, etc.
     */
    private final DefaultResourceLocator resourceLocator;
    
    public DefaultResourceLocator getResourceLocator() {
        
        assertOpen();
        
        return resourceLocator;
    
    }
    
    /**
     * Counters that aggregate across all tasks submitted by the client against
     * the connected federation. Those counters are sampled by a
     * {@link ThreadPoolExecutorStatisticsTask} and reported by the client to
     * the load balancer service.
     */
    private final TaskCounters taskCounters = new TaskCounters();

    /**
     * Counters for each scale-out index accessed by the client.
     * 
     * @todo A hard reference map is used to prevent the counters from being
     *       finalized so that they will reflect the use by the client over the
     *       life of its operations. However, this could be a problem for an
     *       {@link ShardService} or {@link IClientService} since any number of
     *       clients could run over time. If there were a large #of distinct
     *       scale-out indices then this would effectively represent a memory
     *       leak. The other way to handle this would be a
     *       {@link ConcurrentWeakValueCacheWithTimeout} where the timeout was
     *       relatively long - 10m or more.
     * 
     * @todo indices isolated by a read-write tx will need their own counters
     *       for each isolated view or we can just let the counters indicates
     *       the unisolated writes.
     */
    private final Map<String, ScaleOutIndexCounters> scaleOutIndexCounters = new HashMap<String, ScaleOutIndexCounters>();
    
    /**
     * Return the {@link TaskCounters} which aggregate across all operations
     * performed by the client against the connected federation. These
     * {@link TaskCounters} are sampled by a
     * {@link ThreadPoolExecutorStatisticsTask} and the sampled data are
     * reported by the client to the load balancer service.
     */
    public TaskCounters getTaskCounters() {

        return taskCounters;

    }
    
    /**
     * Return the {@link ScaleOutIndexCounters} for the specified scale-out index
     * for this client. There is only a single instance per scale-out index and
     * all operations by this client on that index are aggregated by that
     * instance. These counters are reported by the client to the
     * load balancer service.
     * 
     * @param name
     *            The scale-out index name.
     */
    public ScaleOutIndexCounters getIndexCounters(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        synchronized (scaleOutIndexCounters) {

            ScaleOutIndexCounters t = scaleOutIndexCounters.get(name);

            if (t == null) {
        
//BTM                t = new ScaleOutIndexCounters(this);
t = new ScaleOutIndexCounters(getScheduledExecutorService());
                
                scaleOutIndexCounters.put(name, t);
                
                /*
                 * Attach to the counters reported by the client to the LBS.
                 * 
                 * Note: The counters should not exist under this path since we
                 * are just creating them now, but if they do then they are
                 * replaced.
                 */
                getServiceCounterSet().makePath("Indices").makePath(name)
                        .attach(t.getCounters(), true/* replace */);
                
            }
            
            return t;
        
        }
        
    }
    
    /**
     * Collects interesting statistics on the client's host and process
     * for reporting to the load balancer service.
     */
    private AbstractStatisticsCollector statisticsCollector;
    
    /**
     * Adds a task which will run until canceled, until it throws an exception,
     * or until the federation is {@link #shutdown()}.
     * <p>
     * Note: Tasks run on this service generally update sampled values on
     * {@link ICounter}s reported to the load balancer service. Basic
     * information on the {@link #getExecutorService()} is reported
     * automatically. Clients may add additional tasks to report on client-side
     * aspects of their application.
     * <p>
     * Note: Non-sampled counters are automatically conveyed to the
     * load balancer service once added to the basic {@link CounterSet}
     * returned by {@link #getCounterSet()}.
     * 
     * @param task
     *            The task.
     * @param initialDelay
     *            The initial delay.
     * @param delay
     *            The delay between invocations.
     * @param unit
     *            The units for the delay parameters.
     * 
     * @return The {@link ScheduledFuture} for that task.
     */
    public ScheduledFuture addScheduledTask(Runnable task,
            long initialDelay, long delay, TimeUnit unit) {

        if (task == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("Scheduling task: task=" + task.getClass()
                    + ", initialDelay=" + initialDelay + ", delay=" + delay
                    + ", unit=" + unit);

        return scheduledExecutorService.scheduleWithFixedDelay(task, initialDelay, delay,
                unit);

    }

    synchronized public CounterSet getCounterSet() {

        if (countersRoot == null) {

            countersRoot = new CounterSet();

            if (statisticsCollector != null) {

                countersRoot.attach(statisticsCollector.getCounters());

            }

            serviceRoot = countersRoot.makePath(getServiceCounterPathPrefix());

            /*
             * Basic counters.
             */

            AbstractStatisticsCollector.addBasicServiceOrClientCounters(
                    serviceRoot, getServiceName(), getServiceIface(), client
                            .getProperties());

        }

        return countersRoot;
        
    }
    private CounterSet countersRoot;
    private CounterSet serviceRoot;

    public CounterSet getHostCounterSet() {

        final String pathPrefix = ICounterSet.pathSeparator
                + AbstractStatisticsCollector.fullyQualifiedHostName;

        return (CounterSet) getCounterSet().getPath(pathPrefix);
        
    }
    
    public CounterSet getServiceCounterSet() {

        // note: defines [serviceRoot] as side effect.
        getCounterSet();
        
        return serviceRoot;
        
    }

//BTM - FOR_CLIENT_SERVICE - BEGIN - required by the ILocalResourceManagement interface
    public CounterSet getServiceCounterSet(boolean addCounters) {
        return getServiceCounterSet();//ignore addCounters param for now
    }
//BTM - FOR_CLIENT_SERVICE - END

    public String getServiceCounterPathPrefix() {

        final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

        return Util.getServiceCounterPathPrefix(getServiceUUID(),
                                                getServiceIface(),
                                                hostname);

    }

    /**
     * The path prefix under which all of the client or service's counters are
     * located. The returned path prefix is terminated by an
     * {@link ICounterSet#pathSeparator}.
     * 
     * @param serviceUUID
     *            The service {@link UUID}.
     * @param serviceIface
     *            The primary interface or class for the service.
     * @param hostname
     *            The fully qualified name of the host on which the service is
     *            running.
     */
//BTM - MOVED to bigdata-jini/src/java/com/bigdata/util/Util.java
//BTM  static public String getServiceCounterPathPrefix(final UUID serviceUUID,
//BTM            final Class serviceIface, final String hostname) {
//BTM
//BTM        if (serviceUUID == null)
//BTM            throw new IllegalArgumentException();
//BTM        
//BTM        if (serviceIface == null)
//BTM            throw new IllegalArgumentException();
//BTM        
//BTM        if (hostname == null)
//BTM            throw new IllegalArgumentException();
//BTM        
//BTM        final String ps = ICounterSet.pathSeparator;
//BTM
//BTM        final String pathPrefix = ps + hostname + ps + "service" + ps
//BTM                + serviceIface.getName() + ps + serviceUUID + ps;
//BTM
//BTM        return pathPrefix;
//BTM
//BTM    }
    
    public ExecutorService getExecutorService() {
        
        assertOpen();
        
        return threadPool;
        
    }

    protected AbstractFederation(final IBigdataClient<T> client) {

        if (client == null)
            throw new IllegalArgumentException();

        this.open = true;
        
        this.client = (AbstractClient<T>) client;

        if (this.client.getDelegate() == null) {

            /*
             * If no one has set the delegate by this point then we setup a
             * default delegate.
             */

            this.client
                    .setDelegate(new DefaultClientDelegate<T>(this, null/* clientOrService */));

        }
        
        final int threadPoolSize = client.getThreadPoolSize();

        if (threadPoolSize == 0) {

            threadPool = (ThreadPoolExecutor) Executors
                    .newCachedThreadPool(new DaemonThreadFactory
                            (getClass().getName()+".executorService"));

        } else {

            threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    threadPoolSize, new DaemonThreadFactory
                    (getClass().getName()+".executorService"));

        }

        tempStoreFactory = new TemporaryStoreFactory(client.getProperties());

//        tempStoreFactory = new TemporaryStoreFactory(this.client
//                .getTempStoreMaxExtent());

//BTM - FOR_CLIENT_SERVICE - BEGIN - for passing concurrencyMgr to getGlobalFileSystem and TemporaryStoreFactory.getTempStore
        this.fedResourceMgr =
             new FedResourceManager
                     ( (IBigdataDiscoveryManagement)this,
                       (ILocalResourceManagement)this,
                       (IIndexManager)this,
                       client.getProperties() );
        this.fedConcurrencyMgr =
             new ConcurrencyManager
                 (client.getProperties(),
                  new LocalTransactionManager
                          ( (IBigdataDiscoveryManagement)this ),
                  this.fedResourceMgr);
        (this.fedResourceMgr).setConcurrencyManager(this.fedConcurrencyMgr);
//BTM - FOR_CLIENT_SERVICE - END

//BTM
//BTM - PRE_CLIENT_SERVICE this.sendEventsTask = new SendEventsTask(events);
this.sendEventsTask = new EventQueueSenderTask(events, this, getServiceName(), null);

        addScheduledTask(
//BTM                new SendEventsTask(),// task to run.
(Runnable)(this.sendEventsTask),// task to run.
                100, // initialDelay (ms)
                2000, // delay
                TimeUnit.MILLISECONDS // unit
                );                

        getExecutorService().execute(new StartDeferredTasksTask());
        
        // Setup locator.
        resourceLocator = new DefaultResourceLocator(this,
                null, // delegate
                ((AbstractClient<T>) client).getLocatorCacheCapacity(),
                ((AbstractClient<T>) client).getLocatorCacheTimeout());
        
    }
    
    public void registerIndex(final IndexMetadata metadata) {

        assertOpen();

        registerIndex(metadata, null);

    }

    public UUID registerIndex(final IndexMetadata metadata, UUID dataServiceUUID) {

        assertOpen();

        if (dataServiceUUID == null) {
            
            // see if there is an override.
            dataServiceUUID = metadata.getInitialDataServiceUUID();
            
            if (dataServiceUUID == null) {

                final LoadBalancer loadBalancerService = getLoadBalancerService();

                if (loadBalancerService == null) {

                    try {

                        /*
                         * As a failsafe (or at least a failback) we ask the
                         * client for ANY data service that it knows about and
                         * use that as the data service on which we will
                         * register this index. This lets us keep going if the
                         * load balancer is dead when this request comes
                         * through.
                         */

//BTM                        dataServiceUUID = getAnyDataService().getServiceUUID();
ShardService shardService = getAnyDataService();
if(shardService != null) {
    if(shardService instanceof IService) {
        dataServiceUUID = ((IService)shardService).getServiceUUID();
    } else {
        dataServiceUUID = ((Service)shardService).getServiceUUID();
    }
} else {
    log.warn("no shard service available for index registration");
}
                    } catch (Exception ex) {

                        log.error(ex);

                        throw new RuntimeException(ex);

                    }

                } else {

                    try {

                        dataServiceUUID = loadBalancerService
                                .getUnderUtilizedDataService();

                    } catch (Exception ex) {

                        throw new RuntimeException(ex);

                    }

                }

            }
            
        }

        return registerIndex(//
                metadata, //
                new byte[][] { new byte[] {} },//
                new UUID[] { dataServiceUUID } //
            );

    }

    public UUID registerIndex(final IndexMetadata metadata,
            final byte[][] separatorKeys, final UUID[] dataServiceUUIDs) {

        assertOpen();

        try {

            UUID indexUUID = getMetadataService().registerScaleOutIndex(
                    metadata, separatorKeys, dataServiceUUIDs);

            return indexUUID;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * Return the cache for {@link IIndex} objects.
     */
    abstract protected AbstractIndexCache<? extends IClientIndex> getIndexCache();
    
    /**
     * Applies an {@link AbstractIndexCache} and strengthens the return type.
     * 
     * {@inheritDoc}
     */
    public IClientIndex getIndex(final String name, final long timestamp) {

        if (log.isInfoEnabled())
            log.info("name="+name+" @ "+timestamp);
        
        assertOpen();

//BTM        return getIndexCache().getIndex(name, timestamp);

AbstractIndexCache cache = getIndexCache();
com.bigdata.btree.IRangeQuery index = cache.getIndex(name, timestamp);
log.warn("\n>>>>AbstractFederation.getIndex: name="+name+", timestamp="+timestamp+", indexCache="+cache+", index="+index+"\n");
return (IClientIndex)index;
    }

    public void dropIndex(String name) {

String dbgFlnm = "TestEmbeddedClient.txt";
        if (log.isInfoEnabled())
            log.info("name=" + name);

        assertOpen();
com.bigdata.util.Util.printStr(dbgFlnm, "    AbstractFederation.dropIndex[name="+name+"] - assertOpen = OK");

        try {

com.bigdata.util.Util.printStr(dbgFlnm, "    AbstractFederation.dropIndex - metadataService = "+getMetadataService());
            getMetadataService().dropScaleOutIndex(name);

            if (log.isInfoEnabled())
                log.info("dropped scale-out index.");
            
com.bigdata.util.Util.printStr(dbgFlnm, "    AbstractFederation.dropIndex - getIndexCache = "+getIndexCache());
            getIndexCache().dropIndexFromCache(name);

        } catch (Exception e) {

            throw new RuntimeException( e );
            
        }

    }

    public SparseRowStore getGlobalRowStore() {
        
        return globalRowStoreHelper.getGlobalRowStore();

    }

    private final GlobalRowStoreHelper globalRowStoreHelper = new GlobalRowStoreHelper(
            this);

    public BigdataFileSystem getGlobalFileSystem() {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        return globalFileSystemHelper.getGlobalFileSystem();
        return globalFileSystemHelper.getGlobalFileSystem
                                          (fedConcurrencyMgr,
                                           (IBigdataDiscoveryManagement)this);
//BTM - PRE_CLIENT_SERVICE - END
    }

    private final GlobalFileSystemHelper globalFileSystemHelper = new GlobalFileSystemHelper(this);

    public TemporaryStore getTempStore() {

//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE        return tempStoreFactory.getTempStore();
        return tempStoreFactory.getTempStore
                                    (fedConcurrencyMgr,
                                     (IBigdataDiscoveryManagement)this);
//BTM - PRE_CLIENT_SERVICE - END
    }

    private final TemporaryStoreFactory tempStoreFactory;

    /**
     * Forces the immediate reporting of the {@link CounterSet} to the
     * load balancer service. Any errors will be logged, not thrown.
     */
    public void reportCounters() {

        new ReportTask(this).run();
        
    }

    /**
     * Delegated. {@inheritDoc}
     */
    public T getService() {
    
        assertOpen();

        return (T)client.getDelegate().getService();
        
    }

    /**
     * Delegated. {@inheritDoc}
     */
    public String getServiceName() {
    
        assertOpen();

        return client.getDelegate().getServiceName();
        
    }
    
    /**
     * Delegated. {@inheritDoc}
     */
    public Class getServiceIface() {

        assertOpen();

        return client.getDelegate().getServiceIface();
        
    }
    
    /**
     * Delegated. {@inheritDoc}
     */
    public UUID getServiceUUID() {
        
        assertOpen();

        return client.getDelegate().getServiceUUID();
        
    }
    
    /**
     * Delegated. {@inheritDoc}
     */
    public boolean isServiceReady() {

        final AbstractClient<T> thisClient = this.client;

        if (thisClient == null)
            return false;

        final IFederationDelegate<T> delegate = thisClient.getDelegate();

        if (delegate == null)
            return false;
        
        // assertOpen();

        return delegate.isServiceReady();
        
    }
    
    /**
     * Delegated. {@inheritDoc}
     */
    public void reattachDynamicCounters() {
        
        assertOpen();

        client.getDelegate().reattachDynamicCounters();
        
    }

//BTM - FOR_CLIENT_SERVICE - BEGIN - required by ILocalResourceManagement
    public void reattachDynamicCounters
                                 (ResourceManager resourceMgr,
                                  IConcurrencyManager concurrencyMgr)
    {
        reattachDynamicCounters();
    }
//BTM - FOR_CLIENT_SERVICE - END

    /**
     * Delegated. {@inheritDoc}
     */
    public void didStart() {

        assertOpen();

        client.getDelegate().didStart();
        
    }

    /**
     * Delegated. {@inheritDoc}
     */
    public AbstractHTTPD newHttpd(final int httpdPort,
            final CounterSet counterSet) throws IOException {

        assertOpen();

        return client.getDelegate().newHttpd(httpdPort, counterSet);
        
    }

    /**
     * Delegated. {@inheritDoc}
     */
    public void serviceJoin(final IService service, final UUID serviceUUID) {

        if (!isOpen()) return;

        if (log.isInfoEnabled()) {

            log.info("service=" + service + ", serviceUUID" + serviceUUID);

        }

        client.getDelegate().serviceJoin(service, serviceUUID);

    }

//BTM - BEGIN
    public void serviceJoin(final Service service, final UUID serviceUUID) {
        if (!isOpen()) return;
        if (log.isInfoEnabled()) {
            log.info("service=" + service + ", serviceUUID" + serviceUUID);
        }
        client.getDelegate().serviceJoin(service, serviceUUID);
    }
//BTM - END

    /**
     * Delegated. {@inheritDoc}
     */
    public void serviceLeave(final UUID serviceUUID) {

        if(!isOpen()) return;
        
        if(log.isInfoEnabled()) {
            
            log.info("serviceUUID="+serviceUUID);
            
        }

        // @todo really, we should test like this everywhere.
        final AbstractClient thisClient = this.client;

        if (thisClient != null && thisClient.isConnected()) {

            thisClient.getDelegate().serviceLeave(serviceUUID);

        }

    }
    
//    /**
//     * Return <code>true</code> if the service startup preconditions are
//     * noticably satisified before the timeout elapsed.
//     * 
//     * @param timeout
//     * @param unit
//     * 
//     * @return <code>true</code> if the preconditions are satisified.
//     * 
//     * @throws InterruptedException
//     */
//    protected boolean awaitPreconditions(final long timeout,
//            final TimeUnit unit) throws InterruptedException {
//        
//        return client.getDelegate().isServiceReady();
//        
//    }
    
    static private String ERR_NO_SERVICE_UUID = "Service UUID is not assigned yet.";

    static private String ERR_SERVICE_NOT_READY = "Service is not ready yet.";
    
    /**
     * This task runs once. Once {@link #getServiceUUID()} reports a
     * non-<code>null</code> value, it will start an (optional)
     * {@link AbstractStatisticsCollector}, an (optional) httpd service, and
     * the (required) {@link ReportTask}.
     * <p>
     * Note: The {@link ReportTask} will relay any collected performance
     * counters to the load balancer service, but it also lets the
     * load balancer service know which services exist, which is
     * important for some of its functions.
     * <p>
     * 
     * FIXME This should explicitly await jini registrar discovery, zookeeper
     * client connected, and whatever other preconditions must be statisified
     * before the service can be started.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class StartDeferredTasksTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final protected Logger log = Logger.getLogger(StartDeferredTasksTask.class);
        
        /**
         * The timestamp when we started running this task.
         */
        final long begin = System.currentTimeMillis();
        
        private StartDeferredTasksTask() {
        }

        /**
         * @throws RuntimeException
         *             once the deferred task(s) are running to prevent
         *             re-execution of this startup task.
         */
        public void run() {

            try {
                
//                /*
//                 * @todo Work on event driven startup and event drive service
//                 * up/down more.
//                 * 
//                 * @todo Specify the timeout via IBigdataClient.Options.
//                 */
//                if (!awaitPreconditions(1000, TimeUnit.MILLISECONDS)) {
//                    
//                    if(log.isInfoEnabled())
//                        log.info("Preconditions not yet satisified.");
//                    
//                    return;
//                    
//                }
                startDeferredTasks();
            } catch (Throwable t) {

                log.warn("Problem in report task?", t);

                return;
                
            }

        }

        /**
         * Starts performance counter collection once the service {@link UUID}
         * is known.
         * 
         * @throws IOException
         *             if {@link IService#getServiceUUID()} throws this
         *             exception (it never should since it is a local method
         *             call).
         */
        protected void startDeferredTasks() throws IOException {

            // elapsed time since we started running this task.
            final long elapsed = System.currentTimeMillis() - begin;
            
            // Wait for the service ID to become available, trying every
            // two seconds, while logging failures.
            while (true) {
                if (getServiceUUID() != null) {
                    break;
                }
                if (elapsed > 1000 * 10)
                    log.warn(ERR_NO_SERVICE_UUID + " : iface="
                            + getServiceIface() + ", name=" + getServiceName()
                            + ", elapsed=" + elapsed);
                else if (log.isInfoEnabled())
                     log.info(ERR_NO_SERVICE_UUID);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                }
            }

            // Wait for the service to become ready, trying every
            // two seconds, while logging failures.
            while (true) {
                if (isServiceReady()) {
                    break;
                }
                if (elapsed > 1000 * 10)
                    log.warn(ERR_SERVICE_NOT_READY + " : iface="
                            + getServiceIface() + ", name=" + getServiceName()
                            + ", elapsed=" + elapsed);
                else if (log.isInfoEnabled())
                    log.info(ERR_SERVICE_NOT_READY + " : " + elapsed);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                }
            }
            
            /*
             * start collection on various work queues.
             * 
             * Note: The data service starts collection for its queues
             * (actually, the concurrency managers queues) once it is up and
             * running.
             * 
             * @todo have it collect using the same scheduled thread pool.
             */
            startQueueStatisticsCollection();
            
            // start collecting performance counters (if enabled).
            startPerformanceCounterCollection();

//            // notify the load balancer of this service join.
//            notifyJoin();
            
            // start reporting to the load balancer.
            startReportTask();

            // start the local httpd service reporting on this service.
            startHttpdService();
            
            // notify delegates that deferred startup has occurred.
            AbstractFederation.this.didStart();

        }

        /**
         * Setup sampling on the client's thread pool. This collects interesting
         * statistics about the thread pool for reporting to the load balancer
         * service.
         */
        protected void startQueueStatisticsCollection() {

            if (!client.getCollectQueueStatistics()) {

                if (log.isInfoEnabled())
                    log.info("Queue statistics collection disabled: "
                            + getServiceIface());

                return;

            }

            final long initialDelay = 0; // initial delay in ms.
            final long delay = 1000; // delay in ms.
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            final String relpath = "Thread Pool";

            final ThreadPoolExecutorStatisticsTask threadPoolExecutorStatisticsTask = new ThreadPoolExecutorStatisticsTask(
                    relpath, threadPool, taskCounters);

            getServiceCounterSet().makePath(relpath).attach(
                    threadPoolExecutorStatisticsTask.getCounters());

            addScheduledTask(threadPoolExecutorStatisticsTask, initialDelay,
                    delay, unit);

        }
        
        /**
         * Start collecting performance counters from the OS (if enabled).
         */
        protected void startPerformanceCounterCollection() {

            final UUID serviceUUID = getServiceUUID();

            final Properties p = getClient().getProperties();

            if (getClient().getCollectPlatformStatistics()) {

                p.setProperty(AbstractStatisticsCollector.Options.PROCESS_NAME,
                        "service" + ICounterSet.pathSeparator
                                + getServiceIface().getName()
                                + ICounterSet.pathSeparator
                                + serviceUUID.toString());

                statisticsCollector = AbstractStatisticsCollector
                        .newInstance(p);

                statisticsCollector.start();

                /*
                 * Attach the counters that will be reported by the statistics
                 * collector service.
                 */
                ((CounterSet) getCounterSet()).attach(statisticsCollector
                        .getCounters());

                if (log.isInfoEnabled())
                    log.info("Collecting platform statistics: uuid="
                            + serviceUUID);

            }

        }

        /**
         * Start task to report service and counters to the load balancer.
         */
        protected void startReportTask() {

            final Properties p = getClient().getProperties();

            final long delay = Long.parseLong(p.getProperty(
                    Options.REPORT_DELAY, Options.DEFAULT_REPORT_DELAY));

            if (log.isInfoEnabled())
                log.info(Options.REPORT_DELAY + "=" + delay);

            final TimeUnit unit = TimeUnit.MILLISECONDS;

            final long initialDelay = delay;

            addScheduledTask(new ReportTask(AbstractFederation.this),
                    initialDelay, delay, unit);

            if (log.isInfoEnabled())
                log.info("Started ReportTask.");

        }

        /**
         * Start the local httpd service (if enabled). The service is started on
         * the {@link IBigdataClient#getHttpdPort()}, on a randomly assigned
         * port if the port is <code>0</code>, or NOT started if the port is
         * <code>-1</code>. If the service is started, then the URL for the
         * service is reported to the load balancer and also written into the
         * file system. When started, the httpd service will be shutdown with
         * the federation.
         * 
         * @throws UnsupportedEncodingException
         */
        protected void startHttpdService() throws UnsupportedEncodingException {

            final String path = getServiceCounterPathPrefix();
            
            final int httpdPort = client.getHttpdPort();

            if (httpdPort == -1) {

                if (log.isInfoEnabled())
                    log.info("httpd disabled: " + path);

                return;

            }

            final AbstractHTTPD httpd;
            try {

                httpd = newHttpd(httpdPort, getCounterSet());

            } catch (IOException e) {

                log.error("Could not start httpd: port=" + httpdPort
                        + ", path=" + path, e);

                return;
                
            }

            if (httpd != null) {

                // save reference to the daemon.
                AbstractFederation.this.httpd = httpd;
                
                // the URL that may be used to access the local httpd.
                httpdURL = "http://"
                        + AbstractStatisticsCollector.fullyQualifiedHostName
                        + ":" + httpd.getPort() + "/?path="
                        + URLEncoder.encode(path, "UTF-8");

                if (log.isInfoEnabled())
                    log.info("start:\n" + httpdURL);

                // add counter reporting that url to the load balancer.
                serviceRoot.addCounter(IServiceCounters.LOCAL_HTTPD,
                        new OneShotInstrument<String>(httpdURL));

            }

        }
        
    }
    
    /**
     * Periodically report performance counter data to the
     * load balancer service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ReportTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final protected Logger log = Logger.getLogger(ReportTask.class);

        private final AbstractFederation fed;
        
        public ReportTask(AbstractFederation fed) {

            if (fed == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;
            
        }

        /**
         * Note: Don't throw anything here since we don't want to have the task
         * suppressed!
         */
        public void run() {

            try {
                
                fed.reattachDynamicCounters();

            } catch (Throwable t) {

                log.error("Could not update performance counter view : " + t, t);

            }
            
            try {

                /*
                 * Report the performance counters to the load balancer.
                 */
                
                reportPerformanceCounters();
                
            } catch (Throwable t) {

                log.error("Could not report performance counters : " + t, t);

            }

        }
        
        /**
         * Send performance counters to the load balancer.
         * 
         * @throws IOException 
         */
        protected void reportPerformanceCounters() throws IOException {

            // Note: This _is_ a local method call.
            final UUID serviceUUID = fed.getServiceUUID();
System.out.println("\n>>>>> AbstractFederation.reportPerformanceCounters: serviceUUID = "+serviceUUID);
            // Will be null until assigned by the service registrar.
            if (serviceUUID == null) {

                if(log.isInfoEnabled())
                    log.info("Service UUID not assigned yet.");

                return;

            }

            final LoadBalancer loadBalancerService = fed.getLoadBalancerService();

            if (loadBalancerService == null) {
System.out.println(">>>>> AbstractFederation.reportPerformanceCounters: loadBalancerService = NULL");

                log.warn("Could not discover load balancer service.");

                return;

            }
System.out.println(">>>>> AbstractFederation.reportPerformanceCounters: loadBalancerService = "+loadBalancerService);

            /*
             * @todo this is probably worth compressing as there will be a lot
             * of redundency.
             * 
             * @todo allow filter on what gets sent to the load balancer?
             */
            final ByteArrayOutputStream baos = new ByteArrayOutputStream(
                    Bytes.kilobyte32 * 2);

System.out.println(">>>>> AbstractFederation.reportPerformanceCounters: CALLING fed.getCounterSet() ...");
            fed.getCounterSet().asXML(baos, "UTF-8", null/* filter */);

System.out.println(">>>>> AbstractFederation.reportPerformanceCounters: CALLING loadBalancer.notify ...");
            loadBalancerService.notify(serviceUUID, baos.toByteArray());
System.out.println(">>>>> AbstractFederation.reportPerformanceCounters: DONE CALLING loadBalancer.notify");

            if (log.isInfoEnabled())
                log.info("Notified the load balancer.");
            
        }

    }

    /**
     * @todo it may be possible to optimize this for the jini case.
     */
//BTM    public IDataService[] getDataServices(final UUID[] uuids) {
public ShardService[] getDataServices(final UUID[] uuids) {
        
//BTM        final IDataService[] services = new IDataService[uuids.length];
final ShardService[] services = new ShardService[uuids.length];

        final IBigdataFederation fed = this;
        
        int i = 0;
        
        // UUID of the metadata service (if forced to discover it).
        UUID mdsUUID = null;

        for (UUID uuid : uuids) {

//BTM            IDataService service = fed.getDataService(uuid);
ShardService service = fed.getDataService(uuid);

            if (service == null) {

//BTM
ShardLocator mds = null;
                if (mdsUUID == null) {
                
                    try {
                    
//BTM                        mdsUUID = fed.getMetadataService().getServiceUUID();
mds = fed.getMetadataService();
if( mds instanceof IService ) {
    mdsUUID = ((IService)mds).getServiceUUID();
} else if( mds instanceof Service ) {
    mdsUUID = ((Service)mds).getServiceUUID();
} else {
    log.warn("wrong type for shard locator service ["+mds.getClass()+"]");
}                        
                    } catch (IOException ex) {
                        
                        throw new RuntimeException(ex);
                    
                    }
                    
                }
                
                if (uuid == mdsUUID) {

                    /*
                     * @todo getDataServices(int maxCount) DOES NOT return MDS
                     * UUIDs because we don't want people storing application
                     * data there, but getDataService(UUID) should probably work
                     * for the MDS UUID also since once you have the UUID you
                     * want the service.
                     */

//BTM                    service = fed.getMetadataService();
//BTM if( (mds != null) && (mds instanceof IDataService) ) service = (IDataService)mds;
if( (mds != null) && (mds instanceof ShardService) ) service = (ShardService)mds;

                }
                
            }

            if (service == null) {

                throw new RuntimeException("Could not discover service: uuid="
                        + uuid);

            }

            services[i++] = service;

        }
        
        return services;

    }

//BTM - BEGIN - REMOVAL OF NEED FOR ABSTRACT_FEDERATION IN EVENT SENDING MECHANISM
    /**
     * Queues up an event to be sent to the load balancer service.
     * Events are maintained on a non-blocking queue (no fixed capacity) and
     * sent by a scheduled task.
     * 
     * @param e
     * 
     * @see SendEventsTask
     */
//BTM    protected void sendEvent(final Event e) {
//BTM
//BTM        if (isOpen()) {
//BTM
//BTM            events.add(e);
//BTM
//BTM        }
//BTM        
//BTM    }
    
    /**
     * Queue of events sent periodically to the load balancer service.
     */
    final private BlockingQueue<Event> events = new LinkedBlockingQueue<Event>();

//BTM
//BTM - PRE_CLIENT_SERVICE final private EventQueue sendEventsTask;
final private EventQueueSender sendEventsTask;
    
//BTM - PRE_CLIENT_SERVICE     /**
//BTM - PRE_CLIENT_SERVICE      * Sends events to the load balancer service.
//BTM - PRE_CLIENT_SERVICE      * 
//BTM - PRE_CLIENT_SERVICE      * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//BTM - PRE_CLIENT_SERVICE      * @version $Id$
//BTM - PRE_CLIENT_SERVICE      * 
//BTM - PRE_CLIENT_SERVICE      * FIXME should discard events if too many build up on the client.
//BTM - PRE_CLIENT_SERVICE      */
//BTM - PRE_CLIENT_SERVICE //BTM    private class SendEventsTask implements Runnable {
//BTM - PRE_CLIENT_SERVICE private class SendEventsTask implements EventQueue, Runnable {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE final private BlockingQueue<Event> eventQueue;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM        public SendEventsTask() {
//BTM - PRE_CLIENT_SERVICE public SendEventsTask(BlockingQueue<Event> eventQueue) {
//BTM - PRE_CLIENT_SERVICE     this.eventQueue = eventQueue;
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM
//BTM - PRE_CLIENT_SERVICE public void queueEvent(Event e) {
//BTM - PRE_CLIENT_SERVICE     if (isOpen()) {
//BTM - PRE_CLIENT_SERVICE         eventQueue.add(e);
//BTM - PRE_CLIENT_SERVICE     }
//BTM - PRE_CLIENT_SERVICE }
//BTM - PRE_CLIENT_SERVICE         
//BTM - PRE_CLIENT_SERVICE         /**
//BTM - PRE_CLIENT_SERVICE          * Note: Don't throw anything - it will cancel the scheduled task.
//BTM - PRE_CLIENT_SERVICE          */
//BTM - PRE_CLIENT_SERVICE         public void run() {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             try {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 final LoadBalancer lbs = getLoadBalancerService();
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 if (lbs == null) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                     // Can't drain events
//BTM - PRE_CLIENT_SERVICE                     return;
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 final long begin = System.currentTimeMillis();
//BTM - PRE_CLIENT_SERVICE                 
//BTM - PRE_CLIENT_SERVICE                 final LinkedList<Event> c = new LinkedList<Event>();
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM                events.drainTo(c);
//BTM - PRE_CLIENT_SERVICE eventQueue.drainTo(c);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 /*
//BTM - PRE_CLIENT_SERVICE                  * @todo since there is a delay before events are sent along it
//BTM - PRE_CLIENT_SERVICE                  * is quite common that the end() event will have been generated
//BTM - PRE_CLIENT_SERVICE                  * such that the event is complete before we send it along.
//BTM - PRE_CLIENT_SERVICE                  * there should be an easy way to notice this and avoid sending
//BTM - PRE_CLIENT_SERVICE                  * an event twice when we can get away with just sending it
//BTM - PRE_CLIENT_SERVICE                  * once. however the decision must be atomic with respect to the
//BTM - PRE_CLIENT_SERVICE                  * state change in the event so that we do not lose any data by
//BTM - PRE_CLIENT_SERVICE                  * concluding that we have handled the event when in fact its
//BTM - PRE_CLIENT_SERVICE                  * state was not complete before we sent it along.
//BTM - PRE_CLIENT_SERVICE                  */
//BTM - PRE_CLIENT_SERVICE                 
//BTM - PRE_CLIENT_SERVICE                 for (Event e : c) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                     // avoid modification when sending the event.
//BTM - PRE_CLIENT_SERVICE                     synchronized(e) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE //BTM                        lbs.notifyEvent(e);
//BTM - PRE_CLIENT_SERVICE ((com.bigdata.service.EventReceivingService)lbs).notifyEvent(e);
//BTM - PRE_CLIENT_SERVICE                         
//BTM - PRE_CLIENT_SERVICE                     }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 if (log.isInfoEnabled()) {
//BTM - PRE_CLIENT_SERVICE                     
//BTM - PRE_CLIENT_SERVICE                     final int nevents = c.size();
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                     if (nevents > 0)
//BTM - PRE_CLIENT_SERVICE                         log.info("Sent " + c.size() + " events in "
//BTM - PRE_CLIENT_SERVICE                                 + (System.currentTimeMillis() - begin) + "ms");
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                 }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             } catch (Throwable t) {
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE                log.warn(getServiceName(), t);
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE             }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE         }
//BTM - PRE_CLIENT_SERVICE 
//BTM - PRE_CLIENT_SERVICE     }
//BTM - PRE_CLIENT_SERVICE 

    // Required by ILocalResourceManagement

    public String getHostname() {
        assertOpen();
        try {
            return ( NicUtil.getIpAddress
                         ("default.nic", 
                          ConfigDeployUtil.getString
                              ("node.serviceNetwork"),
                          false) );
        } catch(Throwable t) {
            return "UNKNOWN";
        }
    }

    public ExecutorService getThreadPool() {
        return this.getExecutorService();
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return this.getScheduledExecutorService();
    }

    public Session getSession() {
        assertOpen();
        Object serviceRef = this.getService();
        if( (serviceRef != null) && (serviceRef instanceof ISession) ) {
            return ( ((ISession)serviceRef).getSession() );
        }
        return null;
    }

    public EventQueueSender getEventQueueSender() {
        return this.sendEventsTask;
    }

    public void terminate(long timeout) {
        this.shutdown();
    }

    // For tests

    public IConcurrencyManager getConcurrencyManager() {
        return fedConcurrencyMgr;
    }

    // Nested classes

    class FedResourceManager extends ResourceManager {

        private IBigdataDiscoveryManagement discoveryManager;
        private ILocalResourceManagement localResources;
        private IIndexManager indexManager;

        FedResourceManager(IBigdataDiscoveryManagement discoveryManager,
                           ILocalResourceManagement localResources,
                           IIndexManager indexManager,
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
        public IIndexManager getIndexManager() {
            return indexManager;
        }
            
        @Override
        public ShardService getDataService() {
            throw new UnsupportedOperationException
                  ("AbstractFederation#FedResourceManager.getDataService");
        }
            
        @Override
        public UUID getDataServiceUUID() {
            throw new UnsupportedOperationException
                  ("AbstractFederation#FedResourceManager"
                   +".getDataServiceUUID");
        }
    }
//BTM - END - REMOVAL OF NEED FOR ABSTRACT_FEDERATION IN EVENT SENDING MECHANISM


}
