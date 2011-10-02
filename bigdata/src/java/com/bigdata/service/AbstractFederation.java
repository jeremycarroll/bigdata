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
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.IServiceCounters;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.counters.query.QueryUtil;
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
abstract public class AbstractFederation<T> implements IBigdataFederation<T> {

    protected static final Logger log = Logger.getLogger(IBigdataFederation.class);

    /**
     * The client (if connected).
     */
    private AbstractClient<T> client;
    
    private final boolean collectPlatformStatistics;
    private final boolean collectQueueStatistics;
    private final int httpdPort;

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
        new SendEventsTask().run();

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
     * {@inheritDoc}
     * @see IBigdataClient.Options#COLLECT_PLATFORM_STATISTICS
     */
    public boolean getCollectPlatformStatistics() {
        
        return collectPlatformStatistics;
        
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see IBigdataClient.Options#COLLECT_QUEUE_STATISTICS
     */
    public boolean getCollectQueueStatistics() {
        
        return collectQueueStatistics;
        
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see IBigdataClient.Options#HTTPD_PORT
     */
    public int getHttpdPort() {
        
        return httpdPort;
        
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
     * the {@link ILoadBalancerService}.
     */
    private final TaskCounters taskCounters = new TaskCounters();

    /**
     * Counters for each scale-out index accessed by the client.
     * 
     * @todo A hard reference map is used to prevent the counters from being
     *       finalized so that they will reflect the use by the client over the
     *       life of its operations. However, this could be a problem for an
     *       {@link IDataService} or {@link IClientService} since any number of
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
     * reported by the client to the {@link ILoadBalancerService}.
     */
    public TaskCounters getTaskCounters() {

        return taskCounters;

    }
    
    /**
     * Return the {@link ScaleOutIndexCounters} for the specified scale-out index
     * for this client. There is only a single instance per scale-out index and
     * all operations by this client on that index are aggregated by that
     * instance. These counters are reported by the client to the
     * {@link ILoadBalancerService}.
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
        
                t = new ScaleOutIndexCounters(this);
                
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
     * for reporting to the {@link ILoadBalancerService}.
     */
    private AbstractStatisticsCollector statisticsCollector;
    
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

    final synchronized public CounterSet getCounters() {

        if (countersRoot == null) {

        countersRoot = new CounterSet();
        {

            final AbstractStatisticsCollector tmp = statisticsCollector;

            if (tmp != null) {

                countersRoot.attach(tmp.getCounters());

            }
        
        }

        serviceRoot = countersRoot.makePath(getServiceCounterPathPrefix());

        {

            final String s = httpdURL;
            
            if (s != null) {
            
                // add counter reporting that url to the load balancer.
                serviceRoot.addCounter(IServiceCounters.LOCAL_HTTPD,
                        new OneShotInstrument<String>(httpdURL));

            }
            
        }
            
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

        return (CounterSet) getCounters().getPath(pathPrefix);
        
    }
    
    public CounterSet getServiceCounterSet() {

        // note: defines [serviceRoot] as side effect.
        getCounters();
        
        return serviceRoot;
        
    }

    public String getServiceCounterPathPrefix() {

        final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

        return getServiceCounterPathPrefix(getServiceUUID(), getServiceIface(),
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
    static public String getServiceCounterPathPrefix(final UUID serviceUUID,
            final Class serviceIface, final String hostname) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        if (serviceIface == null)
            throw new IllegalArgumentException();
        
        if (hostname == null)
            throw new IllegalArgumentException();
        
        final String ps = ICounterSet.pathSeparator;

        final String pathPrefix = ps + hostname + ps + "service" + ps
                + serviceIface.getName() + ps + serviceUUID + ps;

        return pathPrefix;

    }
    
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

        final Properties properties = client.getProperties();
        {
            
            collectPlatformStatistics = Boolean.parseBoolean(properties
                    .getProperty(Options.COLLECT_PLATFORM_STATISTICS,
                            Options.DEFAULT_COLLECT_PLATFORM_STATISTICS));

            if (log.isInfoEnabled())
                log.info(Options.COLLECT_PLATFORM_STATISTICS + "="
                        + collectPlatformStatistics);
            
        }

        {
            
            collectQueueStatistics = Boolean.parseBoolean(properties
                    .getProperty(Options.COLLECT_QUEUE_STATISTICS,
                            Options.DEFAULT_COLLECT_QUEUE_STATISTICS));

            if (log.isInfoEnabled())
                log.info(Options.COLLECT_QUEUE_STATISTICS + "="
                        + collectQueueStatistics);
            
        }

        {

            httpdPort = Integer.parseInt(properties.getProperty(
                    Options.HTTPD_PORT,
                    Options.DEFAULT_HTTPD_PORT));

            if (log.isInfoEnabled())
                log.info(Options.HTTPD_PORT+ "="
                        + httpdPort);

            if (httpdPort < 0 && httpdPort != -1)
                throw new RuntimeException(
                        Options.HTTPD_PORT
                                + " must be -1 (disabled), 0 (random port), or positive");

        }
        
        addScheduledTask(
                new SendEventsTask(),// task to run.
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

                final ILoadBalancerService loadBalancerService = getLoadBalancerService();

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

                        dataServiceUUID = getAnyDataService().getServiceUUID();

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

        return getIndexCache().getIndex(name, timestamp);
        
    }

    public void dropIndex(String name) {

        if (log.isInfoEnabled())
            log.info("name=" + name);

        assertOpen();

        try {

            getMetadataService().dropScaleOutIndex(name);

            if (log.isInfoEnabled())
                log.info("dropped scale-out index.");
            
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

        return globalFileSystemHelper.getGlobalFileSystem();

    }

    private final GlobalFileSystemHelper globalFileSystemHelper = new GlobalFileSystemHelper(
            this);

    public TemporaryStore getTempStore() {

        return tempStoreFactory.getTempStore();

    }

    private final TemporaryStoreFactory tempStoreFactory;

    /**
     * Forces the immediate reporting of the {@link CounterSet} to the
     * {@link ILoadBalancerService}. Any errors will be logged, not thrown.
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
            final ICounterSetAccess accessor) throws IOException {

        assertOpen();

        return client.getDelegate().newHttpd(httpdPort, accessor);
        
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
     * This task starts an (optional) {@link AbstractStatisticsCollector}, an
     * (optional) httpd service, and the (required) {@link ReportTask}.
     * <p>
     * Note: The {@link ReportTask} will relay any collected performance
     * counters to the {@link ILoadBalancerService}, but it also lets the
     * {@link ILoadBalancerService} know which services exist, which is
     * important for some of its functions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    protected class StartDeferredTasksTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final private Logger log = Logger.getLogger(StartDeferredTasksTask.class);
        
        /**
         * The timestamp when we started running this task.
         */
        final long begin = System.currentTimeMillis();
        
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
         * 
         * @throws IOException
         *             if {@link IDataService#getServiceUUID()} throws this
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
	     * Start collecting performance counters (if enabled).
	     *
	     * Note: This needs to be done first since the counters from the
	     * platform will otherwise not be incorporated into those
	     * reported by the federation.
	     */
            startPlatformStatisticsCollection();

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

            if (!getCollectQueueStatistics()) {

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
        protected void startPlatformStatisticsCollection() {

            final UUID serviceUUID = getServiceUUID();

            final Properties p = getClient().getProperties();

            if (!getCollectPlatformStatistics()) {

                return;

            }

            p.setProperty(AbstractStatisticsCollector.Options.PROCESS_NAME,
                    "service" + ICounterSet.pathSeparator
                            + getServiceIface().getName()
                            + ICounterSet.pathSeparator
                            + serviceUUID.toString());

            {

                final AbstractStatisticsCollector tmp = AbstractStatisticsCollector
                        .newInstance(p);

                tmp.start();
                
                // Note: synchronized to keep findbugs happy.
                synchronized (AbstractFederation.this) {
                
                    statisticsCollector = tmp;
                    
                }
                
            }

            if (log.isInfoEnabled())
                log.info("Collecting platform statistics: uuid=" + serviceUUID);

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

			if (delay > 0L) {

				final TimeUnit unit = TimeUnit.MILLISECONDS;

				final long initialDelay = delay;

				addScheduledTask(new ReportTask(AbstractFederation.this),
						initialDelay, delay, unit);

				if (log.isInfoEnabled())
					log.info("Started ReportTask.");

			}

        }

		/**
		 * Start the local httpd service (if enabled). The service is started on
		 * the {@link #getHttpdPort()}, on a randomly assigned port if the port
		 * is <code>0</code>, or NOT started if the port is <code>-1</code>. If
		 * the service is started, then the URL for the service is reported to
		 * the load balancer and also written into the file system. When
		 * started, the httpd service will be shutdown with the federation.
		 * 
		 * @throws UnsupportedEncodingException
		 */
        protected void startHttpdService() throws UnsupportedEncodingException {

            final String path = getServiceCounterPathPrefix();
            
            final int httpdPort = getHttpdPort();

            if (httpdPort == -1) {

                if (log.isInfoEnabled())
                    log.info("httpd disabled: " + path);

                return;

            }

            final AbstractHTTPD httpd;
            try {

                httpd = newHttpd(httpdPort, AbstractFederation.this);

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

            }

        }
        
    } // class StartDeferredTasks
    
    /**
     * Periodically report performance counter data to the
     * {@link ILoadBalancerService}.
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

            // Will be null until assigned by the service registrar.
            if (serviceUUID == null) {

                if(log.isInfoEnabled())
                    log.info("Service UUID not assigned yet.");

                return;

            }

            final ILoadBalancerService loadBalancerService = fed.getLoadBalancerService();

            if (loadBalancerService == null) {

                log.warn("Could not discover load balancer service.");

                return;

            }

	    if(serviceUUID.equals(loadBalancerService.getServiceUUID())) {
		// Do not notify ourselves.
		return;
	    }

			/*
			 * @todo When sending all counters, this is probably worth
			 * compressing as there will be a lot of redundancy and a lot of
			 * data.
			 */
			final ByteArrayOutputStream baos = new ByteArrayOutputStream(
					Bytes.kilobyte32 * 2);

			final Properties p = fed.getClient().getProperties();

			final boolean reportAll = Boolean.valueOf(p.getProperty(
					Options.REPORT_ALL, Options.DEFAULT_REPORT_ALL));

			fed.getCounters().asXML(
					baos,
					"UTF-8",
					reportAll ? null/* filter */: QueryUtil
							.getRequiredPerformanceCountersFilter());

			if (log.isInfoEnabled())
				log.info("reportAll=" + reportAll + ", service="
						+ fed.getServiceName() + ", #bytesReported="
						+ baos.size());

            loadBalancerService.notify(serviceUUID, baos.toByteArray());

            if (log.isInfoEnabled())
                log.info("Notified the load balancer.");

		}

	}

    /**
     * @todo it may be possible to optimize this for the jini case.
     */
    public IDataService[] getDataServices(final UUID[] uuids) {
        
        final IDataService[] services = new IDataService[uuids.length];

        final IBigdataFederation fed = this;
        
        int i = 0;
        
        // UUID of the metadata service (if forced to discover it).
        UUID mdsUUID = null;

        for (UUID uuid : uuids) {

            IDataService service = fed.getDataService(uuid);

            if (service == null) {

                if (mdsUUID == null) {
                
                    try {
                    
                        mdsUUID = fed.getMetadataService().getServiceUUID();
                        
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

                    service = fed.getMetadataService();
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

    /**
     * Queues up an event to be sent to the {@link ILoadBalancerService}.
     * Events are maintained on a non-blocking queue (no fixed capacity) and
     * sent by a scheduled task.
     * 
     * @param e
     * 
     * @see SendEventsTask
     */
    protected void sendEvent(final Event e) {

        if (isOpen()) {

            events.add(e);

        }
        
    }
    
    /**
     * Queue of events sent periodically to the {@link ILoadBalancerService}.
     */
    final private BlockingQueue<Event> events = new LinkedBlockingQueue<Event>();
    
    /**
     * Sends events to the {@link ILoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * FIXME should discard events if too many build up on the client.
     */
    private class SendEventsTask implements Runnable {

        public SendEventsTask() {
            
        }
        
        /**
         * Note: Don't throw anything - it will cancel the scheduled task.
         */
        public void run() {

            try {

                final ILoadBalancerService lbs = getLoadBalancerService();

                if (lbs == null) {

                    // Can't drain events
                    return;

                }

                final long begin = System.currentTimeMillis();
                
                final LinkedList<Event> c = new LinkedList<Event>();

                events.drainTo(c);

                /*
                 * @todo since there is a delay before events are sent along it
                 * is quite common that the end() event will have been generated
                 * such that the event is complete before we send it along.
                 * there should be an easy way to notice this and avoid sending
                 * an event twice when we can get away with just sending it
                 * once. however the decision must be atomic with respect to the
                 * state change in the event so that we do not lose any data by
                 * concluding that we have handled the event when in fact its
                 * state was not complete before we sent it along.
                 */
                
                for (Event e : c) {

                    // avoid modification when sending the event.
                    synchronized(e) {

                        lbs.notifyEvent(e);
                        
                    }

                }

                if (log.isInfoEnabled()) {
                    
                    final int nevents = c.size();

                    if (nevents > 0)
                        log.info("Sent " + c.size() + " events in "
                                + (System.currentTimeMillis() - begin) + "ms");

                }

            } catch (Throwable t) {

                log.warn(getServiceName(), t);

            }

        }

    }

}
