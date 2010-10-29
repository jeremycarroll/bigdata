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

package com.bigdata.executor;

import static com.bigdata.executor.Constants.*;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.ICounterSet;
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.jini.BigdataDiscoveryManager;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.LocalTransactionManager;
import com.bigdata.journal.ScaleOutIndexManager;
import com.bigdata.journal.TransactionService;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.resources.LocalResourceManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.CallableExecutor;
import com.bigdata.service.ClientTaskWrapper;
import com.bigdata.service.IClientServiceCallable;
import com.bigdata.service.ISession;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.MetadataIndexCachePolicy;
import com.bigdata.service.Service;
import com.bigdata.service.Session;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardService;
import com.bigdata.util.config.LogUtil;
import com.bigdata.zookeeper.ZooKeeperAccessor;

import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import org.apache.zookeeper.data.ACL;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

// Make package protected in the future when possible
public
class EmbeddedCallableExecutor implements CallableExecutor,
                                          Service, 
                                          ISession,
                                          IServiceShutdown
{
    public static Logger logger =
        LogUtil.getLog4jLogger((EmbeddedCallableExecutor.class).getName());

    public interface Options extends 
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
    private boolean open = true;

    public EmbeddedCallableExecutor
               (final UUID serviceUUID,
                final String hostname,
                final ServiceDiscoveryManager sdm,
                final TransactionService embeddedTxnService,
                final LoadBalancer embeddedLoadBalancer,
                final ShardLocator embeddedShardLocator,
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
                final Properties properties)
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
                                        embeddedShardLocator,
                                        embeddedDataServiceMap,
                                        logger);

// BTM boolean collectPlatformStatistics = 
//            Boolean.parseBoolean
//                ( this.properties.getProperty
//                    ( Options.COLLECT_PLATFORM_STATISTICS,
//                      Options.DEFAULT_COLLECT_PLATFORM_STATISTICS) );
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

System.out.println("\nEmbeddedCallableExecutor >>> NEW StoreManager - BEGIN");
        this.resourceMgr = 
            new ClientResourceManager(this.discoveryMgr,
                                      this.localResources,
                                      this.indexMgr,
                                      this.properties);
System.out.println("\nEmbeddedCallableExecutor >>> NEW StoreManager - END");

        this.localTransactionMgr = new LocalTransactionManager(discoveryMgr);
        this.concurrencyMgr = 
            new ConcurrencyManager(this.properties,
                                   this.localTransactionMgr,
                                   this.resourceMgr);
        //WARN: circular refs
        (this.resourceMgr).setConcurrencyManager(this.concurrencyMgr);
        (this.indexMgr).setConcurrencyManager(this.concurrencyMgr);

        //start event queue/sender task (sends events every 2 secs)
        long sendEventsDelay = 100L;//one-time initial delay
        long sendEventsPeriod = 2000L;
        (localResources.getScheduledExecutor())
            .scheduleWithFixedDelay(localResources.getEventQueueSender(),
                                    sendEventsDelay, sendEventsPeriod,
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

    @Override
    final public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized void shutdown() {
        if( !isOpen() ) return;

        if (concurrencyMgr != null) {
            concurrencyMgr.shutdown();
        }
        if (localTransactionMgr != null) {
            localTransactionMgr.shutdown();
        }
        if (resourceMgr != null) {
            resourceMgr.shutdown();
        }
        if (indexMgr != null) indexMgr.destroy();
        if (localResources != null) {
            localResources.terminate(EXECUTOR_TERMINATION_TIMEOUT);
        }
        open = false;
    }

    @Override
    public synchronized void shutdownNow() {
        shutdown();
    }

    // Required by CallableExecutor interface

    /**
     * @todo Map/reduce can be handled in the this manner.
     *       <p>
     *       Note that we have excellent locators for the best data service when
     *       the map/reduce input is the scale-out repository since the task
     *       should run on the data service that hosts the file block(s). When
     *       failover is supported, the task can run on the service instance
     *       with the least load. When the input is a networked file system,
     *       then additional network topology smarts would be required to make
     *       good choices.
     */
    public <T> Future<T> submit(final IClientServiceCallable<T> task) {
        setupLoggingContext();
        try {
            if (task == null) {
                throw new IllegalArgumentException("null task");
            }
            // submit the task and return its Future.
            return (localResources.getThreadPool()).submit
                       ( new ClientTaskWrapper(indexMgr, localResources,
                                               this, task,
                                               zkAccessor.getZookeeper(),
                                               zkAcl, zkRoot) );
        } catch(InterruptedException e) {
            logger.warn("interrupt received while retrieving zookeeper client");
            return null;
        } finally {
            clearLoggingContext();
        }
    }

    // Private methods of this class

    private void setupLoggingContext() {
        try {
            MDC.put("serviceUUID", localResources.getServiceUUID());
            MDC.put("serviceName", localResources.getServiceName());
            MDC.put("hostname", localResources.getHostname());
        } catch(Throwable t) { /* swallow */ }
    }

    private void clearLoggingContext() {
        MDC.remove("serviceName");
        MDC.remove("serviceUUID");
        MDC.remove("hostname");
    }

    // Nested classes

    class ClientResourceManager extends ResourceManager {

        private IBigdataDiscoveryManagement discoveryMgr;
        private ILocalResourceManagement localResources;
        private ScaleOutIndexManager indexMgr;

        ClientResourceManager(IBigdataDiscoveryManagement discoveryMgr,
                              ILocalResourceManagement localResources,
                              ScaleOutIndexManager indexMgr,
                              Properties properties)
        {
            super(properties);
            this.discoveryMgr = discoveryMgr;
            this.localResources = localResources;
            this.indexMgr = indexMgr;
        }

        @Override
        public IBigdataDiscoveryManagement getDiscoveryManager() {
            return discoveryMgr;
        }

        @Override
        public ILocalResourceManagement getLocalResourceManager() {
            return localResources;
        }

        @Override
        public ScaleOutIndexManager getIndexManager() {
            return indexMgr;
        }
            
        @Override
        public ShardService getDataService() {
            throw new UnsupportedOperationException
                          ("EmbeddedCallableExecutor.ClientResourceManager");
        }
            
        @Override
        public UUID getDataServiceUUID() {
            throw new UnsupportedOperationException
                          ("EmbeddedCallableExecutor.ClientResourceManager");
        }
    }
}
