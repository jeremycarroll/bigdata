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

package com.bigdata.resources;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IDataServiceCounters;
import com.bigdata.counters.IProcessCounters;
import com.bigdata.counters.IStatisticsCollector;
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.event.EventQueueSender;
import com.bigdata.event.EventQueueSenderTask;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ConcurrencyManager.IConcurrencyManagerCounters;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.IndexManager.IIndexManagerCounters;
import com.bigdata.resources.ResourceManager.IResourceManagerCounters;
import com.bigdata.service.Event;
import com.bigdata.service.Session;
import com.bigdata.service.ndx.ScaleOutIndexCounters;
import com.bigdata.util.Util;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.TaskCounters;
import com.bigdata.util.concurrent.ThreadPoolExecutorStatisticsTask;
import com.bigdata.util.config.LogUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of <code>ILocalResourceManagement</code> that that
 * encapsulates functionality for creating and managing information and
 * resources in the local VM.
 */
public class LocalResourceManager implements ILocalResourceManagement {

    private Class serviceIface;
    private String serviceName;
    private UUID serviceUUID;
    private String hostname;
    private IBigdataDiscoveryManagement discoveryMgr;
    private Logger logger;
    private Properties properties;

    private final ThreadPoolExecutor threadPool;
    private final ScheduledExecutorService scheduledExecutor =
                      Executors.newSingleThreadScheduledExecutor
                          (new DaemonThreadFactory
                                   (getClass().getName()+".sampleService"));

    private ScheduledFuture<?> queueStatsTaskFuture;
    private TaskCounters taskCounters = new TaskCounters();
    private AbstractStatisticsCollector statisticsCollector;
    private CounterSet countersRoot = new CounterSet();

    private BlockingQueue<Event> blockingEventQueue =
                                     new LinkedBlockingQueue<Event>();
    private EventQueueSender eventQueueSenderTask;

    private Session session = new Session();

    private long lastReattachMillis = 0L;

    /**
     * Counters for each scale-out index that is accessed.
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
    private final Map<String, ScaleOutIndexCounters> scaleOutIndexCounters =
                      new HashMap<String, ScaleOutIndexCounters>();

    public LocalResourceManager(Class serviceIface,
                                String serviceName,
                                UUID serviceUUID,
                                String hostname,
                                IBigdataDiscoveryManagement discoveryMgr,
                                Logger logger,
                                int threadPoolSize,
                                boolean collectQueueStatistics,
                                boolean collectPlatformStatistics,
                                Properties properties)
    {
        if (serviceIface == null) {
            throw new NullPointerException("null serviceIface");
        }
        if (serviceName == null) {
            throw new NullPointerException("null serviceName");
        }
        if (serviceUUID == null) {
            throw new NullPointerException("null serviceUUID");
        }
        if (hostname == null) {
            throw new NullPointerException("null hostname");
        }
        if (discoveryMgr == null) {
            throw new NullPointerException("null discoveryMgr");
        }
        if (threadPoolSize < 0) {
            throw new IllegalArgumentException
                          ("negative threadPoolSize ["+threadPoolSize+"]");
        }
        this.serviceIface = serviceIface;
        this.serviceName = serviceName;
        this.serviceUUID = serviceUUID;
        this.hostname = hostname;
        this.discoveryMgr = discoveryMgr;
        this.statisticsCollector = statisticsCollector;
        this.logger  = (logger == null ? 
                        LogUtil.getLog4jLogger((this.getClass()).getName()) :
                        logger);
        this.properties = (Properties)properties.clone();

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

        this.statisticsCollector = 
            AbstractStatisticsCollector.newInstance(this.properties);

        getServiceCounterSet(true);//add basic counters

        //start queue statistics collection task (runs every 1 sec)

        if(collectQueueStatistics) {//queue stats collected every 1 sec
            long queueStatsDelay = 0L;//no initial delay
            long queueStatsPeriod = 1000L;
            String threadPoolName = 
                "LocalResourceManager-Queue-Statistics-Collection";
            ThreadPoolExecutorStatisticsTask queueStatsTask =
                new ThreadPoolExecutorStatisticsTask
                ( threadPoolName,
                  (ThreadPoolExecutor)(this.getThreadPool()),
                  taskCounters );
            (this.getServiceCounterSet()).makePath(threadPoolName).attach
                                               (queueStatsTask.getCounters());
            this.queueStatsTaskFuture = 
                (this.getScheduledExecutor()).scheduleWithFixedDelay
                                                  (queueStatsTask,
                                                   queueStatsDelay,
                                                   queueStatsPeriod,
                                                   TimeUnit.MILLISECONDS);
        }

        if (collectPlatformStatistics) {
            this.statisticsCollector.start();
            if( logger.isDebugEnabled() ) {
                logger.debug("collecting platform statistics "
                             +"["+this.serviceUUID+"]");
            }
        }

        this.eventQueueSenderTask = 
            new EventQueueSenderTask
                    (this.blockingEventQueue, this.discoveryMgr,
                     this.serviceName, this.logger);
    }


    // Required by ILocalResourceManagement

    public Class getServiceIface() {
        return serviceIface;
    }

    public String getServiceName() {
        return serviceName;
    }

    public UUID getServiceUUID() {
        return serviceUUID;
    }

    public String getHostname() {
        return hostname;
    }

    public ExecutorService getThreadPool() {
        return threadPool;
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    public Session getSession() {
        return session;
    }

    public ScaleOutIndexCounters getIndexCounters(String name) {

        if (name == null) {
            throw new NullPointerException("null name");
        }

        synchronized (scaleOutIndexCounters) {
            ScaleOutIndexCounters t = scaleOutIndexCounters.get(name);
            if (t == null) {
                t = new ScaleOutIndexCounters(scheduledExecutor);
                scaleOutIndexCounters.put(name, t);

                //attach to the counters reported to the load balancer
                //replace them if they already exist
                CounterSet counterSet = getServiceCounterSet();
                counterSet.makePath("Indices").makePath(name).attach
                                               (t.getCounters(), true);
            }
            return t;
        }
    }

    synchronized public void reattachDynamicCounters
                                 (ResourceManager resourceMgr,
                                  IConcurrencyManager concurrencyMgr)
    {
        final long now = System.currentTimeMillis();
        final long elapsed = now - lastReattachMillis;

        if(resourceMgr.isRunning() && (elapsed > 5000)) {
            CounterSet serviceRoot = getServiceCounterSet();

            //ensure path exists
            final CounterSet tmp1 = 
                             serviceRoot.makePath(IProcessCounters.Memory);

            //add counters reporting on the various DirectBufferPools.
            synchronized (tmp1) {

                tmp1.detach("DirectBufferPool");//detach old counters (if any)

                //attach current counters.
                tmp1.makePath("DirectBufferPool").attach
                                           (DirectBufferPool.getCounters());
            }

            // lock manager
            {
                // the lock manager is a direct child of this node.
                final CounterSet tmp2 = 
                        (CounterSet) serviceRoot.makePath
                            (IDataServiceCounters.concurrencyManager
                             + ICounterSet.pathSeparator
                             + IConcurrencyManagerCounters.writeService);

                synchronized (tmp2) {

                    // Note: detach and then attach since that wipes out
                    // any counter set nodes for queues which no longer
                    // exist. Otherwise they will build up forever.

                    // detach the old counters.
                    tmp2.detach(IConcurrencyManagerCounters.LockManager);

                    // attach the new counters.
                    ((CounterSet) tmp2.makePath
                        (IConcurrencyManagerCounters.LockManager)).attach
                            (concurrencyMgr.getWriteService()
                                .getLockManager().getCounters());
                }
            }//end lock manager
                
            // live indices.
            {
                // The counters for the index manager within the
                // service's counter hierarchy.
                //
                // Note: indices are a direct child of current node.

                final CounterSet tmp3 = 
                    (CounterSet) serviceRoot.getPath
                        ( IDataServiceCounters.resourceManager
                          + ICounterSet.pathSeparator
                          + IResourceManagerCounters.IndexManager );

                synchronized (tmp3) {

                    // Note: detach and then attach since that wipes out
                    // any counter set nodes for index partitions which no
                    // longer exist. Otherwise they will build up forever.

                    final boolean exists = 
                        tmp3.getPath(IIndexManagerCounters.Indices) != null;

                    // detach the index partition counters.
                    tmp3.detach(IIndexManagerCounters.Indices);

                    // attach the current index partition counters.
                    ((CounterSet) tmp3.makePath
                        (IIndexManagerCounters.Indices)).attach
                            (resourceMgr.getIndexCounters());

                    if (logger.isDebugEnabled()) {
                        logger.debug("attached index partition counters "
                                     +"[preexisting="+exists+", path="
                                     + tmp3.getPath()+"]");
                    }
                }
            }//end live indices

            lastReattachMillis = now;

        }//endif(resourceMgr.isRunning && elapsed > 5000)
    }

    public CounterSet getServiceCounterSet() {
        return getServiceCounterSet(false);
    }

    public CounterSet getServiceCounterSet(boolean addCounters) {
        if (countersRoot == null) {
            countersRoot = getCounterSet();
        }
        String serviceCounterPathPrefix = getServiceCounterPathPrefix();
        CounterSet serviceRoot = 
                       countersRoot.makePath(serviceCounterPathPrefix);
        if(addCounters) {
            AbstractStatisticsCollector.addBasicServiceOrClientCounters
                (serviceRoot, serviceName, serviceIface, properties);
        }
        return serviceRoot;        
    }

    public CounterSet getHostCounterSet() {
        String pathPrefix = 
               ICounterSet.pathSeparator
               + AbstractStatisticsCollector.fullyQualifiedHostName;
        CounterSet countersRoot = getCounterSet();
        return (CounterSet) countersRoot.getPath(pathPrefix);
    }
    
    public CounterSet getCounterSet() {
        CounterSet countersRoot = new CounterSet();
        if (statisticsCollector != null) {
            countersRoot.attach(statisticsCollector.getCounters());
        }
        return countersRoot;
    }

    public String getServiceCounterPathPrefix() {
        final String ps = ICounterSet.pathSeparator;
        final String pathPrefix = ps+hostname+ps+"service"
                                  +ps+serviceIface.getName()
                                  +ps+serviceUUID
                                  +ps;
        return pathPrefix;
    }

    public TaskCounters getTaskCounters() {
        return taskCounters;
    }

    public EventQueueSender getEventQueueSender() {
        return eventQueueSenderTask;
    }

    public void terminate(long timeout) {
        if (statisticsCollector != null) {
            statisticsCollector.stop();
            statisticsCollector = null;
        }

        //false ==> allow in-progress tasks to complete
        queueStatsTaskFuture.cancel(false);

        Util.shutdownExecutorService
                 (scheduledExecutor, timeout,
                  serviceName+".scheduledExecutor", logger);

        threadPool.shutdownNow();

        //send one last event report (same logic as in AbstractFederation)
        new EventQueueSenderTask
            (blockingEventQueue, discoveryMgr, serviceName, logger).run();
    }
}
