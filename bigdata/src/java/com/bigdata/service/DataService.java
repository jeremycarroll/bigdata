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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.rmi.NoSuchObjectException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.Banner;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IServiceCounters;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexProcedureTask;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.StoreManager;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.httpd.AbstractHTTPD;

/**
 * An implementation of a network-capable {@link IDataService}. The service is
 * started using the {@link DataServer} class. Operations are submitted using an
 * {@link IConcurrentManager#submitAndGetResult(AbstractTask)} and will run with the
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
abstract public class DataService extends AbstractService
    implements IDataService, IServiceShutdown //IWritePipeline
{

    public static final Logger log = Logger.getLogger(DataService.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();
    
    /**
     * Options understood by the {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options,
            com.bigdata.journal.ConcurrencyManager.Options,
            com.bigdata.resources.ResourceManager.Options,
            com.bigdata.counters.AbstractStatisticsCollector.Options
            // @todo local tx manager options?
            {
     
//        /**
//         * The delay between scheduled invocations of the {@link StatusTask}.
//         * 
//         * @see #DEFAULT_STATUS_DELAY
//         */
//        String STATUS_DELAY = "statusDelay";
//        
//        /**
//         * The default {@link #STATUS_DELAY}.
//         */
//        String DEFAULT_STATUS_DELAY = "10000";
//    
//        /**
//         * An optional regular expression that will be used to filter the
//         * performance counters reported by the {@link StatusTask}. Some
//         * examples are:
//         * <dl>
//         * <dt>.*Unisolated.*</dt>
//         * <dd>All counters dealing with unisolated operations.</dd>
//         * <dt>.*Unisolated Write Service/#.*</dt>
//         * <dd>All counters for the unisolated write service.</dd>
//         * </dl>
//         * <p>
//         * Note: if the regular expression can not be compiled then an error
//         * message will be logged and ALL counters will be logged by the
//         * {@link StatusTask} (the filter will default to <code>null</code> in
//         * the case of an error).
//         * 
//         * @see #DEFAULT_STATUS_FILTER
//         */
//        String STATUS_FILTER = "statusFilter";
//        
//        /**
//         * @todo work up a more interesting default filter.
//         */
//        // String DEFAULT_STATUS_FILTER = ".*Unisolated.*";
//        String DEFAULT_STATUS_FILTER = ".*Unisolated Write Service/(#.*|averageQueueLength)";        

//        /**
//         * Boolean option for the collection of statistics from the underlying
//         * operating system (default
//         * {@value #DEFAULT_COLLECT_PLATFORM_STATISTICS}).
//         * 
//         * @see AbstractStatisticsCollector#newInstance(Properties)
//         */
//        String COLLECT_PLATFORM_STATISTICS = "collectPlatformStatistics";
//
//        String DEFAULT_COLLECT_PLATFORM_STATISTICS = "true";
//
//        /**
//         * Boolean option for the collection of statistics from the various
//         * queues using to run tasks (default
//         * {@link #DEFAULT_COLLECT_QUEUE_STATISTICS}).
//         * 
//         * @see QueueStatisticsTask
//         */
//        String COLLECT_QUEUE_STATISTICS = "collectQueueStatistics";
//
//        String DEFAULT_COLLECT_QUEUE_STATISTICS = "true";
//
//        /**
//         * Integer option specifies the port on which an httpd service will be
//         * started that exposes the {@link CounterSet} for the client. When ZERO
//         * (0), a random port will be used. The httpd service may be disabled by
//         * specifying <code>-1</code> as the port.
//         */
//        String HTTPD_PORT = "httpdPort";
//
//        /**
//         * The default http service port is ZERO (0), which means that a random
//         * port will be choosen.
//         */
//        String DEFAULT_HTTPD_PORT = "0";
        
        /**
         * The delay between scheduled invocations of the {@link ReportTask} (60
         * seconds).
         * 
         * @see #DEFAULT_REPORT_DELAY
         */
        String REPORT_DELAY = "reportDelay";
        
        /**
         * The default {@link #REPORT_DELAY}.
         */
        String DEFAULT_REPORT_DELAY = ""+(60*1000);
    
    }
    
    /**
     * @todo improve reporting here and for block write as well (goes through
     *       unisolated tasks at the present).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class ReadBlockCounters {
        
        /** #of block read requests. */
        long readBlockCount, readBlockErrorCount, readBlockBytes, readBlockNanos;
        
        public ReadBlockCounters() {
        
        }
        
    }
    
    /**
     * Counters for the block read API.
     */
    final private ReadBlockCounters readBlockApiCounters = new ReadBlockCounters();

    private ResourceManager resourceManager;
    private ConcurrencyManager concurrencyManager;
    private AbstractLocalTransactionManager localTransactionManager;
    
    /**
     * Local httpd service exposing the live {@link CounterSet} for the
     * {@link DataService}.
     */
    private AbstractHTTPD httpd;
    
    /**
     * Note: this value is not bound until the {@link #getServiceUUID()} reports
     * a non-null value.
     * 
     * @see StartDeferredTasksTask
     * @see ReportTask
     */
    private AbstractStatisticsCollector statisticsCollector;

//    /**
//     * <code>true</code> iff we will collect O/S statistics.
//     * 
//     * @see Options#COLLECT_PLATFORM_STATISTICS
//     */
//    final boolean collectPlatformStatistics;
//    
//    /**
//     * <code>true</code> iff we will collect queue statistics.
//     * 
//     * @see Options#COLLECT_QUEUE_STATISTICS
//     */
//    final boolean collectQueueStatistics;
//
//    /**
//     * The port on which the httpd service will be started.
//     * 
//     * @see Options#HTTPD_PORT
//     */
//    final int httpdPort;
    
    /**
     * Runs a {@link ReportTask} communicating performance counters on a
     * periodic basis to the {@link ILoadBalancerService}.
     */
    private ScheduledExecutorService reportService;
    
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
     * Return the proxy used to access other services in the federation.
     * 
     * @todo access to the {@link ITimestampService}, {@link IMetadataService}
     *       and the {@link ILoadBalancerService} now goes through this method.
     *       The code making those requests needs to be modified since it used
     *       to except a <code>null</code> return for the individual services
     *       if they were not available and it can not see an exception if the
     *       federation itself is not available (I believe that this can only
     *       happen for the JiniFederation since the various embedded
     *       federations wind up returning a closely held reference whereas the
     *       JiniFederation is obtained from the JiniClient#getClient().
     */
    abstract public IBigdataFederation getFederation();

    /**
     * Returns the {@link IResourceManager}.
     * 
     * @param properties
     *            Properties to configure that object.
     * 
     * @return The {@link IResourceManager}.
     */
    protected IResourceManager newResourceManager(Properties properties) {

        return new ResourceManager(properties) {

            public IBigdataFederation getFederation() {
                
                return DataService.this.getFederation();
                                
            }
            
            public UUID getDataServiceUUID() {

                return DataService.this.getServiceUUID();
                
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
    protected DataService(Properties properties) {
        
        // show the copyright banner during statup.
        Banner.banner();

        this.properties = (Properties) properties.clone();

//        {
//
//            collectPlatformStatistics = Boolean.parseBoolean(properties
//                    .getProperty(Options.COLLECT_PLATFORM_STATISTICS,
//                            Options.DEFAULT_COLLECT_PLATFORM_STATISTICS));
//
//            if (INFO)
//                log.info(Options.COLLECT_PLATFORM_STATISTICS + "="
//                        + collectPlatformStatistics);
//
//        }
//
//        {
//
//            collectQueueStatistics = Boolean.parseBoolean(properties
//                    .getProperty(Options.COLLECT_QUEUE_STATISTICS,
//                            Options.DEFAULT_COLLECT_QUEUE_STATISTICS));
//
//            if (INFO)
//                log.info(Options.COLLECT_QUEUE_STATISTICS + "="
//                        + collectQueueStatistics);
//
//        }
//
//        {
//
//            httpdPort = Integer.parseInt(properties.getProperty(
//                    Options.HTTPD_PORT, Options.DEFAULT_HTTPD_PORT));
//
//            if (INFO)
//                log.info(Options.HTTPD_PORT + "=" + httpdPort);
//
//            if (httpdPort < 0 && httpdPort != -1)
//                throw new RuntimeException(
//                        Options.HTTPD_PORT
//                                + " must be -1 (disabled), 0 (random port), or positive");
//
//        }
        
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
     * Starts the {@link DataService}.
     * <p>
     * Note: A {@link #start()} is required in order to give subclasses an
     * opportunity to be fully initialized before they are required to begin
     * operations. It is impossible to encapsulate the startup logic cleanly
     * without this ctor() + start() pattern. Those familiar with Objective-C
     * will recognized this.
     * 
     * @return <i>this</i> (the return type should be strengthened by the
     *         concrete implementation to return the actual type).
     * 
     * @todo it would be nice if {@link #start()} could restart after
     *       {@link #shutdown()} but that is hardly necessary.
     */
    synchronized public DataService start() {
        
        if(isOpen()) {
            
            throw new IllegalStateException(); 
            
        }

        resourceManager = (ResourceManager) newResourceManager(properties);

        localTransactionManager = new AbstractLocalTransactionManager(resourceManager) {

            public long nextTimestamp() throws IOException {

                // resolve the timestamp service.
                final ITimestampService timestampService = DataService.this
                        .getFederation().getTimestampService();

                if (timestampService == null)
                    throw new NullPointerException(
                            "TimestampService not discovered");

                // request the next distinct timestamp (robust).
                return timestampService.nextTimestamp();
                
            }

        };

        concurrencyManager = new ConcurrencyManager(properties,
                localTransactionManager, resourceManager);

        localTransactionManager.setConcurrencyManager(concurrencyManager);

        if (resourceManager instanceof ResourceManager) {

            /*
             * Startup the resource manager.
             */

            ((ResourceManager) resourceManager)
                    .setConcurrencyManager(concurrencyManager);

        }

        /*
         * Setup to collect statistics and report about this host.
         * 
         * Note: this is just starting the task that will start reporting once
         * the load balancer has been discovered, so the initialDelay and the
         * delay between retries are relatively short.  We are just waiting for
         * the load balancer before we can start the ReportTask.
         */
        {

            reportService = Executors
                    .newSingleThreadScheduledExecutor(DaemonThreadFactory
                            .defaultThreadFactory());

            reportService.scheduleWithFixedDelay(new StartDeferredTasksTask(),
                    150, // initialDelay (ms)
                    150, // delay
                    TimeUnit.MILLISECONDS // unit
                    );

        }
        
        return this;
        
    }
    
    /**
     * Polite shutdown does not accept new requests and will shutdown once the
     * existing requests have been processed.
     */
    synchronized public void shutdown() {

        if(!isOpen()) return;
        
        notifyLeave(false/*immediateShutdown*/);
        
        if (concurrencyManager != null) {
            concurrencyManager.shutdown();
            concurrencyManager = null;
        }
        
        if(localTransactionManager!=null) {
            localTransactionManager.shutdown();
            localTransactionManager = null;
        }

        if(resourceManager!=null) {
            resourceManager.shutdown();
            resourceManager = null;
        }

        if(reportService!=null) {
            reportService.shutdown();
            reportService = null;
        }

        if (statisticsCollector != null) {

            statisticsCollector.stop();

            statisticsCollector = null;

        }

        if( httpd != null) {
            
            httpd.shutdown();
            
            httpd = null;
            
        }
        
    }

    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon as
     * possible.
     */
    synchronized public void shutdownNow() {

        if(!isOpen()) return;

        notifyLeave(true/*immediateShutdown*/);

        if (concurrencyManager != null) {
        
            concurrencyManager.shutdownNow();

            concurrencyManager = null;
            
        }

        if (localTransactionManager != null) {

            localTransactionManager.shutdownNow();

            localTransactionManager = null;

        }

        if (resourceManager != null) {

            resourceManager.shutdownNow();

            resourceManager = null;

        }

        if (reportService != null) {
        
            reportService.shutdownNow();

            reportService = null;

        }
        
        if (statisticsCollector != null) {

            // Note: value is not bound until after the service UUID is bound.
            
            statisticsCollector.stop();
            
            statisticsCollector = null;
            
        }

        if( httpd != null) {
            
            httpd.shutdownNow();
            
            httpd = null;
            
        }
        
//        if (INFO)
//            log.info(getCounters().toString());
        
    }
    
    private void notifyLeave(boolean immediateShutdown) {

        final ILoadBalancerService loadBalancerService;
        try {
            loadBalancerService = getFederation().getLoadBalancerService();
        } catch(IllegalStateException ex) {
            log.warn("Could not notify load balancer: "+ex);
            return;
        }

        if (loadBalancerService != null) {
            
            final UUID serviceUUID = getServiceUUID();
            
            final String msg = "Goodbye: class=" + getClass().getName()
                    + ", immediateShutdown=" + immediateShutdown;
            
            try {

                // notify leave event.
                loadBalancerService.leave(msg, serviceUUID);

            } catch (NoSuchObjectException e) {
                
                log.warn("Load balancer gone? : "+e);
                
            } catch (IOException e) {

                log.warn(e.getMessage(), e);

            }
            
        }


    }

    /**
     * Interface defines and documents the counters and counter namespaces
     * reported by the {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IDataServiceCounters {
       
        /**
         * The namespace for the counters pertaining to the {@link ConcurrencyManager}.
         */
        String concurrencyManager = "Concurrency Manager";

        /**
         * The namespace for the counters pertaining to the {@link ITransactionManager}.
         */
        String transactionManager = "Transaction Manager";
        
        /**
         * The namespace for the counters pertaining to the {@link ResourceManager}.
         */
        String resourceManager = "Resource Manager";
        
    }
    
    /**
     * Return the {@link ICounterSet} hierarchy used to report on the activity
     * of this service. The counters are automatically setup first time this
     * method is called.
     * <p>
     * The prefix for the counter hierarchy will be
     * <code>hostname/service/<i>serviceIface</i>/serviceUUID</code> where
     * <i>serviceIface</i> is the name of the class returned by
     * {@link #getServiceIface()}. This method therefore has a dependency on
     * {@link #getServiceUUID()} and must be invoked after the
     * <code>serviceUUID</code> is known. The timing of that event depends on
     * whether the service is embedded or using a distributed services framework
     * such as <code>jini</code>.
     * <p>
     * Subclasses MAY extend this method to report additional {@link ICounter}s.
     * 
     * @todo Add some counters providing a histogram of the index partitions
     *       that have touched or that are "hot"?
     * 
     * @see IDataServiceCounters
     */
    synchronized public ICounterSet getCounters() {
     
        if (countersRoot == null) {

            final UUID serviceUUID = getServiceUUID();
            
            if (serviceUUID == null) {
                
                throw new IllegalStateException("The ServiceUUID is not available yet");
                
            }
            
            countersRoot = new CounterSet();

            final String ps = ICounterSet.pathSeparator;
            
            final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;
                       
            final String pathPrefix = ps + hostname + ps + "service" + ps
                    + getServiceIface().getName() + ps + serviceUUID + ps;

            serviceRoot = countersRoot.makePath(pathPrefix);

            /*
             * Service generic counters. 
             */
            AbstractStatisticsCollector.addBasicServiceOrClientCounters(
                    serviceRoot, this, properties);

            /*
             * Service specific counters.
             */
            
            serviceRoot.makePath(IDataServiceCounters.resourceManager).attach(
                    resourceManager.getCounters());

            serviceRoot.makePath(IDataServiceCounters.concurrencyManager).attach(
                    concurrencyManager.getCounters());

            serviceRoot.makePath(IDataServiceCounters.transactionManager).attach(
                    localTransactionManager.getCounters());

            // block API.
            {
            
                CounterSet tmp = serviceRoot.makePath("Block API");

                tmp.addCounter("Blocks Read", new Instrument<Long>() {
                    public void sample() {
                        setValue(readBlockApiCounters.readBlockCount);
                    }
                });

                tmp.addCounter("Blocks Read Per Second",
                        new Instrument<Double>() {
                            public void sample() {

                                // @todo encapsulate this logic.
                                
                                long secs = TimeUnit.SECONDS.convert(
                                        readBlockApiCounters.readBlockNanos,
                                        TimeUnit.NANOSECONDS);

                                final double v;

                                if (secs == 0L)
                                    v = 0d;
                                else
                                    v = readBlockApiCounters.readBlockCount / secs;

                                setValue(v);
                                
                            }
                        });
                
            }
        }
        
        return countersRoot;
        
    }
    private CounterSet countersRoot;
    private CounterSet serviceRoot; 
    
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

    /**
     * An XML Serialization of performance counters.
     */
    public String getStatistics() throws IOException {
        
        return getCounters().asXML(null/*filter*/);
        
    }
    
    /**
     * This task runs periodically. Once {@link IDataService#getServiceUUID()}
     * reports a non-<code>null</code> value AND
     * {@link ResourceManager#isRunning()} reports <code>true</code>, it will
     * start an (optional) {@link AbstractStatisticsCollector}, an (optional)
     * httpd service, and the (required) {@link ReportTask}.
     * <p>
     * Note: The {@link ReportTask} will relay any collected performance
     * counters to the {@link ILoadBalancerService}, but it also lets the
     * {@link ILoadBalancerService} know which services exist which is important
     * for some of its functions.
     * <p>
     * Once these task(s) have been started, this task will throw an exception
     * in order to prevent it from being re-executed by the
     * {@link DataService#reportService}.
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
        
        public StartDeferredTasksTask() {
        
        }

        /**
         * @throws RuntimeException
         *             once the deferred task(s) are running to prevent
         *             re-execution of this startup task.
         */
        public void run() {

            final boolean started;
            
            try {
                
                started = startDeferredTasks();
                
            } catch (Throwable t) {

                log.warn("Problem in report task?", t);

                return;
                
            }

            if (started) {

                /*
                 * Note: This exception is thrown once this task has executed
                 * successfully.
                 */
                
                throw new RuntimeException("Normal completion.");
                
            }
            
        }
        
        /**
         * Starts performance counter collection once the service {@link UUID}
         * is known.
         * 
         * @return <code>true</code> iff performance counter collection was
         *         started.
         * 
         * @throws IOException
         *             if {@link IDataService#getServiceUUID()} throws this
         *             exception (it never should since it is a local method
         *             call).
         */
        protected boolean startDeferredTasks() throws IOException {

            if(!resourceManager.isOpen()) {
                
                /*
                 * This will happen if the store manager is unable to discover
                 * the timestamp service. It will halt its startup process and
                 * report that it is closed. At that point the data service can
                 * not start and will shutdown.
                 */
                
                log.fatal("Store manager not open - will shutdown.");
                
                // shutdown the data service.
                DataService.this.shutdownNow();

                // collection was not started.
                return false;
                
            }
            
            final UUID uuid = getServiceUUID();

            if (uuid == null) {

                log.warn("Service UUID is not assigned yet.");

                return false;

            }
            
            if(!resourceManager.isRunning()) {
                
                log.warn("Resource manager is not running yet.");
                
                return false;
                
            }

            /*
             * Start collecting performance counters from the OS.
             */
            if (getFederation().getClient().getCollectPlatformStatistics()) {

                final Properties p = getProperties();

                p.setProperty(AbstractStatisticsCollector.Options.PROCESS_NAME,
                        "service" + ICounterSet.pathSeparator
                                + getServiceIface().getName()
                                + ICounterSet.pathSeparator + uuid.toString());

                statisticsCollector = AbstractStatisticsCollector
                        .newInstance(p);

                statisticsCollector.start();
                
                /*
                 * Attach the counters that will be reported by the statistics
                 * collector service.
                 */
                ((CounterSet)getCounters()).attach(statisticsCollector.getCounters());

                if (INFO)
                    log.info("Collecting platform statistics: uuid=" + uuid);

            }
            
            /*
             * Start task to report service and counters to the load balancer.
             */
            {
            
                final long delay = Long.parseLong(properties.getProperty(
                        Options.REPORT_DELAY, Options.DEFAULT_REPORT_DELAY));

                if (INFO)
                    log.info(Options.REPORT_DELAY + "=" + delay);

                final TimeUnit unit = TimeUnit.MILLISECONDS;

                /*
                 * Note: We set [initialDelay := 0] so that we run the
                 * ReportTask immediately. This is needed in order to notify()
                 * the load balancer service so that the data service will enter
                 * into its set of active services.
                 * 
                 * Note: If the data service does not notify() the load balancer
                 * service then the load balancer will be unable to recommend a
                 * data service on which to register an index and a new
                 * application will not be able to get started. During new
                 * federation startup it is critical that at least one data
                 * service and the metadata service have discovered and notify()
                 * the load balancer service.
                 * 
                 * Note: This logic is also executed for the MetadataService,
                 * which is just a subclass of the DataService.
                 */
                final long initialDelay = 0; // Note: Immediate notify()!

                reportService.scheduleWithFixedDelay(new ReportTask(),
                        initialDelay, delay, unit);

                if(INFO) log.info("Started ReportTask.");
            
            }

            final int httpdPort = getFederation().getClient().getHttpdPort();

            if (httpdPort != -1) {
                
                /*
                 * HTTPD service reporting out statistics on either a specified
                 * or a randomly assigned port. The port is reported to the load
                 * balancer and also written into the file system. The httpd
                 * service will be shutdown with the data service.
                 * 
                 * Note: some counter sets need to be dynamically (re-)attached
                 * in order to present a current view. This httpd instance
                 * overrides doGet() in order to refresh the data before
                 * generating the view.
                 * 
                 * @todo write port into the [serviceDir], but serviceDir needs
                 * to be declared!
                 */
                
                try {

                    final CounterSet counterSet = (CounterSet) getCounters();
                    
                    DataService.this.httpd = new CounterSetHTTPD(httpdPort, counterSet ) {
                        
                        public Response doGet(String uri, String method, Properties header,
                                Map<String, Vector<String>> parms) throws Exception {
                            
                            try {
                                
                                reattachDynamicCounters();
                                
                            } catch(Exception ex) {
                                
                                /*
                                 * Typically this is because the live journal
                                 * has been concurrently closed during the
                                 * request.
                                 */
                                
                                log.warn("Could not re-attach dynamic counters: "+ex, ex);
                                
                            }
                            
                            return super.doGet(uri, method, header, parms);
                            
                        }
                        
                    };
                    
                    // the URL that may be used to access the local httpd.
                    final String url = "http://"
                            + AbstractStatisticsCollector.fullyQualifiedHostName
                            + ":" + DataService.this.httpd.getPort()
                            + "?path="+URLEncoder.encode(serviceRoot.getPath(),"UTF-8");
                    
                    // add counter reporting that url to the load balancer.
                    DataService.this.serviceRoot.addCounter(IServiceCounters.LOCAL_HTTPD, 
                            new OneShotInstrument<String>(url));
                    
                } catch (IOException e) {
                    
                    log.error("Could not start httpd", e);
                    
                }
                
            }

            return true;
            
        }
        
    }
    
    /**
     * Dynamically detach and attach the counters for the named indices
     * underneath of the {@link IndexManager}.
     * <p>
     * Note: This method limits the frequency of update to no more than once per
     * second.
     */
    synchronized protected void reattachDynamicCounters() {

        final long now = System.currentTimeMillis();

        final long elapsed = now - lastReattachMillis;

        if (elapsed > 1000/* ms */) {

            CounterSet tmp = resourceManager.getIndexManagerCounters();

            assert tmp != null;

            synchronized (tmp) {

                tmp.detach("indices");

                tmp.makePath("indices").attach(
                        concurrencyManager.getIndexCounters()
                // resourceManager.getLiveJournal().getNamedIndexCounters()
                        );

            }

            lastReattachMillis = now;

        }

    }
    private long lastReattachMillis = 0L;
    
    /**
     * Periodically send performance counter data to the
     * {@link ILoadBalancerService}.
     * 
     * @see AbstractFederation.ReportTask
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class ReportTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final protected Logger log = Logger.getLogger(ReportTask.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
                .toInt();

        public ReportTask() {

        }

        /**
         * Note: Don't throw anything here since we don't want to have the task
         * suppressed!
         */
        public void run() {

            try {
                
                reattachDynamicCounters();

            } catch (Throwable t) {

                log.warn("Problem trying to update index counter views?", t);

            }
            
            try {

                /*
                 * Report the performance counters to the load balancer.
                 */
                
                reportPerformanceCounters();
                
            } catch (Throwable t) {

                log.warn("Problem in report task?", t);

            }

        }
        
        /**
         * Send performance counters to the load balancer.
         * 
         * @throws IOException 
         */
        protected void reportPerformanceCounters() throws IOException {

            // Note: This _is_ a local method call.
            final UUID serviceUUID = getServiceUUID();

            // Will be null until assigned by the service registrar.
            if (serviceUUID == null) {

                log.info("Service UUID not assigned yet.");

                return;

            }

            final ILoadBalancerService loadBalancerService = getFederation().getLoadBalancerService();

            if (loadBalancerService == null) {

                log.warn("Could not discover load balancer service.");

                return;

            }

            /*
             * @todo this is probably worth compressing as there will be a lot
             * of redundency.
             * 
             * @todo allow filter on what gets sent to the load balancer?
             */
            ByteArrayOutputStream baos = new ByteArrayOutputStream(
                    Bytes.kilobyte32 * 2);

            getCounters().asXML(baos, "UTF-8", null/* filter */);
            
            loadBalancerService.notify("Hello", serviceUUID, getServiceIface()
                    .getName(), baos.toByteArray());

            log.info("Notified the load balancer.");
            
        }

    }

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
            
            MDC.put("serviceUUID", serviceUUID);

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
     * @version $Id$
     */
    public static class GetIndexMetadataTask extends AbstractTask {

        public GetIndexMetadataTask(ConcurrencyManager concurrencyManager,
                long startTime, String name) {

            super(concurrencyManager, startTime, name);
            
        }

        @Override
        protected IndexMetadata doTask() throws Exception {
            
            return getIndex(getOnlyResource()).getIndexMetadata();
            
        }
        
    }
    
    /**
     * Note: This chooses {@link ITx#READ_COMMITTED} if the the index has
     * {@link ITx#UNISOLATED} isolation and the {@link IIndexProcedure} is an
     * read-only operation. This provides better concurrency on the
     * {@link DataService} by moving read-only operations off of the
     * {@link WriteExecutorService}.
     */
    public Object submit(long tx, String name, IIndexProcedure proc)
            throws InterruptedException, ExecutionException {

        setupLoggingContext();

        try {
    
            if (name == null)
                throw new IllegalArgumentException();

            if (proc == null)
                throw new IllegalArgumentException();
            
            // Choose READ_COMMITTED iff proc is read-only and UNISOLATED was requested.
            final long startTime = (tx == ITx.UNISOLATED
                        && proc.isReadOnly() ? ITx.READ_COMMITTED
                        : tx);

            // wrap the caller's task.
            final AbstractTask task = new IndexProcedureTask(
                    concurrencyManager, startTime, name, proc);
            
            if(proc instanceof IDataServiceAwareProcedure) {

                if(log.isInfoEnabled()) {
                    
                    log.info("Data service aware procedure: "+proc.getClass().getName());
                    
                }

                // set the data service on the task.
                ((IDataServiceAwareProcedure)proc).setDataService( this );
                
            }
            
            // submit the procedure and await its completion.
            return concurrencyManager.submit(task).get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }

    /**
     * The task will be run on the {@link IBigdataFederation#getExecutorService()}.
     * <p>
     * The {@link Callable} MAY implement {@link IDataServiceAwareProcedure} to
     * obtain the {@link DataService} reference, which can be used to obtain a
     * local {@link IBigdataClient} reference or to submit additional tasks to
     * the {@link ConcurrencyManager}.
     * 
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
    public Future<? extends Object> submit(Callable<? extends Object> task)
            throws InterruptedException, ExecutionException {
     
        setupLoggingContext();

        try {
    
            if (task == null)
                throw new IllegalArgumentException();
            
            if(task instanceof IDataServiceAwareProcedure) {
         
                if(log.isInfoEnabled()) {
                    
                    log.info("Data service aware procedure: "+task.getClass().getName());
                    
                }
                
                // set the data service on the task.
                ((IDataServiceAwareProcedure)task).setDataService( this );
                
            }
            
            // submit the task and await its completion.
            return getFederation().getExecutorService().submit(task);
        
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
                    ByteBuffer buf = store.read(addr);

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
     * @version $Id$
     */
    static protected class RangeIteratorTask extends AbstractTask {

        private final byte[] fromKey;
        private final byte[] toKey;
        private final int capacity;
        private final int flags;
        private final IFilterConstructor filter;
        
        public RangeIteratorTask(ConcurrencyManager concurrencyManager,
                long startTime, String name, byte[] fromKey, byte[] toKey,
                int capacity, int flags, IFilterConstructor filter) {

            super(concurrencyManager, startTime, name);

            this.fromKey = fromKey;
            this.toKey = toKey;
            this.capacity = capacity;
            this.flags = flags;
            this.filter = filter; // MAY be null.

        }

        public ResultSet doTask() throws Exception {

            final IIndex ndx = getIndex(getOnlyResource());
            
            /*
             * Figure out the upper bound on the #of tuples that could be
             * materialized.
             * 
             * Note: the upper bound on the #of key-value pairs in the range is
             * truncated to an [int].
             */
            
            final int rangeCount = (int) ndx.rangeCount(fromKey, toKey);

            final int limit = (rangeCount > capacity ? capacity : rangeCount);

            /*
             * Iterator that will visit the key range.
             * 
             * Note: We always visit the keys regardless of whether we pass them
             * on to the caller. This is necessary in order for us to set the
             * [lastKey] field on the result set and that is necessary to
             * support continuation queries.
             */
            
            final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey, limit,
                    flags | IRangeQuery.KEYS, filter);
            
            /*
             * Populate the result set from the iterator.
             */

            return new ResultSet(ndx, capacity, flags, itr);

        }
        
    }

    /*
     * 
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

                // trigger overflow on the next group commit.
                writeService.forceOverflow.set(true);

            }

        } finally {

            clearLoggingContext();

        }
        
    }
    
    public long getOverflowCounter() throws IOException {
    
        setupLoggingContext();

        try {

            if (!(resourceManager instanceof ResourceManager)) {

                throw new UnsupportedOperationException();

            }

            return resourceManager.getOverflowCount();

        } finally {

            clearLoggingContext();

        }
        
    }
    
    /**
     * Interface for procedures that require access to the {@link IDataService}
     * and or the federation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo register index and drop index could be rewritten as submitted
     *       procedures derived from this class. This would simplify the
     *       {@link IDataService} API and metrics collection further. The
     *       implementations would have to be distinct from
     *       {@link RegisterIndexTask} and {@link DropIndexTask} since those
     *       extend {@link AbstractTask} - that class does not implement
     *       {@link IIndexProcedure} and can not be sent across the wire.
     */
    public static interface IDataServiceAwareProcedure {

        /**
         * Invoked before the task is executed to given the procedure a
         * reference to the {@link IDataService} on which it is executing.
         */
        void setDataService(DataService dataService);
        
    }
    
}
