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

package com.bigdata.loadbalancer;

import static com.bigdata.loadbalancer.Constants.*;

//BTM*** - replace with ShardService after DataService smart proxy conversion?
//BTM*** - replace with EmbeddedShardService.IDataServiceCounters?
//BTM*** - replace with ShardLocator after smart proxy conversion?
import com.bigdata.counters.httpd.AbstractStatisticsCollector;
import com.bigdata.service.DataService.IDataServiceCounters;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.DefaultInstrumentFactory;
import com.bigdata.counters.History;
import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.ICounterSet.IInstrumentFactory;
import com.bigdata.counters.IHostCounters;
import com.bigdata.counters.IRequiredHostCounters;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.jini.lookup.entry.Hostname;
import com.bigdata.jini.lookup.entry.ServiceUUID;
import com.bigdata.jini.start.IServicesManagerService;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ConcurrencyManager.IConcurrencyManagerCounters;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.ResourceManager.IResourceManagerCounters;
import com.bigdata.resources.StoreManager.IStoreManagerCounters;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.AbstractRoundRobinServiceLoadHelper;
import com.bigdata.service.AbstractServiceLoadHelperWithScores;
import com.bigdata.service.AbstractServiceLoadHelperWithoutScores;
import com.bigdata.service.Event;
import com.bigdata.service.EventReceiver;
import com.bigdata.service.EventReceiver.EventBTree;
import com.bigdata.service.EventReceivingService;
import com.bigdata.service.HostScore;
import com.bigdata.service.IClientService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IEventReportingService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.IService;
import com.bigdata.service.LoadBalancer;
import com.bigdata.service.Service;
import com.bigdata.service.ServiceScore;
import com.bigdata.service.ShardLocator;
import com.bigdata.service.ShardService;
import com.bigdata.util.EntryUtil;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.IQueueCounters.IThreadPoolExecutorTaskCounters;
import com.bigdata.util.config.LogUtil;

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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.rmi.RemoteException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class EmbeddedLoadBalancer implements LoadBalancer,
                                             Service,
                                             EventReceivingService,
                                             IEventReportingService
{
    public static Logger logger =
        LogUtil.getLog4jLogger((EmbeddedLoadBalancer.class).getName());

    private UUID thisServiceUUID;
    private String hostname;

//BTM***    private IBigdataFederation federation;
private CounterSet countersRoot;
private CounterSet serviceRoot;
private AbstractStatisticsCollector statisticsCollector;
private Map<UUID, String> serviceNameMap = new ConcurrentHashMap<UUID, String>();
private ServiceDiscoveryManager sdm;
private Map<UUID, IDataService> embeddedDataServiceMap;

private LookupCache remoteShardLocatorCache;
private LookupCache shardLocatorCache;

private LookupCache remoteShardCache;
private LookupCache shardCache;

//for populating the serviceName map when services join the federation
private LookupCache remoteServiceCache;
private LookupCache serviceCache;
//BTM***

    final protected String ps = ICounterSet.pathSeparator;
    
    /**
     * Used to read {@link CounterSet} XML. This must support overwrite since
     * new data arrives every minute and eventually the ring buffer must
     * overwrite old values. In addition, this may support multiple levels of
     * aggregation so that minutes may be rolled into hours and hours into days.
     */
    final private IInstrumentFactory instrumentFactory =
                      DefaultInstrumentFactory.OVERWRITE_60M;
    
    /**
     * Service join timeout in milliseconds - used when we need to wait for a
     * service to join before we can recommend an under-utilized service.
     * 
     * @see Options#SERVICE_JOIN_TIMEOUT
     */
    final protected long serviceJoinTimeout;

    /**
     * Lock is used to control access to data structures that are not
     * thread-safe.
     */
    final protected ReentrantLock lock = new ReentrantLock();

    /**
     * Used to await a service join when there are no services.
     */
    final protected Condition joined = lock.newCondition();

    /**
     * The active hosts (one or more services).
     * 
     * @todo get rid of hosts that are no longer active. e.g., we no longer
     *       receive {@link #notify(String, byte[])} events from the host and
     *       the host can not be pinged. this will require tracking the #of
     *       services on the host which we do not do directly right now.
     */
    protected ConcurrentHashMap<String/* hostname */, HostScore> activeHosts =
                  new ConcurrentHashMap<String, HostScore>();

    /**
     * The set of active services.
     */
    protected ConcurrentHashMap<UUID/* serviceUUID */, ServiceScore> activeDataServices
                  = new ConcurrentHashMap<UUID, ServiceScore>();

    /**
     * Scores for the hosts in ascending order (least utilized to most
     * utilized).
     * <p>
     * This array is initially <code>null</code> and gets updated periodically
     * by the {@link UpdateTask}. The main consumer of this information is the
     * logic in {@link UpdateTask} that computes the service utilization.
     */
    protected AtomicReference<HostScore[]> hostScores =
                  new AtomicReference<HostScore[]>(null);
    
    /**
     * Scores for the services in ascending order (least utilized to most
     * utilized).
     * <p>
     * This array is initially <code>null</code> and gets updated periodically
     * by the {@link UpdateTask}. The methods that report service utilization
     * and under-utilized services are all based on the data in this array.
     * Since services can leave at any time, that logic MUST also test for
     * existence of the service in {@link #activeDataServices} before assuming that the
     * service is still live.
     */
    protected AtomicReference<ServiceScore[]> serviceScores =
                  new AtomicReference<ServiceScore[]>(null);
    
    /**
     * The #of {@link UpdateTask}s which have run so far.
     * 
     * @see Options#INITIAL_ROUND_ROBIN_UPDATE_COUNT
     * 
     * @see #getUnderUtilizedDataServices(int, int, UUID)
     */
    protected long nupdates = 0;
    
    /**
     * The #of updates during which
     * {@link #getUnderUtilizedDataServices(int, int, UUID)} will
     * apply a round robin policy.
     * 
     * @see Options#INITIAL_ROUND_ROBIN_UPDATE_COUNT
     */
    protected final long initialRoundRobinUpdateCount;

    /**
     * Used to make round-robin assignments.
     */
    private final RoundRobinServiceLoadHelper roundRobinServiceLoadHelper;
    
    /**
     * The directory in which the service will log the {@link CounterSet}s
     * and {@link Event}s.
     * 
     * @see Options#LOG_DIR
     */
    protected final File logDir;

    /**
     * <code>true</code> iff the loadbalancer will refrain from writing
     * state on the disk. This option causes the loadbalancer to use an
     * in memory {@link #eventStore}. In addition, it will refuse to
     * write counter snapshots when this option is specified.
     * 
     * @see Options#TRANSIENT
     */
    protected final boolean isTransient;
    
    /**
     * A copy of the properties used to start the service.
     */
    private final Properties properties;
    
    /**
     * An object wrapping the properties provided to the constructor.
     */
    public Properties getProperties() {
        
        return new Properties(properties);
        
    }
    
    /**
     * Runs a periodic {@link UpdateTask}.
     */
    final protected ScheduledExecutorService updateService;

    /**
     * The delay between writes of the {@link CounterSet} on a log file.
     */
    private final long logDelayMillis;

    /**
     * The #of distinct log files to retain.
     */
    private final long logMaxFiles;

    /**
     * Time that the {@link CounterSet} was last written onto a log file.
     */
    private long logLastMillis = System.currentTimeMillis();

    /**
     * A one-up counter of the #of times the {@link CounterSet} was written onto
     * a log file.
     */
    private int logFileCount = 0;

    /**
     * The #of minutes of history that will be smoothed into an average when
     * {@link UpdateTask} updates the {@link HostScore}s and the
     * {@link ServiceScore}s.
     * 
     * @see Options#HISTORY_MINUTES
     */
    protected final int historyMinutes;
    
    /**
     * Used to persist the logged events.
     */
    final protected Journal eventStore;

    protected final EventReceiver eventReceiver;

    /**
     * Options understood by the {@link LoadBalancer}.
     *
     * @todo The loadbalancer needs to support a 'transient' option in which it
     *       (a) does not log counters; and (b) keeps the events in a transient
     *       B+Tree (not backed by a file on the disk). Without this we can not
     *       have a transient {@link EmbeddedFederation} or
     *       {@link LocalDataServiceFederation} instances.
     */
    public interface Options {

        /**
         * The load balancer service will use a round robin approach to
         * recommending under-utilized services until the load balancer has
         * re-computed the service scores N times (default
         * {@value #DEFAULT_INITIAL_ROUND_ROBIN_UPDATE_COUNT}). This makes it
         * more likely that the initial index partitions will be allocated on
         * services on different hosts for a new federation, but it is really a
         * hack since it depends entirely on the time elapsed since the load
         * balancer service (re-)started. This "feature" may be disabled by
         * setting this property to ZERO (0).
         */
        String INITIAL_ROUND_ROBIN_UPDATE_COUNT = 
            COMPONENT_NAME+".initialRoundRobinUpdateCount";

        /**
         * The default gives you a few minutes after you setup the federation in
         * which newly registered indices will be allocated based on a
         * round-robin.
         */
        String DEFAULT_INITIAL_ROUND_ROBIN_UPDATE_COUNT = "5";

        /**
         * The delay between scheduled invocations of the {@link UpdateTask}.
         * <p>
         * Note: the {@link AbstractStatisticsCollector} implementations SHOULD
         * sample at one minute intervals by default and clients SHOULD report
         * the collected performance counters at approximately one minute
         * intervals. The update rate can be no more frequent than the reporting
         * rate, but could be 2-5x slower, especially if we use WARN and URGENT
         * events to immediately re-score services.
         * 
         * @see #DEFAULT_UPDATE_DELAY
         * 
         * @see AbstractStatisticsCollector.Options#PERFORMANCE_COUNTERS_SAMPLE_INTERVAL
         */
        String UPDATE_DELAY = COMPONENT_NAME+".updateDelay";
        
        /**
         * The default {@link #UPDATE_DELAY}.
         */
        String DEFAULT_UPDATE_DELAY = ""+(60*1000);

        /**
         * The #of minutes of history that will be smoothed into an average when
         * {@link UpdateTask} updates the {@link HostScore}s and the
         * {@link ServiceScore}s (default {@value #DEFAULT_HISTORY_MINUTES}).
         * 
         * @see ThreadPoolExecutorStatisticsTask
         */
        String HISTORY_MINUTES = COMPONENT_NAME+".historyMinutes"; 

        String DEFAULT_HISTORY_MINUTES = "5";

        /**
         * When <code>true</code> the load balancer will not record any
         * state on the disk (neither events nor counters). The default is
         * <code>false</code>. This option is used by some unit tests to
         * simplify cleanup.
         */
        String TRANSIENT = COMPONENT_NAME+".transient";

        String DEFAULT_TRANSIENT = "false";

        /**
         * The path of the data directory for the load balancer. The load
         * balancer will log a copy of the counters every time it runs its
         * {@link UpdateTask}. It will also log {@link Event}s received from
         * other services here. By default, the load balancer will use the
         * directory in which it was started. You may specify an alternative
         * directory using this property.
         */
        String LOG_DIR = COMPONENT_NAME+".log.dir";
        
        String DEFAULT_LOG_DIR = ".";

        /**
         * The delay in milliseconds between writes of the {@link CounterSet} on
         * a log file (default is {@value #DEFAULT_LOG_DELAY}, which is
         * equivalent to one hour).
         */
        String LOG_DELAY = COMPONENT_NAME+".log.delay";
        
        String DEFAULT_LOG_DELAY = "" + 1000 * 60 * 60;

        /**
         * The maximum #of distinct log files to retain (default is one week
         * based on a {@link #LOG_DELAY} equivalent to one hour).
         */
        String LOG_MAX_FILES = COMPONENT_NAME+".log.maxFiles";

        String DEFAULT_LOG_MAX_FILES = "" + 24 * 7;

        /**
         * Service join timeout in milliseconds - used when we need to wait for
         * a service to join before we can recommend an under-utilized service.
         */
        String SERVICE_JOIN_TIMEOUT = COMPONENT_NAME+".serviceJoinTimeout";

        String DEFAULT_SERVICE_JOIN_TIMEOUT = "" + (3 * 1000);

        /**
         * The maximum age of an {@link Event} that will be keep "on the books".
         * Events older than this are purged. An error is logged if an event is
         * purged before its end() event arrives. This generally indicates a
         * code path where {@link Event#end()} is not getting called but could
         * also indicate a disconnected client or service.
         * 
         * @see EventReceiver
         */
        String EVENT_HISTORY_MILLIS = COMPONENT_NAME+".eventHistoryMillis";

        /**
         * Default is one hour of completed events.
         */
        String DEFAULT_EVENT_HISTORY_MILLIS = "" + (60 * 60 * 1000);
        
    }

    /**
     * 
     * Note: The load balancer MUST NOT collect host statistics unless it is the
     * only service running on that host. Normally it relies on another service
     * running on the same host to collect statistics for that host and those
     * statistics are then reported to the load balancer and aggregated along
     * with the rest of the performance counters reported by the other services
     * in the federation. However, if the load balancer itself collects host
     * statistics then it will only know about and report the current (last 60
     * seconds) statistics for the host rather than having the historical data
     * for the host.
     * 
     * @param properties
     *            See {@link Options}
     */
    public EmbeddedLoadBalancer(
final UUID serviceUUID,
final String hostname,
final ServiceDiscoveryManager sdm,
final Map<UUID, IDataService> embeddedDataServiceMap,//BTM  ShardService
final String persistenceDir,
                                final Properties         properties)
    {
        if (serviceUUID == null) {
            throw new NullPointerException("null serviceUUID");
        }   
        this.thisServiceUUID = serviceUUID;

        if (hostname == null) {
            throw new NullPointerException("null hostname");
        }   
        this.hostname = hostname;

//BTM - BEGIN
if (sdm != null) {
    this.sdm = sdm;

    //for smart proxy implementation of shard locator service
    Class[] shardLocatorType = new Class[] {ShardLocator.class};
    ServiceTemplate shardLocatorTmpl = 
                            new ServiceTemplate(null, shardLocatorType, null);
    ServiceItemFilter shardLocatorFilter = null;

    //for remote implementation of shard locator service
    Class[] remoteShardLocatorType = new Class[] {IMetadataService.class};
    ServiceTemplate remoteShardLocatorTmpl = 
                    new ServiceTemplate(null, remoteShardLocatorType, null);
    ServiceItemFilter remoteShardLocatorFilter = 
                          new IMetadataServiceOnlyFilter();

    //for smart proxy implementation of shard service
    Class[] shardType = new Class[] {ShardService.class};
    ServiceTemplate shardTmpl = 
                            new ServiceTemplate(null, shardType, null);
    ServiceItemFilter shardFilter = null;

    //for remote implementation of shard service
    Class[] remoteShardType = new Class[] {IDataService.class};
    ServiceTemplate remoteShardTmpl = 
                            new ServiceTemplate(null, remoteShardType, null);
    ServiceItemFilter remoteShardFilter = new IDataServiceOnlyFilter();


    //for smart proxy implementation of any services
    Class[] serviceType = new Class[] {Service.class};
    ServiceTemplate serviceTmpl = 
                            new ServiceTemplate(null, serviceType, null);
    ServiceItemFilter serviceFilter = null;

    //for remote implementation of any services
    Class[] remoteServiceType = new Class[] {IDataService.class};
    ServiceTemplate remoteServiceTmpl = 
                            new ServiceTemplate(null, remoteServiceType, null);
    ServiceItemFilter remoteServiceFilter = null;

    //create the caches
    try {
        //for shard locator services
        this.shardLocatorCache = sdm.createLookupCache
                                     ( shardLocatorTmpl,
                                       shardLocatorFilter,
                                       null );
        this.remoteShardLocatorCache = sdm.createLookupCache
                                     ( remoteShardLocatorTmpl,
                                       remoteShardLocatorFilter,
                                       null );

        //for shard services
        this.shardCache = sdm.createLookupCache(shardTmpl, shardFilter, null);
        this.remoteShardCache = sdm.createLookupCache
                                   (remoteShardTmpl, remoteShardFilter, null);

        //for populating the serviceNameMap when services join/leave the fed
        this.serviceCache = sdm.createLookupCache
                                     ( serviceTmpl, 
                                       serviceFilter,
                                       new CacheListener(logger) );
        this.remoteServiceCache = sdm.createLookupCache
                                     ( remoteServiceTmpl, 
                                       remoteServiceFilter,
                                       new CacheListener(logger) );
    } catch(RemoteException e) {
        logger.warn(e.getMessage(), e);
    }
}
//BTM - END


//BTM***        if (federation == null) {
//BTM***            throw new IllegalArgumentException("null federation");
//BTM***        }   
//BTM***        this.federation = federation;
this.embeddedDataServiceMap = embeddedDataServiceMap;

        if (properties == null) {
            throw new NullPointerException("null properties");
        }   
        this.properties = (Properties) properties.clone();

        this.isTransient = 
            Boolean.valueOf
                (properties.getProperty(Options.TRANSIENT,
                                        Options.DEFAULT_TRANSIENT));
        if (logger.isDebugEnabled()) {
            logger.debug(Options.TRANSIENT + "=" + isTransient);
        }
        
//BTM        if(isTransient) {
//BTM            logDir = null;
//BTM        } else {
//BTM            // setup the log directory.
//BTM            final String val = 
//BTM                properties.getProperty(Options.LOG_DIR,
//BTM                                       Options.DEFAULT_LOG_DIR);
//BTM            logDir = new File(val);
//BTM
if(persistenceDir != null) {
    this.logDir = new File(persistenceDir);
} else {
    this.logDir = new File(".");
}
            if (logger.isDebugEnabled()) {
                logger.debug(Options.LOG_DIR + "=" + logDir);                
            }
//BTM
//BTM            // ensure exists.
//BTM            logDir.mkdirs();
//BTM        }

        logDelayMillis = 
            Long.parseLong(properties.getProperty
                               (Options.LOG_DELAY,
                                Options.DEFAULT_LOG_DELAY));

        if (logger.isDebugEnabled()) {
            logger.debug(Options.LOG_DELAY + "=" + logDelayMillis);
        }

        logMaxFiles = 
            Integer.parseInt
                (properties.getProperty(Options.LOG_MAX_FILES,
                                        Options.DEFAULT_LOG_MAX_FILES));

        if (logger.isDebugEnabled()) {
            logger.debug(Options.LOG_MAX_FILES + "=" + logMaxFiles);
        }
        
        historyMinutes = 
            Integer.parseInt
                (properties.getProperty(Options.HISTORY_MINUTES,
                                        Options.DEFAULT_HISTORY_MINUTES));
            
        if (logger.isDebugEnabled()) {
            logger.debug(Options.HISTORY_MINUTES+"="+historyMinutes);
        }

        // a reasonable range check.
        if (historyMinutes <= 0 || historyMinutes > 60) {
            throw new RuntimeException(Options.HISTORY_MINUTES
                                       + " must be in [1:60].");
        }

        serviceJoinTimeout = 
            Long.parseLong
                (properties.getProperty(Options.SERVICE_JOIN_TIMEOUT,
                                        Options.DEFAULT_SERVICE_JOIN_TIMEOUT));
            
        if (logger.isDebugEnabled()) {
            logger.debug(Options.SERVICE_JOIN_TIMEOUT+"="+serviceJoinTimeout);
        }
            
        if (serviceJoinTimeout <= 0L) {
            throw new RuntimeException(Options.SERVICE_JOIN_TIMEOUT
                                       + " must be positive.");
        }
        
        // setup scheduled runnable for periodic updates of the service scores.
        initialRoundRobinUpdateCount = 
            Long.parseLong
                (properties.getProperty
                     (Options.INITIAL_ROUND_ROBIN_UPDATE_COUNT,
                      Options.DEFAULT_INITIAL_ROUND_ROBIN_UPDATE_COUNT));

        if (logger.isDebugEnabled()) {
            logger.debug(Options.INITIAL_ROUND_ROBIN_UPDATE_COUNT+"="
                     +initialRoundRobinUpdateCount);
        }

//BTM***        this.roundRobinServiceLoadHelper = new RoundRobinServiceLoadHelper();
this.roundRobinServiceLoadHelper = 
    new RoundRobinServiceLoadHelper(this.shardLocatorCache,
                                    this.remoteShardLocatorCache,
                                    this.shardCache,
                                    this.remoteShardCache,
                                    embeddedDataServiceMap);
        final long delay = 
            Long.parseLong
                (properties.getProperty(Options.UPDATE_DELAY,
                                        Options.DEFAULT_UPDATE_DELAY));

        if (logger.isDebugEnabled()) {
            logger.debug(Options.UPDATE_DELAY+"="+delay);
        }

        /*
         * Wait a bit longer for the first update task since service may be
         * starting up as well and we need to have the performance counter
         * data on hand before we can do anything.
         */
        final long initialDelay = delay * 2;
            
        final TimeUnit unit = TimeUnit.MILLISECONDS;

        updateService = 
            Executors.newSingleThreadScheduledExecutor
                (new DaemonThreadFactory
                         (getClass().getName()+".updateService"));
            
        updateService.scheduleWithFixedDelay
                          (new UpdateTask(), initialDelay, delay, unit);

        // eventHistoryMillis
        final long eventHistoryMillis = 
            Long.parseLong
                (properties.getProperty
                     (Options.EVENT_HISTORY_MILLIS,
                      Options.DEFAULT_EVENT_HISTORY_MILLIS));

        if (logger.isDebugEnabled()) {
            logger.debug(Options.EVENT_HISTORY_MILLIS+"="+eventHistoryMillis);
        }

        /*
         * Setup a BTree backend that will be used to persist the completed
         * events. This is passed to the EventReceiver. The BTree is used to
         * get the events out of RAM and to decouple the reporting from the
         * receiving. We delegate everything dealing with the events to that
         * class.
         */

        if(isTransient) {
            /*
             * Use an in-memory store.
             */

            final Properties p = new Properties();

            p.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                          BufferMode.Transient.toString());

            eventStore = new Journal(p);
        } else {
            /*
             * Use a restart-safe store.
             */
            final Properties p = new Properties();

            p.setProperty
                (com.bigdata.journal.Options.FILE, 
                 new File
                     (logDir, 
                      "events"+com.bigdata.journal.Options.JNL).toString());

            eventStore = new Journal(p);    
        }

        EventBTree eventBTree = (EventBTree) eventStore.getIndex("events");

        if (eventBTree == null) {
            eventStore.registerIndex
                           ("events",
                            eventBTree = EventBTree.create(eventStore));
        }
        eventReceiver = new EventReceiver(eventHistoryMillis, eventBTree);
    }

//BTM
// Required by Service interface (extended by LoadBalancer)

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
// Required by LoadBalancer interface

    public void notify(final UUID serviceUUID, final byte[] data) {
        setupLoggingContext();
        try {        
            if (logger.isDebugEnabled()) {
                logger.debug("load balancer received notification [from "+serviceUUID+"]");
            }
            if (!serviceUUID.equals(thisServiceUUID)) {
                try {
                    // read the counters into our local history.
//BTM***                    federation.getCounterSet().readXML(
//BTM***                            new ByteArrayInputStream(data), instrumentFactory,
//BTM***                            null/* filter */);

getCounterSet().readXML(new ByteArrayInputStream(data), instrumentFactory, null/* filter */);
                } catch (Exception e) {
                    logger.warn(e.getMessage(), e);

if (logger.isEnabledFor(org.apache.log4j.Level.DEBUG)) {
    logger.warn("***** EmbeddedLoadBalancer.notify: byte[] data - CONTAINS SLASH-SLASH???");
    logger.warn("***** EmbeddedLoadBalancer.notify: data.length = "+data.length);
    StringBuffer strBuf = new StringBuffer();
    if( (data[0] < 32) || (data[0] > 126) ) {
        strBuf.append("X");
    }else{
        strBuf.append( new String(new byte[] {data[0]}) );
    }
    for(int i=1;i<data.length; i++) {
        if( (data[i] < 32) || (data[i] > 126) ) {
            strBuf.append("X");
        }else{
            strBuf.append( new String(new byte[] {data[i]}) );
        }
        if( (data[i-1] == 47) && (data[i] == 47) ) {
            logger.warn("***** EmbeddedLoadBalancer.notify: data array CONTAINS SLASH-SLASH at indices "+(i-1)+" & "+i);
        }
    }
    logger.warn("***** EmbeddedLoadBalancer.notify: CONTAINS SLASH-SLASH: path CONVERTED = "+strBuf.toString());
}

                    throw new RuntimeException(e);
                }
            }
        } finally {
            clearLoggingContext();
        }
    }

    public void warn(String msg, UUID serviceUUID) {
        setupLoggingContext();
        try {
            logger.warn(msg+" : serviceUUID="+serviceUUID);
        } finally {
            clearLoggingContext();
        }
    }

    public void urgent(String msg, UUID serviceUUID) {
        setupLoggingContext();
        try {
            logger.error(msg+" : serviceUUID="+serviceUUID);
        } finally {
            clearLoggingContext();  
        }
    }

    public boolean isHighlyUtilizedDataService(final UUID serviceUUID)
                       throws IOException {
    
        setupLoggingContext();
        try {
            final ServiceScore[] scores = this.serviceScores.get();

            // No scores yet?
            if (scores == null) {
                if(logger.isDebugEnabled()) logger.debug("No scores yet");
                return false;
            }

            final ServiceScore score = activeDataServices.get(serviceUUID);

            if (score == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug
                        ("shard service is not scored ["+serviceUUID+"]");
                }
                return false;
            }
            return isHighlyUtilizedDataService(score, scores);
        } finally {
            clearLoggingContext();
        }
        
    }

    public boolean isUnderUtilizedDataService(final UUID serviceUUID)
            throws IOException {

        setupLoggingContext();
        try {
            final ServiceScore[] scores = this.serviceScores.get();

            // No scores yet?
            if (scores == null) {
                if(logger.isDebugEnabled()) logger.debug("No scores yet");

                return false;
            }

            final ServiceScore score = activeDataServices.get(serviceUUID);

            if (score == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug
                        ("shard service is not scored ["+serviceUUID+"]");
                }
                return false;
            }
            return isUnderUtilizedDataService(score, scores);
        } finally {
            clearLoggingContext();
        }
    }

    protected boolean isHighlyUtilizedDataService(final ServiceScore score,
            final ServiceScore[] scores) {

        if (score == null) throw new IllegalArgumentException();

        if (scores == null) throw new IllegalArgumentException();

        boolean highlyUtilized = false;

        if (score.drank > .8) {
            // top 20% is considered to be highly utilized.
            highlyUtilized = true;
        } else if (score.rank == scores.length - 1) {
            // top rank is considered to be highly utilized.
            highlyUtilized = true;
        }

        if (logger.isDebugEnabled()) {
            logger.debug
                ("highlyUtilized="+highlyUtilized+": [score="+score+"]");
        }
        return highlyUtilized;

    }

    protected boolean isUnderUtilizedDataService(final ServiceScore score,
                                                 final ServiceScore[] scores)
    {
        if (score == null) throw new IllegalArgumentException();
        if (scores == null) throw new IllegalArgumentException();

        boolean underUtilized = false;

        if (score.drank < .2) {
            // bottom 20% is considered to be under-utilized.
            underUtilized = true;
        } else if (score.rank == 0) {
            // bottom rank is considered to be under-utilized.
            underUtilized = true;
        }

        if (logger.isDebugEnabled()) {
            logger.debug
                ("underUtilized="+underUtilized+" : [score="+score+"]");
        }
        return underUtilized;
    }


    /**
     * Logs the counters to a temporary file.
     */
    public void sighup() throws IOException {
        if (isTransient) {
            logger.warn
                ("transient load balancer service - request ignored");
            return;
        }
        final File file = 
            File.createTempFile("counters-hup", ".xml", logDir);
        logCounters(file);
    }

//BTM
// Required by IEventReportingService interface

    /**
     * {@inheritDoc}
     */
    public Iterator<Event> rangeIterator(long fromTime, long toTime) {
        if(!isOpen()) throw new IllegalStateException();
        return eventReceiver.rangeIterator(fromTime, toTime);
    }

    /**
     * {@inheritDoc}
     */
    public long rangeCount(long fromTime, long toTime) {
        if(!isOpen()) throw new IllegalStateException();
        return eventReceiver.rangeCount(fromTime, toTime);
    }

//BTM
// Required by IServiceShutdown interface

    public boolean isOpen() {
        return !updateService.isShutdown();
    }
    
    synchronized public void shutdown() {
logger.warn("XXXXX LOAD BALANCER EmbeddedLoadBalancer.shutdown");
        if(!isOpen()) return;
        
        if (logger.isDebugEnabled()) {
            logger.debug("begin shutdown [EmbeddedLoadBalancer]");
        }
        updateService.shutdown();

        // log the final state of the counters.
        logCounters("final");

        /*
         * Obtain the exclusive write lock for the event BTree
         * before flushing writes.
         */
        final Lock lock = eventReceiver.getWriteLock();
        try {
            // Flush any buffered writes to the event store.
            eventStore.getIndex("events").flush();

            // Normal shutdown of the event store.
            eventStore.shutdown();
        } catch (Throwable t) {
            logger.error(t, t);
        } finally {
            lock.unlock();
        }        
        if (logger.isDebugEnabled()) {
            logger.debug("shutdown complete [EmbeddedLoadBalancer]");
        }
    }

    synchronized public void shutdownNow() {
logger.warn("XXXXX LOAD BALANCER EmbeddedLoadBalancer.shutdownNow");
        if(!isOpen()) return;

        if (logger.isDebugEnabled()) {
            logger.debug("begin shutdown [EmbeddedLoadBalancer]");
        }
        updateService.shutdownNow();
        
        // log the final state of the counters.
        logCounters("final");

        // immediate shutdown.
        eventStore.shutdownNow();

        if (logger.isInfoEnabled()) {
            logger.debug("shutdown complete [EmbeddedLoadBalancer]");
        }
    }

    synchronized public void destroy() {
logger.warn("XXXXX LOAD BALANCER EmbeddedLoadBalancer.destroy");
        if (!isTransient) {
            eventStore.destroy();

            final File[] logFiles = 
                logDir.listFiles(new FileFilter() {
                    public boolean accept(File pathname) {
                        return pathname.getName().startsWith("counters")
                            && pathname.getName().endsWith(".xml");
                    }

                });

            if (logFiles != null) {
                for (File file : logFiles) {
                    if (!file.delete()) {
                        logger.warn("Could not delete: " + file);
                    }
                }
            }
            // delete the log directory (works iff it is empty).
logger.warn("XXXXX LOAD BALANCER EmbeddedLoadBalancer.destroy >>> DELETING "+logDir);
          logDir.delete();
}else{
logger.warn("XXXXX LOAD BALANCER EmbeddedLoadBalancer.destroy >>> TRANSIENT -- NOT DELETING");
        }
    }

//BTM public methods of EmbeddedLoadBalancer

    /**
BTM     * Notify the {@link LoadBalancerService} that a new service is available.
* Notify the {@link LoadBalancer} service that a new service is available.
     * <p>
     * Note: Embedded services must invoke this method <em>directly</em> when
     * they start up.
     * <p>
     * Note: Distributed services implementations MUST discover services using a
     * framework, such as jini, and invoke this method the first time a given
     * service is discovered.
     * 
     * @param serviceUUID
     * @param serviceIface
     * @param hostname
     * 
     * @see IFederationDelegate#serviceJoin(IService, UUID)
     * @see #leave(String, UUID)
     */
    public void join(final UUID serviceUUID,
                     final Class serviceIface,
final String serviceName,
                     final String hostname)
    {  
System.err.println("\n*** ENTERED EmbeddedLoadBalancer.join ***"); 
        if (serviceUUID == null) throw new IllegalArgumentException("null serviceUUID");
        if (serviceIface == null) throw new IllegalArgumentException("null serviceIface");
//BTM***
if (serviceName == null) throw new IllegalArgumentException("null serviceName");
        if (hostname == null) throw new IllegalArgumentException("null hostname");

        if (logger.isInfoEnabled()) {
            logger.info("serviceUUID=" + serviceUUID + ", serviceIface="
+ serviceIface + ", serviceName=" + serviceName + ", hostname=" + hostname);
//BTM***                    + serviceIface + ", hostname=" + hostname);
        }
        
        /*
         * @todo should really be passed in to avoid boundback RMI. Also, this
         * is available for jini as an attribute on the ServiceItem. And in any
         * case the serviceName can be cached here.
         */
//BTM***        String serviceName;
//BTM***        if (IDataService.class == serviceIface) {
//BTM***            try {
//BTM***                serviceName = federation.getDataService(serviceUUID)
//BTM***                        .getServiceName();
//BTM***            } catch (Throwable t) {
//BTM***                logger.warn(t.getMessage(), t);
//BTM***                serviceName = serviceUUID.toString();
//BTM***            }
//BTM***        } else {
//BTM***            serviceName = serviceUUID.toString();
//BTM***        }
        
        lock.lock();
        try {
//BTM***
serviceNameMap.put(serviceUUID, serviceName);
            if (activeHosts.putIfAbsent(hostname, new HostScore(hostname)) == null) {

                if (logger.isInfoEnabled()) {
                    logger.info("New host joined: hostname=" + hostname);
                }
            }
            if (IDataService.class == serviceIface) {
                /*
                 * Add to set of known services.
                 * 
                 * Only data services are registered as [activeServices] since
                 * we only make load balancing decisions for the data services.
                 */
                if (activeDataServices.putIfAbsent(serviceUUID,
                        new ServiceScore(hostname, serviceUUID, serviceName)) == null)
                {
                    if (logger.isInfoEnabled()) {
                        logger.info("Data service join: hostname=" + hostname
                                + ", serviceUUID=" + serviceUUID);
                    }
                }
            }

            if (thisServiceUUID != null) {
                /*
                 * Create node for the joined service's history in the load
                 * balancer's counter set. This just gives eager feedback in
                 * the load balancer's counter set if you are using the httpd
                 * service to watch for service joins.
                 * 
                 * Note: We can't do this until the load balancer has its own
                 * serviceUUID. If that is not available now, then the node for
                 * the joined service will be created when that service
                 * notify()s the load balancer (60 seconds later).
                 */
//BTM***                federation.getCounterSet().makePath(
//BTM***                        AbstractFederation.getServiceCounterPathPrefix(
//BTM***                                serviceUUID, serviceIface, hostname));
getCounterSet().makePath( AbstractFederation.getServiceCounterPathPrefix(serviceUUID, serviceIface, hostname) );
            }
            joined.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Notify the {@link LoadBalancerService} that a service is no longer
     * available.
     * <p>
     * Note: Embedded services must invoke this method <em>directly</em> when
     * they shut down.
     * <p>
     * <p>
     * Note: Distributed services implementations MUST discover services using a
     * framework, such as jini, and invoke this method when a service is no
     * longer registered.
     * 
     * @param serviceUUID
     *            The service {@link UUID}.
     * 
     * @see IFederationDelegate#serviceLeave(UUID)
     * @see #join(UUID, Class, String)
     */
    public void leave(final UUID serviceUUID) {
System.err.println("\n*** ENTERED EmbeddedLoadBalancer.leave ***"); 
        if (logger.isInfoEnabled()) {
            logger.info("serviceUUID=" + serviceUUID);
        }
        try {
            lock.lock();
            /*
             * Note: [activeServices] only contains the DataServices so a null
             * return means either that this is not a data service -or- that we
             * do not have a score for that data service yet.
             */
            final ServiceScore info = activeDataServices.remove(serviceUUID);

            if (info != null) {
                /*
                 * @todo remove history from counters - path is
                 * /host/serviceUUID? Consider scheduling removal after a few
                 * hours or just sweeping periodically for services with no
                 * updates in the last N hours so that people have access to
                 * post-mortem data. For the same reason, we should probably
                 * snapshot the data prior to the leave (especially if there are
                 * WARN or URGENT events for the service) and perhaps
                 * periodically snapshot all of the counter data onto rolling
                 * log files.
                 */
                // root.deletePath(path);
            }
//BTM***
if( (serviceUUID != null) && (serviceNameMap != null) ) {
    serviceNameMap.remove(serviceUUID);
}
        } finally {
            lock.unlock();
        }
    }

    /**
     * Accepts the event, either updates the existing event with the same
     * {@link UUID} or adds the event to the set of recent events, and then
     * prunes the set of recent events so that all completed events older than
     * {@link #eventHistoryMillis} are discarded.
     * 
     * @see EventReceiver
     */
    public void notifyEvent(Event e) throws IOException {
        if(!isOpen()) throw new IllegalStateException();
        eventReceiver.notifyEvent(e);
    }


    public UUID getUnderUtilizedDataService() 
                    throws IOException, TimeoutException, InterruptedException
    {
        return getUnderUtilizedDataServices(1, 1, null/* exclude */)[0];
    }

    public UUID[] getUnderUtilizedDataServices(final int minCount,
            final int maxCount, final UUID exclude) throws IOException,
            TimeoutException, InterruptedException {

        setupLoggingContext();

        try {
            if (minCount < 0) throw new IllegalArgumentException();
            if (maxCount < 0) throw new IllegalArgumentException();

            final UUID[] uuids;
            
            lock.lock();
            try {

                uuids = getUnderUtilizedDataServicesWithLock(minCount,
                        maxCount, exclude);

            } finally {
                lock.unlock();
            }

            if (logger.isInfoEnabled()) {
                logger.info("minCount=" + minCount + ", maxCount=" + maxCount
                        + ", exclude=" + exclude + " : reporting "
                        + uuids.length
                        + " under-utilized and non-excluded services: "
                        + Arrays.toString(uuids));
            }
            return uuids;
        } finally {
            clearLoggingContext();
        }
    }

    /**
     * Normalizes the {@link ServiceScore}s and set them in place.
     * 
     * @param a The new service scores.
     */
    public void setHostScores(final HostScore[] a) {

        /*
         * sort scores into ascending order (least utilized to most utilized).
         */
        Arrays.sort(a);

        /*
         * Compute the totalRawScore.
         */
        double totalRawScore = 0d;

        for (HostScore s : a) {
            totalRawScore += s.rawScore;
        }
        
        /*
         * Compute normalized score, rank, and drank.
         */
        for (int i = 0; i < a.length; i++) {
            final HostScore score = a[i];
            score.rank = i;
            score.drank = ((double)i)/a.length;
            score.score = HostScore.normalize(score.rawScore, totalRawScore);

            // update score in global map.
            activeHosts.put(score.hostname, score);

            if (logger.isInfoEnabled()) {
                logger.info(score.toString());
            }
        }
        
        if(logger.isInfoEnabled()) {
            logger.info("The most active host was: " + a[a.length - 1]);
            logger.info("The least active host was: " + a[0]);
        }
        
        // Atomic replace of the old scores.
        EmbeddedLoadBalancer.this.hostScores.set( a );
    }
    
    /**
     * Normalizes the {@link ServiceScore}s and set them in place.
     * 
     * @param a The new service scores.
     */
    public void setServiceScores(final ServiceScore[] a) {
        
        /*
         * Sort scores into ascending order (least utilized to most utilized).
         */
        Arrays.sort(a);

        /*
         * Compute the totalRawScore.
         */
        double totalRawScore = 0d;

        for (ServiceScore s : a) {
            totalRawScore += s.rawScore;
        }

        /*
         * compute normalized score, rank, and drank.
         */
        for (int i = 0; i < a.length; i++) {
            final ServiceScore score = a[i];
            score.rank = i;
            score.drank = ((double) i) / a.length;
            score.score = HostScore.normalize(score.rawScore, totalRawScore);

            // update score in global map.
            activeDataServices.put(score.serviceUUID, score);

            if (logger.isInfoEnabled()) {
                logger.info(score.toString());
            }
        }
        
        if (logger.isInfoEnabled()) {
            logger.info("The most active service was: " + a[a.length - 1]);
            logger.info("The least active service was: " + a[0]);
        }

        // Atomic replace of the old scores.
        EmbeddedLoadBalancer.this.serviceScores.set(a);
    }

    /**
     * Writes the counters on a file.
     * 
     * @param basename
     *            The basename of the file. The file will be written in the
     *            {@link #logDir}.
     */
    public void logCounters(final String basename) {
        if(isTransient) {
            logger.warn("load balancer is transient - request ignored.");
            return;
        }
        
        final File file = new File(logDir, "counters" + basename + ".xml");

        logCounters(file);
    }
    
    /**
     * Writes the counters on a file.
     * 
     * @param file The file. If the file exists it will be overwritten.
     */
    public void logCounters(final File file) {

        if (file == null) throw new IllegalArgumentException();
        
        if (logger.isDebugEnabled()) {
            logger.debug("[EmbeddedLoadBalancer] Writing counters on " + file);
        }
        
        OutputStream os = null;
        
        try {
            os = new BufferedOutputStream( new FileOutputStream(file) );
//BTM***            federation.getCounterSet().asXML(os, "UTF-8", null/* filter */);
getCounterSet().asXML(os, "UTF-8", null/* filter */);
        } catch(Exception ex) {
            logger.error(ex.getMessage(), ex);
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (Exception ex) {
                    // Ignore.
                }
            }
        }
    }

//BTM private methods of EmbeddedLoadBalancer

    /**
     * Impl. runs with {@link #lock}.
     * 
     * @param minCount
     * @param maxCount
     * @param exclude
     * @return
     * @throws TimeoutException
     * @throws InterruptedException
     */
    private UUID[] getUnderUtilizedDataServicesWithLock(final int minCount,
                                                        final int maxCount,
                                                        final UUID exclude)
                       throws TimeoutException, InterruptedException
    {
        if (logger.isDebugEnabled()) {
            logger.debug("minCount=" + minCount + ", maxCount=" + maxCount
                    + ", exclude=" + exclude);
        }
        
        if (nupdates < initialRoundRobinUpdateCount) {
            /*
             * Use a round-robin assignment for the first N updates while the
             * LBS develops some history on the hosts and services.
             */
            return roundRobinServiceLoadHelper.getUnderUtilizedDataServices(
                    minCount, maxCount, exclude);
        }

        /*
         * Scores for the services in ascending order (least utilized to most
         * utilized).
         */
        final ServiceScore[] scores = this.serviceScores.get();

        if (scores == null || scores.length == 0) {
            if (minCount == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("No scores, minCount is zero - "
                                 +"will return null.");
                }
                return null;
            }
            /*
             * Scores are not available immediately. This will await a
             * non-excluded service join and then return the "under-utilized"
             * services without reference to computed service scores. This path
             * is used when the load balancer first starts up (unless the round
             * robin is enabled) since it will not have scores for at least one
             * pass of the UpdateTask.
             */

            return new ServiceLoadHelperWithoutScores()
                    .getUnderUtilizedDataServices(minCount, maxCount, exclude);
        }

        /*
         * Count the #of non-excluded active services - this is [nok].
         * 
         * Note: [knownGood] is set to a service that (a) is not excluded; and
         * (b) is active. This is the fallback service that we will recommend if
         * minCount is non-zero and we are using the scores and all of a sudden
         * it looks like there are no active services to recommend. This
         * basically codifies a decision point where we accept that this service
         * is active. We choose this as the first active and non-excluded
         * service so that it will be as under-utilized as possible.
         */
        int nok = 0;
        UUID knownGood = null;
        for (int i = 0; i < scores.length && nok < 1; i++) {

            final UUID serviceUUID = scores[i].serviceUUID;

            if (exclude != null && exclude.equals(serviceUUID)) continue;

            if (!activeDataServices.containsKey(serviceUUID)) continue;

            if (knownGood == null) knownGood = serviceUUID;

            nok++;
        }

        if (nok == 0) {
            /*
             * There are no non-excluded active services.
             */
            if (logger.isDebugEnabled()) {
                logger.debug("No non-excluded services.");
            }
            if (minCount == 0) {
                /*
                 * Since there was no minimum #of services demanded by the
                 * caller, we return [null].
                 */

                if (logger.isDebugEnabled()) {
                    logger.debug("No non-excluded services, minCount "
                                 +"is zero - will return null.");
                }
                return null;
            } else {
                /*
                 * We do not have ANY active and scored non-excluded services
                 * and [minCount GT ZERO]. In this case we use a he variant that
                 * does not use scores and that awaits a service join.
                 */
                if (logger.isDebugEnabled()) {
                    logger.debug("Will await a service join.");
                }
                return new ServiceLoadHelperWithoutScores()
                        .getUnderUtilizedDataServices(minCount, maxCount,
                                exclude);
            }
        }

        /*
         * Use the scores to compute the under-utilized services.
         */

        if (logger.isDebugEnabled()) {
            logger.debug("Will recommend services based on scores: #scored="
                    + scores.length + ", nok=" + nok + ", knownGood="
                    + knownGood + ", exclude=" + exclude);
        }

        assert nok > 0;
        assert knownGood != null;
        assert scores != null;
        assert scores.length != 0;

        return new ServiceLoadHelperWithScores(knownGood, scores)
                .getUnderUtilizedDataServices(minCount, maxCount, exclude);
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

//BTM***
    synchronized private CounterSet getCounterSet() {
        if (countersRoot == null) {
            countersRoot = new CounterSet();
            if (statisticsCollector != null) {
                countersRoot.attach(statisticsCollector.getCounters());
            }
//BTM
String serviceCounterPathPrefix = AbstractFederation.getServiceCounterPathPrefix(thisServiceUUID, SERVICE_TYPE, hostname);
this.serviceRoot = countersRoot.makePath(serviceCounterPathPrefix);
//BTM            serviceRoot = countersRoot.makePath(getServiceCounterPathPrefix());
            /* Basic counters. */
            AbstractStatisticsCollector.addBasicServiceOrClientCounters(
this.serviceRoot, SERVICE_NAME, SERVICE_TYPE, this.getProperties());
//BTM                    serviceRoot, getServiceName(), getServiceIface(), client
//BTM                            .getProperties());
        }
        return countersRoot;
    }

    public CounterSet getServiceCounterSet() {
        getCounterSet();// defines [serviceRoot] as side effect.
        return this.serviceRoot;
        
    }
//BTM***



//BTM nested classes of EmbeddedLoadBalancer

    /**
     * Computes and updates the {@link ServiceScore}s based on an examination
     * of aggregated performance counters.
     * 
     * @todo There could be a score for the last minute, hour, and day or the
     *       last minute, five minutes, and ten minutes.
     * 
     * @todo For starters, we can just run some hand-coded rules. Consider
     *       special transition states for new hosts and services.
     * 
     * @todo The scoring logic should be pluggable so that people can reply on
     *       the data that they have for their platform(s) that seems to best
     *       support decision-making and can apply rules for their platforms,
     *       environment, and applications which provide the best overall QOS.
     * 
     * @todo The logic to choose the under- and over-utilized services based on
     *       the services scores should be configurable (this is different from
     *       the logic to compute those scores).
     * 
     * @todo if a client does not
     *       {@link LoadBalancer#notify(String, byte[])} for 120 seconds
     *       then presume dead? this requires that we compute the age of the
     *       last reported counter value. e.g., do a counter scan for the
     *       service and report the largest value for lastModified() on any
     *       counter for that service.
     */
    protected class UpdateTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final protected transient Logger log = Logger.getLogger(UpdateTask.class);

        public UpdateTask() { }

        /**
         * Note: Don't throw anything here since we don't want to have the task
         * suppressed!
         */
        public void run() {
            try {
                updateHostScores();
                updateServiceScores();
                setupCounters();
                logCounters();
            } catch (Throwable t) {
                logger.error("Problem in update task?", t);
            } finally {
                nupdates++;
            }
        }

        /**
         * (Re-)compute the utilization score for each active host.
         */
        protected void updateHostScores() {
            if(activeHosts.isEmpty()) {
                if (logger.isInfoEnabled()) {
                    logger.info("No active hosts");
                }
                return;
            }

            /*
             * Update scores for the active hosts.
             */
            final Vector<HostScore> scores = new Vector<HostScore>();
            
            // For each host
            final Iterator<ICounterSet> itrh = 
getCounterSet().counterSetIterator();
//BTM***                federation.getCounterSet().counterSetIterator();
            
            while(itrh.hasNext()) {
                final CounterSet hostCounterSet = (CounterSet) itrh.next();

                // Note: name on hostCounterSet is the fully qualified hostname.
                final String hostname = hostCounterSet.getName();

                if(!activeHosts.containsKey(hostname)) {
                    // Host is not active.
                    if (logger.isDebugEnabled()) {
                        logger.debug("Host is not active: " + hostname);
                    }
                    continue;
                }
                
                /*
                 * Compute the score for that host.
                 */
                HostScore score;
                try {
                    score = computeScore(hostname, hostCounterSet);
                } catch (Exception ex) {

                    logger.error("Problem computing host score: " + hostname, ex);

                    /*
                     * Keep the old score if we were not able to compute a new
                     * score.
                     * 
                     * Note: if the returned value is null then the host was
                     * asynchronously removed from the set of active hosts.
                     */
                    score = activeHosts.get(hostname);

                    if (score == null) {
                        logger.warn("Host gone during update task: " + hostname);
                        continue;
                    }
                }

                /*
                 * Add to collection of scores.
                 */
                scores.add(score);
            }

            if (scores.isEmpty()) {
                logger.warn("No performance counters for hosts, but "
                        + activeHosts.size() + " active hosts");

                EmbeddedLoadBalancer.this.hostScores.set( null );

                return;
            }

            // scores as an array.
            final HostScore[] a = scores.toArray(new HostScore[] {});

            setHostScores(a);
        }

        /**
         * (Re-)compute the utilization score for each active service.
         * <p>
         * Note: There is a dependency on
         * {@link AbstractFederation#getServiceCounterPathPrefix(UUID, Class, String)}.
         * This method assumes that the service {@link UUID} is found in a
         * specific place in the constructed path.
         */
        protected void updateServiceScores() {
            if(activeDataServices.isEmpty()) {
                if (logger.isInfoEnabled()) {
                    logger.info("No active services");
                }
                EmbeddedLoadBalancer.this.serviceScores.set( null );

                return;
            }
            
            /*
             * Update scores for the active services.
             */

            final Vector<ServiceScore> scores = new Vector<ServiceScore>();
            
            // For each host
            final Iterator<ICounterSet> itrh = 
getCounterSet().counterSetIterator();
//BTM***                federation.getCounterSet().counterSetIterator();

            while(itrh.hasNext()) {
                
                final CounterSet hostCounterSet = (CounterSet) itrh.next();
                
                // Note: name on hostCounterSet is the fully qualified hostname.
                final String hostname = hostCounterSet.getName();

                // Pre-computed score for the host on which the service is running.
                final HostScore hostScore = activeHosts.get(hostname);

                if (hostScore == null) {
                    // Host is not active.
                    if (logger.isInfoEnabled()) {
                        logger.info("Host is not active: " + hostname);
                    }
                    continue;
                }

                // lookup path: /hostname/service
                final CounterSet serviceIfacesCounterSet = 
                    (CounterSet) hostCounterSet.getPath("service");

                if (serviceIfacesCounterSet == null) {
                    logger.warn("No services interfaces? hostname=" + hostname);
                    continue;
                }

                // for each service interface type: /hostname/service/iface
                final Iterator<ICounterSet> itrx = 
                    serviceIfacesCounterSet.counterSetIterator();

                // for each service under that interface type
                while (itrx.hasNext()) {

                    // path: /hostname/service/iface/UUID
                    final CounterSet servicesCounterSet = 
                                         (CounterSet) itrx.next();

                    // For each service.
                    final Iterator<ICounterSet> itrs = 
                              servicesCounterSet.counterSetIterator();

                    while (itrs.hasNext()) {

                        final CounterSet serviceCounterSet = 
                                             (CounterSet) itrs.next();

                        /*
                         * Note: [name] on serviceCounterSet is the serviceUUID.
                         * 
                         * Note: This creates a dependency on
                         * AbstractFederation#getServiceCounterPathPrefix(...)
                         */
                        final String serviceName = serviceCounterSet.getName();
                        final UUID serviceUUID;
                        try {
                            serviceUUID = UUID.fromString(serviceName);
                        } catch (Exception ex) {
                            logger.error("Could not parse service name as UUID?\n"
                                      + "hostname=" + hostname
                                      + ", serviceCounterSet.path="
                                      + serviceCounterSet.getPath()
                                      + ", serviceCounterSet.name="
                                      + serviceCounterSet.getName(), ex);
                            continue;
                        }

                        if (!activeDataServices.containsKey(serviceUUID)) {
                            /*
                             * Note: Only data services are entered in this map,
                             * so this filters out the non-dataServices from the
                             * load balancer's computations.
                             */
                            continue;
                        }

                        /*
                         * Compute the score for that service.
                         */
                        ServiceScore score;
                        try {
                            score = computeScore(hostScore, serviceUUID,
                                    hostCounterSet, serviceCounterSet);
                        } catch (Exception ex) {
                            logger.error("Problem computing service score: "
                                      + serviceCounterSet.getPath(), ex);
                            /*
                             * Keep the old score if we were not able to compute
                             * a new score.
                             * 
                             * Note: if the returned value is null then the
                             * service asynchronously was removed from the set
                             * of active services.
                             */
                            score = activeDataServices.get(serviceUUID);

                            if (score == null) {
                                if (logger.isInfoEnabled()) {
                                    logger.info("Service leave during update task: "
                                             + serviceCounterSet.getPath());
                                }
                                continue;
                            }
                        }

                        /*
                         * Add to collection of scores.
                         */
                        scores.add(score);
                    }
                }
            }

            if (scores.isEmpty()) {
                logger.warn("No performance counters for services, but "
                        + activeDataServices.size() + " active services");

                EmbeddedLoadBalancer.this.serviceScores.set( null );

                return;
            }
            // scores as an array.
            final ServiceScore[] a = scores.toArray(new ServiceScore[] {});

            // normalize and set in place.
            setServiceScores(a);
        }
        
        /**
         * Compute the score for a host.
         * <p>
         * The host scores MUST reflect critical resource exhaustion, especially
         * DISK free space, which can take down all services on the host, and
         * SWAPPING, which can bring the effective throughput of the host to a
         * halt. All other resources fail soft, by causing the response time to
         * increase.
         * <p>
         * Note: DISK exhaustion can lead to immediate failure of all services
         * on the same host. A host that is nearing DISK exhaustion SHOULD get
         * heavily dinged and an admin SHOULD be alerted.
         * <p>
         * The correct response for heavy swapping is to alert an admin to
         * shutdown one or more processes on that host. <strong>If you do not
         * have failover provisioned for your data services then DO NOT shutdown
         * data services or you WILL loose data!</strong>
         * <p>
         * Note: If we are not getting critical counters for some host then we
         * are assuming a reasonable values for the missing data and computing
         * the utilization based on those assumptions. Note that a value of zero
         * (0) may be interepreted as either critically high utilization or no
         * utilization depending on the performance counter involved and that
         * the impact of the different counters can vary depending on the
         * formula used to compute the utilization score.
         * 
         * @param hostname
         *            The fully qualified hostname.
         * @param hostCounterSet
         *            The performance counters for that host.
         * @param serviceCounterSet
         *            The performance counters for that service.
         * 
         * @return The computed host score.
         */
        protected HostScore computeScore(final String hostname,
                final ICounterSet hostCounterSet) {

            /*
             * Is the host swapping heavily?
             * 
             * @todo if heavy swapping persists then lower the score even
             * further.
             * 
             * @todo The % of the physical memory and the % of the swap space
             * that have been used are also strong indicators.
             */
            final int majorFaultsPerSec = (int) getCurrentValue(hostCounterSet,
                    IRequiredHostCounters.Memory_majorFaultsPerSecond, 0d/* default */);

            /*
             * Is the host out of disk?
             * 
             * FIXME Need the swap space remaining. Low swap presages heavy
             * swapping.
             * 
             * @todo this will issue a warning for a windows host on which a
             * service is just starting up. For some reason, it takes a few
             * cycles to begin reporting performance counters on a windows host
             * and the initial counters will therefore all be reported as zeros.
             * This problem should be fixed, but we also need to discount an
             * average whose result is zero if the #of samples is also zero. In
             * that case we just don't have any information. Likewise, when the
             * #of samples to date (cumulative or just the #of minutes in 0:60
             * of data in the minutes history) is less than 5 minutes worth of
             * data then we may still need to discount the data. Also consider
             * adding a "moving average" computation to the History so that we
             * can smooth short term spikes.
             */
            final double percentDiskFreeSpace = getCurrentValue(hostCounterSet,
                    IRequiredHostCounters.LogicalDisk_PercentFreeSpace, .5d/* default */);

            /*
             * The percent of the time that the CPUs are idle.
             */
            final double percentProcessorIdle = 1d - getAverageValueForMinutes(
                    hostCounterSet, IRequiredHostCounters.CPU_PercentProcessorTime,
                    .5d, historyMinutes);

            /*
             * The percent of the time that the CPUs are idle when there is an
             * outstanding IO request.
             */
            final double percentIOWait = getAverageValueForMinutes(
                    hostCounterSet, IHostCounters.CPU_PercentIOWait,
                    .01d/* default */, historyMinutes);

            /*
             * Note: This reflects the disk IO utilization primarily through
             * IOWAIT.
             * 
             * @todo Play around with other forumulas too.
             */
            double adjustedRawScore;
            final double baseRawScore = adjustedRawScore = (1d + percentIOWait * 100d)
                    / (1d + percentProcessorIdle);

            if (majorFaultsPerSec > 50) {

                // much higher utilization if the host is swapping heavily.
                adjustedRawScore *= 10;

                logger.warn("hostname=" + hostname
                                + " : swapping heavily: pages/sec="
                                + majorFaultsPerSec);

            } else if (majorFaultsPerSec > 10) {

                // higher utilization if the host is swapping.
                adjustedRawScore *= 2d;

                logger.warn("hostname=" + hostname + " : swapping: pages/sec="
                        + majorFaultsPerSec);

            }

            if (percentDiskFreeSpace < .05) {

                // much higher utilization if the host is very short on disk.
                adjustedRawScore *= 10d;

                logger.warn("hostname=" + hostname
                        + " : very short on disk: freeSpace="
                        + percentDiskFreeSpace * 100 + "%");

            } else if (percentDiskFreeSpace < .10) {

                // higher utilization if the host is short on disk.
                adjustedRawScore *= 2d;

                logger.warn("hostname=" + hostname
                        + " : is short on disk: freeSpace="
                        + percentDiskFreeSpace * 100 + "%");

            }

            if (logger.isInfoEnabled()) {

                logger.info("hostname=" + hostname + " : adjustedRawScore("
                        + scoreFormat.format(adjustedRawScore)
                        + "), baseRawScore(" + scoreFormat.format(baseRawScore)
                        + ") = (1d + percentIOWait("
                        + percentFormat.format(percentIOWait)
                        + ") * 100d) / (1d + percentProcessorIdle("
                        + percentFormat.format(percentProcessorIdle)
                        + "), majorFaultsPerSec=" + majorFaultsPerSec
                        + ", percentDiskSpaceFree="
                        + percentFormat.format(percentDiskFreeSpace));

            }
            
            final HostScore hostScore = new HostScore(hostname, adjustedRawScore);

            return hostScore;

        }
        
        /**
         * Format for the computed scores.
         */
        final NumberFormat scoreFormat;
        {
            scoreFormat = NumberFormat.getInstance();
            scoreFormat.setMaximumFractionDigits(2);
            scoreFormat.setMinimumIntegerDigits(1);
        }
        
        /**
         * Format for percentages such as <code>IO Wait</code> where the
         * values are in [0.00:1.00].
         */
        final NumberFormat percentFormat;
        {
            percentFormat = NumberFormat.getInstance();
            percentFormat.setMaximumFractionDigits(2);
            percentFormat.setMinimumIntegerDigits(1);
        }
        
        /**
         * Format for elapsed times measured in milliseconds.
         */
        final NumberFormat millisFormat;
        {
           millisFormat = NumberFormat.getIntegerInstance();
        }
        
        /**
         * Format for bytes.
         */
        final NumberFormat bytesFormat;
        {
           bytesFormat = NumberFormat.getIntegerInstance();
           bytesFormat.setGroupingUsed(true);
        }
        
        /**
         * Compute the score for a service.
         * <p>
         * Note: utilization is defined in terms of transient system resources :
         * CPU, IO (DISK and NET), RAM. A host with enough CPU/RAM/IO/DISK can
         * support more than one data service. Therefore it is important to look
         * at not just host utilization but also at process utilization.
         * 
         * @param hostScore
         *            The pre-computed score for the host on which the service
         *            is running.
         * @param serviceUUID
         *            The service {@link UUID}.
         * @param hostCounterSet
         *            The performance counters for that host (in case you need
         *            anything that is not already in the {@link HostScore}).
         * @param serviceCounterSet
         *            The performance counters for that service.
         * 
         * @return The computed score for that service.
         */
        protected ServiceScore computeScore(final HostScore hostScore,
                final UUID serviceUUID, final ICounterSet hostCounterSet,
                final ICounterSet serviceCounterSet) {

            assert hostScore != null;
            assert serviceUUID != null;
            assert hostCounterSet != null;
            assert serviceCounterSet != null;
            
            // verify that the host score has been normalized.
            assert hostScore.rank != -1 : hostScore.toString();
            
            // resolve the service name : @todo refactor RMI out of this method.
//BTM***
String serviceName = serviceNameMap.get(serviceUUID);
//BTM***            String serviceName = "N/A";
//BTM***            try {
//BTM***                serviceName = federation.getDataService(serviceUUID)
//BTM***                        .getServiceName();
//BTM***            } catch (Throwable t) {
//BTM***                logger.warn(t.getMessage(), t);
//BTM***            }
            
            /*
             * The average queuing time for the unisolated write service is used
             * as the primary indicator of the write load of the service. The
             * average queueing time is preferred to the average queue length as
             * the queueing time is directly correlated to the throughput of the
             * service.
             * 
             * Note: We use the measure of write load to drive load balancing
             * decisions. This is in contrast to high availability for readers,
             * where readers can be directed to failover instances.
             * 
             * @todo verify that the queueing time measurement in millis is
             * sufficient rather than nanos as queuing times can become quite
             * short.
             * 
             * @todo There is a lot more that can be considered and under linux
             * we have access to per-process counters for CPU, DISK, and MEMORY.
             */
//            final double averageQueueLength = getAverageValueForMinutes(
//                    serviceCounterSet, IDataServiceCounters.concurrencyManager
//                            + ps + IConcurrencyManagerCounters.writeService
//                            + ps + IThreadPoolExecutorCounters.AverageQueueLength,
//                    0d/* default (queueLength) */, historyMinutes);

            final double averageQueueingTime = getAverageValueForMinutes(
                    serviceCounterSet, IDataServiceCounters.concurrencyManager
                            + ps + IConcurrencyManagerCounters.writeService
                            + ps + IThreadPoolExecutorTaskCounters.AverageQueuingTime,
                    10d/* default (ms) */, historyMinutes);

            final double dataDirBytesAvailable = getAverageValueForMinutes(
                    serviceCounterSet, IDataServiceCounters.resourceManager
                            + ps + IResourceManagerCounters.StoreManager
                            + ps + IStoreManagerCounters.DataDirBytesAvailable,
                    Bytes.gigabyte * 20/* default */, historyMinutes);
            
            final double tmpDirBytesAvailable = getAverageValueForMinutes(
                    serviceCounterSet, IDataServiceCounters.resourceManager
                            + ps + IResourceManagerCounters.StoreManager
                            + ps + IStoreManagerCounters.TmpDirBytesAvailable,
                    Bytes.gigabyte * 10/* default */, historyMinutes);

            final double rawScore = (averageQueueingTime + 1) * (hostScore.score + 1);

            double adjustedRawScore = rawScore;

            /*
             * dataDir
             * 
             * Note: If you set these threasholds to GT the default value
             * reported when the counters are not yet available then you will
             * see false 'short on disk' claims. They will go away once the
             * performance counters arrive with real disk space measurements.
             */
            if (dataDirBytesAvailable < Bytes.gigabyte * 1) {
                // much higher utilization if the host is very short on disk.
                adjustedRawScore *= 10d;

                logger.warn("service=" + serviceName + " : very short on disk: "
                        + IStoreManagerCounters.DataDirBytesAvailable + "="
                        + bytesFormat.format(dataDirBytesAvailable));

            } else if (dataDirBytesAvailable < Bytes.gigabyte * 10) {
                // higher utilization if the host is short on disk.
                adjustedRawScore *= 2d;

                logger.warn("service=" + serviceName + " : is short on disk: "
                        + IStoreManagerCounters.DataDirBytesAvailable + "="
                        + bytesFormat.format(dataDirBytesAvailable));
            }

            /*
             * tmpDir
             * 
             * Note: If you set these threasholds to GT the default value
             * reported when the counters are not yet available then you will
             * see false 'short on disk' claims. They will go away once the
             * performance counters arrive with real disk space measurements.
             * 
             * These thresholds are currently set to trigger at any value LT the
             * default, which masks the issue.
             */
            if (tmpDirBytesAvailable < Bytes.gigabyte * 1) {
                // much higher utilization if the host is very short on disk.
                adjustedRawScore *= 10d;

                logger.warn("service=" + serviceName + " : very short on disk: "
                        + IStoreManagerCounters.TmpDirBytesAvailable + "="
                        + bytesFormat.format(tmpDirBytesAvailable));

            } else if (tmpDirBytesAvailable < Bytes.gigabyte * 10) {
                // higher utilization if the host is short on disk.
                adjustedRawScore *= 2d;

                logger.warn("service=" + serviceName + " : is short on disk: "
                        + IStoreManagerCounters.TmpDirBytesAvailable + "="
                        + bytesFormat.format(tmpDirBytesAvailable));
            }

            if (logger.isInfoEnabled()) {
                logger.info("serviceName=" + serviceName//
                        + ", serviceUUID=" + serviceUUID //
//                        + ", averageQueueLength=" + averageQueueLength//
                        + ", averageQueueingTime=" + millisFormat.format(averageQueueingTime)//
                        + ", dataDirBytesAvail="+bytesFormat.format(dataDirBytesAvailable)//
                        + ", tmpDirBytesAvail="+bytesFormat.format(tmpDirBytesAvailable)//
                        + ", adjustedRawStore="+adjustedRawScore//
                        + ", rawScore(" + scoreFormat.format(rawScore) + ") "//
                        + "= (averageQueueingTime("+ averageQueueingTime+ ") + 1) "//
                        + "* (hostScore("+ scoreFormat.format(hostScore.score) + ") + 1)"//
                        );
            }

            final ServiceScore score = 
                new ServiceScore(hostScore.hostname, serviceUUID,
                                 serviceName, adjustedRawScore);
            return score;
        }

        protected double getCurrentValue(ICounterSet counterSet,
                                         String path,
                                         double defaultValue)
        {
            assert counterSet != null;
            assert path != null;

            final ICounter c = (ICounter) counterSet.getPath(path);

            if (c == null) return defaultValue;

            try {
                double val = (Double) c.getValue();
                return val;
            } catch (Exception ex) {
                logger.warn("Could not read double value: counterSet="
                            + counterSet.getPath() + ", counter=" + path);
                return defaultValue;
            }
        }

        /**
         * Return the average of the counter having the given path over the last
         * <i>minutes</i> minutes.
         * 
         * @param counterSet
         * @param path
         * @param defaultValue
         * @param minutes
         * @return
         * 
         * FIXME should be a weighted average, right?
         */
        protected double getAverageValueForMinutes(
                final ICounterSet counterSet, final String path,
                final double defaultValue, final int minutes) {

            assert counterSet != null;
            assert path != null;

            final ICounter c = (ICounter) counterSet.getPath(path);

            if (c == null) return defaultValue;

            try {
                if (c.getInstrument() instanceof HistoryInstrument) {
                    final HistoryInstrument inst = 
                              (HistoryInstrument)c.getInstrument();

                    final double val = 
                    ((Number) inst.getHistory().getAverage(minutes)).doubleValue();

                    return val;
                } else {
                    /*
                     * When the LBS is run as an embedded process it can wind up
                     * having the performance counters collected within its
                     * process in which case it will not have histories for the
                     * data and we just return the current value.
                     */
                    logger.warn("Not a history: " + c);
                    return ((Number)c.getValue()).doubleValue();
                }
            } catch (Exception ex) {
                logger.warn("Could not read: counterSet=" + counterSet.getPath()
                            + ", counter=" + path, ex);
                return defaultValue;
            }
        }

        /**
         * Sets up reporting for the computed per-host and per-service scores.
         * These counters are reported under the service {@link UUID} for the
         * {@link LoadBalancerService} itself. This makes it easy to consult the
         * scores for the various hosts and services.
         * <p>
         * Note: The host and service scores will not appear until the
         * {@link UpdateTask} has executed and those scores have been computed.
         * 
         * @see EmbeddedLoadBalancer.Options#UPDATE_DELAY
         * 
         * @todo counters for service scores should be eventually removed after
         *       the service leaves. Likewise for host scores. However, these
         *       counters SHOULD remain available for a while for post-mortem of
         *       the service/host, e.g., at least 2-3 days. This would be fixed
         *       with a persistence model for the scores.
         */
        protected void setupCounters() {
            
            final CounterSet serviceRoot = 
getServiceCounterSet();
//BTM***                                 federation.getServiceCounterSet();

            final long now = System.currentTimeMillis();
            
            // per-host scores.
            {
                final CounterSet hosts = serviceRoot.makePath("hosts");
                final CounterSet tmpScores = hosts.makePath("scores");
                final CounterSet tmpFormula = hosts.makePath("formula");

                synchronized (tmpScores) {
                    for (HostScore hs : activeHosts.values()) {
                        final String hn = hs.hostname;

                        if (tmpScores.getChild(hn) == null) {
                            tmpScores.addCounter
                                (hn,
                                 new HistoryInstrument<Double>
                                         (new History<Double>
                                                  (new Double[60], 
                                                   PeriodEnum.Minutes.getPeriodMillis(),
                                                   true/*overwrite*/)));
                        }

                        {
                            final ICounter counter = 
                                               (ICounter) tmpScores.getChild(hn);

                            final HistoryInstrument<Double> inst = 
                                  (HistoryInstrument<Double>) counter.getInstrument();

                            final HostScore score = activeHosts.get(hn);

                            if (score != null) {
                                inst.add(now, score.drank);
                            }
                        }
                    }
                } // synchronized(scores)

                synchronized (tmpFormula) {

                    for (HostScore hs : activeHosts.values()) {

                        final String hn = hs.hostname;

                        if (tmpFormula.getChild(hn) == null) {

                            tmpFormula.addCounter(hn,
                                    new HistoryInstrument<String>(
                                                    new History<String>(
                                                            new String[60],
                                                            PeriodEnum.Minutes
                                                                    .getPeriodMillis(),
                                                            true/*overwrite*/)));

                        }

                        {

                            final ICounter counter = (ICounter) tmpFormula
                                    .getChild(hn);

                            final HistoryInstrument<String> inst = (HistoryInstrument<String>) counter
                                    .getInstrument();
                            final HostScore score = activeHosts.get(hn);

                            if (score != null) {
                                inst.add(now, score.toString());
                            }
                        }
                    }
                } // synchronized(formula)

            } // host scores
            
            // per-service scores.
            {
                final CounterSet services = serviceRoot.makePath("services");
                final CounterSet tmpScores = services.makePath("scores");
                final CounterSet tmpFormula = services.makePath("formula");

                synchronized (tmpScores) {
                    for (ServiceScore ss : activeDataServices.values()) {
                        /*
                         * @todo use serviceName, but it has embedded slashes
                         * (in bigdata-jini) just like a path which makes life
                         * difficult.
                         */
                        final String idStr = ss.serviceUUID.toString();

                        if (tmpScores.getChild(idStr) == null) {
                            tmpScores.addCounter
                                (idStr,
                                 new HistoryInstrument<Double>
                                         (new History<Double>
                                                  (new Double[60],
                                                   PeriodEnum.Minutes.getPeriodMillis(),
                                                   true/*overwrite*/)));                            
                        }
                        
                        {
                            final ICounter counter = (ICounter) tmpScores.getChild(idStr);
                            final HistoryInstrument<Double> inst = 
                                      (HistoryInstrument<Double>) counter.getInstrument();

                            final ServiceScore score = 
                                      activeDataServices.get(ss.serviceUUID);

                            if (score != null) {
                                inst.add(now, score.drank);
                            }
                        }
                    }
                } // synchronized(scores)

                synchronized (tmpFormula) {
                    for (ServiceScore ss : activeDataServices.values()) {
                        /*
                         * @todo use serviceName, but it has embedded slashes
                         * (in bigdata-jini) just like a path which makes life
                         * difficult.
                         */
                        final String idStr = ss.serviceUUID.toString();

                        if (tmpFormula.getChild(idStr) == null) {

                            tmpFormula.addCounter
                                (idStr,
                                 new HistoryInstrument<String>
                                         (new History<String>
                                                  (new String[60],
                                                   PeriodEnum.Minutes.getPeriodMillis(),
                                                   true/*overwrite*/)));
                            
                        }
                        
                        {
                            final ICounter counter = (ICounter) tmpFormula.getChild(idStr);
                            final HistoryInstrument<String> inst = 
                                      (HistoryInstrument<String>)counter.getInstrument();
                            final ServiceScore score = activeDataServices.get(ss.serviceUUID);
                            if (score != null) {
                                inst.add(now, score.toString());
                            }
                        }
                    }
                }// synchronized(formula)
                
            } // service scores.
            
        } // end method
        
        /**
         * Writes the counters on a file.
         * 
         * @see Options
         */
        protected void logCounters() {
            final long now = System.currentTimeMillis();
            final long elapsed = now - logLastMillis;

            if (elapsed > logDelayMillis) {
                final String basename = "" + (logFileCount % logMaxFiles);
                EmbeddedLoadBalancer.this.logCounters(basename);
                logFileCount++;
                logLastMillis = now;
            }
        }
    }//end class UpdateTask

    /**
     * Integration with the {@link LoadBalancer} service.
     */
    protected class RoundRobinServiceLoadHelper 
                        extends AbstractRoundRobinServiceLoadHelper
    {
//BTM - BEGIN
        private LookupCache shardLocatorCache;
        private LookupCache remoteShardLocatorCache;
        private LookupCache shardCache;
        private LookupCache remoteShardCache;
        private Map<UUID, IDataService> embeddedDataServiceMap;

        RoundRobinServiceLoadHelper
                        (final LookupCache shardLocatorCache,
                         final LookupCache remoteShardLocatorCache,
                         final LookupCache shardCache,
                         final LookupCache remoteShardCache,
                         final Map<UUID, IDataService> embeddedDataServiceMap)
        {
            this.shardLocatorCache = shardLocatorCache;
            this.remoteShardLocatorCache = remoteShardLocatorCache;
            this.shardCache = shardCache;
            this.remoteShardCache = remoteShardCache;

            //BTM*** - remove when DataService converted to smart proxy?
            this.embeddedDataServiceMap = embeddedDataServiceMap;
    }
//BTM - END

        protected UUID[] awaitServices(int minCount, long timeout)
                throws InterruptedException, TimeoutException
        {
//BTM - BEGIN ---------------------------------------------------------------
//BTM            return ((AbstractScaleOutFederation) EmbeddedLoadBalancer.this
//BTM                    .federation).awaitServices(minCount, timeout);

            if(remoteShardLocatorCache != null) {//all caches not null
                ServiceItem shardLocatorItem = 
                                remoteShardLocatorCache.lookup(null);
                if(shardLocatorItem == null) {
                    shardLocatorItem = shardLocatorCache.lookup(null);
                }
                List<UUID> remoteShardIds = 
                               remoteShardServiceIdsToUuidList
                                   (remoteShardCache.lookup(null, minCount));
                List<UUID> shardIds = shardServiceIdsToUuidList
                                          (shardCache.lookup(null, minCount));
                List<UUID> uuids = concatenate(remoteShardIds, shardIds);
                if(uuids.size() >= minCount) {
                    return getShardServiceUUIDs(uuids);
                }

                //less than minCount, wait for late joiners
                long nSecs = timeout/1000L;
                for(long i=0L; i<nSecs; i++) {
                    if(shardLocatorItem == null) {
                        shardLocatorItem = 
                            remoteShardLocatorCache.lookup(null);
                        if(shardLocatorItem == null) {
                            shardLocatorItem = shardLocatorCache.lookup(null);
                        }
                    }
                    remoteShardIds = 
                        remoteShardServiceIdsToUuidList
                            (remoteShardCache.lookup(null, minCount));
                    shardIds = shardServiceIdsToUuidList
                                   (shardCache.lookup(null,minCount));
                    uuids = concatenate(remoteShardIds, shardIds);

                    if( shardLocatorItem != null && uuids.size() >= minCount) {
                        return getShardServiceUUIDs(uuids);
                    }
                    try {
                        Thread.sleep(1000L);//wait a second and try again
                    } catch (InterruptedException e) { }
                }
                String shardLocatorDiscovered = 
                           (shardLocatorItem == null ?
                               "shard locator service NOT discovered" :
                               "shard locator service discovered");
                throw new TimeoutException("elapsed="+timeout+" ms, "
                                           +shardLocatorDiscovered+", "
                                           +"shard services ["+uuids.size()
                                           +" discovered but "+minCount
                                           +" required");
            } else {
                Set<UUID> uuidSet = embeddedDataServiceMap.keySet();
                return uuidSet.toArray(new UUID[uuidSet.size()]);
            }
        }

        private List<UUID> remoteShardServiceIdsToUuidList
                                                 (ServiceItem[] serviceItems)
        {
            List<UUID> idList = new ArrayList<UUID>();
            for(int i=0; i<serviceItems.length; i++) {
                ServiceItem item = serviceItems[i];
                Object service = item.service;
                if(service == null) continue;

                // try to avoid remote calls by getting info from attrs
                ServiceID serviceId = item.serviceID;
                Entry[] attrs = item.attributeSets;
                UUID serviceUUID = null;
                ServiceUUID serviceUUIDAttr = 
                        (ServiceUUID)(EntryUtil.getEntryByType
                                                (attrs, ServiceUUID.class));
                if(serviceUUIDAttr != null) {
                    serviceUUID = serviceUUIDAttr.serviceUUID;
                } else {
                    try {
                        serviceUUID = ((IService)service).getServiceUUID();
                    } catch(IOException e) {
                        if(logger.isDebugEnabled()) {
                            logger.log(Level.DEBUG, "failed to retrieve "
                                       +"serviceUUID [service="
                                       +service.getClass()+", ID="
                                       +serviceId+"]", e);
                        }
                    }
                }
                if(serviceUUID != null) idList.add(serviceUUID);
            }
            return idList;
        }

        private List<UUID> shardServiceIdsToUuidList
                                                 (ServiceItem[] serviceItems)
        {
            List<UUID> idList = new ArrayList<UUID>();
            for(int i=0; i<serviceItems.length; i++) {
                ServiceItem item = serviceItems[i];
                Object service = item.service;
                if(service == null) continue;
                idList.add( ((Service)service).getServiceUUID() );
            }
            return idList;
        }

        private List<UUID> concatenate(List<UUID> list0, List<UUID> list1) {
            List<UUID> retList = new ArrayList<UUID>();
            if(list0 != null) {
                for(UUID id : list0) {
                    retList.add(id);
                }
            }
            if(list1 != null) {
                for(UUID id : list1) {
                    retList.add(id);
                }
            }
            return retList;
        }

        private UUID[] getShardServiceUUIDs(List<UUID> idList) {
            if(idList == null) return new UUID[0];
            return idList.toArray( new UUID[idList.size()] );
        }
//BTM - END -----------------------------------------------------------------
    }

    private class IMetadataServiceOnlyFilter 
                  implements ServiceItemFilter
    {
	public boolean check(ServiceItem item) {
            if((item == null) || (item.service == null)) {
                return false;
            }
            Class serviceType = (item.service).getClass();
            boolean isIMetadataService = 
             (IMetadataService.class).isAssignableFrom(serviceType);
            return isIMetadataService;
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

    /**
     * Integration with the {@link LoadBalancerService}.
     */
    protected class ServiceLoadHelperWithoutScores extends
            AbstractServiceLoadHelperWithoutScores {

        public ServiceLoadHelperWithoutScores() {
            super(serviceJoinTimeout);
        }

        @Override
        protected void awaitJoin(long timeout, TimeUnit unit) throws InterruptedException {
            // await a join.
            joined.await(timeout, unit);
        }

        @Override
        protected UUID[] getActiveServices() {
            return activeDataServices.keySet().toArray(new UUID[] {});
        }

        @Override
        protected boolean isActiveDataService(UUID serviceUUID) {
            return activeDataServices.containsKey(serviceUUID);
        }

        @Override
        protected boolean isUnderUtilizedDataService(ServiceScore score,
                ServiceScore[] scores) {
            return EmbeddedLoadBalancer.this.isUnderUtilizedDataService(score,
                    scores);
        }
    }
    
    /**
     * Integration with the {@link LoadBalancer} service.
     */
    protected class ServiceLoadHelperWithScores
                        extends AbstractServiceLoadHelperWithScores
    {

        public ServiceLoadHelperWithScores(final UUID knownGood,
                                           final ServiceScore[] scores)
        {
            super(serviceJoinTimeout, knownGood, scores);
        }

        @Override
        protected void awaitJoin(long timeout, TimeUnit unit)
                           throws InterruptedException
        {
            // await a join.
            joined.await(timeout, unit);
        }

        @Override
        protected UUID[] getActiveServices() {
            return activeDataServices.keySet().toArray(new UUID[] {});
        }

        @Override
        protected boolean isActiveDataService(UUID serviceUUID) {
            return activeDataServices.containsKey(serviceUUID);
        }

        @Override
        protected boolean isUnderUtilizedDataService(ServiceScore score,
                ServiceScore[] scores)
        {
            return EmbeddedLoadBalancer.this.isUnderUtilizedDataService(score,
                    scores);
        }
    }

//BTM - BEGIN
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

            if(serviceUUID == null) return;

            serviceNameMap.put(serviceUUID, serviceName);

            if( activeHosts.putIfAbsent
                    (hostname, new HostScore(hostname)) == null)
            {
                if(logger.isDebugEnabled()) {
                    logger.debug("new host joined: "
                                 +"[hostname="+hostname+"]");
                }
            }

            // Only data/shard services are registered as [activeServices]
            // since load balancing decisions are made on only 
            // those service types.
            if( (ShardService.class).isAssignableFrom(serviceType) ||
                (  (IDataService.class).isAssignableFrom(serviceType) &&
                  !(IMetadataService.class).isAssignableFrom(serviceType) ) )
            {
                if( activeDataServices.putIfAbsent
                        (serviceUUID,
                         new ServiceScore
                             (hostname, serviceUUID, serviceName)) == null)
                {
                    if (logger.isDebugEnabled()) {
                        logger.debug("shard service join: "
                                     +"[hostname="+hostname+", "
                                     +"serviceUUID="+serviceUUID+"]");
                    }
                }
            }

            // Create a node for the discovered service's history in
            // this load balancer's counter set.
            getCounterSet().makePath
                ( AbstractFederation.getServiceCounterPathPrefix
                      (serviceUUID, serviceIface, hostname) );
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

            if(activeDataServices != null) {
                activeDataServices.remove(serviceUUID);
            }
            if(serviceNameMap != null) {
                serviceNameMap.remove(serviceUUID);
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

    private class CacheFilter implements ServiceItemFilter {
	public boolean check(ServiceItem item) {
            if(item == null) return false;
            Object service = item.service;
            if(service == null) return false;
            Class serviceType = service.getClass();
            if( (LoadBalancer.class).isAssignableFrom(serviceType) ) {
                return false;
            }
            return true;
	}
    }


//BTM - END
}
