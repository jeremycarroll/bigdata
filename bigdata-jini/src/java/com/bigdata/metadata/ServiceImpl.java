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

package com.bigdata.metadata;

import static com.bigdata.metadata.Constants.*;

import com.bigdata.attr.ServiceInfo;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.BigdataZooDefs;
import com.bigdata.jini.util.ConfigMath;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.IServiceShutdown.ShutdownType;
import com.bigdata.service.MetadataIndexCachePolicy;
import com.bigdata.util.BootStateUtil;
import com.bigdata.util.Util;
import com.bigdata.util.config.ConfigDeployUtil;
import com.bigdata.util.config.LogUtil;
import com.bigdata.util.config.NicUtil;
import com.bigdata.zookeeper.ZooKeeperAccessor;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import com.sun.jini.config.Config;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.thread.ReadyState;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationProvider;
import net.jini.config.ConfigurationException;
import net.jini.config.NoSuchEntryException;

import net.jini.core.entry.Entry;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lease.Lease;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;

import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.DiscoveryGroupManagement;
import net.jini.discovery.DiscoveryLocatorManagement;
import net.jini.discovery.LookupDiscoveryManager;

import net.jini.export.Exporter;
import net.jini.lookup.JoinManager;
import net.jini.lookup.ServiceDiscoveryManager;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

//BTM - PRE_FRED_3481
import com.bigdata.service.IDataServiceCallable;

/**
 * Backend implementation of the shard locator service.
 *
 * Note: this class is currently declared public rather than the preferred
 *       package protected scope. This is so that the JiniServicesHelper
 *       utility can instantiate this class in the tests that are currently
 *       implemented to interact directly with the service's backend;
 *       as opposed to starting the service with the ServiceStarter and
 *       then interacting with the service through the discovered service
 *       frontend.
 */
public
class ServiceImpl implements PrivateInterface {

    private static Logger logger = 
        LogUtil.getLog4jLogger(COMPONENT_NAME);
    private static String shutdownStr;
    private static String killStr;

    private static String zookeeperRoot = null;
    private static String zookeeperServers = null;
    private static int zookeeperSessionTimeout = 300000;
    private static List<ACL> zookeeperAcl = new ArrayList<ACL>();
    private static ZooKeeperAccessor zookeeperAccessor = null;

    private final LifeCycle lifeCycle;//for Jini ServiceStarter framework
    private final ReadyState readyState = new ReadyState();//access when ready

    private Configuration config;
    private UUID proxyId = null;
    private ServiceID serviceId = null;

    private Exporter serverExporter;
    private ServiceProxy outerProxy;// outer (smart) proxy to this server
    private PrivateInterface innerProxy;// stub or dynamic proxy to this server
    private AdminProxy adminProxy;

    private Set<Exporter> futureExporters = new HashSet<Exporter>();

    /* For this service's join state */
    private String[] groupsToJoin = DiscoveryGroupManagement.NO_GROUPS;
    private LookupLocator[] locatorsToJoin = new LookupLocator[0];
    private DiscoveryManagement ldm;
    private JoinManager joinMgr;
    private ServiceDiscoveryManager sdm;

    private EmbeddedShardLocator embeddedShardLocator;

//BTM    private Thread waitThread;

    /* Constructor used by Service Starter Framework to start this service */
    public ServiceImpl(String[] args, LifeCycle lifeCycle) throws Exception {
System.out.println("\nZZZZZ SHARD LOCATOR ServiceImpl: constructor");
if(args == null) {
    System.out.println("ZZZZZ SHARD LOCATOR ServiceImpl: args = NULL ****");
} else {
    System.out.println("ZZZZZ SHARD LOCATOR ServiceImpl: args.length = "+args.length);
    for(int i=0; i<args.length; i++) {
        System.out.println("ZZZZZ SHARD LOCATOR ServiceImpl: arg["+i+"] = "+args[i]);
    }
}
        this.lifeCycle = lifeCycle;
        try {
            init(args);
        } catch(Throwable e) {
            Util.cleanupOnExit
              (innerProxy, serverExporter, futureExporters, joinMgr, sdm, ldm);
            Util.handleInitThrowable(e, logger);
        }
    }

    // Remote method(s) required by PrivateInterface

    public int nextPartitionId(String name)
                   throws RemoteException, IOException,
                          InterruptedException, ExecutionException
    {
	readyState.check();
        return embeddedShardLocator.nextPartitionId(name);
    }

    public void splitIndexPartition(String             name,
                                    PartitionLocator   oldLocator,
                                    PartitionLocator[] newLocators)
                    throws RemoteException, IOException,
                           InterruptedException, ExecutionException
    {
	readyState.check();
        embeddedShardLocator.splitIndexPartition(name,oldLocator,newLocators);
    }

    public void joinIndexPartition(String             name,
                                   PartitionLocator[] oldLocators,
                                   PartitionLocator   newLocator)
                    throws RemoteException, IOException,
                           InterruptedException, ExecutionException
    {
	readyState.check();
        embeddedShardLocator.joinIndexPartition(name, oldLocators, newLocator);
    }

    public void moveIndexPartition(String           name,
                                   PartitionLocator oldLocator,
                                   PartitionLocator newLocator)
                    throws RemoteException, IOException,
                           InterruptedException, ExecutionException
    {
	readyState.check();
        embeddedShardLocator.moveIndexPartition(name, oldLocator, newLocator);
    }

    public UUID registerScaleOutIndex(IndexMetadata metadata,
                                      byte[][]      separatorKeys,
                                      UUID[]        dataServices)
                    throws RemoteException, IOException,
                           InterruptedException, ExecutionException
    {
	readyState.check();
        return embeddedShardLocator.registerScaleOutIndex
                                      (metadata, separatorKeys, dataServices);
    }

    public void dropScaleOutIndex(String name)
                    throws RemoteException, IOException,
                           InterruptedException, ExecutionException
    {
	readyState.check();
        embeddedShardLocator.dropScaleOutIndex(name);
    }

    public PartitionLocator get(String name, long timestamp, byte[] key)
                                throws RemoteException, IOException,
                                       InterruptedException, ExecutionException
    {
	readyState.check();
        return embeddedShardLocator.get(name, timestamp, key);
    }

    public PartitionLocator find(String name, long timestamp, byte[] key)
                                throws RemoteException, IOException,
                                       InterruptedException, ExecutionException
    {
	readyState.check();
        return embeddedShardLocator.find(name, timestamp, key);
    }

    public IndexMetadata getIndexMetadata(String name, long timestamp)
                             throws RemoteException, IOException,
                                    InterruptedException, ExecutionException
    {
        readyState.check();
        return embeddedShardLocator.getIndexMetadata(name, timestamp);
    }

    public ResultSet rangeIterator(long tx,
                                   String name,
                                   byte[] fromKey,
                                   byte[] toKey,
                                   int capacity,
                                   int flags,
                                   IFilterConstructor filter)
                         throws RemoteException, IOException,
                                InterruptedException, ExecutionException
    {
        readyState.check();
        return embeddedShardLocator.rangeIterator
                   (tx, name, fromKey, toKey, capacity, flags, filter);
    }

//BTM - PRE_FRED_3481    public Future<? extends Object> submit(Callable<? extends Object> proc)
    public <T> Future<T> submit(IDataServiceCallable<T> task)
                                        throws RemoteException
    {
        readyState.check();
        Exporter exporter = null;
        try {
            exporter = Util.getExporter(config,
                                        COMPONENT_NAME,
                                        "futureExporter",
                                        true,   //defaultEnableDgc
                                        false); //defaultKeepAlive
            synchronized(futureExporters) {
                if(exporter != null) futureExporters.add(exporter);
            }
        } catch(ConfigurationException e) {
            throw new RemoteException("while retrieving exporter for remote "
                                       +"future from service configuration",
                                       e);
        }
        return Util.wrapFuture( exporter, embeddedShardLocator.submit(task) );
    }

    public Future submit(long tx, String name, IIndexProcedure proc)
                      throws RemoteException
    {
        readyState.check();
        Exporter exporter = null;
        try {
            exporter = Util.getExporter(config,
                                        COMPONENT_NAME,
                                        "futureExporter",
                                        true,   //defaultEnableDgc
                                        false); //defaultKeepAlive
            synchronized(futureExporters) {
                if(exporter != null) futureExporters.add(exporter);
            }
        } catch(ConfigurationException e) {
            throw new RemoteException("while retrieving exporter for remote "
                                       +"future from service configuration",
                                       e);
        }
        return Util.wrapFuture( exporter, 
                                embeddedShardLocator.submit(tx, name, proc) );
    }

    public boolean purgeOldResources(long timeout, boolean truncateJournal)
                throws RemoteException, InterruptedException
    {
        readyState.check();
        return embeddedShardLocator.purgeOldResources(timeout,truncateJournal);
    }

    public void shutdown() throws RemoteException {
logger.warn("ZZZZZ SHARD LOCATOR ServiceImpl: SHUTDOWN CALLED");
	readyState.check();
	readyState.shutdown();
        shutdownDo(ShutdownType.GRACEFUL);
    }

    public void shutdownNow() throws RemoteException {
logger.warn("ZZZZZ SHARD LOCATOR ServiceImpl: SHUTDOWN_NOW CALLED");
	readyState.check();
	readyState.shutdown();
        shutdownDo(ShutdownType.IMMEDIATE);
    }

    public void kill(int status) throws RemoteException {
logger.warn("ZZZZZ SHARD LOCATOR ServiceImpl: KILL CALLED");
	readyState.check();
	readyState.shutdown();
        killDo(status);
    }

    // Required by Administrable

    public Object getAdmin() throws RemoteException {
	readyState.check();
        return adminProxy;
    }

    // Required by DestroyAdmin

    public void destroy() throws RemoteException {
logger.warn("ZZZZZ SHARD LOCATOR ServiceImpl: DESTROY CALLED");
	readyState.check();
	readyState.shutdown();
        shutdownDo(ShutdownType.DESTROY_STATE);
    }

    //Required by JoinAdmin

    public Entry[] getLookupAttributes() throws RemoteException {
	readyState.check();
	return joinMgr.getAttributes();
    }

    public void addLookupAttributes(Entry[] attrSets) throws RemoteException {
	readyState.check();
        joinMgr.addAttributes(attrSets, true);
    }

    public void modifyLookupAttributes(Entry[] attrSetTemplates,
				       Entry[] attrSets)
	throws RemoteException
    {
	readyState.check();
        joinMgr.modifyAttributes(attrSetTemplates, attrSets, true);
    }

    public String[] getLookupGroups() throws RemoteException {
	readyState.check();
	return groupsToJoin;
    }

    public void addLookupGroups(String[] groups) throws RemoteException {
	readyState.check();
        try {
            ((DiscoveryGroupManagement)ldm).addGroups(groups);
        } catch(IOException e) {
            throw new RuntimeException(e.toString());
        }
    }

    public void removeLookupGroups(String[] groups) throws RemoteException {
	readyState.check();
	((DiscoveryGroupManagement)ldm).removeGroups(groups);
    }

    public void setLookupGroups(String[] groups) throws RemoteException {
	readyState.check();
        try {
            ((DiscoveryGroupManagement)ldm).setGroups(groups);
        } catch(IOException e) {
            throw new RuntimeException(e.toString());
        }
    }

    public LookupLocator[] getLookupLocators() throws RemoteException {
	readyState.check();
	return locatorsToJoin;
    }

    public void addLookupLocators(LookupLocator[] locators)
	throws RemoteException
    {
	readyState.check();
	((DiscoveryLocatorManagement)ldm).addLocators(locators);
    }

    public void removeLookupLocators(LookupLocator[] locators)
	throws RemoteException
    {
	readyState.check();
	((DiscoveryLocatorManagement)ldm).removeLocators(locators);
    }

    public void setLookupLocators(LookupLocator[] locators)
	throws RemoteException
    {
	readyState.check();
	((DiscoveryLocatorManagement)ldm).setLocators(locators);
    }

    //Initialize the service from the config
    private void init(String[] args) throws Exception {
        config = ConfigurationProvider.getInstance
                                       ( args,
                                         (this.getClass()).getClassLoader() );
        if(smsProxyId == null) {//service assigns & persists its own proxy id
            BootStateUtil bootStateUtil = 
                new BootStateUtil
                        (config, COMPONENT_NAME, this.getClass(), logger);
            proxyId   = bootStateUtil.getProxyId();
            serviceId = bootStateUtil.getServiceId();
            logger.debug("smsProxyId = null - service generated & persisted "
                         +"(or retreieved) its own proxy id ["+proxyId+"]");

            setZookeeperConfigInfo(config);
            zookeeperAccessor = 
                    new ZooKeeperAccessor
                            (zookeeperServers, zookeeperSessionTimeout);
        } else {//ServicesConfiguration pre-generated the proxy id
            proxyId = smsProxyId;
            serviceId = com.bigdata.jini.util.JiniUtil.uuid2ServiceID(proxyId);
            logger.debug("smsProxyId != null - service retrieved proxy id "
                         +"from supplied configuration ["+proxyId+"]");
        }

        //Service export and proxy creation
        serverExporter = Util.getExporter(config,
                                          COMPONENT_NAME,
                                          "serverExporter",
                                          false, //enableDgc
                                          true); //keepAlive
        innerProxy = (PrivateInterface)serverExporter.export(this);
        String hostname =
            NicUtil.getIpAddress
                ("default.nic",
                 ConfigDeployUtil.getString("node.serviceNetwork"), false);
        outerProxy = ServiceProxy.createProxy
                         (innerProxy, proxyId, hostname);
        adminProxy = AdminProxy.createProxy(innerProxy, proxyId);

        //Setup lookup discovery
        this.ldm = Util.getDiscoveryManager(config, COMPONENT_NAME);
        groupsToJoin   = ((DiscoveryGroupManagement)ldm).getGroups();
        locatorsToJoin = ((DiscoveryLocatorManagement)ldm).getLocators();

        //For the service attribute(s)
        ArrayList<Entry> serviceAttrsList = new ArrayList<Entry>();

        ServiceInfo serviceInfo = 
            ConfigDeployUtil.initServiceInfo(proxyId, SERVICE_NAME);

        serviceAttrsList.add(serviceInfo);

        //array of attributes for JoinManager
        Entry[] serviceAttrs = serviceAttrsList.toArray
                                       (new Entry[serviceAttrsList.size()]);

        //properties object for the EmbeddedShardLocator
        String persistDir = (String)config.getEntry(COMPONENT_NAME,
                                             "persistenceDirectory",
                                             String.class);
        String dataDir = 
            ConfigMath.getAbsolutePath(new File(persistDir,"data"));
        Properties props = new Properties();
        props.setProperty(EmbeddedShardLocator.Options.DATA_DIR, dataDir);
        int threadPoolSize = Config.getIntEntry(config,
                                                COMPONENT_NAME,
                                                "threadPoolSize",
                                                DEFAULT_THREAD_POOL_SIZE, 
                                                LOWER_BOUND_THREAD_POOL_SIZE,
                                                UPPER_BOUND_THREAD_POOL_SIZE);
        int indexCacheSize = 
                Config.getIntEntry(config,
                                   COMPONENT_NAME,
                                   "indexCacheSize",
                                   DEFAULT_INDEX_CACHE_SIZE, 
                                   LOWER_BOUND_INDEX_CACHE_SIZE,
                                   UPPER_BOUND_INDEX_CACHE_SIZE);
        long indexCacheTimeout = 
               Config.getLongEntry(config,
                                   COMPONENT_NAME,
                                   "indexCacheTimeout",
                                   DEFAULT_INDEX_CACHE_TIMEOUT, 
                                   LOWER_BOUND_INDEX_CACHE_TIMEOUT,
                                   UPPER_BOUND_INDEX_CACHE_TIMEOUT);

        MetadataIndexCachePolicy metadataIndexCachePolicy =
            (MetadataIndexCachePolicy)config.getEntry
                                      (COMPONENT_NAME,
                                       "metadataIndexCachePolicy",
                                       MetadataIndexCachePolicy.class,
                                       MetadataIndexCachePolicy.CacheAll);
        if(metadataIndexCachePolicy == null) {
            throw new ConfigurationException("null metadataIndexCachePolicy");
        }

        int resourceLocatorCacheSize = 
                Config.getIntEntry(config,
                                   COMPONENT_NAME,
                                   "resourceLocatorCacheSize",
                                   DEFAULT_RESOURCE_LOCATOR_CACHE_SIZE, 
                                   LOWER_BOUND_RESOURCE_LOCATOR_CACHE_SIZE,
                                   UPPER_BOUND_RESOURCE_LOCATOR_CACHE_SIZE);

        long resourceLocatorCacheTimeout = 
               Config.getLongEntry(config,
                                   COMPONENT_NAME,
                                   "resourceLocatorCacheTimeout",
                                   DEFAULT_RESOURCE_LOCATOR_CACHE_TIMEOUT, 
                                   LOWER_BOUND_RESOURCE_LOCATOR_CACHE_TIMEOUT,
                                   UPPER_BOUND_RESOURCE_LOCATOR_CACHE_TIMEOUT);

        int defaultRangeQueryCapacity = 
                Config.getIntEntry(config,
                                   COMPONENT_NAME,
                                   "defaultRangeQueryCapacity",
                                   DEFAULT_RESOURCE_LOCATOR_CACHE_SIZE, 
                                   LOWER_BOUND_RESOURCE_LOCATOR_CACHE_SIZE,
                                   UPPER_BOUND_RESOURCE_LOCATOR_CACHE_SIZE);
        boolean batchApiOnly =
            (Boolean)Config.getNonNullEntry(config,
                                            COMPONENT_NAME,
                                            "batchApiOnly",
                                            Boolean.class,
                                            DEFAULT_BATCH_API_ONLY);
        long taskTimeout = Config.getLongEntry(config,
                                               COMPONENT_NAME,
                                               "taskTimeout",
                                               DEFAULT_TASK_TIMEOUT, 
                                               LOWER_BOUND_TASK_TIMEOUT,
                                               UPPER_BOUND_TASK_TIMEOUT);
        int maxParallelTasksPerRequest = 
                Config.getIntEntry
                           (config,
                            COMPONENT_NAME,
                            "maxParallelTasksPerRequest",
                            DEFAULT_MAX_PARALLEL_TASKS_PER_REQUEST, 
                            LOWER_BOUND_MAX_PARALLEL_TASKS_PER_REQUEST,
                            UPPER_BOUND_MAX_PARALLEL_TASKS_PER_REQUEST);

        int maxStaleLocatorRetries = 
                Config.getIntEntry
                           (config,
                            COMPONENT_NAME,
                            "maxStaleLocatorRetries",
                            DEFAULT_MAX_STALE_LOCATOR_RETRIES, 
                            LOWER_BOUND_MAX_STALE_LOCATOR_RETRIES,
                            UPPER_BOUND_MAX_STALE_LOCATOR_RETRIES);

        boolean collectQueueStatistics =
            (Boolean)Config.getNonNullEntry(config,
                                            COMPONENT_NAME,
                                            "collectQueueStatistics",
                                            Boolean.class,
                                            DEFAULT_COLLECT_QUEUE_STATISTICS);
        boolean collectPlatformStatistics =
            (Boolean)Config.getNonNullEntry(config,
                                            COMPONENT_NAME,
                                            "collectPlatformStatistics",
                                            Boolean.class,
                                            Boolean.FALSE);

        this.sdm = new ServiceDiscoveryManager(ldm, null, config);
        embeddedShardLocator = 
            new EmbeddedShardLocator
                    (proxyId, hostname,
                     sdm, //for discovering txn, lbs, shard services
                     null,//embedded txn service   - for embedded fed testing
                     null,//embedded lbs service   - for embedded fed testing
                     null,//embedded data services - for embedded fed testing
                     zookeeperAccessor,
                     zookeeperAcl,
                     zookeeperRoot,
                     threadPoolSize,
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
                     collectQueueStatistics,
                     collectPlatformStatistics,
                     props);

        //advertise this service
        joinMgr = new JoinManager(outerProxy, serviceAttrs, serviceId, ldm,
                                  null, config);

        String srvcIdStr = (ServiceImpl.this.serviceId).toString();
        String startStr = SERVICE_NAME+" started: [id="+srvcIdStr+"]";
        shutdownStr = SERVICE_NAME+" shutdown: [id="+srvcIdStr+"]";
        killStr = SERVICE_NAME+" killed: [id="+srvcIdStr+"]";

        logger.log(Level.INFO, startStr);
        logger.log(Level.INFO,
                   "groups="
                   +Util.writeGroupArrayToString(groupsToJoin)
                   +", locators="
                   +Util.writeArrayElementsToString(locatorsToJoin));

//BTM        waitThread = new Util.WaitOnInterruptThread(logger);
//BTM        waitThread.start();

        readyState.ready();//ready to accept calls from clients
    }

    private void shutdownDo(ShutdownType type) {
        (new ShutdownThread(type)).start();
    }

    /**
     * Used to shutdown the service asynchronously.
     */
    private class ShutdownThread extends Thread {

        private long EXECUTOR_TERMINATION_TIMEOUT = 1L*60L*1000L;
        private ShutdownType type;

        public ShutdownThread(ShutdownType type) {
            super("shard locator shutdown thread");
            setDaemon(false);
            this.type = type;
        }

        public void run() {

            switch (type) {
                case GRACEFUL:
                    embeddedShardLocator.shutdown();
                case IMMEDIATE:
                    embeddedShardLocator.shutdownNow();
                case DESTROY_STATE:
                    embeddedShardLocator.destroy();
                    break;
                default:
                    logger.warn("unexpected shutdown type ["+type+"]");
            }

            //Before terminating the discovery manager, retrieve the
            // current groups and locs; which may have been administratively
            // changed between the time this service was started and now.
            // This information will be logged on exit below.
            String[] groups = ((DiscoveryGroupManagement)ldm).getGroups();
            LookupLocator[] locs = 
                          ((DiscoveryLocatorManagement)ldm).getLocators();

            if( Util.unexportRemoteObject(serverExporter) ) {
                innerProxy = null;
                serverExporter = null;
            }

            Set<Exporter> removeSet = new HashSet<Exporter>();
            synchronized(futureExporters) {
                for(Exporter exporter : futureExporters) {
                    if( Util.unexportRemoteObject(exporter) ) {
                        exporter = null;
                        removeSet.add(exporter);
                    }
                }
                futureExporters.removeAll(removeSet);
            }

//BTM            waitThread.interrupt();
//BTM            try {
//BTM                waitThread.join();
//BTM            } catch (InterruptedException e) {/*exiting, so swallow*/}

            Util.cleanupOnExit
              (innerProxy, serverExporter, futureExporters, joinMgr, sdm, ldm);

            // Tell the ServiceStarter framework it's ok to release for gc
            if(lifeCycle != null)  {
                logger.log(Level.DEBUG,"Unregistering the service from "
                           +"the ServiceStarter framework ...");
                lifeCycle.unregister(ServiceImpl.this);
            }
            logger.log(Level.INFO, shutdownStr+" [groups="
                       +Util.writeGroupArrayToString(groupsToJoin)
                       +", locators="
                       +Util.writeArrayElementsToString(locatorsToJoin)+"]");
        }
    }

    private void killDo(int status) {
        String[] groups = ((DiscoveryGroupManagement)ldm).getGroups();
        LookupLocator[] locs = ((DiscoveryLocatorManagement)ldm).getLocators();
        logger.log(Level.INFO, killStr+" [groups="
                   +Util.writeGroupArrayToString(groupsToJoin)
                   +", locators="
                   +Util.writeArrayElementsToString(locatorsToJoin)+"]");

        System.exit(status);
    }

    /**
     * This main() method is provided because it may be desirable (for
     * testing or other reasons) to be able to start this service using
     * a command line that is either manually entered in a command window
     * or supplied to the java ProcessBuilder class for execution.
     * <p>
     * The mechanism that currently employs the ProcessBuilder class to
     * execute a dynamically generated command line will be referred to
     * as the 'ServiceConfiguration mechanism', which involves the use of
     * the ServiceConfiguration class hierarchy; that is, 
     * <p>
     * <ul>
     *   <li> <service-name>Configuration
     *   <li> BigdataServiceConfiguration
     *   <li> JiniServiceConfiguration
     *   <li> ManagedServiceConfiguration
     *   <li> JavaServiceConfiguration
     *   <li> ServiceConfiguration
     * </ul>
     * </p>
     * The ServicesConfiguration mechanism may involve the use of the
     * ServicesManagerService directly to execute this service, or it may
     * involve the use of the junit framework to start this service. In
     * either case, a command line is constructed from information that is 
     * specified at each of the various ServiceConfiguration levels, and
     * is ultimately executed in a ProcessBuilder instance (in the
     * ProcessHelper class).
     * <p>
     * In order for this method to know whether or not the 
     * ServiceConfiguration mechanism is being used to start the service,
     * this method must be told that the ServiceConfiguration mechanism is
     * being used. This is done by setting the system property named
     * <code>usingServiceConfiguration</code> to any non-null value.
     * <p>
     * When the ServiceConfiguration mechanism is <i>not</i> used to start
     * this service, this method assumes that the element at index 0
     * of the args array references the path to the jini configuration
     * file that will be input by this method to this service's constructor.
     * On the other hand, when the ServiceConfiguration mechanism <i>is</i>
     * used to start this service, the service's configuration is handled
     * differently, as described below.
     * <p>   
     * When using the ServiceConfiguration mechanism, in addition to
     * generating a command line to start the service, although an initial,
     * pre-constructed jini configuration file is supplied (to the
     * ServicesManagerService or the test framework, for example), a
     * second jini configuration file is generated <i>on the fly</i> as
     * well. When generating that new configuration file, a subset of the
     * components and entries specified in the initial jini configuration
     * are retrieved and placed in the new configuration being generated.
     * It is that second, newly-generated configuration file that is input
     * to this method through the args elements at index 0. It is important
     * to note that the generated configuration file is also stored in
     * zookeeper for retrieval when the ServicesManagerService restarts 
     * this service.
     * <p>
     * When the ServiceConfiguration mechanism is used to invoke this method,
     * this method makes a number of assumptions. One assumption is that
     * there is a component with name equal to the fully qualified name
     * of this class. Another assumption is that an entry named 'args' 
     * is contained in that component. The 'args' entry is assumed to be
     * a <code>String</code> array in which one of the elments is specified
     * to be a system property named 'config' whose value is equal to the
     * path and filename of yet a third jini configuration file; that is,
     * something of the form, "-Dconfig=<path-to-another-jini-config>".
     * It is this third jini configuration file that the service will
     * ultimately use to initialize itself when the ServiceConfiguration
     * mechanism is being used to start the service. In that case then,
     * this method will retrieve the path to the third jini configuration
     * file from the configuration file supplied to this method in the args
     * array at index 0, and then replace the element at index 0 with that
     * path; so that when the service is instantiated (using this class'
     * constructor), that third configuration file is made available to
     * the service instance.
     * <p>
     * A final assumption that this method makes when the ServiceConfiguration 
     * mechanism is used to start the service is that the generated
     * configuration file supplied to this method at args[0] includes a
     * section with component name, "com.bigdata.service.jini.JiniClient";
     * which contains among its configuration entries, an array whose elements
     * are each instances of <code>net.jini.core.entry.Entry</code>, where
     * those elements are specified in the following order:
     * <p>
     * <ul>
     *   <li> net.jini.lookup.entry.Name
     *   <li> com.bigdata.jini.lookup.entry.Hostname
     *   <li> com.bigdata.jini.lookup.entry.ServiceDir
     *   <li> com.bigdata.jini.lookup.entry.ServiceUUID
     *   <li> net.jini.lookup.entry.Comment
     * </ul>
     * </p>
     * Note that the item at index 3 (<code>ServiceUUID</code>) means
     * that a service id is generated for the service being started; as 
     * opposed to the service generating its own service id. Thus, if
     * the ServiceConfiguration mechanism is being used, then this
     * method retrieves the pre-generated service id from the entries
     * array described above, stores that value in the <code>smsProxyId</code>
     * field so that this service's <code>init</code> method can take
     * the appropriate action with respect to the service id. That is,
     * during initialization, the service will examine the value of
     * <code>smsProxyId</code> and if it has not been set (indicating 
     * that the ServiceConfiguration mechanism is not being used),
     * will generate and persist (or retrieve from persistent storage)
     * its own service id; otherwise, the service will use the service
     * id passed in through the generated configuration and retrieved
     * by this method.
     * <p>
     * Finally, once an instance of this service implementation class
     * has been created, that instance is stored in the <code>thisImpl</code>
     * field to prevent the instance from being garbage collected until
     * the service is actually shutdown.
     */

    private static UUID smsProxyId = null;
    private static ServiceImpl thisImpl;

    public static void main(String[] args) {
        logger.debug("[main]: appHome="+System.getProperty("appHome"));
        try {
            // If the system property with name "config" is set, then
            // use the value of that property to override the value
            // input in the first element of the args array
            ArrayList<String> argsList = new ArrayList<String>();
            int begIndx = 0;
            String configFile = System.getProperty("config");
            if(configFile != null) {
                // Replace args[1] with config file location
                argsList.add(configFile);
                begIndx = 1;
            }
            for(int i=begIndx; i<args.length; i++) {
                argsList.add(args[i]);
            }

            // The ServiceConfiguration mechanism waits on the discovery
            // of a service of this type having the same service id as
            // that placed in the generated config file that was passed
            // to this method in args[0].
            if(System.getProperty("usingServiceConfiguration") != null) {
                Configuration smsConfig = 
                    ConfigurationProvider.getInstance
                        ( args, (ServiceImpl.class).getClassLoader() );

                Entry[] smsEntries = 
                    (Entry[])smsConfig.getEntry
                        ("com.bigdata.service.jini.JiniClient",
                         "entries",
                         net.jini.core.entry.Entry[].class,
                         null);
                if(smsEntries != null) {
                    // See JiniServiceConfiguration.getEntries to see why
                    // the element at index 3 is retrieved.
                    smsProxyId = 
                        ((com.bigdata.jini.lookup.entry.ServiceUUID)
                             smsEntries[3]).serviceUUID;
                    logger.debug("[main]: smsProxyId="+smsProxyId);
                }

                setZookeeperConfigInfo(smsConfig);
                zookeeperAccessor = 
                    new ZooKeeperAccessor
                            (zookeeperServers, zookeeperSessionTimeout);

                String logicalServiceZPath = 
                    (String)smsConfig.getEntry((ServiceImpl.class).getName(),
                                               "logicalServiceZPath",
                                               String.class,
                                               null);
                String physicalServiceZPath = null;
                if(logicalServiceZPath != null) {
                    physicalServiceZPath = 
                        logicalServiceZPath
                        +BigdataZooDefs.ZSLASH 
                        +BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER
                        +BigdataZooDefs.ZSLASH 
                        +smsProxyId;
                }
                logger.debug
                    ("[main]: logicalServiceZPath="+logicalServiceZPath);
                if(physicalServiceZPath != null) {
                    byte[] data = SerializerUtil.serialize(smsEntries);
                    ZooKeeper zookeeperClient = zookeeperAccessor.getZookeeper();
                    logger.debug("[main]: zookeeper client created");
                    try {
                        zookeeperClient.create
                                  (physicalServiceZPath, data, zookeeperAcl,
                                   org.apache.zookeeper.CreateMode.PERSISTENT);
                        logger.debug("[main]: zookeeper znode created "
                                     +"[physicalServiceZPath="
                                     +physicalServiceZPath+"]");
                    } catch(NodeExistsException e) {
                        zookeeperClient.setData(physicalServiceZPath, data, -1);
                        logger.debug("[main]: zookeeper znode updated "
                                     +"[physicalServiceZPath="
                                     +physicalServiceZPath+"]");
                    } catch(Throwable z) {
                        logger.error("[main]: problem creaating/updating "
                                     +"zookeeper znode", z);
                        throw z;
                    }
                }
            }
            logger.debug("[main]: instantiating service [new ServiceImpl]");
            thisImpl = new ServiceImpl
                ( argsList.toArray(new String[argsList.size()]),
                  new com.bigdata.service.jini.FakeLifeCycle() );
        } catch(Throwable t) {
            logger.log(Level.WARN, "failed to start shard locator service", t);
        }
    }

    private static void setZookeeperConfigInfo(Configuration zkConfig)
                            throws ConfigurationException
    {
        String zkComponent = "org.apache.zookeeper.ZooKeeper";

        zookeeperRoot = (String)zkConfig.getEntry
                               (zkComponent, "zroot", String.class, null);
        if(zookeeperRoot == null) {
            throw new ConfigurationException
                          ("zookeeper zroot path not specified");
        }
        logger.debug("zookeepeRoot="+zookeeperRoot);

        zookeeperServers = 
            (String)zkConfig.getEntry
                                 (zkComponent, "servers", String.class, null);
        if(zookeeperServers == null) {
            throw new ConfigurationException
                          ("zookeeper servers not specified");
        }
        logger.debug("zookeeperServers="+zookeeperServers);

        zookeeperSessionTimeout = 
            (Integer)zkConfig.getEntry
                         (zkComponent, "sessionTimeout", int.class, 300000);
        logger.debug("zookeeperSessionTimeout="+zookeeperSessionTimeout);

        ACL[] acl = (ACL[])zkConfig.getEntry
                               (zkComponent, "acl", ACL[].class, null);
        if(acl == null) {
            throw new ConfigurationException("zookeeper acl not specified");
        }
        zookeeperAcl = Arrays.asList(acl);
        logger.debug("zookeeperAcl="+zookeeperAcl);
    }
}
