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

package com.bigdata.quorum;

import static com.bigdata.quorum.Constants.*;

import com.bigdata.attr.QuorumPeerAttr;
import com.bigdata.attr.ServiceInfo;
import com.bigdata.service.QuorumPeerService;
import com.bigdata.service.QuorumPeerService.QuorumPeerData;
import com.bigdata.util.EntryUtil;
import com.bigdata.util.Util;
import com.bigdata.util.config.ConfigDeployUtil;
import com.bigdata.util.config.LogUtil;
import com.bigdata.util.config.NicUtil;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sun.jini.config.Config;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.thread.InterruptedStatusThread;
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
import net.jini.jeri.BasicILFactory;
import net.jini.jeri.BasicJeriExporter;
import net.jini.jeri.InvocationLayerFactory;
import net.jini.jeri.ServerEndpoint;
import net.jini.jeri.tcp.TcpServerEndpoint;
import net.jini.lookup.JoinManager;
import net.jini.lookup.ServiceDiscoveryManager;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.server.ExportException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Backend (admin) zookeeper based implementation of the quorum peer service.
 */
class ServiceImpl implements PrivateInterface {

    private static Logger logger = LogUtil.getLog4jLogger
                                          ( (ServiceImpl.class).getName() ) ;
    private static String shutdownStr;
    private static String killStr;

    private final LifeCycle lifeCycle;//for Jini ServiceStarter framework
    private final ReadyState readyState = new ReadyState();//access when ready

    private Configuration config;
    private UUID proxyId = null;
    private ServiceID serviceId = null;

    private Exporter serverExporter;
    private ServiceProxy outerProxy;// outer (smart) proxy to this server
    private PrivateInterface innerProxy;// stub or dynamic proxy to this server
    private AdminProxy adminProxy;

    /* For this service's join state */
    private String[] groupsToJoin = DiscoveryGroupManagement.NO_GROUPS;
    private LookupLocator[] locatorsToJoin = new LookupLocator[0];
    private DiscoveryManagement ldm;
    private JoinManager joinMgr;

    private ConfigStateInfo configStateInfo;

    private QuorumPeerMainTask quorumPeerMainTask;
    private ExecutorService quorumPeerMainTaskExecutor;

    /* Constructor used by Service Starter Framework to start this service */
    public ServiceImpl(String[] args, LifeCycle lifeCycle) throws Exception {
        this.lifeCycle = lifeCycle;
        try {
            init(args);
        } catch(Throwable e) {
            Util.cleanupOnExit(innerProxy, serverExporter, joinMgr, ldm);
            Util.handleInitThrowable(e, logger);
        }
    }

    // Required by PrivateInterface

    public long getPeerId() throws RemoteException {
	readyState.check();
        return (configStateInfo.getQuorumPeerState()).getPeerId();
    }

    public Map<Long, QuorumPeerData> getPeerDataMap() throws RemoteException {
	readyState.check();
        return (configStateInfo.getQuorumPeerState()).getPeerDataMap();
    }

    public void kill(int status) throws RemoteException {
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
	readyState.check();
	readyState.shutdown();
        shutdownDo();
    }

    // Required by JoinAdmin

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
        configStateInfo = 
           new ConfigStateInfo(config, COMPONENT_NAME, logger);
        proxyId   = configStateInfo.getProxyId();
        serviceId = configStateInfo.getServiceId();

        //Service export and proxy creation
        ServerEndpoint endpoint = TcpServerEndpoint.getInstance(0);
        InvocationLayerFactory ilFactory = new BasicILFactory();
        Exporter defaultExporter = new BasicJeriExporter(endpoint,
                                                         ilFactory,
                                                         false,
                                                         true);
        //Get the exporter that will be used to export this service
        serverExporter = (Exporter)config.getEntry(COMPONENT_NAME,
                                                   "serverExporter",
                                                   Exporter.class,
                                                   defaultExporter);
        if(serverExporter == null) {
            throw new ConfigurationException("null serverExporter");
        }

        innerProxy = (PrivateInterface)serverExporter.export(this);
        QuorumPeerState peerState = configStateInfo.getQuorumPeerState();

        outerProxy = ServiceProxy.createProxy
                                (innerProxy, proxyId, peerState);
        adminProxy = AdminProxy.createProxy(innerProxy, proxyId);

        //Setup lookup discovery
        this.ldm = Util.getDiscoveryManager(config, COMPONENT_NAME);
        groupsToJoin   = ((DiscoveryGroupManagement)ldm).getGroups();
        locatorsToJoin = ((DiscoveryLocatorManagement)ldm).getLocators();

        //For the service attribute(s)
        ArrayList<Entry> serviceAttrsList = new ArrayList<Entry>();

        //ServiceInfo attribute
        ServiceInfo serviceInfo = 
            ConfigDeployUtil.initServiceInfo(proxyId, SERVICE_NAME);

        serviceAttrsList.add(serviceInfo);

        //QuorumPeerAttr

        QuorumPeerAttr peerAttr = new QuorumPeerAttr();
        peerAttr.source = proxyId;
        peerAttr.address = peerState.getPeerAddress();
        peerAttr.peerPort = peerState.getPeerPort();
        peerAttr.electionPort = peerState.getElectionPort();
        peerAttr.clientPort = peerState.getClientPort();
        peerAttr.nQuorumPeers = peerState.getNQuorumPeers();
        peerAttr.peerId = peerState.getPeerId();

        serviceAttrsList.add(peerAttr);

        //array of attributes for JoinManager
        Entry[] serviceAttrs = serviceAttrsList.toArray
                                       (new Entry[serviceAttrsList.size()]);

        //advertise this service
        joinMgr = new JoinManager(outerProxy, serviceAttrs, serviceId, ldm,
                                  null, config);

        // If not standalone and if this is the very first time this
        // service instance has started (that is, it has not previously
        // persisted any state related to the other peers in the 
        // ensemble), then attempt to discover all other peers in the
        // ensemble to determine peer ids and server info so the entire
        // quorum peer ensemble can be initialized & persisted.
        if((peerState.getNQuorumPeers() > 1L)&&(peerState.getPeerId() == 0L)) {
            long waitPeriod =
                    Config.getLongEntry(config,
                                        COMPONENT_NAME,
                                        "peerDiscoveryPeriod",
                                        DEFAULT_PEER_DISCOVERY_PERIOD, 
                                        LOWER_BOUND_PEER_DISCOVERY_PERIOD, 
                                        UPPER_BOUND_PEER_DISCOVERY_PERIOD);

            initQuorumPeerData(peerState, configStateInfo, waitPeriod);

            //update the peerId of the QuorumPeerAttr
            QuorumPeerAttr tmplVal = new QuorumPeerAttr();
            QuorumPeerAttr changeVal = new QuorumPeerAttr();
            tmplVal.source = peerAttr.source;
            changeVal.peerId  = peerState.getPeerId();
            Entry[] attrTmpl = new Entry[] { tmplVal };
            Entry[] changeAttr = new Entry[] { changeVal };
            joinMgr.modifyAttributes(attrTmpl, changeAttr);
        }

        PrintWriter myidOut = 
            new PrintWriter( new File(peerState.getDataDir(), "myid"));
        myidOut.println(peerState.getPeerId());
        myidOut.flush();
        myidOut.close();

        // zookeeperJmxLog4j item is not part of the persisted state
        Boolean defaultJmxLog4j =
                   ConfigDeployUtil.getBoolean("federation.zookeeperJmxLog4j");
        boolean zookeeperJmxLog4j = (Boolean)config.getEntry
                                                        (COMPONENT_NAME,
                                                         "zookeeperJmxLog4j",
                                                         Boolean.class,
                                                         defaultJmxLog4j);
        if (!zookeeperJmxLog4j) {
            System.setProperty("zookeeper.jmx.log4j.disable", "true");
        }

        this.quorumPeerMainTaskExecutor = Executors.newFixedThreadPool(1);
        this.quorumPeerMainTask = new QuorumPeerMainTask(configStateInfo);
        this.quorumPeerMainTaskExecutor.execute(this.quorumPeerMainTask);

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

        readyState.ready();//ready to accept calls from clients
    }

    private boolean initQuorumPeerData(QuorumPeerState peerState,
                                       ConfigStateInfo configStateInfo,
                                       long peerDiscoveryPeriod) 
        throws IOException, ConfigurationException
    {
        int nPeersInEnsemble = peerState.getNQuorumPeers();
        if(nPeersInEnsemble <= 1) return false;

        ServiceDiscoveryManager sdm =
            new ServiceDiscoveryManager(ldm, null, config);

        ServiceID peerTmplId    = null;
        Class[]   peerTmplTypes = new Class[] { QuorumPeerService.class };

        QuorumPeerAttr peerAttr = new QuorumPeerAttr();

//BTM  Ports can all be different (which helps when running multiple
//BTM  servers on the same node), so no need to do exact matching
//BTM  on the client, peer, and election ports. Thus, discover all
//BTM  servers in the ensemble by federation group and service type,
//BTM  and wildcard the port values.

//BTM        peerAttr.peerPort = peerState.getPeerPort();
//BTM        peerAttr.electionPort = peerState.getElectionPort();
//BTM        peerAttr.clientPort = peerState.getClientPort();

        Entry[] peerTmplAttrs = new Entry[] { peerAttr };
        ServiceTemplate peerTmpl = new ServiceTemplate(peerTmplId,
                                                       peerTmplTypes,
                                                       peerTmplAttrs);
        ServiceItem[] peerItems = null;
        try {
            peerItems = sdm.lookup(peerTmpl, nPeersInEnsemble,
                                   nPeersInEnsemble, null,
                                   peerDiscoveryPeriod);
            if((peerItems == null) || (peerItems.length < nPeersInEnsemble)) {
                return false;
            }
        } catch(InterruptedException e) {
            return false;
        }

        // Found all peers, including self. Set peerId based on serviceId:
        // "smallest" serviceId is set to 1, next "smallest" set to 2, etc.
        // unless a discovered service already has a non-zero peerId field
        // in its QuroumPeerAttr attribute.
        //
        // Use TreeMap to order the proxyId's from lowest to highest
        // (the UUID elements provide a compareTo method for consistent
        // ordering).
        //
        // Also, while populating the ordered map, determine this service's
        // own proxyId and peerId so they can be used later when constructing
        // and persisting the map of QuorumPeerData.

        UUID thisProxyId = peerState.getProxyId();
        if(thisProxyId == null) {
            throw new NullPointerException("initQuorumPeerData: "
                                           +"null proxyId from peerState");
        }
        Long thisPeerId = 0L;//will replace this with non-zero below

        // Populate the map with the ordered proxy id keys and either
        // a non-zero peer id (indicating the discovered service had a
        // previously initialized - and persisted - peerId) or 0 to
        // indicate that the discovered service has been started for
        // the very first time

        Map<UUID, Long> orderedPeerIdMap = new TreeMap<UUID, Long>();
        for(int i=0; i<peerItems.length; i++) {
            UUID proxyId =
                 ((QuorumPeerService)(peerItems[i].service)).getServiceUUID();
            Entry[] attrs = peerItems[i].attributeSets;
            QuorumPeerAttr quorumPeerAttr =
                EntryUtil.getEntryByType(attrs, QuorumPeerAttr.class);
            Long peerId = (quorumPeerAttr == null ? 0L:quorumPeerAttr.peerId);
            orderedPeerIdMap.put(proxyId, peerId);
            logger.debug("PUT peerId >>> ["+proxyId+", "+peerId+"]");
        }

        // Replace any 0-valued peer ids from above with a non-zero value
        // in the correct order.

        Set<Map.Entry<UUID, Long>> orderedSet = orderedPeerIdMap.entrySet();
        Iterator<Map.Entry<UUID, Long>> itr = orderedSet.iterator();
        for(Long peerIdCntr=1L; itr.hasNext(); peerIdCntr++ ) {
            Map.Entry<UUID, Long> pair = itr.next();
            UUID curProxyId = pair.getKey();
            Long curPeerId  = pair.getValue();
            if (curPeerId == 0L) {
                curPeerId = peerIdCntr;
                orderedPeerIdMap.put(curProxyId, curPeerId);//replace
                logger.debug("REPLACE peerId >>> "
                             +"["+curProxyId+", "+curPeerId+"]");
            }
            if( thisProxyId.equals(curProxyId) ) {//determine own peerId
                thisPeerId = curPeerId;
            }
        }

        //verify this service's peerId was indeed determined above
        if(thisPeerId == 0) return false;

        peerState.setPeerId(thisPeerId);

        Map<Long, QuorumPeerData> peerDataMap = 
            new TreeMap<Long, QuorumPeerData>();
        for(int i=0; i<peerItems.length; i++) {
            QuorumPeerService peerService = 
                (QuorumPeerService)(peerItems[i].service);

            UUID proxyId = peerService.getServiceUUID();
            InetAddress peerAddress = peerService.getPeerAddress();
            int peerPort = peerService.getPeerPort();
            int electionPort = peerService.getElectionPort();

            long peerId = orderedPeerIdMap.get(proxyId);

            QuorumPeerData peerData = new QuorumPeerDataV0();
            peerData.setPeerId(peerId);
            peerData.setPeerAddress
                ( new InetSocketAddress(peerAddress, peerPort) );
            peerData.setElectionAddress
                ( new InetSocketAddress(peerAddress, electionPort) );

            peerDataMap.put(peerId, peerData);
        }
        peerState.setPeerDataMap(peerDataMap);

        // Update the service's persisted configuration state
        configStateInfo.persistQuorumPeerState(peerState);

        if(sdm != null) sdm.terminate();

        return true;
    }

    private void shutdownDo() {
        (new ShutdownThread()).start();
    }

    /**
     * Used to shutdown the service asynchronously.
     */
    private class ShutdownThread extends Thread {

        private long EXECUTOR_TERMINATION_TIMEOUT = 1L*60L*1000L;

        public ShutdownThread() {
            super("ZooKeeper Quorum Peer Shutdown thread");
            setDaemon(false);
        }

        public void run() {

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

            Util.cleanupOnExit(innerProxy, serverExporter, joinMgr, ldm);

            //TERMINATE quorumPeerMainTask and its executor
            logger.log(Level.DEBUG, "shutdown "
                       +"quourmPeerMainTaskExecutor [BEGIN]");

            //terminate the zookeeper server process running in the thread
            if(quorumPeerMainTask != null) {
                quorumPeerMainTask.terminate(1000L, TimeUnit.MILLISECONDS);
                logger.log(Level.DEBUG, "terminated quorumPeerMainTask");
            }

            quorumPeerMainTaskExecutor.shutdown();//no new tasks
            try {
                // Wait for existing tasks to terminate
                if( !quorumPeerMainTaskExecutor.awaitTermination
                                              (EXECUTOR_TERMINATION_TIMEOUT,
                                               TimeUnit.MILLISECONDS) )
                {
                    //cancel current tasks
                    quorumPeerMainTaskExecutor.shutdownNow();
                    // Wait for tasks to respond to being cancelled
                    if( !quorumPeerMainTaskExecutor.awaitTermination
                                              (EXECUTOR_TERMINATION_TIMEOUT,
                                               TimeUnit.MILLISECONDS) )
                    {
                        logger.log(Level.WARN, "shutdown "
                                   +"quorumPeerMainTaskExecutor "
                                   +"[FAILURE]");
                    }//endif
                }//endif
            } catch (InterruptedException ie) {
                //(Re-)Cancel if current thread also interrupted
                quorumPeerMainTaskExecutor.shutdownNow();
                Thread.currentThread().interrupt();//preserve interrupt
            }
            logger.log(Level.DEBUG,
                       "shutdown quorumPeerMainTaskExecutor [END]");

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

    private static class QuorumPeerMainTask implements Runnable {

        private QuorumPeerState  quorumPeerState;

        private QuorumPeerMain   quorumPeerMain = null;
        private QuorumPeerConfig quorumPeerConfig = null;

        private ZooKeeperServerMain zookeeperServerMain = null;
        private ServerConfig        serverConfig = null;

        public QuorumPeerMainTask(ConfigStateInfo stateInfo) 
            throws IOException, QuorumPeerConfig.ConfigException
        {
            this.quorumPeerConfig = new QuorumPeerConfig();
            (this.quorumPeerConfig).parseProperties
                (stateInfo.getConfigProperties());

            this.quorumPeerState = stateInfo.getQuorumPeerState();

            if(quorumPeerState.getNQuorumPeers() > 1) {
                this.quorumPeerMain = new QuorumPeerMain();
            } else {//running zookeeper standalone
                this.zookeeperServerMain = new ZooKeeperServerMain();
                this.serverConfig = new ServerConfig();
                (this.serverConfig).readFrom(quorumPeerConfig);
            }
        }

        public void run() {
            try{
                if(quorumPeerMain != null) {
                    logger.log(Level.DEBUG, "QuorumPeerMainTask: "
                               +"[QuorumPeerMain.runFromConfig] - "
                               +"zookeeper ensemble (size="
                               +quorumPeerState.getNQuorumPeers()+")");
                    quorumPeerMain.runFromConfig(quorumPeerConfig);
                } else if(zookeeperServerMain != null) {
                    logger.log(Level.DEBUG, "QuorumPeerMainTask: "
                               +"[ZookeeperServerMain.runFromConfig] - "
                               +"standalone zookeeper");
                    zookeeperServerMain.runFromConfig(serverConfig);
                } else {
                    logger.error("QuorumPeerMainTask: failed to "
                                 +"initialize either QuorumPeerMain or "
                                 +"standalone ZooKeeperServerMain");
                }
            } catch (Throwable e) {
                if( !(Thread.currentThread()).isInterrupted() ) {
                    logger.error("QuorumPeerMainTask: ", e);//stackTrace
                }
            }
        }

        public void terminate() {
            terminate(0L, TimeUnit.MILLISECONDS);
        }

        /**
         * Terminates the zookeeper server referenced by this service by
         * sending an interrupt; for both the case of an ensemble -- that
         * is, for the case of QuorumPeerMain -- and the case of a
         * standalone zookeeper server -- that is, ZooKeeperMain. For
         * the case of an ensemble, sending an interrutp ultimately causes
         * the connection factory to be shutdown by QuorumPeerMain.
         * For the case of a standalone zookeeper server, ZooKeeperMain
         * shuts itself down when interrupted.
         *
         * Note that because both QuorumPeerMain and ZooKeeperMain are
         * in a different package than this class, and because the
         * shutdown method provided by each of those classes is package
         * protected, neither such method can be called directly here;
         * instead, an interrupt must be sent.
         */
        public void terminate(long timeout, TimeUnit unit) {

            // Signal the server to exit; preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    private static class ConfigStateInfo {

        private Logger logger;

        private File      persistBase;
        private UUID      proxyId;
        private ServiceID serviceId;

        private QuorumPeerState peerState;

        public ConfigStateInfo(final Configuration config,
                               final String        componentName,
                                     Logger        logger)
                                      throws ConfigurationException,
                                             IOException,
                                             ClassNotFoundException,
                                             QuorumPeerConfig.ConfigException
        {
            if(config == null) {
                throw new NullPointerException("config null");
            }
            if(componentName == null) {
                throw new NullPointerException("componentName null");
            }
            this.logger = (logger == null ?
                           Logger.getLogger(this.getClass()) : logger);

            recoverQuorumPeerState(config, componentName);
        }

        public String getPersistenceDirectory() {
            return ( (persistBase != null) ? persistBase.toString() : null);
        }

        public ServiceID getServiceId() {
            return serviceId;
        }

        public UUID getProxyId() {
            return proxyId;
        }

        public QuorumPeerState getQuorumPeerState() {
            return peerState;
        }

        //Return new Properties constructed from current quorum peer state
        public Properties getConfigProperties() {
            Properties configProps = new Properties();

            configProps.setProperty
                ("clientPort",
                 (new Integer(peerState.getClientPort())).toString());
            configProps.setProperty("dataDir", peerState.getDataDir());
            configProps.setProperty("dataLogDir", peerState.getDataLogDir());
            configProps.setProperty
                ("tickTime", 
                 (new Integer(peerState.getTickTime())).toString());
            configProps.setProperty
                ("initLimit",
                 (new Integer(peerState.getInitLimit())).toString());
            configProps.setProperty
                ("syncLimit",
                 (new Integer(peerState.getSyncLimit())).toString());
            configProps.setProperty
                ("electionAlg",
                 (new Integer(peerState.getElectionAlg())).toString());
            configProps.setProperty
                ("maxClientCnxns",
                 (new Integer(peerState.getMaxClientCnxns())).toString());

            Map<Long, QuorumPeerData> peerDataMap = peerState.getPeerDataMap();
            logger.debug("peerDataMap.size() = "+peerDataMap.size());
            for(QuorumPeerData peerData : peerDataMap.values()) {
                long peerId = peerData.getPeerId();
                InetSocketAddress pAddr = peerData.getPeerAddress();
                InetAddress peerAddr = pAddr.getAddress();
                int peerPort = pAddr.getPort();
                InetSocketAddress eAddr = peerData.getElectionAddress();
                int electionPort = eAddr.getPort();
                String serverKey = "server."+peerId;
                String serverVal = peerAddr.getHostAddress()
                                   +":"+peerPort+":"+electionPort;
                logger.debug("serverKey="+serverKey+", serverVal="+serverVal);
                configProps.setProperty(serverKey, serverVal);
            }
            return configProps;
        }

        public QuorumPeerState getPersistedQuorumPeerState() 
            throws IOException, ClassNotFoundException
        {
            File peerStateFd = new File(persistBase, "peer.state");
            logger.log(Level.DEBUG,"path to config state - "+peerStateFd);
            FileInputStream fis = new FileInputStream(peerStateFd);
            ClassLoaderObjectInputStream ois = 
                    new ClassLoaderObjectInputStream
                         (new BufferedInputStream(fis),
                          ((Thread.currentThread()).getContextClassLoader()));
            try {
                return ((QuorumPeerState)ois.readObject());
            } finally {
                ois.close();
            }
        }

        public void persistQuorumPeerState(QuorumPeerState newState)
            throws IOException
        {
            File peerStateFd = new File(persistBase, "peer.state");
            ObjectOutputStream oos = 
                new ObjectOutputStream
                    ( new BufferedOutputStream
                      (new FileOutputStream(peerStateFd)) );
            try {
                oos.writeObject(newState);
                this.peerState = newState;
            } finally {
                oos.close();
            }
        }

        /* Performs the actual retrieval of the service's state related
         * to its behavior as a peer in a quorum. This method is called
         * only once, from the constructor of this class whenever the
         * service is started.
         * <p>
         * If the service is being started for the first time -- that is,
         * the service has no previously persisted state -- then this
         * method retrieves the peer state from the given jini Configuration,
         * and persists that state for retrieval the next time the service
         * is started.
         * <p>
         * On the other hand, if the service was previously started with
         * a given configuration that was persisted, then this method
         * recovers that previously persisted state from local storage.
         * <p>
         * Note that when the service's proxy id is recovered by this
         * method, that proxy id is used to construct the jini service id
         * with which the service was initially registered.
         */
        private void recoverQuorumPeerState(final Configuration config,
                                            final String        componentName)
            throws ConfigurationException, IOException, ClassNotFoundException,
                   QuorumPeerConfig.ConfigException
        {
            // PERSISTENCE DIRECTORY - get the directory under which the
            // service persists it's config state. If no configuration entry
            // exists, or if the config entry is null, then use the current 
            // directory from which the service is being run.
            String defaultPersistBaseStr = 
                System.getProperty("user.dir") + F_SEP + "zookeeper";
            String persistBaseStr = 
                    (String)config.getEntry(componentName,
                                            "persistenceDirectory",
                                            String.class,
                                            defaultPersistBaseStr);
            logger.log(Level.DEBUG, "zookeeper state persistence directory: "
                       +persistBaseStr);
            if(persistBaseStr == null) {
                throw new ConfigurationException
                    ("no base directory for config state persistence");
            }
            this.persistBase = new File(persistBaseStr);

            // If the requested directory does not exist, create it
            if(!(persistBase.exists() ? persistBase.isDirectory()
                                      : persistBase.mkdirs()) )
            {
                throw new IOException("could not create persistence "
                                      +"directory: ["+persistBase+"]");
            }

            UUID defaultProxyId = null;
            defaultProxyId = 
                (UUID)config.getEntry(componentName, "defaultProxyId",
                                      UUID.class, defaultProxyId);

            // CONFIG STATE - retrieve/store

            try {
                this.peerState = getPersistedQuorumPeerState();
                logger.log(Level.DEBUG, "RESTART: "
                           +"retrieve previously persisted config state");

                // retrieve proxy id
                proxyId = peerState.getProxyId();
                if( proxyId == null ) {
                    throw new IOException
                                       ("config state contains null proxyId");
                }
                // construct service id
                serviceId = 
                    new ServiceID(proxyId.getMostSignificantBits(),
                                  proxyId.getLeastSignificantBits());
            } catch(FileNotFoundException e) {/* FIRST EVER STARTUP */

                // This service requires that a jini config be provided.
                // But if that jini config specifies a standard zookeeper
                // config file (an entry named "zookeeperConfigFile"),
                // then retrieve all zookeeper-specific config info from
                // that file, and retrieve all other non-zookeeper-specific
                // config info from the remaining information in the
                // jini config.
                //
                // If the jini config does not contain an entry that
                // references a standard zookeeper config, then all
                // non-optional zookeeper-specific config information
                // must be specified in the jini config.

                this.proxyId = createProxyId(defaultProxyId);
                this.serviceId = 
                    new ServiceID( proxyId.getMostSignificantBits(),
                                   proxyId.getLeastSignificantBits() );
                this.peerState = new QuorumPeerStateV0( proxyId, serviceId );

                //retrieve from standard zookeeper config file if it exists
                String zConfigFile = null;
                try {
                    zConfigFile = (String)config.getEntry
                                               (COMPONENT_NAME,
                                                "zookeeperConfigFile",
                                                String.class);
                    if(zConfigFile == null) {
                        throw new ConfigurationException
                            ("null entry [zookeeperConfigurationPath]");
                    }
                } catch (NoSuchEntryException e1) { }

                if(zConfigFile != null) {//retrieve from zookeeper config

                    logger.log(Level.DEBUG,  "INITIAL START: "
                               +"[use zookeeper config]");

                    QuorumPeerConfig zConfig = new QuorumPeerConfig();

                    logger.log(Level.DEBUG, "parsing "+zConfigFile);
                    zConfig.parse(zConfigFile);

                    //clientPort
//for zookeeper 3.2.1
//                    this.peerState.setClientPort(zConfig.getClientPort());
//for zookeeper 3.3.0+
                    this.peerState.setClientPort
                        (zConfig.getClientPortAddress().getPort());

                    //dataDir
                    this.peerState.setDataDir(zConfig.getDataDir());

                    //dataLogDir
                    this.peerState.setDataLogDir(zConfig.getDataLogDir());

                    //tickTime
                    this.peerState.setTickTime(zConfig.getTickTime());

                    //initLimit
                    this.peerState.setInitLimit(zConfig.getInitLimit());

                    //syncLimit
                    this.peerState.setSyncLimit(zConfig.getSyncLimit());

                    //electionAlg
                    this.peerState.setElectionAlg(zConfig.getElectionAlg());

                    //electionPort
                    this.peerState.setElectionPort(zConfig.getElectionPort());

                    //maxClientCnxns
                    this.peerState.setMaxClientCnxns
                        (zConfig.getMaxClientCnxns());

                    //peerConfigMap: server.n=<host>:peerPort:electionPort
                    Map<Long, QuorumPeer.QuorumServer> peerMap =
                        zConfig.getServers();
                    if(peerMap != null) {

                        //prepare for finding this server's id (match addr)
                        NetworkInterface[] nics = null;
                        Enumeration<NetworkInterface> en = 
                            NetworkInterface.getNetworkInterfaces();
                        if(en == null) {
                            nics = new NetworkInterface[0];
                        } else {
                            List<NetworkInterface> nicList =
                                new ArrayList<NetworkInterface>();
                            while( en.hasMoreElements() ) {
                                nicList.add( en.nextElement() );
                            }
                            nics = nicList.toArray
                                       (new NetworkInterface[nicList.size()]);
                        }

                        Map<Long, QuorumPeerData> peerDataMap = 
                         new HashMap<Long, QuorumPeerData>(peerMap.size());
                        boolean peerIdFound = false;
                        for(QuorumPeer.QuorumServer server : peerMap.values()){
                            QuorumPeerData peerData = new QuorumPeerDataV0();
                            peerData.setPeerId(server.id);
                            peerData.setPeerAddress(server.addr);
                            peerData.setElectionAddress(server.electionAddr);
                            peerDataMap.put(server.id, peerData);
                            logger.log(Level.DEBUG, "[server.id="+server.id
                                       +", server.addr="+server.addr+", "
                                       +"server.electionAddr="
                                       +server.electionAddr+"]");

                            if( peerIdFound ) continue;

                            for(NetworkInterface nic : nics) {
                                List<InterfaceAddress> iFaceAddrs = 
                                   nic.getInterfaceAddresses();
                                for(InterfaceAddress iFaceAddr : iFaceAddrs) {
                                    InetAddress inetAddr =
                                        iFaceAddr.getAddress();
                                    if( inetAddr.equals
                                            (server.addr.getAddress()) )
                                    {
                                        this.peerState.setPeerId(server.id);
                                        this.peerState.setPeerAddress
                                            (server.addr.getAddress());
                                        peerIdFound = true;
                                        logger.log(Level.DEBUG, 
                                                   "peer id found [peerId="
                                                   +server.id+", "
                                                   +"peerAddress="
                                                   +server.addr.getAddress()
                                                   +"]");
                                        break;
                                    }
                                }
                                if( peerIdFound ) break;
                            }
                        }

                        this.peerState.setPeerDataMap(peerDataMap);
                        this.peerState.setNQuorumPeers(peerDataMap.size());
                    }

                } else {//retrieve from jini config

                    logger.log(Level.DEBUG, "INITIAL START: "
                               +"[use deployment config]");

                    //clientPort
                    Integer defaultClientPort =
                        ConfigDeployUtil.getInt
                            ("federation.zookeeperClientPort");
                    Integer zClientPort = 
                       (Integer)config.getEntry(COMPONENT_NAME,
                                                "zookeeperClientPort",
                                                Integer.class,
                                                defaultClientPort);
                    if(zClientPort == null) {
                        throw new ConfigurationException
                            ("null zookeeperClientPort");
                    }
                    this.peerState.setClientPort(zClientPort);

                    //dataDir
                    String defaultDataDir =
                        ConfigDeployUtil.getString
                            ("federation.zookeeperDataDir");
                    String zDataDir = persistBaseStr + F_SEP
                        + (String)config.getEntry(COMPONENT_NAME,
                                                  "zookeeperDataDir",
                                                  String.class,
                                                  defaultDataDir);
                    if(zDataDir == null) {
                        throw new ConfigurationException
                            ("null zookeeperDataDir");
                    }
                    this.peerState.setDataDir(zDataDir);

                    //dataLogDir
                    String defaultDataLogDir =
                        ConfigDeployUtil.getString
                            ("federation.zookeeperDataLogDir");
                    String zDataLogDir = persistBaseStr + F_SEP
                        + (String)config.getEntry
                                          (COMPONENT_NAME,
                                           "zookeeperDataLogDir",
                                           String.class,
                                           defaultDataLogDir);
                    if(zDataLogDir == null) {
                        throw new ConfigurationException
                            ("null zookeeperDataLogDir");
                    }
                    this.peerState.setDataLogDir(zDataLogDir);

                    //tickTime
                    Integer defaultTickTime =
                        ConfigDeployUtil.getInt
                            ("federation.zookeepeTickTime");
                    Integer zTickTime = 
                       (Integer)config.getEntry(COMPONENT_NAME,
                                                "zookeeperTickTime",
                                                Integer.class,
                                                defaultTickTime);
                    if(zTickTime == null) {
                        throw new ConfigurationException
                            ("null zookeeperTickTime");
                    }
                    this.peerState.setTickTime(zTickTime);

                    //initLimit
                    Integer defaultInitLimit =
                        ConfigDeployUtil.getInt
                            ("federation.zookeeperInitLimit");
                    Integer zInitLimit = 
                       (Integer)config.getEntry(COMPONENT_NAME,
                                                "zookeeperInitLimit",
                                                Integer.class, 5);
                    if(zInitLimit == null) {
                        throw new ConfigurationException
                            ("null zookeeperInitLimit");
                    }
                    this.peerState.setInitLimit(zInitLimit);

                    //syncLimit
                    Integer defaultSyncLimit =
                        ConfigDeployUtil.getInt
                            ("federation.zookeeperSyncLimit");
                    Integer zSyncLimit = 
                       (Integer)config.getEntry(COMPONENT_NAME,
                                                "zookeeperSyncLimit",
                                                Integer.class, 
                                                defaultSyncLimit);
                    if(zSyncLimit == null) {
                        throw new ConfigurationException
                            ("null zookeeperSyncLimit");
                    }
                    this.peerState.setSyncLimit(zSyncLimit);

                    //electionAlg
                    Integer defaultElectionAlg =
                        ConfigDeployUtil.getInt
                            ("federation.zookeeperElectionAlg");
                    Integer zElectionAlg = 
                       (Integer)config.getEntry(COMPONENT_NAME,
                                                "zookeeperElectionAlg",
                                                Integer.class,
                                                defaultElectionAlg);
                    if(zElectionAlg == null) {
                        throw new ConfigurationException
                            ("null zookeeperElectionAlg");
                    }
                    this.peerState.setElectionAlg(zElectionAlg);

                    //maxClientCnxns
                    Integer defaultMaxClientCnxns =
                        ConfigDeployUtil.getInt
                            ("federation.zookeeperMaxClientCnxns");
                    Integer zMaxClientCnxns = 
                        (Integer)config.getEntry(COMPONENT_NAME,
                                                 "zookeeperMaxClientCnxns",
                                                 Integer.class,
                                                 defaultMaxClientCnxns);
                    if(zMaxClientCnxns == null) {
                        throw new ConfigurationException
                            ("null zookeeperMaxClientCnxns");
                    }
                    this.peerState.setMaxClientCnxns(zMaxClientCnxns);

                    // Because this is the first time this service is started,
                    // and because the config is retrieved from a deployment
                    // specific configuration, there is no knowledge (yet)
                    // of the other zookeeper servers, other than the total
                    // number of peers that are expected to make up the
                    // ensemble (zookeeperEnsembleSize). If the ensemble
                    // will consist of only this service peer, then the
                    // peerConfigMap can be created and populated with this
                    // service's server information. But if the ensemble will
                    // include other peers as well, then the peerConfigMap
                    // cannot be fully populated until all other zookeeper
                    // servers are discovered and a leader is elected.

                    //zookeeperNetwork (peerAddress)
                    String zookeeperNetwork =
                               NicUtil.getIpAddress
                                   ("default.nic",
                                    ConfigDeployUtil.getString
                                        ("node.serviceNetwork"),
                                    false);

                    if(zookeeperNetwork == null) {
                        throw new ConfigurationException
                            ("null zookeeperNetwork");
                    }
                    InetAddress peerAddress = 
                       NicUtil.getInetAddress
                                   (zookeeperNetwork, 0, null, true);
                    this.peerState.setPeerAddress(peerAddress);

                    //peerPort
                    Integer defaultPeerPort =
                        ConfigDeployUtil.getInt
                            ("federation.zookeeperPeerPort");
                    Integer peerPort = 
                       (Integer)config.getEntry(COMPONENT_NAME,
                                                "zookeeperPeerPort",
                                                Integer.class,
                                                defaultPeerPort);
                    if(peerPort == null) {
                        throw new ConfigurationException
                            ("null zookeeperPeerPort");
                    }
                    this.peerState.setPeerPort(peerPort);

                    //electionPort
                    Integer defaultElectionPort =
                        ConfigDeployUtil.getInt
                            ("federation.zookeeperElectionPort");
                    Integer electionPort = 
                       (Integer)config.getEntry(COMPONENT_NAME,
                                                "zookeeperElectionPort",
                                                Integer.class, 3888);
                    if(electionPort == null) {
                        throw new ConfigurationException
                            ("null zookeeperElectionPort");
                    }
                    this.peerState.setElectionPort(electionPort);

                    //nQuorumPeers
                    Integer defaultEnsembleSize =
                        ConfigDeployUtil.getInt
                            ("federation.zookeeperEnsembleSize");
                    Integer nQuorumPeers = 
                       (Integer)config.getEntry(COMPONENT_NAME,
                                                "zookeeperEnsembleSize",
                                                Integer.class,
                                                defaultEnsembleSize);
                    if(nQuorumPeers == null) {
                        throw new ConfigurationException
                            ("null nQuorumPeers");
                    }
                    if(nQuorumPeers < 1) {
                        throw new ConfigurationException
                            ("nQuorumPeers less than 1 ["+nQuorumPeers+"]");
                    }
                    this.peerState.setNQuorumPeers(nQuorumPeers);

                    if(nQuorumPeers > 1) {

                        // nQuorumPeers > 1 means that the ensemble is 
                        // configured to not be standalone; in which case,
                        // this service's peer id is initially set to 0
                        // to indicate to this service's init method that
                        // the initQuorumPeerData method must be invoked
                        // to dicover the other peers so in the ensemble
                        // so that each peer's id (myid) can be set to a
                        // unique value between 1 and nQuorumPeers

                        this.peerState.setPeerId(0L);

                    } else {//nQuorumPeers == 1, populate peerConfigMap
                        long peerId = 1L;
                        this.peerState.setPeerId(peerId);

                        QuorumPeerData peerData = new QuorumPeerDataV0();
                        Map<Long, QuorumPeerData> peerDataMap =
                            new HashMap<Long, QuorumPeerData>(1);

                        peerData.setPeerId(peerId);
                        peerData.setPeerAddress
                            (new InetSocketAddress(peerAddress, peerPort));
                        peerData.setElectionAddress
                            (new InetSocketAddress(peerAddress, electionPort));

                        peerDataMap.put(peerId, peerData);
                        this.peerState.setPeerDataMap(peerDataMap);
                    }

                }//endif(zConfigFile != null)

                // Persist the configuration retrieved above
                persistQuorumPeerState(this.peerState);
            }
            File dataDir = new File(this.peerState.getDataDir());
            if(!(dataDir.exists() ? dataDir.isDirectory() : dataDir.mkdir())) {
                throw new IOException
                    ("could not create data directory: " + dataDir);
            }
            File dataLogDir = new File(this.peerState.getDataLogDir());
            if(!(dataLogDir.exists() ? dataLogDir.isDirectory()
                                     : dataLogDir.mkdir()))
            {
                throw new IOException
                    ("could not create data log directory: " + dataLogDir);
            }

            if(logger.isDebugEnabled()) {
                logger.log(Level.DEBUG, "nQuorumPeers   = "
                           +peerState.getNQuorumPeers());
                logger.log(Level.DEBUG, "peerId         = "
                           +peerState.getPeerId());
                logger.log(Level.DEBUG, "peerAddress    = "
                           +peerState.getPeerAddress());
                logger.log(Level.DEBUG, "peerPort       = "
                           +peerState.getPeerPort());
                logger.log(Level.DEBUG, "clientPort     = "
                           +peerState.getClientPort());
                logger.log(Level.DEBUG, "dataDir        = "
                           +peerState.getDataDir());
                logger.log(Level.DEBUG, "dataLogDir     = "
                           +peerState.getDataLogDir());
                logger.log(Level.DEBUG, "tickTime       = "
                           +peerState.getTickTime());
                logger.log(Level.DEBUG, "initLimit      = "
                           +peerState.getInitLimit());
                logger.log(Level.DEBUG, "syncLimit      = "
                           +peerState.getSyncLimit());
                logger.log(Level.DEBUG, "electionAlg    = "
                           +peerState.getElectionAlg());
                logger.log(Level.DEBUG, "electionPort   = "
                           +peerState.getElectionPort());
                logger.log(Level.DEBUG, "maxClientCnxns = "
                           +peerState.getMaxClientCnxns());
                logger.log(Level.DEBUG, "peerDataMap    = "
                           +peerState.getPeerDataMap());
                logger.log(Level.DEBUG, 
                           "zookeeper server config state recovery complete");
            }
        }

        /*
         * Creates a globally unique (across space and time) id for the
         * service proxy. If a default proxy id is specified then that
         * value is returned, otherwise a randomly-generated id is returned.
         */ 
        private UUID createProxyId(UUID defaultProxyId) {
            if(defaultProxyId != null) {
                return defaultProxyId;
            } else {
                return UUID.randomUUID();
            }
        }

    }

    /**
     * Subclass of <code>java.io.ObjectInputStream</code> that overrides the
     * behavior of the <code>ObjectInputStream#resolveClass</code> method by
     * allowing one to specify a non-<code>null</code> <code>ClassLoader</code>
     * to be used when loading the local class equivalent of the stream class
     * description that is input to <code>resolveClass</code>.
     */
    private static class ClassLoaderObjectInputStream
                                                   extends ObjectInputStream
    {
        private ClassLoader loader;

        /** 
         * @see java.io.ObjectInputStream
         *
         * @throws java.lang.NullPointerException when <code>null</code> is
         *         input for the <code>loader</code> parameter; otherwise uses
         *         the given <code>loader</code> when <code>resolveClass</code>
         *         is invoked to perform class resolution.
         */
        public ClassLoaderObjectInputStream(InputStream in,
                                            ClassLoader loader)
                                                          throws IOException
        {
            super(in);
            if(loader == null) throw new NullPointerException("null loader");
            this.loader = loader;
        }

        /** 
         * @see java.io.ObjectInputStream#resolveClass
         */
        protected Class<?> resolveClass(ObjectStreamClass desc)
                             throws IOException, ClassNotFoundException
        {
            try {
                return Class.forName(desc.getName(), false, loader);
            } catch (ClassNotFoundException e) {
                return super.resolveClass(desc);
            }
        }
    }
}
