/*

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
package com.bigdata.jini.quorum;

import com.bigdata.attr.QuorumPeerAttr;
import com.bigdata.service.QuorumPeerService;
import com.bigdata.service.Service;
import com.bigdata.util.EntryUtil;
import com.bigdata.util.Util;
import com.bigdata.util.config.LogUtil;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class that wraps the <code>org.apache.zookeeper.ZooKeeper</code>
 * client class, providing additional covenient functionality related to
 * dynamic discovery of the connection information associated with the
 * peers in the federation's ensemble, as well as exception and session
 * expiry handling.
 */
public class QuorumPeerManager {

    private Logger logger;

    private ServiceDiscoveryManager sdm;
    private int sessionTimeout;

    // How long to wait for the ensemble to be discovered (and the 
    // connectString to be constructed)
    // 0L ==> don't wait, the caller will provide its own retry logic
    // Long.MAX_VALUE ==> wait forever
    // negative ==> use backoff strategy specified by this class
    private long discoverWait;

    // How long to wait for a connection before declaring failure
    // 0L ==> try to connect only once
    // Long.MAX_VALUE ==> wait forever
    // negative ==>  use backoff strategy specified by this class
    private long connectWait;

    private LookupCache quorumServiceCache;
    private Map<UUID, String> hostPortMap =
                                  new ConcurrentHashMap<UUID, String>();
    private Map<UUID, QuorumPeerAttr> quorumPeerAttrMap =
                                 new ConcurrentHashMap<UUID, QuorumPeerAttr>();

    private volatile String connectString = null;
    private volatile ZooKeeper zkClient;
    private volatile boolean terminated = false;

    private Object syncObj = new Object();
    private static long[] discoverBackoff =
                   {1L, 2L, 4L, 8L, 16L, 32L, 64L, 128L, 256L};//seconds
    private static long[] connectBackoff =
                   {1L, 2L, 4L, 8L, 16L, 32L, 64L, 128L};//seconds

    public QuorumPeerManager(ServiceDiscoveryManager sdm,
                             int sessionTimeout,
                             Logger logger)
    {
        this(sdm, sessionTimeout, -1L, -1L, logger);
    }

    public QuorumPeerManager(ServiceDiscoveryManager sdm,
                             int sessionTimeout,
                             long discoverWait,
                             long connectWait,
                             Logger logger)
    {
        if (sdm == null) {
            throw new NullPointerException("null sdm");
        }
        this.sdm = sdm;
        ServiceDiscoveryListener cacheListener = new CacheListener(logger);

        // Discover all QuorumPeerServices that have the join the federation
        // the given sdm is configured to discover (by groups and/or locs)

        Class[] quorumServiceType = new Class[] {QuorumPeerService.class};
        ServiceTemplate quorumServiceTmpl = 
                            new ServiceTemplate(null, quorumServiceType, null);
        try {
            this.quorumServiceCache = sdm.createLookupCache(quorumServiceTmpl, 
                                                            null,//filter
                                                            cacheListener);
        } catch(RemoteException e) {
            logger.warn(e.getMessage(), e);
        }

        this.sessionTimeout = sessionTimeout;
        this.discoverWait = discoverWait;
        this.connectWait = connectWait;
        this.logger = (logger == null ? 
                        LogUtil.getLog4jLogger((this.getClass()).getName()) :
                        logger);
    }

    // Wrapped methods from org.apache.zookeper.ZooKeeper client class

    public long getSessionId() throws IOException {
        checkTerminated();
        return getClient().getSessionId();
    }

    public byte[] getSessionPasswd() throws IOException {
        checkTerminated();
        return getClient().getSessionPasswd();
    }

    public int getSessionTimeout() throws IOException {
        checkTerminated();
        return getClient().getSessionTimeout();
    }

    public void addAuthInfo(String scheme, byte[] auth) throws IOException {
        checkTerminated();
        getClient().addAuthInfo(scheme, auth);
    }

    public void register(Watcher watcher) throws IOException {
        checkTerminated();
        getClient().register(watcher);
    }

    public void close() {
        if (terminated) return;
        if ( (zkClient != null) && (zkClient.getState().isAlive()) ) {
            try {
                zkClient.close();
            } catch(InterruptedException e) {//swallow
            }
        }
        terminated = true;
        connectString = null;
    }

    public String create(String path,
                         byte[] data,
                         List<ACL> acl,
                         CreateMode createMode)
                      throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().create(path, data, acl, createMode);
    }

    public void create(String path,
                       byte[] data,
                       List<ACL> acl,
                       CreateMode createMode,
                       AsyncCallback.StringCallback cb,
                       Object ctx) throws IOException
    {
        checkTerminated();
        getClient().create(path, data, acl, createMode, cb, ctx);
    }

    public void delete(String path, int version)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        getClient().delete(path, version);
    }

    public void delete(String path,
                       int version,
                       AsyncCallback.VoidCallback cb,
                       Object ctx) throws IOException
    {
        checkTerminated();
        getClient().delete(path, version, cb, ctx);
    }

    public Stat exists(String path, Watcher watcher)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().exists(path, watcher);
    }

    public Stat exists(String path, boolean watch)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().exists(path, watch);
    }

    public void exists(String path,
                       Watcher watcher,
                       AsyncCallback.StatCallback cb,
                       Object ctx)
                    throws IOException
    {
        checkTerminated();
        getClient().exists(path, watcher, cb, ctx);
    }

    public void exists(String path,
                       boolean watch,
                       AsyncCallback.StatCallback cb,
                       Object ctx)
                    throws IOException
    {
        checkTerminated();
        getClient().exists(path, watch, cb, ctx);
    }

    public byte[] getData(String path, Watcher watcher, Stat stat)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().getData(path, watcher, stat);
    }

    public byte[] getData(String path, boolean watch, Stat stat)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().getData(path, watch, stat);
    }

    public void getData(String path,
                        Watcher watcher,
                        AsyncCallback.DataCallback cb,
                        Object ctx) throws IOException
    {
        checkTerminated();
        getClient().getData(path, watcher, cb, ctx);
    }

    public void getData(String path,
                        boolean watch,
                        AsyncCallback.DataCallback cb,
                        Object ctx) throws IOException
    {
        checkTerminated();
        getClient().getData(path, watch, cb, ctx);
    }

    public Stat setData(String path, byte[] data, int version)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().setData(path, data, version);
    }

    public void setData(String path,
                        byte[] data,
                        int version,
                        AsyncCallback.StatCallback cb,
                        Object ctx) throws IOException
    {
        checkTerminated();
        getClient().setData(path, data, version, cb, ctx);
    }

    public List<ACL> getACL(String path, Stat stat)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().getACL(path, stat);
    }

    public void getACL(String path,
                       Stat stat,
                       AsyncCallback.ACLCallback cb,
                       Object ctx) throws IOException
    {
        checkTerminated();
        getClient().getACL(path, stat, cb, ctx);
    }

    public Stat setACL(String path, List<ACL> acl, int version)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().setACL(path, acl, version);
    }

    public void setACL(String path,
                       List<ACL> acl,
                       int version,
                       AsyncCallback.StatCallback cb,
                       Object ctx) throws IOException
    {
        checkTerminated();
        getClient().setACL(path, acl, version, cb, ctx);
    }

    public List<String> getChildren(String path, Watcher watcher)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().getChildren(path, watcher);
    }

    public List<String> getChildren(String path, boolean watch)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().getChildren(path, watch);
    }

    public void getChildren(String path,
                            Watcher watcher,
                            AsyncCallback.ChildrenCallback cb,
                            Object ctx) throws IOException
    {
        checkTerminated();
        getClient().getChildren(path, watcher, cb, ctx);
    }

    public void getChildren(String path,
                            boolean watch,
                            AsyncCallback.ChildrenCallback cb,
                            Object ctx) throws IOException
    {
        checkTerminated();
        getClient().getChildren(path, watch, cb, ctx);
    }

    public List<String> getChildren(String path, Watcher watcher, Stat stat)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().getChildren(path, watcher, stat);
    }

    public List<String> getChildren(String path, boolean watch, Stat stat)
                    throws IOException, KeeperException, InterruptedException
    {
        checkTerminated();
        return getClient().getChildren(path, watch, stat);
    }

    public void getChildren(String path,
                            Watcher watcher,
                            AsyncCallback.Children2Callback cb,
                            Object ctx) throws IOException
    {
        checkTerminated();
        getClient().getChildren(path, watcher, cb, ctx);
    }

    public void getChildren(String path,
                            boolean watch,
                            AsyncCallback.Children2Callback cb,
                            Object ctx) throws IOException
    {
        checkTerminated();
        getClient().getChildren(path, watch, cb, ctx);
    }

    public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx)
                         throws IOException
    {
        checkTerminated();
        getClient().sync(path, cb, ctx);
    }

    public ZooKeeper.States getState() throws IOException {
        checkTerminated();
        return getClient().getState();
    }

    public String toString() {
        checkTerminated();
        try {
            return getClient().toString();
        } catch(IOException e) {
            // default value when client unavailable
            return "[connectString="+connectString
                     +", sessionTimeout="+sessionTimeout
                     +", connectWait="+connectWait
                     +", terminated="+terminated+"]";
        }
    }

    // Other public methods defined by this class, not defined by ZooKeeper
    public String getConnectString() {
        return connectString;
    }

    // Need to keep the addresses in order with their respective ports
    public List<List<String>> getServerInfo() {
        List<List<String>> retList = new ArrayList<List<String>>();
        Collection<QuorumPeerAttr> attrs = quorumPeerAttrMap.values();
        if ( attrs.isEmpty() ) {
            logger.debug("no zookeeper servers discovered");
            return retList;
        }
        for (QuorumPeerAttr attr : attrs) {
            String addr = (attr.address).getHostAddress();
            String peerPort = String.valueOf(attr.peerPort);
            String electionPort = String.valueOf(attr.electionPort);
            List<String> subList = new ArrayList<String>();
            subList.add(addr);
            subList.add(peerPort);
            subList.add(electionPort);
            retList.add(subList);
        }
        return retList;
    }

    // Client ports should be the same for all zookeeper servers
    public int getClientPort() {
        Iterator<QuorumPeerAttr> itr = (quorumPeerAttrMap.values()).iterator();
        if ( !itr.hasNext() ) {
            logger.debug("no zookeeper servers discovered [clientPort=-1]");
            return -1;
        }

        int port0 = (itr.next()).clientPort;

        // client ports from each zookeeper server should be the same
        boolean allEqual = true;
        while( itr.hasNext() ) {
            int port = (itr.next()).clientPort;
            if (port != port0) allEqual = false;
        }
        if (!allEqual) {
            logger.warn("not all zookeeper servers configured with same "
                        +"client port - "+quorumPeerAttrMap.values());
        }
        return port0;
    }

    // Private methods

    private ZooKeeper getClient() throws IOException {
        if ( (zkClient != null) && (zkClient.getState().isAlive()) ) {
            return zkClient;
        }

        // Determine if ensemble has been discovered yet

        if (connectString == null) {
            if(discoverWait > 0L) {//retry for discoverWait seconds
               for (long i=0L; i<discoverWait; i++) {
                    Util.delayMS(1L*1000L);
                    if (connectString != null) break;
                    if (logger.isDebugEnabled()) {
                        logger.debug("ensemble still not discovered "
                                     +"[try #"+(i+1)+"]");
                    }
                }
            } else if (discoverWait < 0L) {//retry with backoff
                for (int i=0; i<discoverBackoff.length; i++) {
                    Util.delayMS(discoverBackoff[i]*1000L);
                    if (connectString != null) break;
                    if (logger.isDebugEnabled()) {
                        logger.debug("ensemble still not discovered "
                                     +"[try #"+(i+1)+"]");
                    }
                }
            }
            if (connectString == null) {//still not discovered ==> fail
                zkClient = null;
                throw new IllegalStateException
                              ("never discovered zookeeper ensemble");
            }
        }

        // Ensemble discovered and connectString constructed, construct client

        zkClient = new ZooKeeper(connectString,
                                 sessionTimeout,
                                 new ZookeeperEventListener(logger));

        // Connect to ensemble

        ZooKeeper.States state = zkClient.getState();
        logger.debug("state[try #0] = "+state);

        if ( !state.equals(ZooKeeper.States.CONNECTED) ) {
            boolean connected = false;
            if (connectWait == 0L) {//tried once above
                zkClient = null;
            } else if (connectWait > 0L) {//retry until connected or timeout
                for (long i=0L; i<connectWait; i++) {
                    Util.delayMS(1L*1000L);
                    state = zkClient.getState();
                    if (logger.isDebugEnabled()) {
                        logger.debug("state[try #"+(i+1)+"] = "+state);
                    }
                    if ( state.equals(ZooKeeper.States.CONNECTED) ) {
                        connected = true;
                        break;
                    }
                }
            } else { //connectWait < 0L ==> retry with default backoff
                for (int i=0; i<connectBackoff.length; i++) {
                    Util.delayMS(connectBackoff[i]*1000L);
                    state = zkClient.getState();
                    if (logger.isDebugEnabled()) {
                        logger.debug("state[try #"+(i+1)+"] = "+state);
                    }
                    if ( state.equals(ZooKeeper.States.CONNECTED) ) {
                        connected = true;
                        break;
                    }
                }
            }
            if (!connected) zkClient = null;//never connected
        }
        if (zkClient == null) {
            throw new IllegalStateException("zookeeper ensemble unavailable");
        }
        return zkClient;
    }

    private void checkTerminated() {
        if (terminated) {
            throw new IllegalStateException("QuorumPeerManager terminated");
        }
    }

    // Nested class(es)

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
            UUID serviceUUID = ((Service)service).getServiceUUID();

            QuorumPeerAttr quorumPeerAttr = 
                (QuorumPeerAttr)(EntryUtil.getEntryByType
                                               (attrs, QuorumPeerAttr.class));

            InetAddress peerAddr = quorumPeerAttr.address;
            int clientPort = quorumPeerAttr.clientPort;
            int ensembleSize = quorumPeerAttr.nQuorumPeers;

            if(logger.isDebugEnabled()) {
                logger.log(Level.DEBUG, "1 of "+ensembleSize+" quorum peer(s) "
                           +"DISCOVERED [addr="+peerAddr.getHostAddress()
                           +", port="+clientPort+"]");
            }
            hostPortMap.put
                (serviceUUID, peerAddr.getHostAddress()+":"+clientPort);

            // Build connectString when all expected peers found
            synchronized(syncObj) {
                if (hostPortMap.size() == ensembleSize) {
                    Iterator<String> itr = (hostPortMap.values()).iterator();
                    //build connectString
                    StringBuffer strBuf = null;
                    if (itr.hasNext()) {
                        strBuf = new StringBuffer(itr.next());
                    }
                    while( itr.hasNext() ) {
                        strBuf.append(","+itr.next());
                    }
                    connectString = strBuf.toString();
                    logger.debug("connectString = "+connectString);
                }
            }
	}

	public void serviceRemoved(ServiceDiscoveryEvent event) {
            ServiceItem item = event.getPreEventServiceItem();

            ServiceID serviceId = item.serviceID;
            Object service = item.service;
            Entry[] attrs = item.attributeSets;

            Class serviceType = service.getClass();
            UUID serviceUUID = ((Service)service).getServiceUUID();

            QuorumPeerAttr quorumPeerAttr = 
                (QuorumPeerAttr)(EntryUtil.getEntryByType
                                               (attrs, QuorumPeerAttr.class));

            InetAddress peerAddr = quorumPeerAttr.address;
            int clientPort = quorumPeerAttr.clientPort;
            int ensembleSize = quorumPeerAttr.nQuorumPeers;

            if(logger.isDebugEnabled()) {
                logger.log(Level.DEBUG, "1 of "+ensembleSize+" quorum peer(s) "
                           +"DOWN [addr="+peerAddr+", port="+clientPort+"]");
            }
            hostPortMap.remove(serviceUUID);
        }

        public void serviceChanged(ServiceDiscoveryEvent event) {

            ServiceItem preItem  = event.getPreEventServiceItem();
            ServiceItem postItem = event.getPostEventServiceItem();

            ServiceID serviceId = postItem.serviceID;
            Object service = postItem.service;

            Class serviceType = service.getClass();

            Entry[] preAttrs  = preItem.attributeSets;
            Entry[] postAttrs = postItem.attributeSets; 

            UUID serviceUUID = ((Service)service).getServiceUUID();

            QuorumPeerAttr preQuorumPeerAttr = null;
            QuorumPeerAttr postQuorumPeerAttr = null;

            if (preAttrs != null) {
                preQuorumPeerAttr =
                    (QuorumPeerAttr)(EntryUtil.getEntryByType
                                            (preAttrs, QuorumPeerAttr.class));
            }
            if (postAttrs != null) {
                postQuorumPeerAttr =
                    (QuorumPeerAttr)(EntryUtil.getEntryByType
                                            (postAttrs, QuorumPeerAttr.class));
            }

            InetAddress prePeerAddr = null;
            int preClientPort = Integer.MIN_VALUE;
            int preEnsembleSize = Integer.MIN_VALUE;
            if (preQuorumPeerAttr != null) {
                prePeerAddr = preQuorumPeerAttr.address;
                preClientPort = preQuorumPeerAttr.clientPort;
                preEnsembleSize = preQuorumPeerAttr.nQuorumPeers;
            }

            InetAddress postPeerAddr = null;
            int postClientPort = Integer.MIN_VALUE;
            int postEnsembleSize = Integer.MIN_VALUE;
            if (postQuorumPeerAttr != null) {
                postPeerAddr = postQuorumPeerAttr.address;
                postClientPort = postQuorumPeerAttr.clientPort;
                postEnsembleSize = postQuorumPeerAttr.nQuorumPeers;
            }

            if ((preQuorumPeerAttr != null) && (postQuorumPeerAttr != null)) {
                String logStr = "quorum peer(s) CHANGED [pre: addr="
                                 +prePeerAddr+", port="+preClientPort
                                 +", ensembleSize="+preEnsembleSize
                                 +" >>> post: addr="+postPeerAddr
                                 +", port="+postClientPort+", ensembleSize="
                                 +postEnsembleSize+"]";
                if ( (prePeerAddr == null) || (postPeerAddr == null) ) {
                    logger.warn(logStr);
                    return;
                }
                if ( !(prePeerAddr.equals(postPeerAddr)) ) {
                    logger.warn(logStr);
                    return;
                }
                if (preClientPort != postClientPort) {
                    logger.warn(logStr);
                    return;
                }
                if (preEnsembleSize != postEnsembleSize) {
                    logger.warn(logStr);
                    return;
                }
                logger.debug(logStr);
            } else if( (preQuorumPeerAttr == null) &&
                       (postQuorumPeerAttr != null))
            {
                logger.warn("quorum peer(s) CHANGED [attribute added >>> "
                            +"post: addr="+postPeerAddr+", port="
                            +postClientPort+", ensembleSize="
                            +postEnsembleSize+"]");
                return;
            } else {// pre != null, post == null ==> removed attr
                logger.warn("quorum peer(s) CHANGED [pre: addr="+prePeerAddr
                            +", port="+preClientPort+", ensembleSize="
                            +preEnsembleSize+" >>> attribute removed]");
                return;
            }
        }
    }

    private static class ZookeeperEventListener implements Watcher {
        private Logger logger;

        public ZookeeperEventListener(Logger logger) {
            this.logger =
                (logger == null ? 
                     LogUtil.getLog4jLogger((this.getClass()).getName()) :
                     logger);
        }

	public void process(WatchedEvent event) {
            KeeperState eventState = event.getState();
            switch (eventState) {
                case Unknown:
                    logger.warn
                        ("zookeeper event [state="+eventState
                         +", event="+event+"]");
                    break;
                case Disconnected:
                    logger.debug("zookeeper event [state="+eventState+"]");;
                    break;
                case SyncConnected:
                    logger.debug("zookeeper event [state="+eventState+"]");;
                    break;
                case Expired:
                    logger.warn("zookeeper event [state="+eventState+"]");
                    break;
            }

	}
    }

}
